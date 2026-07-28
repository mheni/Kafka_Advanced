# Workshop 3 — Full TLS/SASL/ACL Security Audit

## Workshop Objective

This is not a basics-discovery workshop (the team already masters TLS at level 3/5 and has everything in place in production), but a **validation, audit, and hardening** workshop — consistent with their explicit request: *"The goal is to validate and improve what is already in place, not to relearn everything."*

## What the Client Already Has in Production (Reminder)

| Element | Client Status |
| :-- | :-- |
| TLS inter-broker, broker↔controller, apps↔broker, Connect↔broker, Schema Registry↔broker, exporters↔Mimir | All active |
| Certificates | Enterprise PKI, 5-year duration, **manual** rotation |
| SASL | SCRAM-SHA-512, `security.inter.broker.protocol=SASL_SSL` |
| ACLs | Configured least-privilege, never formally audited |
| External audit | **Never performed** despite declared DORA compliance |

The workshop must therefore reproduce this architecture on the lab, then apply an **audit checklist** that the team can directly reuse on their production cluster.

***

## Prerequisites

| Element | Machine |
| :-- | :-- |
| Active 6-broker/5-controller Kafka cluster | VM1, VM2, VM3 |
| OpenSSL and Java Keytool installed | All VMs (already done in Workshop 0) |
| Active JCDecaux producer (background load) | Producer execution VM |

***

## Part A — TLS Setup (Reproducing the Client Environment)

### Step A1 — Create a Local Certificate Authority (CA)

**Machine: VM1 (10.18.0.5)** — the CA is generated once, then distributed to the 3 VMs.

```bash
mkdir -p /home/kafka/security/ca
cd /home/kafka/security/ca

openssl req -new -x509 -keyout ca-key -out ca-cert -days 1825 \
  -subj "/CN=LuxSE-Kafka-Lab-CA/OU=IT/O=BourseLuxembourg/L=Luxembourg/C=LU" \
  -passout pass:ca-lab-password
```

### Expected Result

```
Generating a RSA private key
..............+++++
writing new private key to 'ca-key'
```

Two files created: `ca-key` (CA private key) and `ca-cert` (CA public certificate).

✅ **Validation:**

```bash
openssl x509 -in ca-cert -text -noout | grep "Subject:"
```

Should display `CN=LuxSE-Kafka-Lab-CA`.

***

### Step A2 — Generate Keystores for Each Broker

```bash
cd /home/kafka/security

for broker in broker1 broker2; do
  keytool -genkey -keystore ${broker}.keystore.jks \
    -alias ${broker} -validity 1825 -keyalg RSA \
    -dname "CN=${broker},OU=IT,O=BourseLuxembourg,L=Luxembourg,C=LU" \
    -storepass lab-password -keypass lab-password
done
```

### Expected Result

```
Generating 2,048 bit RSA key pair and self-signed certificate...
[Storing broker1.keystore.jks]
[Storing broker2.keystore.jks]
```

**Machine: VM1** — for broker1 and broker2 (repeat the adapted logic on VM2 for broker3/4 and VM3 for broker5/6).

```bash
mkdir -p /home/kafka/security
cd /home/kafka/security
for broker in broker3 broker4; do
  keytool -genkey -keystore ${broker}.keystore.jks \
    -alias ${broker} -validity 1825 -keyalg RSA \
    -dname "CN=${broker},OU=IT,O=BourseLuxembourg,L=Luxembourg,C=LU" \
    -storepass lab-password -keypass lab-password
done
```

and VM3

```bash
mkdir -p /home/kafka/security
cd /home/kafka/security
for broker in broker5 broker6; do
  keytool -genkey -keystore ${broker}.keystore.jks \
    -alias ${broker} -validity 1825 -keyalg RSA \
    -dname "CN=${broker},OU=IT,O=BourseLuxembourg,L=Luxembourg,C=LU" \
    -storepass lab-password -keypass lab-password
done
```

***

### Step A3 — Sign Certificates with the Local CA

**Machine: VM1**

```bash
for broker in broker1 broker2; do
  keytool -certreq -keystore ${broker}.keystore.jks -alias ${broker} \
    -file ${broker}.csr -storepass lab-password

  openssl x509 -req -CA ca/ca-cert -CAkey ca/ca-key \
    -in ${broker}.csr -out ${broker}-signed.crt \
    -days 1825 -CAcreateserial -passin pass:ca-lab-password

  keytool -importcert -keystore ${broker}.keystore.jks -alias CARoot \
    -file ca/ca-cert -storepass lab-password -noprompt

  keytool -importcert -keystore ${broker}.keystore.jks -alias ${broker} \
    -file ${broker}-signed.crt -storepass lab-password -noprompt
done
```

### Expected Result

```
Certificate reply was installed in keystore
```

## ------------------------------------------------------------------------------
**Before working on node 2 or 3, the content of the security/ca folder must be copied.**

## Step 1 — Temporarily Allow Password Authentication on kafka-node1

Already done on VM1, keep it active throughout the key exchange:

```bash
sudo sshd -T 2>/dev/null | grep -i passwordauthentication
```

Should display `passwordauthentication yes`.

## Step 2 — Generate an SSH Key on VM2 and VM3 (if not already done)

**On kafka-node2 AND kafka-node3, run separately:**

```bash
ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519
```

## Step 3 — Copy the Public Keys to kafka-node1

**On kafka-node2:**

```bash
ssh-copy-id kafka@10.18.0.5
```

**On kafka-node3:**

```bash
ssh-copy-id kafka@10.18.0.5
```

A password will be requested once for each VM.

## Step 4 — Verify Password-less Access

**From VM2 and VM3:**

```bash
ssh kafka@10.18.0.5 "hostname"
```

Should return `kafka-node1` without a prompt.

## Step 5 — Copy the CA Folder Using a For Loop

The folder to copy is `~/security/ca` (containing `ca-cert` and `ca-key`), generated once on VM1 as per Workshop 3.

**On kafka-node2 and 3**
```bash
scp -r kafka@10.18.0.5:/home/kafka/security/ca /home/kafka/security/
scp kafka@10.18.0.5:/home/kafka/security/kafka.truststore.jks /home/kafka/security/
```

## Step 6 — Verify the Copy on Each VM

**On kafka-node2 AND kafka-node3:**

```bash
ls -la ~/security/ca/
```

Should display `ca-cert` and `ca-key`.

## Step 7 — Also Copy the Shared Truststore (If Already Generated)

Per Workshop 3, the `kafka.truststore.jks` file must also be identical on the 3 VMs. **On kafka-node1:**

```bash
for ip in 10.118.0.5 10.128.0.5; do
  scp /home/kafka/security/kafka.truststore.jks kafka@${ip}:/home/kafka/security/
done
```

## Step 8 — Close Password Authentication Back Down (Security)

Once all keys have been exchanged, restore the original hardening on **kafka-node1**:

```bash
sudo sed -i 's/PasswordAuthentication yes/PasswordAuthentication no/' /etc/ssh/sshd_config.d/50-cloud-init.conf
sudo sed -i 's/PasswordAuthentication yes/PasswordAuthentication no/' /etc/ssh/sshd_config.d/60-cloudimg-settings.conf
sudo systemctl restart ssh
sudo sshd -T 2>/dev/null | grep -i passwordauthentication
```

Should switch back to `passwordauthentication no`, consistent with the workshop's security posture.

## ---------------------------------------------------------------------------

✅ **Validation:**

```bash
keytool -list -v -keystore broker1.keystore.jks -storepass lab-password | grep "Owner:"
```

Should display `Owner: CN=broker1,...` with a certificate chain leading back to the CA.

***

### Step A4 — Create the Shared Truststore

**Machine: VM1**, then copy to VM2 and VM3.

```bash
keytool -importcert -keystore kafka.truststore.jks -alias CARoot \
  -file ca/ca-cert -storepass lab-password -noprompt
```

Copy the truststore and CA certificate to the other VMs from the other VMs:

```bash
scp kafka@10.18.0.5:/home/kafka/security/kafka.truststore.jks /home/kafka/security/
```

### Expected Result

Identical `kafka.truststore.jks` file present on all 3 VMs — this single file allows each broker to trust the others' certificates, all signed by the same CA.

**Machine: VM2** — repeat Steps A2-A3 for broker3, broker4.
**Machine: VM3** — repeat for broker5, broker6.

***

### Step A5 — Configure TLS on the Brokers

**Machine: VM1** — edit `broker1.properties`:

```bash
nano /opt/kafka/config/kraft-lab/broker1.properties
```

modify the 4 lines and add the rest:

```properties

listeners=SSL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9192
advertised.listeners=SSL://10.18.0.5:9092
listener.security.protocol.map=SSL:SSL,CONTROLLER:SSL
inter.broker.listener.name=SSL

ssl.keystore.location=/home/kafka/security/broker1.keystore.jks
ssl.keystore.password=lab-password
ssl.key.password=lab-password
ssl.truststore.location=/home/kafka/security/kafka.truststore.jks
ssl.truststore.password=lab-password
ssl.client.auth=required
```

Repeat for broker2 on VM1 (with `broker2.keystore.jks` and port 9093), then for brokers 3-6 on VM2/VM3 respectively.

### Expected Result

After restarting (`systemctl restart kafka-broker1`), the logs should show:

```
INFO [SocketServer] Enabling SSL for listener SSL
```

## broker2.properties (VM1 — 10.18.0.5)

```properties
node.id=2
listeners=SSL://0.0.0.0:9093,CONTROLLER://0.0.0.0:9193
advertised.listeners=SSL://10.18.0.5:9093
listener.security.protocol.map=SSL:SSL,CONTROLLER:SSL
inter.broker.listener.name=SSL

ssl.keystore.location=/home/kafka/security/broker2.keystore.jks
ssl.keystore.password=lab-password
ssl.key.password=lab-password
ssl.truststore.location=/home/kafka/security/kafka.truststore.jks
ssl.truststore.password=lab-password
ssl.client.auth=required

log.dirs=/data/kafka/broker2
```

## broker3.properties (VM2 — 10.118.0.5)

```properties
node.id=3
listeners=SSL://0.0.0.0:9094,CONTROLLER://0.0.0.0:9194
advertised.listeners=SSL://10.118.0.5:9094
listener.security.protocol.map=SSL:SSL,CONTROLLER:SSL
inter.broker.listener.name=SSL

ssl.keystore.location=/home/kafka/security/broker3.keystore.jks
ssl.keystore.password=lab-password
ssl.key.password=lab-password
ssl.truststore.location=/home/kafka/security/kafka.truststore.jks
ssl.truststore.password=lab-password
ssl.client.auth=required

log.dirs=/data/kafka/broker3
```

## broker4.properties (VM2 — 10.118.0.5)

```properties
node.id=4
listeners=SSL://0.0.0.0:9095,CONTROLLER://0.0.0.0:9195
advertised.listeners=SSL://10.118.0.5:9095
listener.security.protocol.map=SSL:SSL,CONTROLLER:SSL
inter.broker.listener.name=SSL

ssl.keystore.location=/home/kafka/security/broker4.keystore.jks
ssl.keystore.password=lab-password
ssl.key.password=lab-password
ssl.truststore.location=/home/kafka/security/kafka.truststore.jks
ssl.truststore.password=lab-password
ssl.client.auth=required

log.dirs=/data/kafka/broker4
```

## broker5.properties (VM3 — 10.128.0.5)

```properties
node.id=5
listeners=SSL://0.0.0.0:9096,CONTROLLER://0.0.0.0:9196
advertised.listeners=SSL://10.128.0.5:9096
listener.security.protocol.map=SSL:SSL,CONTROLLER:SSL
inter.broker.listener.name=SSL

ssl.keystore.location=/home/kafka/security/broker5.keystore.jks
ssl.keystore.password=lab-password
ssl.key.password=lab-password
ssl.truststore.location=/home/kafka/security/kafka.truststore.jks
ssl.truststore.password=lab-password
ssl.client.auth=required

log.dirs=/data/kafka/broker5
```

## broker6.properties (VM3 — 10.128.0.5)

```properties
node.id=6
listeners=SSL://0.0.0.0:9097
advertised.listeners=SSL://10.128.0.5:9097
listener.security.protocol.map=SSL:SSL,CONTROLLER:SSL
inter.broker.listener.name=SSL

ssl.keystore.location=/home/kafka/security/broker6.keystore.jks
ssl.keystore.password=lab-password
ssl.key.password=lab-password
ssl.truststore.location=/home/kafka/security/kafka.truststore.jks
ssl.truststore.password=lab-password
ssl.client.auth=required

log.dirs=/data/kafka/broker6
```

## Points of Attention

- `controller.quorum.voters` remains identical and unchanged across all 6 files, as it describes the entire quorum, not just the local broker.
- Only the controller nodes (101 to 105, i.e. broker1 to broker5 in your topology) must have `controller.listener.names=CONTROLLER` active — broker6 (node.id=6) does not appear as a voter in `controller.quorum.voters`, so it has no controller role, which explains the absence of the `CONTROLLER` port in its `listeners` block above.
- Each `brokerX.keystore.jks` must have been individually generated and signed in step A2-A3 before this restart.

✅ **Validation - Machine VM2:**

```bash
openssl s_client -connect 10.18.0.5:9092 -tls1_2 </dev/null 2>/dev/null | grep "Verify return code"
```

Should display `Verify return code: 0 (ok)`.

***

## Part B — SASL/SCRAM Configuration (Consistent with the Client)

### Step B1 — Create SCRAM-SHA-512 Users

**Machine: VM1** — use one of the active controllers as the entry point.

```bash
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --command-config /home/kafka/security/admin-ssl.properties \
  --alter --add-config 'SCRAM-SHA-512=[password=lab-admin-pwd]' \
  --entity-type users --entity-name admin

/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --command-config /home/kafka/security/admin-ssl.properties \
  --alter --add-config 'SCRAM-SHA-512=[password=lab-producer-pwd]' \
  --entity-type users --entity-name jcdecaux-producer
```

### Expected Result

```
Completed updating config for entity: user-principal 'admin'.
Completed updating config for entity: user-principal 'jcdecaux-producer'.
```

***

### Step B2 — Enable SASL_SSL on the Brokers

**Machine: VM1** (and equivalent on VM2/VM3):

```properties
listeners=SASL_SSL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9192
listener.security.protocol.map=SASL_SSL:SASL_SSL,CONTROLLER:SSL
inter.broker.listener.name=SASL_SSL

sasl.enabled.mechanisms=SCRAM-SHA-512
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
security.inter.broker.protocol=SASL_SSL

listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config=\
  org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="admin" \
  password="lab-admin-pwd";
```

Restart: `sudo systemctl restart kafka-broker1`

### Expected Result

```
INFO [SocketServer] Enabling SASL_SSL for listener SASL_SSL
```

✅ **Validation:** a connection attempt without credentials must fail explicitly (`SaslAuthenticationException`).

***

## Part C — ACL Configuration

### Step C1 — Enable the Authorizer

**Machine: VM1, VM2, VM3** (on each `brokerX.properties`):

```properties
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
allow.everyone.if.no.acl.found=false
super.users=User:admin
```

### Step C2 — Create ACLs for the JCDecaux Producer

**Machine: VM1**

```bash
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --command-config /home/kafka/security/admin-ssl.properties \
  --add --allow-principal User:jcdecaux-producer \
  --operation Write --operation Describe \
  --topic vls-stations-nancy

/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --command-config /home/kafka/security/admin-ssl.properties \
  --add --allow-principal User:jcdecaux-producer \
  --operation Write --operation Describe \
  --topic vls-stations-toulouse
```

### Expected Result

```
Adding ACLs for resource `ResourcePattern(resourceType=TOPIC, name=vls-stations-nancy, patternType=LITERAL)`:
	(principal=User:jcdecaux-producer, host=*, operation=WRITE, permissionType=ALLOW)
	(principal=User:jcdecaux-producer, host=*, operation=DESCRIBE, permissionType=ALLOW)
```

✅ **Validation:**

```bash
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --command-config /home/kafka/security/admin-ssl.properties \
  --list --topic vls-stations-nancy
```

***

## Part D — Full Audit Script

**Machine: VM1** — this script centralizes all security checks, and can be reused on their production cluster.

```bash
nano /home/kafka/security_audit.sh
```

```bash
#!/bin/bash
# =============================================================
# Kafka security audit script - TLS/SASL/ACL
# To be run from VM1 (or any node with admin access)
# =============================================================

set -euo pipefail

BROKERS=("10.18.0.5:9092" "10.18.0.5:9093" "10.118.0.5:9094" "10.118.0.5:9095" "10.128.0.5:9096" "10.128.0.5:9097")
ADMIN_CONFIG="/home/kafka/security/admin-ssl.properties"
REPORT="/home/kafka/audit_report_$(date +%Y%m%d_%H%M%S).txt"
KAFKA_BIN="/opt/kafka/bin"

echo "=============================================" | tee "$REPORT"
echo "KAFKA SECURITY AUDIT REPORT - $(date)" | tee -a "$REPORT"
echo "=============================================" | tee -a "$REPORT"

echo -e "\n--- 1. TLS CHECK PER BROKER ---" | tee -a "$REPORT"
for broker in "${BROKERS[@]}"; do
    host=$(echo $broker | cut -d: -f1)
    port=$(echo $broker | cut -d: -f2)
    result=$(echo | openssl s_client -connect ${host}:${port} -tls1_2 2>/dev/null | grep "Verify return code")
    echo "${broker} -> ${result}" | tee -a "$REPORT"
done

echo -e "\n--- 2. CERTIFICATE EXPIRATION ---" | tee -a "$REPORT"
for cert in /home/kafka/security/*-signed.crt; do
    subject=$(openssl x509 -in "$cert" -noout -subject)
    enddate=$(openssl x509 -in "$cert" -noout -enddate)
    echo "File: $cert | $subject | $enddate" | tee -a "$REPORT"
done

echo -e "\n--- 3. CONFIGURED SASL USERS ---" | tee -a "$REPORT"
$KAFKA_BIN/kafka-configs.sh \
    --bootstrap-server 10.18.0.5:9092 \
    --command-config "$ADMIN_CONFIG" \
    --describe --entity-type users | tee -a "$REPORT"

echo -e "\n--- 4. FULL LIST OF ACLs ---" | tee -a "$REPORT"
$KAFKA_BIN/kafka-acls.sh \
    --bootstrap-server 10.18.0.5:9092 \
    --command-config "$ADMIN_CONFIG" \
    --list | tee -a "$REPORT"

echo -e "\n--- 5. super.users CHECK (RISK IF TOO BROAD) ---" | tee -a "$REPORT"
for host_ip in 10.18.0.5 10.118.0.5 10.128.0.5; do
    echo "Config on $host_ip:" | tee -a "$REPORT"
    ssh kafka@${host_ip} "grep -r 'super.users' /opt/kafka/config/kraft-lab/ 2>/dev/null" | tee -a "$REPORT" || echo "Not found" | tee -a "$REPORT"
done

echo -e "\n--- 6. allow.everyone.if.no.acl.found CHECK ---" | tee -a "$REPORT"
for host_ip in 10.18.0.5 10.118.0.5 10.128.0.5; do
    ssh kafka@${host_ip} "grep -r 'allow.everyone.if.no.acl.found' /opt/kafka/config/kraft-lab/ 2>/dev/null" | tee -a "$REPORT"
done

echo -e "\n--- 7. TEST: ACCESS DENIED WITHOUT ACL (UNAUTHORIZED TOPIC) ---" | tee -a "$REPORT"
$KAFKA_BIN/kafka-topics.sh --create \
    --bootstrap-server 10.18.0.5:9092 \
    --command-config "$ADMIN_CONFIG" \
    --topic audit-test-unauthorized --partitions 1 --replication-factor 3 2>&1 | tee -a "$REPORT" || true

echo -e "\n=============================================" | tee -a "$REPORT"
echo "AUDIT COMPLETE - Full report: $REPORT" | tee -a "$REPORT"
echo "=============================================" | tee -a "$REPORT"
```

```bash
chmod +x /home/kafka/security_audit.sh
./security_audit.sh
```

### Expected Result

A complete text report containing:

1. `Verify return code: 0 (ok)` for each broker (TLS functional)
2. Expiration dates of each certificate — **critical point to monitor** to avoid silent expiration
3. List of configured SCRAM users
4. Exhaustive list of ACLs per topic/principal
5. Confirmation that `super.users` contains only `admin` (no over-privilege)
6. Confirmation of `allow.everyone.if.no.acl.found=false` on the 3 VMs

✅ **Critical validation of Step 7:** topic creation must fail with an authorization error, since `admin-ssl.properties` without explicit `Create` rights should not be sufficient — unless `admin` is in `super.users`, in which case this test must be redone with a non-admin user to be meaningful.

***

## Part E — Non-Regression Test: Certificate Rotation Without Interruption

**Machine: VM1** — addresses the client's E2/E3 request (*"zero-downtime certificate rotation"*).

### Step E1 — Generate a New Certificate

```bash
cd /home/kafka/security
keytool -genkey -keystore broker1-new.keystore.jks \
  -alias broker1 -validity 1825 -keyalg RSA \
  -dname "CN=broker1,OU=IT,O=BourseLuxembourg,L=Luxembourg,C=LU" \
  -storepass lab-password -keypass lab-password
# Then sign with the CA (repeat Step A3)
```

### Step E2 — Live Rotation

```bash
cp broker1.keystore.jks broker1.keystore.jks.backup
cp broker1-new.keystore.jks broker1.keystore.jks

/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --command-config /home/kafka/security/admin-ssl.properties \
  --alter --entity-type brokers --entity-name 1 \
  --add-config 'listener.name.sasl_ssl.ssl.keystore.location=/home/kafka/security/broker1.keystore.jks'
```

### Expected Result

```
Completed updating config for broker: 1.
```

✅ **Validation:** throughout the operation, run a continuous `kafka-console-consumer.sh` on `vls-stations-nancy` in parallel from VM2 — **no interruption in message reception** should be observed, confirming the zero-downtime rotation requested by the client.

***

## Final Audit Checklist (Production-Reusable Deliverable)

| Checkpoint | Lab status | Production status (to be verified by the client) |
| :-- | :-- | :-- |
| TLS active on all flows | ✅ Validated | To confirm with adapted `security_audit.sh` |
| Certificates not expired (< 90 days) | ✅ Validated | Critical point — current manual rotation |
| SASL SCRAM-SHA-512 functional | ✅ Validated | Already in place |
| Active and restrictive ACLs | ✅ Validated | Never formally audited — to be done |
| Minimal `super.users` | ✅ Validated | To be verified |
| `allow.everyone.if.no.acl.found=false` | ✅ Validated | To be verified |
| Certificate rotation without interruption tested | ✅ Validated in lab | Never tested in production per E3 |

***

This audit script (`security_audit.sh`) is designed to be directly adapted and reused on their production cluster — addressing their explicit need for a first formal audit, since no external audit has ever been performed on their Kafka stack.

***

## Cleanup Before Workshop 4 (Corrected Version)

⚠️ **Important:** this step is **mandatory** before starting Workshop 4. The brokers were successively configured with SSL (Part A5) then SASL_SSL (Part B2), and the controllers also switched to SSL on their `CONTROLLER` listener. Workshop 4 uses `--bootstrap-server` commands without authentication or TLS — without this complete cleanup, **all Workshop 4 commands will fail** with `SaslAuthenticationException` or `TimeoutException` errors.

### Why a Partial Cleanup Is Not Enough

A cleanup limited to only the `listeners=` and `inter.broker.listener.name=` lines of the brokers leaves two active inconsistencies:

1. **`advertised.listeners`** and **`listener.security.protocol.map`** of the brokers still reference `SSL`/`SASL_SSL` — the broker will restart with a contradictory configuration (PLAINTEXT protocol declared, but protocol map only knowing SSL/SASL_SSL)
2. **The `controller*.properties` files** are never touched — the `CONTROLLER` listener remains on `SSL` from Step A5, which prevents the KRaft quorum from working in PLAINTEXT once the brokers are cleaned, causing a total cluster breakdown

### Step N1 — Complete Cleanup (Brokers + Controllers)

**Machine: VM1, VM2, VM3** — run on all 3 VMs:

```bash
sed -i 's/SASL_SSL/PLAINTEXT/g; s/CONTROLLER:SSL/CONTROLLER:PLAINTEXT/g' \
  /opt/kafka/config/kraft-lab/broker*.properties \
  /opt/kafka/config/kraft-lab/controller*.properties

sed -i 's/advertised.listeners=SSL:\/\//advertised.listeners=PLAINTEXT:\/\//' \
  /opt/kafka/config/kraft-lab/broker*.properties
```

### Expected Result

```bash
grep -E "listeners=|listener.security.protocol.map=|inter.broker.listener.name=" /opt/kafka/config/kraft-lab/broker1.properties
```

Should display:
```
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9192
advertised.listeners=PLAINTEXT://10.18.0.5:9092
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
```

✅ **Validation before restart:** verify no occurrence of `SSL` or `SASL_SSL` remains:
```bash
grep -i "ssl" /opt/kafka/config/kraft-lab/broker*.properties /opt/kafka/config/kraft-lab/controller*.properties | grep -v "^#"
```
This command should return no results related to listeners (the `ssl.keystore.location=...` lines may remain present but simply become unused, which is acceptable).

### Step N2 — Restart in the Correct Order

⚠️ **Mandatory order:** the controllers must restart and re-form the quorum **before** the brokers, otherwise the brokers start with no quorum available and fail.

**Machine: VM1**
```bash
sudo systemctl restart kafka-controller1 kafka-controller2
```

**Machine: VM2**
```bash
sudo systemctl restart kafka-controller3 kafka-controller4
```

**Machine: VM3**
```bash
sudo systemctl restart kafka-controller5
```

**Wait 10 seconds for the quorum to re-form**, then:

**Machine: VM1**
```bash
sudo systemctl restart kafka-broker1 kafka-broker2
```

**Machine: VM2**
```bash
sudo systemctl restart kafka-broker3 kafka-broker4
```

**Machine: VM3**
```bash
sudo systemctl restart kafka-broker5 kafka-broker6
```

### Expected Result

**Machine: VM1** — verify the quorum:
```bash
/opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-controller 10.18.0.5:9192 \
  describe --status
```
Should show all 5 controllers in `CurrentVoters` with `MaxFollowerLag: 0`.

**Machine: VM1** — verify broker connectivity in PLAINTEXT:
```bash
/opt/kafka/bin/kafka-topics.sh --list \
  --bootstrap-server 10.18.0.5:9092
```
Should display the list of topics (`vls-stations-nancy`, `vls-stations-toulouse`, etc.) with no authentication or TLS handshake error.

✅ **Final validation:** re-run a simple Workshop 4 command to confirm the cluster responds correctly in PLAINTEXT:
```bash
/opt/kafka/bin/kafka-topics.sh --describe \
  --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  --topic vls-stations-nancy
```

### Recommended Alternative: VMware Snapshot

The most reliable method remains **restoring the VMware snapshot** taken right after Workshop 0/1 (before any TLS configuration), rather than depending on `sed` across multiple files. This guarantees a strictly clean state, with no risk of a residual leftover parameter (orphaned key, uncleaned cross-reference, etc.). This should be preferred if training time allows, especially between two sessions with different groups.

### Point to Communicate Verbally to the Group

> "In real production, none of these performance tests would ever be done by disabling TLS/SASL — we are simplifying here only to isolate the performance variable and make the commands easier to read. Your real cluster will of course always remain secured."

***
