# Workshop 3 — Full TLS/SASL/ACL Security Audit (Verified and Rewritten Version)

> **Consolidated version — July 2026**: this version reflects the workshop as corrected after real lab execution, clarifies the blocking issues encountered in practice, and rewrites the important steps with an explicit pedagogical objective, in particular Part E on certificate rotation.

## Workshop Objective

This workshop is not meant to rediscover TLS, SASL, or ACLs, but to **reproduce in the lab an architecture already in place in production**, then **audit it**, **validate it**, and **harden it**. The client's request is concrete: have a reusable procedure to verify that a genuinely secured Kafka cluster remains functional, observable, and administrable.

More specifically, the workshop aims to demonstrate that the team knows how to:

- deploy TLS on brokers and controllers;
- enable strong authentication via SASL/SCRAM;
- set up least-privilege ACLs;
- audit the important security parameters;
- test a certificate rotation without interrupting application flows.

## Production Context Reproduced in the Lab

| Element | Client-side Status |
| :-- | :-- |
| TLS inter-broker, broker↔controller, apps↔broker, Connect↔broker, Schema Registry↔broker | Active |
| Certificates | Internal PKI, 5-year duration, manual rotation |
| SASL | SCRAM-SHA-512 |
| ACLs | Configured as least-privilege, rarely formally audited |
| External audit | Never performed despite declared compliance posture |

The workshop must therefore reproduce this level of security, then provide a **reusable audit checklist** for the production cluster.

## Prerequisites

| Element | Scope |
| :-- | :-- |
| Active 6-broker / 5-controller Kafka cluster | VM1, VM2, VM3 |
| OpenSSL and keytool available | All VMs |
| Shell access on each VM | Required |
| Active JCDecaux producer background load | Recommended for final tests |

## Inter-VM SSH Prerequisites

Before any certificate manipulation, VMs must be able to **retrieve** the necessary files from VM1. In this workshop, file copies are done mainly **pulled from VM2/VM3 towards VM1**, not pushed from VM1 to the other nodes.

⚠️ In practice, an `scp` from VM1 to VM2/VM3 may fail with `Permission denied (publickey)` if that direction has not also been configured. This is not a Kafka bug; it is simply an SSH authentication issue.

### Step SSH1 — Generate a Key on VM2 and VM3

**Objective:** allow VM2 and VM3 to retrieve the CA and truststore files stored on VM1 without a password.

**On VM2 then VM3:**

```bash
ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519
```

### Step SSH2 — Temporarily Allow Password Authentication on VM1

**Objective:** allow `ssh-copy-id` to run once, then immediately return to the hardened posture.

```bash
sudo sshd -T 2>/dev/null | grep -i passwordauthentication
```

If the output remains `no`, fix it directly in the priority file under `sshd_config.d`:

```bash
sudo sed -i 's/PasswordAuthentication no/PasswordAuthentication yes/' /etc/ssh/sshd_config.d/50-cloud-init.conf
sudo systemctl restart ssh
sudo sshd -T 2>/dev/null | grep -i passwordauthentication
```

### Step SSH3 — Copy Keys to VM1

**Objective:** allow passwordless SSH access from VM2 and VM3 to VM1.

```bash
ssh-copy-id kafka@10.18.0.5
```

### Step SSH4 — Verify Passwordless Access

```bash
ssh kafka@10.18.0.5 "hostname"
```

This command must return `kafka-node1` without a prompt.

### Step SSH5 — Close Password Authentication Again

```bash
sudo sed -i 's/PasswordAuthentication yes/PasswordAuthentication no/' /etc/ssh/sshd_config.d/50-cloud-init.conf
sudo systemctl restart ssh
```

---

## Part A — Setting Up TLS

### Objective of Part A

The goal of this part is to **encrypt all Kafka flows in the lab** consistently with the client environment. By the end of this part, brokers and controllers must communicate over TLS, clients must be able to verify certificates, and brokers must no longer accept plaintext traffic.

This part also lays the foundation for all following parts: without valid certificates, a common truststore, and a stable KRaft quorum over TLS, SASL and ACL cannot be tested correctly.

### Step A1 — Create a Local Certificate Authority

**Objective:** have a single CA used to sign all lab certificates, so that brokers and controllers mutually trust each other.

**Machine: VM1**

```bash
mkdir -p /home/kafka/security/ca
cd /home/kafka/security/ca

openssl req -new -x509 -keyout ca-key -out ca-cert -days 1825 \
  -subj "/CN=LuxSE-Kafka-Lab-CA/OU=IT/O=BourseLuxembourg/L=Luxembourg/C=LU" \
  -passout pass:ca-lab-password
```

**Validation:**

```bash
openssl x509 -in ca-cert -text -noout | grep "Subject:"
```

The subject must contain `CN=LuxSE-Kafka-Lab-CA`.

### Step A2 — Generate Each Broker's Keystore

**Objective:** give each broker its own TLS identity. Each broker must have its own keystore, alias, and certificate.

⚠️ Using a specific alias per broker (`broker1`, `broker2`, etc.) is essential. A generic alias will break CSR generation in the next step.

**VM1:**

```bash
cd /home/kafka/security
for broker in broker1 broker2; do
  keytool -genkey -keystore ${broker}.keystore.jks \
    -alias ${broker} -validity 1825 -keyalg RSA \
    -dname "CN=${broker},OU=IT,O=BourseLuxembourg,L=Luxembourg,C=LU" \
    -storepass lab-password -keypass lab-password
done
```

**VM2:**

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

**VM3:**

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

### Step A3 — Have Certificates Signed by the CA

**Objective:** turn the self-signed certificates from `keytool -genkey` into certificates actually signed by the lab CA. Without this step, inter-broker TLS connections will fail.

⚠️ On VM2 and VM3, only the CSR is produced locally. Signing must be done on VM1, since the CA's private key must never leave that machine.

**VM1 — brokers 1 and 2:**

```bash
cd /home/kafka/security
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

**VM2 — brokers 3 and 4:**

```bash
cd /home/kafka/security
for broker in broker3 broker4; do
  keytool -certreq -keystore ${broker}.keystore.jks -alias ${broker} \
    -file ${broker}.csr -storepass lab-password
done

scp broker3.csr broker4.csr kafka@10.18.0.5:/home/kafka/security/ca/
```

**VM1 — signing VM2's CSRs:**

```bash
cd /home/kafka/security/ca
for broker in broker3 broker4; do
  openssl x509 -req -CA ca-cert -CAkey ca-key \
    -in ${broker}.csr -out ${broker}-signed.crt \
    -days 1825 -CAcreateserial -passin pass:ca-lab-password
done
```

**VM2 — retrieval and import:**

```bash
cd /home/kafka/security
scp kafka@10.18.0.5:/home/kafka/security/ca/broker3-signed.crt .
scp kafka@10.18.0.5:/home/kafka/security/ca/broker4-signed.crt .

for broker in broker3 broker4; do
  keytool -importcert -keystore ${broker}.keystore.jks -alias CARoot \
    -file ca/ca-cert -storepass lab-password -noprompt
  keytool -importcert -keystore ${broker}.keystore.jks -alias ${broker} \
    -file ${broker}-signed.crt -storepass lab-password -noprompt
done
```

**VM3 — brokers 5 and 6:** repeat exactly the same logic with `broker5` and `broker6`.

**Critical validation:**

```bash
keytool -list -v -keystore broker1.keystore.jks -storepass lab-password | grep -E "Owner|Issuer|Certificate\["
```

The broker must appear with an `Owner` of type `CN=broker1,...` and an `Issuer` pointing to the lab CA, not to itself.

### Step A4 — Create the Common Truststore

**Objective:** give all nodes the same trust store, containing the common CA.

**VM1:**

```bash
cd /home/kafka/security
keytool -importcert -keystore kafka.truststore.jks -alias CARoot \
  -file ca/ca-cert -storepass lab-password -noprompt
```

**From VM2 and VM3:**

```bash
scp kafka@10.18.0.5:/home/kafka/security/ca/ca-cert /home/kafka/security/ca/
scp kafka@10.18.0.5:/home/kafka/security/kafka.truststore.jks /home/kafka/security/
```

**Validation:**

```bash
md5sum /home/kafka/security/kafka.truststore.jks
```

All three VMs must have the same checksum.

### Expected Result

Identical `kafka.truststore.jks` file present on all 3 VMs — this single file is what will allow each broker to trust the others' certificates, signed by the same CA.

### Step A5 — Configure TLS on Brokers

**Objective:** make each broker listen over TLS, enable mutual trust, and prepare the future move to SASL_SSL.

⚠️ In KRaft, a broker with `process.roles=broker` must not expose `CONTROLLER://...` in its own `listeners=` line. The controller channel belongs to the `kafka-controller@N` processes and their `controllerN.properties` files.

⚠️ In this lab, `ssl.endpoint.identification.algorithm=` must be left empty, since certificates are based on CNs like `broker1` rather than SANs aligned with the IPs in `advertised.listeners`.

**Example — broker1.properties on VM1:**

```properties
process.roles=broker
node.id=1
controller.quorum.voters=101@10.18.0.5:9192,102@10.18.0.5:9193,103@10.118.0.5:9194,104@10.118.0.5:9195,105@10.128.0.5:9196

listeners=SSL://0.0.0.0:9092
advertised.listeners=SSL://10.18.0.5:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=SSL:SSL,CONTROLLER:SSL
inter.broker.listener.name=SSL

ssl.keystore.location=/home/kafka/security/broker1.keystore.jks
ssl.keystore.password=lab-password
ssl.key.password=lab-password
ssl.truststore.location=/home/kafka/security/kafka.truststore.jks
ssl.truststore.password=lab-password
ssl.client.auth=required
ssl.endpoint.identification.algorithm=

log.dirs=/data/kafka/broker1
```

Adapt in the same way for brokers 2 to 6.

## broker2.properties (VM1 — 10.18.0.5)

```properties
node.id=2
listeners=SSL://0.0.0.0:9093
advertised.listeners=SSL://10.18.0.5:9093
controller.listener.names=CONTROLLER
listener.security.protocol.map=SSL:SSL,CONTROLLER:SSL
inter.broker.listener.name=SSL

ssl.keystore.location=/home/kafka/security/broker2.keystore.jks
ssl.keystore.password=lab-password
ssl.key.password=lab-password
ssl.truststore.location=/home/kafka/security/kafka.truststore.jks
ssl.truststore.password=lab-password
ssl.client.auth=required
ssl.endpoint.identification.algorithm=

log.dirs=/data/kafka/broker2
```

## broker3.properties (VM2 — 10.118.0.5)

```properties
node.id=3
listeners=SSL://0.0.0.0:9094
advertised.listeners=SSL://10.118.0.5:9094
controller.listener.names=CONTROLLER
listener.security.protocol.map=SSL:SSL,CONTROLLER:SSL
inter.broker.listener.name=SSL

ssl.keystore.location=/home/kafka/security/broker3.keystore.jks
ssl.keystore.password=lab-password
ssl.key.password=lab-password
ssl.truststore.location=/home/kafka/security/kafka.truststore.jks
ssl.truststore.password=lab-password
ssl.client.auth=required
ssl.endpoint.identification.algorithm=

log.dirs=/data/kafka/broker3
```

## broker4.properties (VM2 — 10.118.0.5)

```properties
node.id=4
listeners=SSL://0.0.0.0:9095
advertised.listeners=SSL://10.118.0.5:9095
controller.listener.names=CONTROLLER
listener.security.protocol.map=SSL:SSL,CONTROLLER:SSL
inter.broker.listener.name=SSL

ssl.keystore.location=/home/kafka/security/broker4.keystore.jks
ssl.keystore.password=lab-password
ssl.key.password=lab-password
ssl.truststore.location=/home/kafka/security/kafka.truststore.jks
ssl.truststore.password=lab-password
ssl.client.auth=required
ssl.endpoint.identification.algorithm=

log.dirs=/data/kafka/broker4
```

## broker5.properties (VM3 — 10.128.0.5)

```properties
node.id=5
listeners=SSL://0.0.0.0:9096
advertised.listeners=SSL://10.128.0.5:9096
controller.listener.names=CONTROLLER
listener.security.protocol.map=SSL:SSL,CONTROLLER:SSL
inter.broker.listener.name=SSL

ssl.keystore.location=/home/kafka/security/broker5.keystore.jks
ssl.keystore.password=lab-password
ssl.key.password=lab-password
ssl.truststore.location=/home/kafka/security/kafka.truststore.jks
ssl.truststore.password=lab-password
ssl.client.auth=required
ssl.endpoint.identification.algorithm=

log.dirs=/data/kafka/broker5
```

## broker6.properties (VM3 — 10.128.0.5)

```properties
node.id=6
listeners=SSL://0.0.0.0:9097
advertised.listeners=SSL://10.128.0.5:9097
controller.listener.names=CONTROLLER
listener.security.protocol.map=SSL:SSL,CONTROLLER:SSL
inter.broker.listener.name=SSL

ssl.keystore.location=/home/kafka/security/broker6.keystore.jks
ssl.keystore.password=lab-password
ssl.key.password=lab-password
ssl.truststore.location=/home/kafka/security/kafka.truststore.jks
ssl.truststore.password=lab-password
ssl.client.auth=required
ssl.endpoint.identification.algorithm=

log.dirs=/data/kafka/broker6
```

## Points of Attention

- `controller.quorum.voters` remains identical and unchanged across all 6 files, since it describes the entire quorum, not just the local broker.
- All brokers keep `controller.listener.names=CONTROLLER` (needed for the broker to know how to route calls to the quorum), even broker6, which hosts no controller role.
- No broker should have a `CONTROLLER://...` port in its own `listeners=` line — this role belongs exclusively to the `controllerN.properties` files (see Step A5bis below).
- Each `brokerX.keystore.jks` must have been generated **and signed** (Step A3 validated with `keytool -list -v`) before this restart.

### Step A5bis — Configure TLS on Controllers

**Objective:** also move the KRaft quorum to TLS. This step is essential: if brokers move to TLS but not the controllers, the quorum will not re-form correctly.

**Example — controller1.properties on VM1:**

```properties
process.roles=controller
node.id=101
controller.quorum.voters=101@10.18.0.5:9192,102@10.18.0.5:9193,103@10.118.0.5:9194,104@10.118.0.5:9195,105@10.128.0.5:9196

listeners=CONTROLLER://0.0.0.0:9192
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:SSL

ssl.keystore.location=/home/kafka/security/broker1.keystore.jks
ssl.keystore.password=lab-password
ssl.key.password=lab-password
ssl.truststore.location=/home/kafka/security/kafka.truststore.jks
ssl.truststore.password=lab-password
ssl.endpoint.identification.algorithm=
```

Repeat the logic for `controller2` to `controller5`, reusing the keystore of the broker co-located on the same VM.

### TLS Restart Order

**Objective:** avoid a partially-TLS cluster where brokers restart before the controller quorum has re-formed.

**Controllers first:**

```bash
sudo systemctl restart kafka-controller@1.service kafka-controller@2.service
sudo systemctl restart kafka-controller@3.service kafka-controller@4.service
sudo systemctl restart kafka-controller@5.service
```

Wait 15 seconds, then **brokers**:

```bash
sudo systemctl restart kafka-broker@1.service kafka-broker@2.service
sudo systemctl restart kafka-broker@3.service kafka-broker@4.service
sudo systemctl restart kafka-broker@5.service kafka-broker@6.service
```

**TLS validation:**

```bash
openssl s_client -connect 10.18.0.5:9092 -tls1_2 -CAfile /home/kafka/security/ca/ca-cert </dev/null 2>/dev/null | grep "Verify return code"
```

Expected result: `Verify return code: 0 (ok)`.

### Quorum Validation over TLS

**Objective:** verify that KRaft controllers are properly reachable over mutual SSL.

Create the client file:

```bash
cat <<EOF2 > /home/kafka/security/admin-ssl.properties
security.protocol=SSL
ssl.truststore.location=/home/kafka/security/kafka.truststore.jks
ssl.truststore.password=lab-password
ssl.keystore.location=/home/kafka/security/broker1.keystore.jks
ssl.keystore.password=lab-password
ssl.key.password=lab-password
ssl.endpoint.identification.algorithm=
EOF2
```

Then:

```bash
/opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-controller 10.18.0.5:9192 \
  --command-config /home/kafka/security/admin-ssl.properties \
  describe --status
```

⚠️ The `ssl.keystore.*` block in `admin-ssl.properties` is **mandatory**, not optional: since `ssl.client.auth=required` (mTLS) is active, a client without its own certificate will fail.

The quorum should show 5 voters and the brokers as observers.

---

## Part B — Setting Up SASL/SCRAM

### Objective of Part B

The goal of this part is to add **strong authentication** on top of TLS. TLS protects the transport; SASL/SCRAM controls **who connects** to the cluster.

By the end of this part, brokers must only accept clients authenticated via `SCRAM-SHA-512`, and a connection attempt without credentials must fail explicitly.

### Step B1 — Create SCRAM Users

**Objective:** declare in Kafka the identities that will later be used by brokers and applications.

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
Completed updating config for user admin.
Completed updating config for user jcdecaux-producer.
```

### Step B2 — Enable SASL_SSL on Brokers

**Objective:** move broker listeners from `SSL` to `SASL_SSL`, without touching the controller channel, which stays plain `SSL`.

⚠️ `inter.broker.listener.name` and `security.inter.broker.protocol` must not both be set. In this lab, only `inter.broker.listener.name` is kept.

**Example — broker1.properties:**

```properties
listeners=SASL_SSL://0.0.0.0:9092
advertised.listeners=SASL_SSL://10.18.0.5:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=SASL_SSL:SASL_SSL,CONTROLLER:SSL
inter.broker.listener.name=SASL_SSL

sasl.enabled.mechanisms=SCRAM-SHA-512
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="lab-admin-pwd";
```

Keep the `ssl.keystore.*`, `ssl.truststore.*`, `ssl.client.auth`, and `ssl.endpoint.identification.algorithm=` lines.

**Restart brokers only:**

```bash
sudo systemctl restart kafka-broker@1.service kafka-broker@2.service
sudo systemctl restart kafka-broker@3.service kafka-broker@4.service
sudo systemctl restart kafka-broker@5.service kafka-broker@6.service
```

### Validation of Part B

Create the admin client file:

```bash
cat <<EOF2 > /home/kafka/security/admin-sasl.properties
security.protocol=SASL_SSL
ssl.truststore.location=/home/kafka/security/kafka.truststore.jks
ssl.truststore.password=lab-password
ssl.endpoint.identification.algorithm=
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="lab-admin-pwd";
EOF2
```

Test:

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server 10.18.0.5:9092 \
  --command-config /home/kafka/security/admin-sasl.properties --list
```

This command must succeed.

Also create a test without JAAS:

```bash
cat <<EOF2 > /home/kafka/security/no-auth-test.properties
security.protocol=SASL_SSL
ssl.truststore.location=/home/kafka/security/kafka.truststore.jks
ssl.truststore.password=lab-password
ssl.endpoint.identification.algorithm=
sasl.mechanism=SCRAM-SHA-512
EOF2
```

Then:

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server 10.18.0.5:9092 \
  --command-config /home/kafka/security/no-auth-test.properties --list
```

This command must fail, proving that an unauthenticated client is not accepted.

---

## Part C — Enabling ACLs

### Objective of Part C

The goal of this part is to move beyond checking **who connects**, to also check **what a user is allowed to do**. From this point on, two authenticated users must no longer automatically have the same privileges.

The lab use case is simple: the JCDecaux producer must be able to **write** and **describe** specific topics, without gaining broad rights over the entire cluster.

### Step C1 — Enable the Authorizer

**Objective:** enable Kafka's authorization engine on all brokers.

Add to each `brokerX.properties`:

```properties
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
allow.everyone.if.no.acl.found=false
super.users=User:admin
```

⚠️ These lines must be added to the **brokers**, not the controllers.

### Step C2 — Create ACLs for the JCDecaux Producer

**Objective:** give the producer only the rights needed on `vls-stations-nancy` and `vls-stations-toulouse`.

```bash
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --command-config /home/kafka/security/admin-sasl.properties \
  --add --allow-principal User:jcdecaux-producer \
  --operation Write --operation Describe \
  --topic vls-stations-nancy

/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --command-config /home/kafka/security/admin-sasl.properties \
  --add --allow-principal User:jcdecaux-producer \
  --operation Write --operation Describe \
  --topic vls-stations-toulouse
```

**Validation:**

```bash
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --command-config /home/kafka/security/admin-sasl.properties \
  --list --topic vls-stations-nancy
```

---

## Part D — Security Audit

### Objective of Part D

The goal of this part is to **turn the configuration in place into verifiable evidence**. A cluster can appear to be working while still having a weakness: a soon-to-expire certificate, an overly broad ACL, disabled auth that goes unnoticed, or an inconsistent truststore between nodes.

This part therefore provides an **audit script** to systematically check the critical points: TLS, certificates, SASL users, ACLs, and overall security posture.

### Basic Audit Script

Create the script on VM1:

```bash
nano /home/kafka/security_audit.sh
```

Then paste:

```bash
#!/bin/bash
set -euo pipefail

BROKERS=("10.18.0.5:9092" "10.18.0.5:9093" "10.118.0.5:9094" "10.118.0.5:9095" "10.128.0.5:9096" "10.128.0.5:9097")
ADMIN_CONFIG="/home/kafka/security/admin-sasl.properties"
CA_FILE="/home/kafka/security/ca/ca-cert"
NO_AUTH_CONFIG="/home/kafka/security/no-auth-test.properties"
REPORT="/home/kafka/audit_report_$(date +%Y%m%d_%H%M%S).txt"
KAFKA_BIN="/opt/kafka/bin"

echo "=============================================" | tee "$REPORT"
echo "KAFKA SECURITY AUDIT REPORT - $(date)" | tee -a "$REPORT"
echo "=============================================" | tee -a "$REPORT"

echo -e "\n--- 1. PER-BROKER TLS CHECK ---" | tee -a "$REPORT"
for broker in "${BROKERS[@]}"; do
    host=$(echo $broker | cut -d: -f1)
    port=$(echo $broker | cut -d: -f2)
    result=$(echo | openssl s_client -connect ${host}:${port} -tls1_2 -CAfile "$CA_FILE" 2>/dev/null | grep "Verify return code")
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

echo -e "\n--- 5. super.users CHECK ---" | tee -a "$REPORT"
for host_ip in 10.18.0.5 10.118.0.5 10.128.0.5; do
    echo "Config on $host_ip:" | tee -a "$REPORT"
    ssh kafka@${host_ip} "grep -r 'super.users' /opt/kafka/config/kraft-lab/ 2>/dev/null" | tee -a "$REPORT" || echo "Not found" | tee -a "$REPORT"
done

echo -e "\n--- 6. allow.everyone.if.no.acl.found CHECK ---" | tee -a "$REPORT"
for host_ip in 10.18.0.5 10.118.0.5 10.128.0.5; do
    ssh kafka@${host_ip} "grep -r 'allow.everyone.if.no.acl.found' /opt/kafka/config/kraft-lab/ 2>/dev/null" | tee -a "$REPORT"
done

echo -e "\n--- 7. TEST: ACCESS DENIED WITHOUT JAAS ---" | tee -a "$REPORT"
if /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server 10.18.0.5:9092 >/tmp/audit_test_nojaas.out 2>&1; then
  echo "ANOMALY: the command without JAAS succeeded" | tee -a "$REPORT"
else
  echo "OK: the command without JAAS fails" | tee -a "$REPORT"
fi

echo -e "\n=============================================" | tee -a "$REPORT"
echo "AUDIT COMPLETE - Full report: $REPORT" | tee -a "$REPORT"
echo "=============================================" | tee -a "$REPORT"
```

Make it executable:

```bash
chmod +x /home/kafka/security_audit.sh
/home/kafka/security_audit.sh
```

### Extension to Test a Clean Negative ACL

Create a user without any ACL:

```bash
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --command-config /home/kafka/security/admin-sasl.properties \
  --alter --add-config 'SCRAM-SHA-512=[password=lab-unauth-pwd]' \
  --entity-type users --entity-name unauthorized-user
```

Create its client file:

```bash
cat > /home/kafka/unauthorized-sasl.properties << 'EOF2'
security.protocol=SASL_SSL
ssl.truststore.location=/home/kafka/security/kafka.truststore.jks
ssl.truststore.password=lab-password
ssl.endpoint.identification.algorithm=
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="unauthorized-user" \
  password="lab-unauth-pwd";
EOF2
```

Test unauthorized topic creation:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --create \
  --bootstrap-server 10.18.0.5:9092 \
  --command-config /home/kafka/unauthorized-sasl.properties \
  --topic audit-test-unauthorized --partitions 1 --replication-factor 3
```

This command must fail. This is the most meaningful negative test in this lab.

---

## New Section 7: Denial with Wrong User / Wrong JAAS

Objective: prove that **without JAAS or with a test user**, the ACL check is not bypassed.

In `security_audit.sh`, replace the old section 7 with something like:

```bash
echo -e "--- 7. TEST ACCESS DENIED WITHOUT JAAS (WRONG CLIENT) ---" | tee -a "$REPORT"

# Test 7.1: no JAAS -> must fail even before ACLs
if /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server 10.18.0.5:9092 >/tmp/audit_test_nojaas.out 2>&1; then
  echo "ANOMALY: the command without JAAS succeeded (expected: JAAS error)" | tee -a "$REPORT"
else
  echo "OK: the command without JAAS fails (expected JAAS error or SaslAuthenticationException)" | tee -a "$REPORT"
fi

# Test 7.2: wrong SASL user without ACL -> must fail on the ACL side
if /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server 10.18.0.5:9092 \
  --command-config /home/kafka/unauthorized-sasl.properties >/tmp/audit_test_baduser.out 2>&1; then
  echo "ANOMALY: user unauthorized-user can list topics (expected: ACL denial)" | tee -a "$REPORT"
else
  echo "OK: user unauthorized-user cannot list topics (ACLs effective)" | tee -a "$REPORT"
fi
```

- 7.1 checks the "no JAAS" behavior (more of a SASL configuration test).
- 7.2 truly verifies that the ACL blocks a user who has no rights at all.

***

## New Section 8: Unauthorized Topic for a Non-admin User

Objective: prove that `unauthorized-user` **cannot create** a topic that has no ACL.

Replace section 8 with:

```bash
echo -e "--- 8. TEST ACCESS DENIED ON UNAUTHORIZED TOPIC ---" | tee -a "$REPORT"

TOPIC_TEST="audit-test-unauthorized"

if /opt/kafka/bin/kafka-topics.sh \
  --create \
  --bootstrap-server 10.18.0.5:9092 \
  --command-config /home/kafka/unauthorized-sasl.properties \
  --topic "$TOPIC_TEST" --partitions 1 --replication-factor 3 >/tmp/audit_test_topic.out 2>&1; then
  echo "ANOMALY: unauthorized-user was able to create topic $TOPIC_TEST (expected: ACL denial)" | tee -a "$REPORT"
else
  echo "OK: unauthorized-user cannot create topic $TOPIC_TEST (ACLs effective)" | tee -a "$REPORT"
fi
```

## Part E — Certificate Rotation Without Interruption

### Objective of Part E

This part has a very concrete goal: **test the rotation of a broker's TLS certificate without interrupting the service delivered to applications**.

In production, certificates have a limited lifespan. When they approach expiration, it must be possible to renew them without causing an application outage. The real challenge is therefore not just generating a new certificate, but demonstrating that:

- the broker continues to start correctly with its new certificate;
- other Kafka components continue to trust it;
- producers and consumers continue to exchange messages;
- the team is able to technically and functionally validate this rotation.

In the lab, the exercise is deliberately limited to **broker1**, but the method generalizes to the other brokers.

### Step E1 — Generate a New Certificate for broker1

**Objective:** prepare a complete new keystore for broker1, signed by the same CA as the old one.

On `kafka-node1`:

```bash
cd /home/kafka/security

keytool -genkey \
  -keystore broker1-new.keystore.jks \
  -alias broker1 \
  -validity 1825 \
  -keyalg RSA \
  -dname "CN=broker1,OU=IT,O=BourseLuxembourg,L=Luxembourg,C=LU" \
  -storepass lab-password \
  -keypass lab-password

keytool -certreq \
  -keystore broker1-new.keystore.jks \
  -alias broker1 \
  -file broker1-new.csr \
  -storepass lab-password

openssl x509 -req \
  -CA ca/ca-cert \
  -CAkey ca/ca-key \
  -in broker1-new.csr \
  -out broker1-new-signed.crt \
  -days 1825 \
  -CAcreateserial \
  -passin pass:ca-lab-password

keytool -importcert \
  -keystore broker1-new.keystore.jks \
  -alias CARoot \
  -file ca/ca-cert \
  -storepass lab-password \
  -noprompt

keytool -importcert \
  -keystore broker1-new.keystore.jks \
  -alias broker1 \
  -file broker1-new-signed.crt \
  -storepass lab-password \
  -noprompt
```

At this stage, the old and new keystores coexist. The rotation is not yet applied; it is simply ready.

### Step E2 — Replace broker1's Keystore

**Objective:** replace the keystore currently used by broker1 with the new one, while keeping an immediate rollback option.

```bash
cd /home/kafka/security
cp broker1.keystore.jks broker1.keystore.jks.backup
cp broker1-new.keystore.jks broker1.keystore.jks
```

The backup allows a quick rollback if the new certificate causes a problem.

### Step E3 — Reload the Broker's Configuration

**Objective:** tell Kafka that broker1 must use the keystore found at this path.

```bash
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --command-config /home/kafka/security/admin-sasl.properties \
  --alter \
  --entity-type brokers \
  --entity-name 1 \
  --add-config listener.name.sasl_ssl.ssl.keystore.location=/home/kafka/security/broker1.keystore.jks
```

In this lab, the path stays the same, but this step materializes the driven-rotation principle.

### Step E4 — Verify broker1 Remains Functional

**Objective:** prove that the broker remains available after the rotation.

```bash
sudo systemctl status kafka-broker@1.service --no-pager | grep Active
```

The service must remain `active (running)`.

### Step E5 — Verify the New Certificate's TLS Validity

**Objective:** verify that the new certificate is indeed presented by broker1 and is still accepted by the lab's chain of trust.

From another VM:

```bash
openssl s_client -connect 10.18.0.5:9092 -tls1_2 -CAfile /home/kafka/security/ca/ca-cert </dev/null 2>/dev/null \
  | grep "subject=\|Verify return code"
```

Expected result:

- a `subject=` matching `CN=broker1`;
- a `Verify return code: 0 (ok)`.

### Step E6 — Verify the JCDecaux Producer Remains Operational

**Objective:** demonstrate that the certificate rotation is not only technically correct, but also transparent to business flows.

The JCDecaux producer must continue publishing to `vls-stations-nancy` and `vls-stations-toulouse` without TLS errors or observable downtime.

During or right after the rotation:

- check the producer logs to make sure no `SSLHandshakeException` appears;
- check that messages keep being produced;
- check on the cluster side that the topic remains accessible.

For example:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --describe \
  --bootstrap-server 10.18.0.5:9092 \
  --command-config /home/kafka/security/admin-sasl.properties \
  --topic vls-stations-nancy
```

"Rotation without interruption" does not just mean the broker restarts; above all it means **applications keep working** during the operation or right after, with no visible functional incident.

### What Part E Actually Validates

If all the checks above are positive, then the team has demonstrated that it knows how to:

- generate a new broker certificate;
- integrate it without breaking TLS trust;
- maintain broker availability;
- maintain production flows during the rotation;
- document a realistic procedure reusable in production.

---

## Final Checklist

| Checkpoint | Expected Lab Status |
| :-- | :-- |
| TLS active on all flows | Passed |
| Controller quorum formed over SSL | Passed |
| Certificates signed by the CA | Passed |
| Certificates not expired | Passed |
| SASL SCRAM-SHA-512 active | Passed |
| Denial of a client without credentials | Passed |
| JCDecaux producer ACLs in place | Passed |
| Denial of a user without ACL on topic creation | Passed |
| Certificate rotation tested on broker1 | Passed |
| Producer continuity during rotation | Passed |

---

## Cleanup Before the Next Workshop

If the next workshop expects a PLAINTEXT cluster, a clean return to a simple configuration is needed. A partial cleanup of the brokers alone is not enough, since the controllers would remain on SSL and would prevent the quorum from working correctly.

### Step N1 — Cleanup of Brokers and Controllers

```bash
sed -i 's/SASL_SSL/PLAINTEXT/g; s/CONTROLLER:SSL/CONTROLLER:PLAINTEXT/g' \
  /opt/kafka/config/kraft-lab/broker*.properties \
  /opt/kafka/config/kraft-lab/controller*.properties

sed -i 's/advertised.listeners=SASL_SSL:\/\//advertised.listeners=PLAINTEXT:\/\//' \
  /opt/kafka/config/kraft-lab/broker*.properties
```

### Step N2 — Restart in the Correct Order

```bash
sudo systemctl restart kafka-controller@1.service kafka-controller@2.service
sudo systemctl restart kafka-controller@3.service kafka-controller@4.service
sudo systemctl restart kafka-controller@5.service
```

Wait 10 to 15 seconds, then:

```bash
sudo systemctl restart kafka-broker@1.service kafka-broker@2.service
sudo systemctl restart kafka-broker@3.service kafka-broker@4.service
sudo systemctl restart kafka-broker@5.service kafka-broker@6.service
```

### Expected Result

```bash
/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-controller 10.18.0.5:9192 describe --status
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server 10.18.0.5:9092
```

### Recommended Alternative

The most reliable method remains restoring a VMware snapshot taken before the TLS/SASL/ACL manipulations.
