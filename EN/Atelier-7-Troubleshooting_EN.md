# Workshop 7 — Production Troubleshooting (Connections, Offsets, Leadership)

## Workshop Objective

Group together three real incident families reported by the client — broker connection errors, offset resynchronization after reloading a SQL source, and `NOT_LEADER_OR_FOLLOWER` errors — to turn production observations into reproducible lab scenarios. This workshop reuses the 6-broker/5-controller KRaft cluster from Workshop 0 and the Kafka Connect worker/JDBC connector deployed in Workshop 5, requiring no new infrastructure.

## Client Context to Keep in Mind

| Questionnaire finding | Implication for the workshop |
| :-- | :-- |
| "Connection to Node could not be established. Node may not be available" cited as an incident to investigate | Part A dedicated to reproduction and diagnosis |
| Need to better master `kafka-consumer-groups.sh --reset-offsets`, currently used "Sometimes" | Part B based on a real JDBC scenario |
| Kafka Connect integration with SQL Server / MongoDB / Elasticsearch already in place or planned | Direct reuse of Workshop 5's JDBC connector |
| Leader-related errors and preferred leader election already requested in Deep Dive | Part C on `NOT_LEADER_OR_FOLLOWER` |
| Daily tools: AKHQ, Kafka CLI, Grafana/Mimir/Prometheus, systemctl, journalctl, SSH | All diagnostic commands rely exclusively on these tools |

***

## Prerequisites

| Element | Machine |
| :-- | :-- |
| Operational 6-broker/5-controller Kafka cluster (Workshop 0) | VM1, VM2, VM3 |
| Kafka Connect worker started with JDBC source connector `jdbc-source-station-metadata` (Workshop 5) | VM1 |
| PostgreSQL database `stations_db` with table `station_metadata` (Workshop 5) | VM1 |
| Active JCDecaux producer (Workshop 1) | Producer VM |
| SSH access and `sudo` rights on the 3 VMs | VM1, VM2, VM3 |

Quick check before starting:

```bash
/opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  describe --status

curl -s http://10.18.0.5:8083/connectors/jdbc-source-station-metadata/status | jq .
```

The quorum must be healthy and the JDBC connector must show `"state": "RUNNING"` before starting the workshop.

***

## Part A — Broker Connection Errors ("Node may not be available")

This scenario reproduces the exact incident reported by the client, where a client or admin tool can no longer reach a specific cluster node. The goal is to distinguish a process outage, a network outage, and an `advertised.listeners` configuration issue.

### Step A1 — Trigger a Network Connectivity Error

**Machine: VM2 (10.118.0.5)** — temporarily block broker 3's port with the firewall, without stopping the Kafka process:

```bash
sudo ufw deny out to 10.118.0.5 port 9094
sudo ufw deny in on any to any port 9094
sudo ufw status
```

> **Teaching tip:** unlike a `systemctl stop`, this approach leaves the broker alive from the cluster's point of view (visible in metadata), but unreachable over the network — this exact mismatch is what triggers the error message reported by the client.

### Step A2 — Reproduce the Error on the Client Side

**Machine: VM1** — attempt an admin operation explicitly targeting the blocked broker:

```bash
/opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server 10.118.0.5:9094
```

### Expected Result

```
[2026-07-14 15:10:22,341] WARN [AdminClient clientId=adminclient-1] Connection to node 3 (10.118.0.5:9094) could not be established. Node may not be available.
```

✅ **Key point:** the message matches word for word what the client reported in the questionnaire — proof that this behavior does not necessarily indicate a stopped broker, but a network reachability problem at the listener level.

### Step A3 — Diagnose Without Assuming the Cause

**Machine: VM2** — verify the Kafka process is indeed active despite the observed error:

```bash
sudo systemctl status kafka-broker3
```

```bash
sudo ss -lntp | grep 9094
```

### Expected Result

```
● kafka-broker3.service - Kafka Broker 3
   Active: active (running) since ...
```

```
LISTEN  0  50  0.0.0.0:9094  0.0.0.0:*  users:(("java",pid=...))
```

✅ **Teaching point:** the process is indeed listening locally, proving the block is network-related, not application-related — useful for escalating to the infrastructure team rather than the Kafka team.

### Step A4 — Confirm via Broker Logs

**Machine: VM2**

```bash
sudo journalctl -u kafka-broker3 --since "10 minutes ago" | tail -n 30
```

### Expected Result

No error on the broker side itself — no application exception trace, confirming the problem lies between client and broker, not within Kafka.

### Step A5 — Restore Connectivity

**Machine: VM2**

```bash
sudo ufw delete deny out to 10.118.0.5 port 9094
sudo ufw delete deny in on any to any port 9094
sudo ufw status
```

Revalidate immediately:

```bash
/opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server 10.118.0.5:9094
```

### Expected Result

```
10.118.0.5:9094 (id: 3 rack: North) -> (
	...
	ApiKeys object...
)
```

✅ **Validation:** the command must now return the list of APIs supported by the broker, confirming a return to normal.

### Step A6 — Reusable Diagnostic Grid

| Observed symptom | Check | Command |
| :-- | :-- | :-- |
| "Node may not be available" | Process active? | `systemctl status kafka-brokerX` |
| "Node may not be available" | Port open locally? | `ss -lntp \| grep <port>` |
| "Node may not be available" | Reachable from another node? | `nc -zv <ip> <port>` |
| "Node may not be available" | Application error on broker side? | `journalctl -u kafka-brokerX` |
| "Node may not be available" | Local or network firewall? | `ufw status`, network-side check |

***

## Part B — Offset Resynchronization After SQL Reload

This scenario addresses the explicit need to better master `kafka-consumer-groups.sh --reset-offsets`, currently used "Sometimes" without a formalized procedure. It directly relies on the JDBC source connector deployed in Workshop 5, which reads the `station_metadata` table in `timestamp` mode.

### Step B1 — Observe the Connector's Normal Behavior

**Machine: VM1** — check the connector's current state and position:

```bash
curl -s http://10.18.0.5:8083/connectors/jdbc-source-station-metadata/status | jq .
```

**Machine: VM2** — check the current content of the generated topic:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 10.118.0.5:9094 \
  --topic connect-station_metadata \
  --from-beginning
```

### Expected Result

The three initial `station_metadata` records (stations 12, 45, 7), already known from Workshop 5.

### Step B2 — Simulate a Full SQL Table Reload

**Machine: VM1** — truncate then reload the table, as during a full reload on the SQL Server side in production:

```bash
sudo -u postgres psql -d stations_db -c "TRUNCATE TABLE station_metadata RESTART IDENTITY;"

sudo -u postgres psql -d stations_db -c "
INSERT INTO station_metadata (station_number, city, installation_date) VALUES
  (12, 'nancy', '2018-03-15'),
  (45, 'nancy', '2019-06-01'),
  (7, 'toulouse', '2017-11-20'),
  (99, 'metz', '2022-01-10');
"
```

### Expected Result

```
TRUNCATE TABLE
INSERT 0 4
```

✅ **Key point:** the JDBC connector's `timestamp` mode relies on the `updated_at` column, not identifiers — after a `TRUNCATE`/reinsert, the new rows have a new `updated_at`, which can trigger either full republication or a temporary lack of new data depending on exact timing, to be observed concretely with the group.

### Step B3 — Observe the Effect on the Topic and Consumer Group

**Machine: VM2**

```bash
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server 10.118.0.5:9094 \
  --describe --group connect-cluster-lab
```

### Expected Result

```
GROUP               TOPIC                      PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
connect-cluster-lab connect-station_metadata   0          3               7               4
```

✅ **Teaching point:** a positive `LAG` after a reload shows the connector correctly detected and published the new rows, but the application consumer (simulated here) has not yet consumed these new messages — exactly the situation where the client wonders whether to "reset the offsets".

### Step B4 — Decide: Reset or Simple Catch-up?

Before touching any offsets, have the group answer this simple question: is the application consumer just lagging behind (`LAG` naturally decreasing), or persistently stuck? Check before acting:

```bash
watch -n 5 "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 10.118.0.5:9094 --describe --group connect-cluster-lab"
```

If `LAG` progressively drops to zero, **no action is needed** — this is normal catch-up, not an incident.

### Step B5 — Reset Offsets if Necessary (Blocking Case)

If the consumer group is genuinely stuck (e.g. after an application error), demonstrate the three main reset strategies:

**Option 1 — Go back to the beginning of the topic:**

```bash
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server 10.118.0.5:9094 \
  --group connect-cluster-lab \
  --topic connect-station_metadata \
  --reset-offsets --to-earliest --dry-run
```

**Option 2 — Position on the latest data only:**

```bash
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server 10.118.0.5:9094 \
  --group connect-cluster-lab \
  --topic connect-station_metadata \
  --reset-offsets --to-latest --dry-run
```

**Option 3 — Position at a specific point in time (most suitable after a dated SQL reload):**

```bash
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server 10.118.0.5:9094 \
  --group connect-cluster-lab \
  --topic connect-station_metadata \
  --reset-offsets --to-datetime 2026-07-14T15:00:00.000 --dry-run
```

### Expected Result

```
GROUP                TOPIC                      PARTITION  NEW-OFFSET
connect-cluster-lab  connect-station_metadata   0          3
```

✅ **Important:** `--dry-run` shows the result without applying it — remove this parameter and add `--execute` only after explicit group validation, to avoid any accidental reset in real conditions.

### Step B6 — Apply and Verify

```bash
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server 10.118.0.5:9094 \
  --group connect-cluster-lab \
  --topic connect-station_metadata \
  --reset-offsets --to-datetime 2026-07-14T15:00:00.000 --execute
```

```bash
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server 10.118.0.5:9094 \
  --describe --group connect-cluster-lab
```

### Expected Result

`CURRENT-OFFSET` aligned with the newly chosen position, `LAG` recalculated accordingly.

***

## Part C — `NOT_LEADER_OR_FOLLOWER` Diagnosis

This part directly extends the DRP observations already made in Workshop 2, precisely isolating the `NOT_LEADER_OR_FOLLOWER` error as a Kafka client can encounter it just after a leader change. It addresses the explicit "Deep Dive" need on leader behavior after an incident.

### Step C1 — Identify a Partition and Its Current Leader

**Machine: VM1**

```bash
/opt/kafka/bin/kafka-topics.sh --describe \
  --bootstrap-server 10.18.0.5:9092 \
  --topic test-drp-lab
```

### Expected Result

```
Topic: test-drp-lab  Partition: 0  Leader: 1  Replicas: 1,3,5  Isr: 1,3,5
```

Note the leader broker (here broker 1) for the rest of the scenario.

### Step C2 — Launch a Continuous Producer Before the Incident

**Machine: VM1**

```bash
/opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic test-drp-lab \
  --num-records 100000 \
  --record-size 200 \
  --throughput 20 \
  --producer-props bootstrap.servers=10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096
```

Let this producer run in the background during the following steps.

### Step C3 — Trigger a Sudden Leader Change

**Machine: VM1** — directly stop the leader broker identified in C1:

```bash
sudo systemctl stop kafka-broker1
```

### Step C4 — Observe the Error on the Client Side

**Machine: VM2** — launch a second producer explicitly targeting the old leader broker in its bootstrap list, to force contact with a stale node:

```bash
/opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --topic test-drp-lab \
  --producer-property retries=0 \
  --producer-property metadata.max.age.ms=600000
```

Type a few messages manually right after stopping broker 1.

### Expected Result

```
[2026-07-14 15:22:07,884] WARN [Producer clientId=console-producer] Received invalid metadata error in produce request on partition test-drp-lab-0 due to org.apache.kafka.common.errors.NotLeaderOrFollowerException
```

✅ **Key point:** this behavior is deliberately triggered here by forcing a high `metadata.max.age.ms`, to delay client-side metadata refresh and make the error visible instead of being silently fixed within milliseconds.

### Step C5 — Explain the Mechanism to the Group

Have the group articulate, from the observation:

- The client held metadata indicating broker 1 as the leader of partition 0.
- After broker 1 stopped, a new leader was elected among the remaining replicas.
- As long as the client hasn't refreshed its metadata, it keeps targeting the old leader, triggering `NOT_LEADER_OR_FOLLOWER`.
- The client eventually refreshes its metadata (automatically, based on `metadata.max.age.ms`) and finds the correct leader.

### Step C6 — Verify the New Leader

**Machine: VM2**

```bash
/opt/kafka/bin/kafka-topics.sh --describe \
  --bootstrap-server 10.118.0.5:9094 \
  --topic test-drp-lab
```

### Expected Result

```
Topic: test-drp-lab  Partition: 0  Leader: 3  Replicas: 1,3,5  Isr: 3,5
```

Leadership has moved to broker 3, with a temporarily reduced ISR until broker 1 returns.

### Step C7 — Restart the Broker and Return to the Preferred Leader

**Machine: VM1**

```bash
sudo systemctl start kafka-broker1
```

Wait for resynchronization, then force a return to the preferred leader, directly linked to the client's explicit request about this command:

```bash
/opt/kafka/bin/kafka-leader-election.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --topic test-drp-lab \
  --partition 0 \
  --election-type preferred
```

### Expected Result

```
Successfully completed leader election (PREFERRED) for partitions Set(test-drp-lab-0)
```

Re-verify with `--describe` that broker 1 has become leader again once the ISR is fully rebuilt.

### Step C8 — Leader-Related Error Reading Grid

| Observed error | Likely cause | Recommended action |
| :-- | :-- | :-- |
| `NOT_LEADER_OR_FOLLOWER` | Stale client metadata after election | Wait for automatic refresh, avoid forcing aggressive retries |
| Persistent `NOT_LEADER_OR_FOLLOWER` | Genuinely orphaned partition / empty ISR | Check `--describe`, consider `unclean.leader.election` as a last resort (not recommended here) |
| Leader not returned to preferred broker after DRP | No automatic preferred election | Run `kafka-leader-election.sh --election-type preferred` |

***

## Part D — Consolidated Diagnostic Runbook

This final part synthesizes the three incident families into a single decision tree, designed to be displayed or printed as an operational cheat sheet, consistent with the tools already used daily by the team.

### Simplified Decision Tree

1. **A client can't reach a specific broker?**
   → Check `systemctl status`, then `ss -lntp`, then `journalctl`, then the network/firewall layer (Part A).

2. **A consumer group accumulates lag after a source-side event (SQL, reload, migration)?**
   → Check if the lag naturally decreases before any action; otherwise use `--reset-offsets` with `--dry-run` then `--execute` (Part B).

3. **`NOT_LEADER_OR_FOLLOWER` errors appear after a DRP or broker restart?**
   → Check `--describe` of the affected topic, distinguish a transient error (metadata refresh) from a persistent one (empty ISR), then force a preferred election if needed (Part C).

### Quick Reference Commands

| Need | Command |
| :-- | :-- |
| Quorum state | `kafka-metadata-quorum.sh --bootstrap-server ... describe --status` |
| Topic state | `kafka-topics.sh --describe --bootstrap-server ... --topic <topic>` |
| Consumer group state | `kafka-consumer-groups.sh --describe --bootstrap-server ... --group <group>` |
| Reset offsets (simulation) | `kafka-consumer-groups.sh --reset-offsets --to-datetime ... --dry-run` |
| Preferred leader election | `kafka-leader-election.sh --election-type preferred --topic <topic> --partition <n>` |
| Broker process state | `systemctl status kafka-brokerX` |
| Broker logs | `journalctl -u kafka-brokerX --since "10 minutes ago"` |
| Connect connector state | `curl http://<host>:8083/connectors/<name>/status` |

***

## Cleanup After the Workshop

**Machine: VM1**

```bash
sudo ufw status
# Verify that no temporary blocking rule (Part A) remains active
```

**Machine: VM2**

```bash
sudo systemctl status kafka-broker3
sudo systemctl status kafka-broker1 2>/dev/null || true
```

**Machine: VM1** — remove test records added in Part B if necessary:

```bash
sudo -u postgres psql -d stations_db -c "DELETE FROM station_metadata WHERE station_number = 99;"
```

Revalidate the overall cluster state before closing the session:

```bash
/opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  describe --status

/opt/kafka/bin/kafka-topics.sh --describe \
  --bootstrap-server 10.18.0.5:9092 \
  --topic test-drp-lab
```

The cluster must show a healthy quorum and a fully synchronized `test-drp-lab` topic (`Isr` = `Replicas` on all partitions) before moving to the next workshop.

***

This workshop closes the loop on three concrete incidents identified as early as the scoping questionnaire, giving the team a structured diagnostic reflex rather than a case-by-case reaction to connection errors, offset synchronization, or leader changes.
