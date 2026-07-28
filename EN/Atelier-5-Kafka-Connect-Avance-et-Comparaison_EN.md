# Workshop 5 — Advanced Kafka Connect and Architecture Comparison

## Workshop Objective

Cover the last two priorities checked in H1 of the questionnaire: advanced Kafka Connect configuration with error handling, and a reasoned comparison of Kafka architectures (current single cluster vs MirrorMaker 2 vs active-active).

## Client Context to Keep in Mind

| Questionnaire finding | Implication for the workshop |
| :-- | :-- |
| Connectors used: JDBC, MongoDB, SQL, Debezium | The lab must cover at least JDBC Source, the most universal one |
| No current DLQ management, explicit request to understand "designing and operating Dead Letter Queues" | Central part of the workshop |
| Current cluster = single multi-region cluster, not active-active nor active-passive | Basis of the comparative discussion |
| CSSF/DORA regulatory obligations on resilience | Analysis angle for the architecture comparison |

***

## Prerequisites

| Element | Machine |
| :-- | :-- |
| 6-broker/5-controller Kafka cluster in PLAINTEXT (post-Workshop 3 cleanup) | VM1, VM2, VM3 |
| Active JCDecaux producer | Producer VM |
| Java 17+ and Maven | VM1 |

***

## Part A — Kafka Connect Installation

### Step A1 — Prepare the Connect Directory

**Machine: VM1 (10.18.0.5)**

```bash
mkdir -p /opt/kafka/connect-plugins
mkdir -p /home/kafka/connect-data
```

### Step A2 — Download the JDBC Connector (Confluent)

**Machine: VM1**

```bash
cd /opt/kafka/connect-plugins
wget https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-jdbc/versions/10.7.15/confluentinc-kafka-connect-jdbc-10.7.15.zip
unzip confluentinc-kafka-connect-jdbc-10.7.15.zip
```

### Expected Result
```
Archive:  confluentinc-kafka-connect-jdbc-10.7.4.zip
   creating: confluentinc-kafka-connect-jdbc-10.7.4/
   inflating: .../lib/kafka-connect-jdbc-10.7.4.jar
```
✅ **Validation:** the directory should contain a `lib/` subfolder with several `.jar` files.

### Step A3 — Install PostgreSQL to Simulate the Data Source

**Machine: VM1**

```bash
sudo apt install -y postgresql postgresql-contrib
sudo -u postgres psql -c "CREATE USER kafka_connect WITH PASSWORD 'connect-pwd';"
sudo -u postgres psql -c "CREATE DATABASE stations_db OWNER kafka_connect;"
```

### Expected Result
```
CREATE ROLE
CREATE DATABASE
```

Create a test table simulating a station reference dataset (complementary to the JCDecaux flow):

```bash
sudo -u postgres psql -d stations_db -c "
CREATE TABLE station_metadata (
  id SERIAL PRIMARY KEY,
  station_number INT NOT NULL,
  city VARCHAR(50),
  installation_date DATE,
  updated_at TIMESTAMP NOT NULL DEFAULT NOW() 
);
INSERT INTO station_metadata (station_number, city, installation_date) VALUES
  (12, 'nancy', '2018-03-15'),
  (45, 'nancy', '2019-06-01'),
  (7, 'toulouse', '2017-11-20');
"
```

### Expected Result
```
CREATE TABLE
INSERT 0 3
```
Change the owner:
```bash
sudo -u postgres psql -d stations_db -c \
"GRANT USAGE ON SCHEMA public TO kafka_connect;
 GRANT SELECT ON TABLE station_metadata TO kafka_connect;"

sudo -u postgres psql -d stations_db -c "\dt+ station_metadata"
sudo -u postgres psql -d stations_db -c \
"ALTER TABLE station_metadata OWNER TO kafka_connect;"
```
***

## Part B — Deploying the Connect Worker (Distributed Mode)

### Step B1 — Create the Worker Configuration File

**Machine: VM1**

```bash
nano /opt/kafka/config/connect-distributed.properties
```

```properties
bootstrap.servers=10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096

group.id=connect-cluster-lab

key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false

offset.storage.topic=connect-offsets
offset.storage.replication.factor=3
offset.storage.partitions=25

config.storage.topic=connect-configs
config.storage.replication.factor=3

status.storage.topic=connect-status
status.storage.replication.factor=3
status.storage.partitions=5

plugin.path=/opt/kafka/connect-plugins

rest.port=8083
rest.advertised.host.name=10.18.0.5
```

> **Teaching note:** these topic names (`connect-offsets`, `connect-configs`, `connect-status`) match exactly those already used in the client's production environment.

### Step B2 — Start the Connect Worker

**Machine: VM1**

```bash
nohup /opt/kafka/bin/connect-distributed.sh \
  /opt/kafka/config/connect-distributed.properties \
  > /home/kafka/connect-data/connect.log 2>&1 &
  # verify
   sudo ss -lntp | grep 8083
```

### Expected Result
```bash
tail -f /home/kafka/connect-data/connect.log
```
Should show:
```
INFO Kafka Connect started
INFO REST server listening at http://10.18.0.5:8083
```

✅ **Validation:**
```bash
curl -s http://10.18.0.5:8083/ | jq .
```
Should return:
```json
{"version":"4.2.0","commit":"...","kafka_cluster_id":"k7F3nQzXTHmR9vK2wLpQAg"}
```

***

## Part C — Deploying the JDBC Source Connector

### Step C1 — Create and Deploy the Connector Configuration

**Machine: VM1**

```bash
nano /home/kafka/connect-data/jdbc-source-config.json
```

```json
{
  "name": "jdbc-source-station-metadata",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:postgresql://localhost:5432/stations_db",
    "connection.user": "kafka_connect",
    "connection.password": "connect-pwd",
    "table.whitelist": "station_metadata",
    "mode": "timestamp",
    "timestamp.column.name": "updated_at",
    "topic.prefix": "connect-",
    "poll.interval.ms": "10000",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "connect-dlq-station-metadata",
    "errors.deadletterqueue.topic.replication.factor": 3,
    "errors.deadletterqueue.context.headers.enable": true,
    "errors.log.enable": true,
    "errors.log.include.messages": true
  }
}
```

Deploy via the REST API:

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @/home/kafka/connect-data/jdbc-source-config.json \
  http://10.18.0.5:8083/connectors
```

### Expected Result
```json
{"name":"jdbc-source-station-metadata","config":{...},"tasks":[],"type":"source"}
```

✅ **Validation:**
```bash
curl -s http://10.18.0.5:8083/connectors/jdbc-source-station-metadata/status | jq .
```
Should show `"state": "RUNNING"` for the connector and its task.

### Step C2 — Verify the Published Data

**Machine: VM2 (10.118.0.5)**

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 10.118.0.5:9094 \
  --topic connect-station_metadata \
  --from-beginning
```

### Expected Result
```json
{"id":1,"station_number":12,"city":"nancy","installation_date":17970,"updated_at":1752396000000}
{"id":2,"station_number":45,"city":"nancy","installation_date":18048,"updated_at":1752396000000}
{"id":3,"station_number":7,"city":"toulouse","installation_date":17490,"updated_at":1752396000000}
```

✅ **Teaching point:** the connector automatically created the `connect-station_metadata` topic by concatenating the `topic.prefix` and the table name — a behavior worth explaining well, as it often surprises administrators used to creating their topics manually.

***

## Part D — Error Handling and Dead Letter Queue

### Step D1 — Deliberately Trigger a Connection Error

**Machine: VM1** — temporarily change the database password to simulate an authentication error:

```bash
sudo -u postgres psql -d stations_db -c "ALTER USER kafka_connect WITH PASSWORD 'wrong-password-temp';"
```

Restart the connector's task:
```bash
curl -X POST http://10.18.0.5:8083/connectors/jdbc-source-station-metadata/restart
```

### Expected Result
```bash
curl -s http://10.18.0.5:8083/connectors/jdbc-source-station-metadata/status | jq .
```
```json
{"state": "FAILED", "trace": "org.postgresql... FATAL: password authentication failed"}
```

✅ **Immediately restore the correct password to continue the workshop:**
```bash
sudo -u postgres psql -d stations_db -c "ALTER USER kafka_connect WITH PASSWORD 'connect-pwd';"
curl -X POST http://10.18.0.5:8083/connectors/jdbc-source-station-metadata/restart
```

### Step D2 — Test the DLQ with a Malformed Message (via a Sink Connector)

To illustrate the DLQ in a **failed deserialization** situation (explicit request in G4), a Sink Connector is added that writes to a file, with a deliberately incompatible converter.

**Machine: VM1**

```bash
nano /home/kafka/connect-data/file-sink-config.json
```

```json
{
  "name": "file-sink-test-dlq",
  "config": {
    "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
    "tasks.max": "1",
    "topics": "vls-stations-nancy",
    "file": "/home/kafka/connect-data/output-nancy.txt",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "connect-dlq-file-sink",
    "errors.deadletterqueue.topic.replication.factor": 3,
    "errors.log.enable": true
  }
}
```
Add the connector found at this path to:
```bash
nano /opt/kafka/config/connect-distributed.properties

plugin.path=/opt/kafka/connect-plugins,/opt/kafka/libs/connect-file-4.2.1.jar
```
> **Teaching tip:** using `JsonConverter` for the key while the keys of the `vls-stations-nancy` topic are simple strings (produced by our Java producer) deliberately triggers a deserialization error on every message.

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @/home/kafka/connect-data/file-sink-config.json \
  http://10.18.0.5:8083/connectors
```

### Expected Result
```json
{"name":"file-sink-test-dlq","config":{...},"tasks":[],"type":"sink"}
```

**Machine: VM2** — verify that the failed messages land in the DLQ:
```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 10.118.0.5:9094 \
  --topic connect-dlq-file-sink \
  --from-beginning \
  --property print.headers=true
```

### Expected Result
```
__connect.errors.exception.class.name:org.apache.kafka.connect.errors.DataException,__connect.errors.exception.message:Converting byte[] to Kafka Connect data failed due to serialization error	{"number":12,"name":"12 - REPUBLIQUE",...}
```

✅ **Key point to observe:** the Kafka headers (`__connect.errors.*`) contain the exact error detail — this richness of information enables precise diagnosis, addressing the client's request to "monitor schema-related issues".

### Step D3 — Cleanup of the Test Connector

**Machine: VM1**
```bash
curl -X DELETE http://10.18.0.5:8083/connectors/file-sink-test-dlq
```

***

## Part E — Connector Monitoring

### Step E1 — Overview via the REST API

**Machine: VM1**

```bash
curl -s http://10.18.0.5:8083/connectors | jq .
curl -s http://10.18.0.5:8083/connectors/jdbc-source-station-metadata/tasks | jq .
```

### Expected Result
```json
["jdbc-source-station-metadata"]
```
Then the detail of the task(s) with their `id` and configuration.

### Step E2 — Expose Connect JMX Metrics

**Machine: VM1**

```bash
# this command does not work with KAFKA 4.2.1 but works with earlier versions
/opt/kafka/bin/kafka-run-class.sh kafka.tools.JmxTool \
  --object-name kafka.connect:type=connector-task-metrics,connector=jdbc-source-station-metadata,task=0 \
  --jmx-url service:jmx:rmi:///jndi/rmi://10.18.0.5:9999/jmxrmi \
  --one-time true
```

### Expected Result
A set of metrics including `batch-size-avg`, `offset-commit-success-percentage`, `source-record-poll-rate` — exactly the type of metrics to integrate into Grafana/Mimir, consistent with their existing stack.

***

## Part F — Architecture Comparison (Structured Discussion)

### Step F1 — Map the Client's Current Architecture

**Facilitate as a group**, whiteboard or shared document — recap the confirmed facts:

- **Single** cluster with 6 brokers spread 3 West / 3 North, neither active-active nor active-passive
- `replication.factor=3`, `min.insync.replicas=2`, `acks=all` with multi-region validation for critical topics
- Rebalancing of about 1h after a regional incident, the main cause of the DRP problem (Workshop 2)

### Step F2 — Present the Alternative Architectures

| Architecture | Principle | Advantage | Disadvantage |
| :-- | :-- | :-- | :-- |
| **Single multi-region cluster (current)** | One single KRaft cluster, brokers spread across 2 Azure regions | Operational simplicity, strong consistency (acks=all) | Heavy rebalancing after a regional failure, RTO at risk |
| **MirrorMaker 2 (active-passive)** | Two independent clusters, asynchronous replication from primary cluster to a secondary cluster | Complete failure isolation, secondary cluster immediately usable | Replication latency (non-zero RPO), application failover complexity, dual infrastructure |
| **Active-active (2 clusters + bidirectional MirrorMaker 2)** | Two clusters active simultaneously, cross-replication | No downtime in case of total site failure | Risk of data conflicts, offset management complexity, doubled cost, replication loops to avoid |

### Step F3 — Practical Exercise: Simulate MirrorMaker 2 (Simplified Demonstration)

**Machine: VM3 (10.128.0.5)** — create a minimal second logical cluster to illustrate the principle (a single broker is enough for the demonstration).

Create the data directory and the DR broker's configuration file:

```bash
mkdir -p /data/kafka/broker-dr
nano /opt/kafka/config/kraft-lab/broker-dr.properties
```

```properties
process.roles=broker,controller
node.id=7
listeners=PLAINTEXT://0.0.0.0:9098,CONTROLLER://0.0.0.0:9198
advertised.listeners=PLAINTEXT://10.128.0.5:9098
controller.quorum.voters=7@10.128.0.5:9198
controller.listener.names=CONTROLLER
log.dirs=/data/kafka/broker-dr
```

Format the storage with a **different Cluster ID** from the main cluster's, then start the DR broker in the background:

```bash
NEW_CLUSTER_ID=$(/opt/kafka/bin/kafka-storage.sh random-uuid)
/opt/kafka/bin/kafka-storage.sh format \
  -t $NEW_CLUSTER_ID \
  -c /opt/kafka/config/kraft-lab/broker-dr.properties

nohup /opt/kafka/bin/kafka-server-start.sh \
  /opt/kafka/config/kraft-lab/broker-dr.properties \
  > /home/kafka/broker-dr.log 2>&1 &
```

### Expected Result
```bash
tail -f /home/kafka/broker-dr.log
```
Should show:
```
INFO [KafkaServer id=7] started
```

✅ **Validation:**
```bash
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server 10.128.0.5:9098
```
Should respond without error (empty list at startup), confirming the DR broker is reachable and separate from the primary cluster.

Configure and launch MirrorMaker 2:

**Machine: VM3**

```bash
nano /opt/kafka/config/kraft-lab/mm2.properties
```

```properties
clusters = primary, dr
primary.bootstrap.servers = 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096
dr.bootstrap.servers = 10.128.0.5:9098

primary->dr.enabled = true
dr->primary.enabled = false

topics = vls-stations-nancy
replication.factor = 1
offset.storage.replication.factor=1
mm2-offsets.primary.internal.replication.factor=1
mm2-configs.primary.internal.replication.factor=1
mm2-status.primary.internal.replication.factor=1
mm2-offset-syncs.primary.internal.replication.factor=1
heartbeats.topic.replication.factor=1
config.storage.replication.factor=1
status.storage.replication.factor=1
offset.syncs.topic.replication.factor=1
```

```bash
nohup /opt/kafka/bin/connect-mirror-maker.sh \
  /opt/kafka/config/kraft-lab/mm2.properties \
  > /home/kafka/mm2.log 2>&1 &
```

### Expected Result
```bash
tail -f /home/kafka/mm2.log
```
```
INFO Starting MirrorMaker with 1 herder(s)
INFO Successfully joined group with generation
```

**Machine: VM3** — verify that messages arrive correctly on the DR cluster, prefixed with the source cluster name:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 10.128.0.5:9098 \
  --topic primary.vls-stations-nancy \
  --from-beginning
```

### Expected Result
The same bike station JSON messages, now present on the DR cluster, with the topic automatically renamed `primary.vls-stations-nancy` — concretely illustrating the MirrorMaker 2 prefixing mechanism that needs to be anticipated on the consuming application side in case of a real failover.

✅ **Discussion point:** point out that this automatic topic renaming is a **major operational constraint** of MirrorMaker 2 — all consuming applications would need to be reconfigured to read `primary.vls-stations-nancy` instead of `vls-stations-nancy` in case of failover, unless additional identical-renaming configuration is used.

***

## Teaching Reminder — RTO and RPO

Before proceeding to lag measurement and the comparative discussion, make sure these two concepts are well understood by the whole group, as they structure the rest of the workshop.

### RTO (Recovery Time Objective)

**Simple definition:** the maximum acceptable time between the start of an incident and the return to normal service.

**Concrete example already measured in the workshop:** in Workshop 2, the `drp_test.sh` script measured the exact duration between the West region outage and the return to "0 under-replicated partitions". This measured duration **is** the real RTO of the client's current architecture for this type of incident.

### RPO (Recovery Point Objective)

**Simple definition:** the maximum amount of data acceptable to lose, expressed in time — "how far back in time can we go without data loss after an incident?"

**Key difference from RTO to make the group understand:**
- RTO answers: "how long is the system unavailable?"
- RPO answers: "how much data produced right before the incident is potentially lost?"

**Concrete example for their case:** with `acks=all` and `min.insync.replicas=2` on the current single cluster, the RPO is nearly zero in the event of a single broker failure (data is already replicated to 2+ replicas before acknowledgment). But in the case of **asynchronous replication** (MirrorMaker 2 to a second cluster), the RPO becomes the measure of the **replication lag** at the moment of the incident — this is exactly what we will concretely measure below, rather than estimate theoretically.

***

## Step F3bis — Real Measurement of MirrorMaker 2 Replication Lag (Measured RPO)

**Machine: VM3 (10.128.0.5)** — this step fits right after launching MirrorMaker 2 (end of Step F3), while the JCDecaux producer continues to feed `vls-stations-nancy` on the primary cluster.

### Measurement Objective

Replace the generic "a few minutes" RPO estimate for the active-passive architecture with an **actually measured value** on this lab, under the JCDecaux producer's test load.

### Step F3bis.1 — Identify MirrorMaker 2's Consumer Group

MirrorMaker 2 automatically creates an internal consumer group to read the primary cluster. Verify its exact name:

```bash
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --list | grep mirrormaker
```

### Expected Result
```
mm2-mirror-maker-connect-cluster-source
```
(the exact name may vary depending on the version — it always contains `mirrormaker` or the `name` defined in `mm2.properties`)

✅ **If `grep mirrormaker` returns nothing:** list all groups without a filter to identify the correct name manually:
```bash
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --list
```
Look for the group containing `mm2` or the source connector name defined in `mm2.properties`.

### Step F3bis.2 — Continuously Measure the Lag

```bash
watch -n 5 '/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --describe --group mm2-mirror-maker-connect-cluster-source'
```

### Expected Result
```
GROUP                                    TOPIC               PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
mm2-mirror-maker-connect-cluster-source  vls-stations-nancy  0          1248            1251            3
mm2-mirror-maker-connect-cluster-source  vls-stations-nancy  1          892             895             3
```

✅ **What the LAG column represents:** it is the **real RPO in number of messages, measured per partition**. With a producer publishing every 60 seconds (Workshop 1) and a LAG of 3 messages on each partition, this concretely means that in case of a sudden primary cluster outage, **up to 3 publication cycles (~3 minutes) of data might not yet have reached the DR cluster, for each affected partition**.

⚠️ **Caution point:** do not confuse "lag per partition" with "total topic lag" — a LAG of 3 on each of 2 partitions does not mean 6 cycles of delay, but 3 cycles of delay at most, since partitions are replicated in parallel. It's the **maximum LAG observed among the partitions**, not their sum, that gives the real RPO in time.

### Step F3bis.3 — Convert the Lag into a Time-Based RPO

**Machine: VM3** — conversion script, based on the **maximum lag per partition** (not the sum):

```bash
nano /home/kafka/mesure_rpo.sh
```

```bash
#!/bin/bash
GROUP="mm2-mirror-maker-connect-cluster-source"
POLL_INTERVAL_PRODUCER=60  # seconds, see Workshop 1

lag_max=$(/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --describe --group $GROUP 2>/dev/null | \
  awk 'NR>1 {print $NF}' | sort -n | tail -1)

echo "Maximum observed lag (per partition): $lag_max messages"
echo "Estimated RPO: $((lag_max * POLL_INTERVAL_PRODUCER)) seconds of potentially lost data"
```

```bash
chmod +x /home/kafka/mesure_rpo.sh
./mesure_rpo.sh
```

### Expected Result
```
Maximum observed lag (per partition): 3 messages
Estimated RPO: 180 seconds of potentially lost data
```

✅ **Key point to document:** this figure (180 seconds, i.e. 3 minutes) now replaces the generic estimate in the table — it is **real lab data**, measured under actual test conditions, not a theoretical hypothesis. Note this value, it will be used directly in Step F4.

### Step F3bis.4 — Test the Lag Under Increased Load (Optional, Enhanced Realism)

For a more representative test of a heavier production load, temporarily increase the publication frequency of the JCDecaux producer (Workshop 1) from 60s to 5s, then re-measure:

**Machine: Producer VM**
```bash
export POLL_INTERVAL_SECONDS=5  # instead of 60, if the Java producer reads this variable
```

Re-run the measurement from Steps F3bis.2 and F3bis.3 and compare the resulting RPO — this shows that **RPO directly depends on throughput and cross-region network bandwidth**, not just the MirrorMaker 2 configuration itself.

✅ **If this optional test is performed**, note the two RPO values obtained (normal load vs increased load): both will be used in Step F4, question 2, to discuss the effect of real production throughput on RPO.

***

## Step F4 — Guided Facilitation: Decision Grid Completed by the Group

**Format:** whiteboard, Miro, or shared projected document — **the trainer provides no pre-filled values**, they ask the questions and note the group's answers live, building on the actual measurements already obtained (RTO from Workshop 2, RPO from Step F3bis, and RPO under increased load from Step F3bis.4 if performed).

### Questions to Ask the Group, Criterion by Criterion

**1. RTO — Recovery Time**

> "Based on the test we did in Workshop 2, how long did the full rebalancing take after the simulated West region failure?"

Have the group recall the exact value noted in their `drp_test.sh` report (e.g., "187 seconds").

> "For a MirrorMaker 2 active-passive architecture, if the DR cluster is already started and the applications already configured to fail over, how long do you think it would actually take — not for Kafka to be ready, but for the entire application chain to fail over?"

> "For an active-active architecture where both clusters are already receiving traffic continuously, what would the downtime be if one site is lost?"

**2. RPO — Data Loss**

> "We just measured a MirrorMaker 2 RPO of [value measured in Step F3bis.3, e.g. 180 seconds] under our test load at 60 seconds per cycle. With your real production volume, far greater than our lab, do you think this RPO would be higher, lower, or comparable? Why?"

*(If Step F3bis.4 was performed)* > "We also tested under increased load (5-second cycle) and got an RPO of [value]. What does this difference teach you about the factors that actually influence RPO in a MirrorMaker 2 architecture?"

> "On the current single cluster with `acks=all`, what do you think is the real RPO in case of a single broker failure? And in case of a simultaneous failure of an entire region (2-3 brokers)?"

**3. Operational Complexity**

> "Just from the MirrorMaker 2 exercise we just did, what new operational tasks did you identify that you don't have today with your single cluster?"

(Prompt further on: automatic topic renaming, managing two sets of ACLs/certificates, monitoring an additional connector, offset management during application failover)

**4. Infrastructure Cost**

> "You currently pay for 6 brokers across 2 regions. For an active-passive architecture, would you need a DR cluster of identical size, or could it be smaller? For an active-active architecture?"

**5. Migration Effort**

> "On a scale of 1 (almost nothing to do) to 5 (complete overhaul), how would you rate the migration effort from your current architecture to each of the two alternatives? What would make this complex or simple specifically for you?"

**6. Regulatory Compliance (DORA/CSSF)**

> "You stated you are DORA compliant without ever having had an external audit. In your opinion, would an RTO of about 1h, as measured today, be judged acceptable by a DORA auditor for your critical systems? What would be missing to prove it?"

### Grid to Fill In Live (Empty at the Start)

| Criterion | Single cluster (current) | MirrorMaker 2 active-passive | Active-active |
| :-- | :-- | :-- | :-- |
| Measured/estimated RTO | _____ (Workshop 2 value) | _____ (group estimate) | _____ (group estimate) |
| Measured/estimated RPO | _____ (group estimate) | _____ (Step F3bis.3 value, + F3bis.4 value if performed) | _____ (group estimate) |
| Operational complexity | _____ | _____ | _____ |
| Relative infrastructure cost | _____ | _____ | _____ |
| Migration effort (1-5) | _____ | _____ | _____ |
| DORA compliance judged sufficient? | _____ | _____ | _____ |

✅ **Expected result of this facilitation:** this is not a "right or wrong" table — the goal is for the client to **take ownership** of the tradeoff themselves, relying on factual data they measured themselves in the lab (RTO in Workshop 2, RPO in Step F3bis), rather than receiving a ready-made conclusion. The trainer can challenge overly optimistic or pessimistic answers, but should not fill in the table for them.

### Closing the Facilitation

End with an open question that turns the discussion into action:

> "Based on this grid, what would be the concrete next step you would propose to your management: staying on the current architecture while improving the measured RTO, or launching a MirrorMaker 2 pilot on a subset of critical topics?"

Note the group's answer — it is a direct deliverable for the end-of-training report.

***

## Cleanup After the Workshop

**Machine: VM1**
```bash
curl -X DELETE http://10.18.0.5:8083/connectors/jdbc-source-station-metadata
kill %1  # stop the background connect-distributed process
```

**Machine: VM3**

Stop MirrorMaker 2 and the DR broker, both launched in the background with `nohup` in Steps F3:

```bash
pkill -f connect-mirror-maker
pkill -f "broker-dr.properties"
```

✅ **Validation:**
```bash
ps aux | grep -E "mirror-maker|broker-dr" | grep -v grep
```
Should return no active process.

Clean up the DR broker's local data to allow clean reuse in the next session:
```bash
rm -rf /data/kafka/broker-dr/*
```

***

This workshop wraps up the 7 priorities checked in H1 of the questionnaire, turning the client's open question about the robustness of their current architecture into a **quantified comparative deliverable**, directly usable for a future evolution decision.
