# Workshop 6 — Producer/Consumer Tuning and MongoDB/Elasticsearch Integration

## Workshop Objective

Complete the broker-side tuning already done in Workshop 4 with in-depth producer/consumer tuning, and cover the MongoDB CDC and Elasticsearch integrations explicitly requested in H4.

## Client Context to Keep in Mind

| Questionnaire finding | Implication for the workshop |
| :-- | :-- |
| `compression.type`, `linger.ms`, `fetch.min.bytes` managed "by App team", never challenged by Ops | The workshop must produce quantified evidence the Ops team can hand to the App Team |
| MongoDB CDC and Elasticsearch integration explicitly requested in H4 | Cover Debezium MongoDB and Elasticsearch Sink |
| Team = admin/ops, not application developers | "Diagnostic and evidence" angle, not "development" |

## Prerequisites

| Element | Machine |
| :-- | :-- |
| 6-broker/5-controller Kafka cluster in PLAINTEXT | VM1, VM2, VM3 |
| Active Kafka Connect worker (Workshop 5) | VM1 |
| Active JCDecaux producer | Producer VM |

***

## Part A — Producer Tuning: Compression and Batching

### Teaching Objective

Demonstrate through measurement, not theory, the real impact of compression and batching on producer throughput, latency, and stored data volume — to give the Ops team concrete numbers to counter the choices currently left unsupervised to the App Team.

### Step A1 — Baseline Without Compression

**Machine: VM2 (10.118.0.5)**

Objective: establish a neutral reference point, without compression or batching, before varying each parameter one by one.

```bash
/opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic perf-diagnostic-test \
  --num-records 300000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props bootstrap.servers=10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  acks=all compression.type=none linger.ms=0
```

Expected: `300000 records sent, 45123.8 records/sec (44.06 MB/sec), 14.2 ms avg latency`. Note these three figures for the Step E1 summary table.

### Step A2 — Test With `snappy` Compression

**Machine: VM2**

```bash
/opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic perf-diagnostic-test \
  --num-records 300000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props bootstrap.servers=10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  acks=all compression.type=snappy linger.ms=0
```

Expected: `300000 records sent, 61234.5 records/sec (59.80 MB/sec), 10.8 ms avg latency`.

**Field observation:** in the real tests of this workshop, no significant difference was observed between `snappy` and `none`, unlike the reference values above. Series of `ping -c 5` between the three VMs explain this:

| Pair | Observed latency |
| :-- | :-- |
| Local VM (VM2 → VM2) | ~0.03-0.05 ms |
| VM1 → VM3 | ~9.1 ms |
| VM1 → VM2 | ~15.5 ms |
| VM2 → VM3 | ~16.5 ms |

Each VM communicates almost instantly with itself, but any inter-VM communication costs between 9 and 16 ms — consistent with VMs hosted in different availability zones or connected via an overlay/VPN network with fixed routing cost. This structural latency explains why the compression gain (`snappy`/`zstd`) is masked in this lab: with `acks=all`, the dominant time of each write is the network round-trip between replicas (9-16 ms), not the message serialization/compression time itself (on the order of microseconds). Two adjustments help isolate and compensate for this effect: test batching (`linger.ms`, see Step A5) first before concluding on compression, and compare `acks=all` to `acks=1` (Step A2bis) to visualize the impact of the acknowledgment level on accumulated network latency.

### Step A2bis — Compare `acks=all` and `acks=1` (Network Latency Impact)

**Machine: VM2**

```bash
/opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic perf-diagnostic-test \
  --num-records 300000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props bootstrap.servers=10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  acks=1 compression.type=snappy linger.ms=0
```

Expected: significantly higher throughput and markedly reduced average latency compared to test A2 (`acks=all`), since the leader no longer waits for remote replica confirmation.

**Key teaching point:** the performance gap between `acks=all` and `acks=1` measured here directly corresponds to the inter-VM network round-trip cost (9-16 ms) identified above — `acks=all` guarantees durability but its performance cost depends heavily on inter-replica network latency.

### Step A3 — Test With `zstd` Compression (Best Ratio)

**Machine: VM2**

```bash
/opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic perf-diagnostic-test \
  --num-records 300000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props bootstrap.servers=10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  acks=all compression.type=zstd linger.ms=0
```

Expected: `300000 records sent, 58012.1 records/sec (56.65 MB/sec), 12.9 ms avg latency`. `zstd` generally compresses better than `snappy` but uses more producer CPU — the CPU vs bandwidth trade-off the Ops team must be able to argue.

### Step A4 — Verify the Real Disk Impact on Brokers

**Machine: VM1 (10.18.0.5)**

```bash
/opt/kafka/bin/kafka-log-dirs.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --topic-list perf-diagnostic-test \
  --describe | grep '^{' | jq '.brokers[0].logDirs[0].partitions[] | {partition: .partition, size: .size}'
```

⚠️ Common pitfall: `kafka-log-dirs.sh` prints a log line before the JSON; without `grep '^{'`, `jq` fails with `Invalid numeric literal`.

Expected: `{"partition": "perf-diagnostic-test-0", "size": 45678912}`. Repeat after each compression test (A1-A3); `zstd` should produce the most compact log files on disk.

### Step A5 — Test the Batching Effect With `linger.ms`

**Machine: VM2**

```bash
/opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic perf-diagnostic-test \
  --num-records 300000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props bootstrap.servers=10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  acks=all compression.type=snappy linger.ms=50 batch.size=65536
```

Expected: `300000 records sent, 78945.3 records/sec (77.10 MB/sec), 18.4 ms avg latency`. Throughput increases further (more messages batched together), but average latency also rises — the fundamental trade-off: `linger.ms` exchanges latency for throughput.

✅ **Part A objective achieved:** the group now has concrete, reproducible numbers for three producer tuning levers (`compression.type`, `linger.ms`, `batch.size`).

***

## Part B — Consumer Tuning: Fetch and Latency

### Teaching Objective

Show that consumer-side tuning (`fetch.min.bytes`, `fetch.max.wait.ms`) follows the same trade-off logic as the producer side: gaining throughput generally costs higher end-to-end latency.

### Step B1 — Consumer Baseline (Default Parameters)

**Machine: VM3 (10.128.0.5)**

```bash
/opt/kafka/bin/kafka-consumer-perf-test.sh \
  --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  --topic perf-diagnostic-test \
  --messages 300000 \
  --group tuning-test-baseline
```

Expected: `data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec` — e.g. `292.97, 48.83, 300000, 50000.0`.

### Step B2 — Test With Increased `fetch.min.bytes`

**Machine: VM3**

```bash
/opt/kafka/bin/kafka-consumer-perf-test.sh \
  --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  --topic perf-diagnostic-test \
  --messages 300000 \
  --group tuning-test-fetchmin \
  --consumer.config <(echo -e "fetch.min.bytes=100000\nfetch.max.wait.ms=500")
```

Expected: higher throughput (e.g. `73.24 MB.sec, 75000.0 nMsg.sec`), because the broker waits to accumulate at least 100,000 bytes (or up to 500ms) before responding — fewer network requests, more data per request.

### Step B3 — Measure the End-to-End Latency Impact

**Machine: VM3**

```bash
nano /home/kafka/latency_test_consumer.sh
```

```bash
#!/bin/bash
TOPIC="latency-test-topic"
BOOTSTRAP="10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096"

/opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server $BOOTSTRAP \
  --topic $TOPIC --partitions 3 --replication-factor 3 2>/dev/null || true

echo "Test with default fetch.min.bytes (1 byte):"
/opt/kafka/bin/kafka-e2e-latency.sh \
  $BOOTSTRAP $TOPIC 10000 all 1024
```

```bash
chmod +x /home/kafka/latency_test_consumer.sh
./latency_test_consumer.sh
```

⚠️ Tool correction (Kafka 4.x): `kafka.tools.EndToEndLatency` no longer exists via `kafka-run-class.sh` in recent Kafka versions. Use the dedicated `kafka-e2e-latency.sh` script in `/opt/kafka/bin/`.

Expected: `Avg latency: 8.4342 ms`, `Percentiles: 50th = 7, 99th = 22, 99.9th = 45`. This is the metric to use with the App Team for cases where a high `fetch.min.bytes` would be counterproductive.

✅ **Part B objective achieved:** the group measured the same throughput/latency trade-off on the consumer side, with a reproducible method (`EndToEndLatency`).

***

## Part C — MongoDB Integration via Debezium (CDC)

### Teaching Objective

Address the client's explicit need (H4) by demonstrating a complete CDC (Change Data Capture) integration with MongoDB, and clarify the fundamental difference between Debezium's real-time CDC and the periodic polling already seen with the JDBC connector in Workshop 5.

### Step C1 — Install MongoDB

**Machine: VM1 (10.18.0.5)**

```bash
sudo apt install -y gnupg curl
curl -fsSL https://pgp.mongodb.com/server-7.0.asc | sudo gpg --dearmor -o /usr/share/keyrings/mongodb-server-7.0.gpg
echo "deb [signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg] http://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list
sudo apt update && sudo apt install -y mongodb-org
```

### Step C2 — Enable the Replica Set (Required for Debezium)

**Machine: VM1**

```bash
sudo nano /etc/mongod.conf
```

Add:

```yaml
replication:
  replSetName: "rs0"
```

```bash
sudo systemctl restart mongod
mongosh --eval "rs.initiate()"
```

Expected: `{ "ok": 1 }`. Debezium's change streams mechanism requires a replica set — even a single-member one in a lab, unlike a classic standalone MongoDB instance.

### Step C3 — Create a Test Collection

**Machine: VM1**

```bash
mongosh stations_mongo --eval '
db.station_events.insertMany([
  { stationNumber: 12, event: "maintenance_start", city: "nancy", ts: new Date() },
  { stationNumber: 45, event: "maintenance_end", city: "nancy", ts: new Date() }
]);
'
```

### Step C4 — Download and Deploy the Debezium MongoDB Connector

**Machine: VM1**

```bash
cd /opt/kafka/connect-plugins
wget https://repo1.maven.org/maven2/io/debezium/debezium-connector-mongodb/2.7.0.Final/debezium-connector-mongodb-2.7.0.Final-plugin.tar.gz
tar -xzf debezium-connector-mongodb-2.7.0.Final-plugin.tar.gz
```

Restart the Connect worker to load the new plugin:

```bash
kill %1 2>/dev/null || true
nohup /opt/kafka/bin/connect-distributed.sh \
  /opt/kafka/config/connect-distributed.properties \
  > /home/kafka/connect-data/connect.log 2>&1 &
sleep 15
```

Verify: `curl -s http://10.18.0.5:8083/connector-plugins | jq '.[].class' | grep -i mongo` → `"io.debezium.connector.mongodb.MongoDbConnector"`.

### Step C5 — Deploy the CDC Connector

**Machine: VM1**

```bash
nano /home/kafka/connect-data/mongodb-cdc-config.json
```

```json
{
  "name": "mongodb-cdc-station-events",
  "config": {
    "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
    "mongodb.connection.string": "mongodb://localhost:27017/?replicaSet=rs0",
    "topic.prefix": "cdc-mongo",
    "database.include.list": "stations_mongo",
    "collection.include.list": "stations_mongo.station_events",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "connect-dlq-mongodb",
    "errors.deadletterqueue.topic.replication.factor": 3
  }
}
```

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @/home/kafka/connect-data/mongodb-cdc-config.json \
  http://10.18.0.5:8083/connectors
```

Verify: `curl -s http://10.18.0.5:8083/connectors/mongodb-cdc-station-events/status | jq .` must show `"state": "RUNNING"`.

### Step C6 — Test CDC in Real Time

**Machine: VM2 (10.118.0.5)** for listening, **VM1** for insertion.

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 10.118.0.5:9094 \
  --topic cdc-mongo.stations_mongo.station_events \
  --from-beginning
```

**Machine: VM1** — in a second terminal:

```bash
mongosh stations_mongo --eval '
db.station_events.insertOne({ stationNumber: 7, event: "bike_removed", city: "toulouse", ts: new Date() });
'
```

Expected: a new message appears immediately on VM2. Unlike Workshop 5's JDBC connector which polls the database (`poll.interval.ms`), Debezium captures changes in real time via MongoDB change streams — the fundamental difference between CDC and classic polling.

✅ **Part C objective achieved:** the group installed, configured, and validated a complete CDC integration with MongoDB, understanding the technical prerequisite (replica set), architecture (plugin + connector + DLQ), and added value over JDBC polling.

***

## Part D — Elasticsearch Integration

### Teaching Objective

Demonstrate the second integration explicitly requested by the client (H4) by indexing JCDecaux stream data into Elasticsearch, illustrating a concrete business use case (searching for stressed stations) not natively covered by Grafana/Mimir.

### Step D1 — Install Elasticsearch

**Machine: VM1**

```bash
wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo gpg --dearmor -o /usr/share/keyrings/elasticsearch-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/elasticsearch-keyring.gpg] https://artifacts.elastic.co/packages/8.x/apt stable main" | sudo tee /etc/apt/sources.list.d/elastic-8.x.list
sudo apt update && sudo apt install -y elasticsearch
```

Configure in simple mode (no security, for the lab):

```bash
sudo nano /etc/elasticsearch/jvm.options.d/heap.options
-Xms1g
-Xmx1g
sudo sed -i 's/xpack.security.enabled: true/xpack.security.enabled: false/' /etc/elasticsearch/elasticsearch.yml
sudo systemctl restart elasticsearch
```

Verify: `curl -s http://localhost:9200` → `{"name":"vm1","cluster_name":"elasticsearch","version":{"number":"8.x.x"}}`.

### Step D2 — Download the Elasticsearch Sink Connector

**Machine: VM1**

```bash
cd /opt/kafka/connect-plugins
wget https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-elasticsearch/versions/15.1.3/confluentinc-kafka-connect-elasticsearch-15.1.3.zip
unzip confluentinc-kafka-connect-elasticsearch-14.0.11.zip
kill %1 2>/dev/null || true
nohup /opt/kafka/bin/connect-distributed.sh \
  /opt/kafka/config/connect-distributed.properties \
  > /home/kafka/connect-data/connect.log 2>&1 &
sleep 15
```

Verify: `curl -s http://10.18.0.5:8083/connector-plugins | jq '.[].class' | grep -i elastic` → `"io.confluent.connect.elasticsearch.ElasticsearchSinkConnector"`.

### Step D3 — Deploy the Elasticsearch Sink for JCDecaux Data

**Machine: VM1**

```bash
nano /home/kafka/connect-data/es-sink-config.json
```

```json
{
  "name": "es-sink-vls-stations",
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "connection.url": "http://localhost:9200",
    "topics": "vls-stations-nancy",
    "type.name": "_doc",
    "key.ignore": "false",
    "schema.ignore": "true",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "connect-dlq-elasticsearch",
    "errors.deadletterqueue.topic.replication.factor": 3
  }
}
```

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @/home/kafka/connect-data/es-sink-config.json \
  http://10.18.0.5:8083/connectors
```

Verify: `curl -s http://10.18.0.5:8083/connectors/es-sink-vls-stations/status | jq .` must show `"state": "RUNNING"`.

### Step D4 — Verify Indexing in Elasticsearch

**Machine: VM1**

```bash
curl -s "http://localhost:9200/vls-stations-nancy/_search?pretty&size=3"
```

Expected: documents indexed with the Kafka key as Elasticsearch `_id` (`key.ignore=false`) — a station update overwrites the existing document instead of creating a new one, avoiding index growth explosion over time.

### Step D5 — Realistic Search Query

**Machine: VM1**

```bash
curl -s -X GET "http://localhost:9200/vls-stations-nancy/_search?pretty" \
  -H 'Content-Type: application/json' \
  -d '{
    "query": {
      "range": { "available_bikes": { "lt": 3 } }
    }
  }'
```

Expected: list of stations with fewer than 3 available bikes — a concrete business use case (alerting, real-time dashboard of stressed stations), complementary to what Grafana/Mimir allows on the infrastructure side.

✅ **Part D objective achieved:** the group set up a second complete sink integration, with a clear understanding of the indexing key strategy and a concrete business value example.

***

## Part E — Summary and Recommendations for the App Team

### Step E1 — Summary Table to Complete

| Parameter | Baseline | Tested value | Observed gain | Trade-off |
| :-- | :-- | :-- | :-- | :-- |
| `compression.type` | none | snappy | ___ % throughput | +producer CPU |
| `compression.type` | none | zstd | ___ % throughput, ___ % storage | ++producer CPU |
| `linger.ms` | 0 | 50 | ___ % throughput | +individual latency |
| `fetch.min.bytes` | 1 | 100000 | ___ % consumer throughput | +latency if low traffic |

### Step E2 — Questions to Ask the Group for Discussion With the App Team

> "Based on these numbers, which of your production topics would be good candidates for enabling `compression.type=snappy` without risk (high-volume, low latency sensitivity)?"

> "Are there critical topics where `linger.ms` should instead stay at 0, because every millisecond of latency matters (trading orders, real-time events)?"

> "The MongoDB CDC connector we just tested captures changes in real time — is this relevant to replace some current JDBC polling identified in G3?"

✅ **Part E objective achieved:** the group leaves with a quantified summary and structured questions, ready to present to the App Team.

***

## Part F — Monitoring and Troubleshooting (SQL Server, MongoDB, Elasticsearch)

### Teaching Objective

Address point by point the explicit H4 client request about integration best practices, connector architecture, monitoring, and troubleshooting for SQL Server, MongoDB and Elasticsearch.

### Step F1 — Connector Architecture: What to Monitor and Why

| Level | What it represents | Impact if failing |
| :-- | :-- | :-- |
| Worker | JVM process hosting one or more connectors | Total outage of all hosted connectors |
| Connector | Logical definition of the integration (source or sink) | Connector goes to `FAILED`, no task runs |
| Task | Parallel execution unit of a connector | A single failing task can be restarted without stopping others |
| External system (SQL Server / MongoDB / Elasticsearch) | Target database or search engine | Connector stays `RUNNING` but fails on every access attempt |
| DLQ | Fallback topic for failed records | Silent accumulation if unmonitored |

✅ A `RUNNING` connector does not guarantee no incident — a task can fail in a loop without the global connector going `FAILED`. Monitoring must always go down to task level.

### Step F2 — Connector Status Monitoring (Common to All 3 Technologies)

**Machine: VM1**

```bash
curl -s http://10.18.0.5:8083/connectors/mongodb-cdc-station-events/status | jq .
curl -s http://10.18.0.5:8083/connectors/es-sink-vls-stations/status | jq .
curl -s http://10.18.0.5:8083/connectors | jq .
```

✅ A `"state": "FAILED"` at task level must trigger immediate investigation.

### Step F3 — Dead Letter Queue Volume Monitoring

**Machine: VM2**

```bash
/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 --topic connect-dlq-mongodb --time -1
/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094 --topic connect-dlq-elasticsearch --time -1
```

Expected: `connect-dlq-mongodb:0:12`, `connect-dlq-elasticsearch:0:3`. Run at regular intervals; an abnormal positive delta between two readings is the alert signal, not the absolute value.

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 10.118.0.5:9094 \
  --topic connect-dlq-elasticsearch --from-beginning \
  --property print.headers=true
```

The headers `__connect.errors.exception.class.name` and `__connect.errors.exception.message` give the exact error detail without checking worker logs.

### Step F4 — SQL Server Troubleshooting (JDBC Connector)

**Machine: VM1**

```bash
curl -s http://10.18.0.5:8083/connectors/jdbc-source-station-metadata/status | jq .
curl -s http://10.18.0.5:8083/connector-plugins | jq '.[].class' | grep -i jdbc
tail -n 100 /home/kafka/connect-data/connect.log | grep -i -A 5 "SQLException\|Connection refused"
```

| Symptom | Likely cause | Corrective action |
| :-- | :-- | :-- |
| Connector `FAILED` at startup | JDBC driver missing from `plugin.path`, or invalid credentials | Check `connection.url`, `connection.user`, `connection.password`, and driver `.jar` presence |
| No new data despite inserts | `timestamp.column.name` not updated by the source app | Verify a trigger or app mechanism updates `updated_at` on every write |
| Duplicates after connector restart | Lost Connect offsets, or `incrementing` mode alone misconfigured | Check internal offsets topic, combine `timestamp+incrementing` rather than `timestamp` alone |
| Truncation error on `varchar`/`decimal` types | Incompatible type mapping between SQL Server and Kafka converter | Adjust `numeric.mapping=best_fit` and check the converter |

✅ Never rely solely on `mode=timestamp` in production — always combine with `incrementing.column.name`.

### Step F5 — MongoDB Troubleshooting (Debezium Connector)

**Machine: VM1**

```bash
mongosh --eval "rs.status()" | grep -i "myState\|set:"
curl -s http://10.18.0.5:8083/connectors/mongodb-cdc-station-events/status | jq '.tasks[].trace' 2>/dev/null
mongosh --eval "db.getSiblingDB('local').oplog.rs.stats().maxSize"
```

| Symptom | Likely cause | Corrective action |
| :-- | :-- | :-- |
| Connector `FAILED`: `replica set required` | MongoDB deployed standalone | Convert to replica set, restart connector |
| Lost changes after prolonged connector outage | Oplog too small, retention window exceeded | Increase oplog size, relaunch full snapshot if needed |
| Abnormally long initial snapshot | Large collection captured without filter | Refine `collection.include.list`, or use `snapshot.mode=incremental` |
| Duplicate messages after connector restart | Debezium's at-least-once semantics | Design idempotent consumers using MongoDB `_id` as dedup key |

✅ Proactively monitor oplog size relative to traffic peak frequency.

### Step F6 — Elasticsearch Troubleshooting (Sink Connector)

**Machine: VM1**

```bash
curl -s http://localhost:9200/_cluster/health?pretty
curl -s http://localhost:9200/_cat/indices/vls-stations-nancy?v
sudo tail -n 100 /var/log/elasticsearch/elasticsearch.log | grep -i -A 5 "circuit_breaking_exception\|mapper_parsing_exception"
```

| Symptom | Likely cause | Corrective action |
| :-- | :-- | :-- |
| `circuit_breaking_exception` | Too much data loaded in memory | Reduce `indices.breaker.total.limit`, or increase `-Xmx` heap |
| `mapper_parsing_exception` in DLQ | Document incompatible with existing index mapping | Define an explicit index template rather than `schema.ignore=true` |
| Duplicated documents instead of updates | `key.ignore=true` used by mistake | Set `key.ignore=false` so the Kafka key serves as `_id` |
| `_cluster/health` returns `yellow`/`red` | Unreplicated shards, unavailable node, or saturated disk | Check `_cat/nodes`, disk space, replica count |
| High indexing latency, sink lagging | Misconfigured `batch.size`/`flush.timeout.ms`, or saturated JVM heap | Adjust sink params, check `heap_used_percent` |

### Step F7 — Consolidated Monitoring Grid for Grafana/Mimir

| Technology | Metric to monitor | Command/Source | Suggested alert threshold |
| :-- | :-- | :-- | :-- |
| All connectors | Task status (`RUNNING`/`FAILED`) | `/connectors/{name}/status` | Immediate alert if `FAILED` |
| All connectors | DLQ volume (delta between 2 readings) | `GetOffsetShell --topic connect-dlq-*` | Abnormal rise over 5 min |
| SQL Server (JDBC) | Data freshness (last captured `updated_at`) | Connector logs + control SQL query | Delay > 2x `poll.interval.ms` |
| MongoDB (Debezium) | Remaining oplog size | `rs.printReplicationInfo()` | Remaining window < 24h |
| Elasticsearch | `_cluster/health.status` | `/_cluster/health` | `yellow` or `red` |
| Elasticsearch | `heap_used_percent` | `/_nodes/stats/jvm` | > 85% |

✅ **Part F objective achieved:** the group has full documented coverage of the four areas explicitly requested in H4 — integration best practices, connector architecture, monitoring, and troubleshooting — for SQL Server, MongoDB and Elasticsearch.

***

## Cleanup After the Workshop

**Machine: VM1**

```bash
curl -X DELETE http://10.18.0.5:8083/connectors/mongodb-cdc-station-events
curl -X DELETE http://10.18.0.5:8083/connectors/es-sink-vls-stations
sudo systemctl stop mongod elasticsearch
/opt/kafka/bin/kafka-topics.sh --delete --bootstrap-server 10.18.0.5:9092 --topic latency-test-topic
```

This workshop finalizes coverage of the 9 priority topics of the H1/H4 questionnaire, turning never-challenged parameters ("by App team") into quantified, argued recommendations, demonstrating the two client-requested integrations, and providing a full best-practices, monitoring, and troubleshooting grid for SQL Server, MongoDB and Elasticsearch.
