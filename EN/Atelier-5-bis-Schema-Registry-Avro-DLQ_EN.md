# Workshop 5 bis — Schema Registry, Avro Schema Evolution and Dead Letter Queues

## Workshop Objective

Address the client's explicit need on Schema Registry / Avro, currently rated at a low level in the questionnaire, by covering Schema Registry architecture, compatibility strategies, safe schema evolution, serialization/deserialization errors, operating a Dead Letter Queue, and monitoring schema-related incidents. This workshop logically fits after Workshop 5 dedicated to advanced Kafka Connect and prepares Workshop 6 by leaving a clean environment after compatibility and invalid message tests.

## Client Context to Keep in Mind

| Questionnaire finding | Implication for the workshop |
| :-- | :-- |
| Current level "Schema Registry / Avro (compatibility, schema evolution)" = 2/5 | The workshop must be very practical and avoid unnecessary jargon |
| Explicit need: Schema Registry architecture, Backward/Forward/Full compatibility, safe evolution, (de)serialization errors, DLQ, monitoring | Parts A to E must cover exactly these six expectations |
| Daily tools: AKHQ, Kafka CLI, Grafana/Mimir/Prometheus, systemctl, journalctl, SSH | All diagnostics must remain operable with these tools |
| Workshop 5 already centered on Kafka Connect and its DLQ | Here the DLQ is handled on the Avro data side and consumer side, not just the connector side |
| Workshop 6 starts on an already cleaned and stable cluster | End of workshop = deletion of topics and stopping of temporary processes |

***

## Positioning in the Program

This workshop is deliberately named **Workshop 5 bis** to fit between Workshop 5 "Advanced Kafka Connect and Architecture Comparison" and Workshop 6 "Producer/Consumer Tuning and MongoDB/Elasticsearch Integration". The pedagogical link is twofold: on one hand, it complements Workshop 5 by extending the DLQ concept to the Avro and Schema Registry world; on the other, it prepares Workshop 6 by requiring a rigorous cleanup of test topics, test schemas, and temporary processes so that the tuning benchmarks start from a clean environment.

## Prerequisites

| Element | Machine |
| :-- | :-- |
| Operational 6-broker/5-controller Kafka cluster | VM1, VM2, VM3 |
| Kafka Connect already known from Workshop 5 | VM1 |
| Java 17+ installed | VM1, VM2 |
| Python 3 available for simple test scripts | VM1, VM2 |
| SSH access and `sudo` rights | VM1, VM2, VM3 |
| Internet allowed or pre-downloaded artifacts | VM1 |

Quick check before starting:

```bash
/opt/kafka/bin/kafka-metadata-quorum.sh   --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096   describe --status
```

The quorum must be healthy before adding Schema Registry and test topics.

***

## Part A — Schema Registry Architecture and Administration

### Teaching Objective

Understand where Schema Registry sits in the client's Kafka architecture, what it actually stores, how it interacts with producers/consumers, and what an administrator must check before using it in production.

### Step A1 — Download and Prepare Schema Registry

**Machine: VM1 (10.18.0.5)**

```bash
mkdir -p /opt/schema-registry /home/kafka/schema-lab
cd /opt/schema-registry
wget https://packages.confluent.io/archive/7.7/confluent-community-7.7.0.tar.gz
sudo tar -xzf confluent-community-7.7.0.tar.gz --strip-components=1
```

### Expected Result

The directory should contain `bin/`, `share/`, `etc/schema-registry/` and the libraries needed to start the service.

### Step A2 — Configure Schema Registry for the Lab Cluster

**Machine: VM1**

```bash
nano /opt/schema-registry/etc/schema-registry/schema-registry.properties
```

```properties
listeners=http://0.0.0.0:8081
host.name=10.18.0.5
kafkastore.bootstrap.servers=PLAINTEXT://10.18.0.5:9092,PLAINTEXT://10.118.0.5:9094,PLAINTEXT://10.128.0.5:9096
schema.registry.group.id=schema-registry-lab
log4j.root.logger=INFO, stdout
```
In schema-registry.properties, the default lines such as:

```bash
kafkastore.topic=_schemas
debug=false
metadata.encoder.secret=REPLACE_ME_WITH_HIGH_ENTROPY_STRING
resource.extension.class=io.confluent.dekregistry.DekRegistryResourceExtension
```

are different parameters from the ones you're adding (listeners, host.name, kafkastore.bootstrap.servers, etc.). So there's no conflict — leave them as is.

The only line to replace (not just add) is:

```bash
kafkastore.bootstrap.servers=PLAINTEXT://localhost:9092
```

which must become:

```bash
kafkastore.bootstrap.servers=PLAINTEXT://10.18.0.5:9092,PLAINTEXT://10.118.0.5:9094,PLAINTEXT://10.128.0.5:9096
```
> **Teaching point:** Schema Registry does not write schemas to persistent local files; it stores its metadata in Kafka, which explains why the service remains a Kafka-native component to supervise like the other platform services.

### Step A3 — Start the Service and Verify Its State

**Machine: VM1**

```bash
nohup /opt/schema-registry/bin/schema-registry-start   /opt/schema-registry/etc/schema-registry/schema-registry.properties   > /home/kafka/schema-lab/schema-registry.log 2>&1 &

sleep 20
curl -s http://10.18.0.5:8081/subjects
```

### Expected Result

```json
[]
```

✅ **Validation:** an empty list is normal on first startup; it confirms that the API responds and that no Avro subject has been registered yet.

### Step A4 — Verify the Minimal Administration Infrastructure

**Machine: VM1**

```bash
curl -s http://10.18.0.5:8081/config
curl -s http://10.18.0.5:8081/mode
```

### Expected Result

The API must return a default global configuration and the service's operating mode, giving the group the first two administration checks to know before discussing compatibility or application incidents.

✅ **Objective achieved:** by the end of this part, the group knows where Schema Registry is located, how it starts, and what checks to perform before discussing compatibility or application incidents.

***

## Part B — Backward, Forward and Full Compatibility

### Teaching Objective

Before manipulating Schema Registry, it is essential to understand **why** these modes exist: they protect the data chain against schema changes that would silently break applications already in production. A compatibility mode is not just a technical parameter — it is a **deployment policy** that determines the order in which producers and consumers can be updated without service interruption.

Concretely:

- **BACKWARD** (default mode) guarantees that a **new consumer** can read data written with an **old schema**. Used when updating consumers before producers.
- **FORWARD** guarantees the opposite: an **old consumer** can read data written with a **new schema**. Used when updating producers before consumers.
- **FULL** requires both guarantees simultaneously — the strictest mode, useful when the deployment order between producers and consumers cannot be guaranteed.

The group should keep this simple question in mind to choose a mode in production: "Who will be updated first, my producers or my consumers?"

### Step B1 — Create a Dedicated Topic and Set the Global Compatibility

**Machine: VM1**

```bash
/opt/kafka/bin/kafka-topics.sh --create   --bootstrap-server 10.18.0.5:9092   --topic avro-orders   --partitions 3   --replication-factor 3

curl -s -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json"   --data '{"compatibility":"BACKWARD"}'   http://10.18.0.5:8081/config
```

### Expected Result

The `avro-orders` topic must be created and the global compatibility must return `{"compatibility":"BACKWARD"}`.

### Step B2 — Register Schema Version 1

**Machine: VM1**

```bash
cat > /home/kafka/schema-lab/order-v1.avsc <<'JSON'
{
  "type": "record",
  "name": "OrderEvent",
  "namespace": "lab.avro",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "customer_id", "type": "string"},
    {"name": "amount", "type": "double"}
  ]
}
JSON

jq -Rs '{schema: .}' /home/kafka/schema-lab/order-v1.avsc > /home/kafka/schema-lab/order-v1.json

curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json"   --data @/home/kafka/schema-lab/order-v1.json   http://10.18.0.5:8081/subjects/avro-orders-value/versions
```

### Expected Result

```json
{"id":1}
```

### Step B3 — Test a BACKWARD-Compatible Evolution

**Machine: VM1**
Schema v2 adds the `currency` field with a default value of `"EUR"`. This is the safest case in schema evolution: adding an optional field never breaks anything, for either old or new consumers.
```bash
cat > /home/kafka/schema-lab/order-v2-backward.avsc <<'JSON'
{
  "type": "record",
  "name": "OrderEvent",
  "namespace": "lab.avro",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "customer_id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "currency", "type": "string", "default": "EUR"}
  ]
}
JSON

jq -Rs '{schema: .}' /home/kafka/schema-lab/order-v2-backward.avsc > /home/kafka/schema-lab/order-v2-backward.json

curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json"   --data @/home/kafka/schema-lab/order-v2-backward.json   http://10.18.0.5:8081/subjects/avro-orders-value/versions
```

**Expected result:** this schema is accepted regardless of the active mode (BACKWARD, FORWARD or FULL), because adding a field with a default value respects both reading directions at once. This test serves as a "safe reference case" before tackling the more subtle cases that follow.

### Step B4 — Trigger a Visible Incompatibility

**Machine: VM1**
Schema v3 removes `customer_id` without a default value and changes the type of `amount` (`double` → `string`).
```bash
cat > /home/kafka/schema-lab/order-v3-incompatible.avsc <<'JSON'
{
  "type": "record",
  "name": "OrderEvent",
  "namespace": "lab.avro",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "amount", "type": "string"}
  ]
}
JSON

jq -Rs '{schema: .}' /home/kafka/schema-lab/order-v3-incompatible.avsc > /home/kafka/schema-lab/order-v3-incompatible.json

curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json"   --data @/home/kafka/schema-lab/order-v3-incompatible.json   http://10.18.0.5:8081/subjects/avro-orders-value/versions
```

**Expected result:** this schema is **rejected in all modes**, because a type change and a field removal without a default break reading in both directions simultaneously. This test shows that a compatibility mode, whichever it is, never protects against a poorly designed evolution — it only arbitrates between two compatibility directions, not a substitute for good schema design.

## Step B5 — Observe a Real Divergence Between BACKWARD and FORWARD (Corrected Version)

### Teaching Objective

Steps B3 and B4 give the same result regardless of the active mode, which could give the impression that BACKWARD and FORWARD are interchangeable. This step corrects that impression by isolating a schema change that breaks **only one reading direction at a time**, so the group can observe a truly different result depending on the configured mode.

### Test 1 — Removing a Required Field (Validates BACKWARD Behavior)

**Machine: VM1**

```bash
cat > /home/kafka/schema-lab/order-v4-drop-field.avsc <<'JSON'
{
  "type": "record",
  "name": "OrderEvent",
  "namespace": "lab.avro",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "currency", "type": "string", "default": "EUR"}
  ]
}
JSON

jq -Rs '{schema: .}' /home/kafka/schema-lab/order-v4-drop-field.avsc > /home/kafka/schema-lab/order-v4-drop-field.json

curl -s -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility":"BACKWARD"}' \
  http://10.18.0.5:8081/config/avro-orders-value

curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @/home/kafka/schema-lab/order-v4-drop-field.json \
  http://10.18.0.5:8081/subjects/avro-orders-value/versions
```

**Expected result:** registration is **accepted** (`{"id":3}` observed in practice). The new schema no longer requires `customer_id`, so it has no difficulty reading old data that contains this extra field — it simply ignores it. This test confirms that removing a field is almost always safe in BACKWARD.

### Test 2 — Adding a Required Field Without a Default Value (Reveals the Real Divergence)

**Machine: VM1**

```bash
curl -s -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility":"BACKWARD"}' \
  http://10.18.0.5:8081/config/avro-orders-value

cat > /home/kafka/schema-lab/order-v5-required-no-default.avsc <<'JSON'
{
  "type": "record",
  "name": "OrderEvent",
  "namespace": "lab.avro",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "customer_id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "currency", "type": "string", "default": "EUR"},
    {"name": "region", "type": "string"}
  ]
}
JSON

jq -Rs '{schema: .}' /home/kafka/schema-lab/order-v5-required-no-default.avsc > /home/kafka/schema-lab/order-v5-required-no-default.json

curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @/home/kafka/schema-lab/order-v5-required-no-default.json \
  http://10.18.0.5:8081/subjects/avro-orders-value/versions
```

**Expected result (BACKWARD):** registration must be **rejected**. The new schema requires `region`, but the old data never had this field, and since it has no default value, the new reader cannot compensate for this absence.

Then switch to FORWARD and retry the exact same schema:

```bash
curl -s -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility":"FORWARD"}' \
  http://10.18.0.5:8081/config/avro-orders-value

curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @/home/kafka/schema-lab/order-v5-required-no-default.json \
  http://10.18.0.5:8081/subjects/avro-orders-value/versions
```

**Expected result (FORWARD):** registration must be **accepted**. An old reader does not know the `region` field; in FORWARD, we only check that the old schema can read the new data, and since an Avro reader inherently ignores fields it doesn't know, the presence of `region` on the producer side does not block reading on the old consumer side.

### What the Group Should Remember from This Contrast

| Test | BACKWARD | FORWARD | Explanation |
| :-- | :-- | :-- | :-- |
| Removing `customer_id` (Test 1) | Accepted | Not tested here | The new reader ignores a field that no longer exists |
| Adding required `region` without default (Test 2) | Rejected | Accepted | The new reader cannot compensate for a missing field without a default; the old reader simply ignores an extra field |

Test 2 should serve as the reference demonstration in the workshop, as it is the only one of the two that produces a **different** result depending on the active mode. The group should leave with this simple operational rule: adding a required field without a default is the riskiest change in BACKWARD, while removing a field is generally safe in this same mode.

✅ **Objective achieved:** the group now observes a concrete case where BACKWARD and FORWARD produce opposite results on the same schema, anchoring the distinction between the two modes in experience rather than theory alone.

***

## Part C — Safe Schema Evolution and Day-to-Day Operations

### Teaching Objective

Turn compatibility theory into an operational method: how to prepare a new version, validate it, deploy it, and verify it doesn't break existing consumers.

### Step C1 — Define a Validation Procedure Before Deployment

**Machine: VM1**

```bash
# the mode must be set back to BACKWARD before Step C1, otherwise the compatibility test (is_compatible) might return false since it evaluates against the mode currently active on the subject.

curl -s -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility":"BACKWARD"}' \
  http://10.18.0.5:8081/config/avro-orders-value

curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json"   --data @/home/kafka/schema-lab/order-v2-backward.json   http://10.18.0.5:8081/compatibility/subjects/avro-orders-value/versions/latest
```

### Expected Result

```json
{"is_compatible":true}
```

✅ **Key point:** this check must become a standard pre-check before any application change, in the same way as a `--dry-run` on offsets or a certificate validation before rotation.

## Step C2 — Produce with Schema v1, Then Consume with Schema v2

### Teaching Objective

Unlike the initial version, this sequence deliberately separates the schema used by the **producer** (v1, without `currency`) from the one used by the **consumer** (v2, with `currency` and its default value). This difference between writer schema and reader schema is what actually activates the Avro default value mechanism — a mechanism that manual console entry cannot demonstrate otherwise.

### Sub-step C2.1 — Produce a Message with Schema v1 (Without `currency`)

**both machines VM1 and VM2**
```bash
sudo ufw allow 8081/tcp
sudo ufw status
# verify connectivity from VM1 and VM2
curl -v --max-time 5 http://10.18.0.5:8081/subjects
```
**Machine: VM1**

```bash
chown kafka:kafka /home/kafka/shema-
/opt/schema-registry/bin/kafka-avro-console-producer \
  --topic avro-orders \
  --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  --property schema.registry.url=http://10.18.0.5:8081 \
  --property value.schema="$(tr -d '\n' < /home/kafka/schema-lab/order-v1.avsc)"
```

Then enter this message, conforming to schema v1 (so without `currency`):

```json
{"order_id":"A-2001","customer_id":"C-99","amount":75.0}
```

Then exit with `Ctrl+D`.

**Expected result:** the message is accepted and serialized with schema v1 (schema id returned by Schema Registry for v1, different from v2's), as it matches this schema exactly with no missing field.

### Sub-step C2.2 — Produce a Second Message with Schema v2 (With Explicit `currency`, for Comparison)

Still on **VM1**:

```bash
/opt/schema-registry/bin/kafka-avro-console-producer \
  --topic avro-orders \
  --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  --property schema.registry.url=http://10.18.0.5:8081 \
  --property value.schema="$(tr -d '\n' < /home/kafka/schema-lab/order-v2-backward.avsc)"
```

Enter:

```json
{"order_id":"A-2002","customer_id":"C-88","amount":30.0,"currency":"USD"}
```

Then `Ctrl+D`.

**Expected result:** this message is serialized with schema v2 (different id), explicitly including `currency=USD`.

### Step C3 — Consume Both Messages with Schema v2

**Machine: VM2**
Don't forget to create /opt/schema-registry
```bash
mkdir -p /opt/schema-registry /home/kafka/schema-lab
cd /opt/schema-registry
wget https://packages.confluent.io/archive/7.7/confluent-community-7.7.0.tar.gz
sudo tar -xzf confluent-community-7.7.0.tar.gz --strip-components=1

sudo mkdir -p /opt/schema-registry/logs
sudo chown -R kafka:kafka /opt/schema-registry/logs
```
```bash
/opt/schema-registry/bin/kafka-avro-console-consumer \
  --topic avro-orders \
  --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  --from-beginning \
  --property schema.registry.url=http://10.18.0.5:8081 \
  --max-messages 4
```

**Expected result:**

```json
{"order_id":"A-1001","customer_id":"C-12","amount":49.9,"currency":"EUR"}
{"order_id":"A-1002","customer_id":"C-77","amount":12.5,"currency":"EUR"}
{"order_id":"A-2001","customer_id":"C-99","amount":75.0,"currency":"EUR"}
{"order_id":"A-2002","customer_id":"C-88","amount":30.0,"currency":"USD"}
```

The key point to observe is the `A-2001` message: it was **written with schema v1** (never mentioning `currency`), but the consumer uses schema v2 to deserialize, so Avro **automatically applies the default value `"EUR"`** at read time, filling in the field missing from the writer schema.

This precise line is what concretely demonstrates the BACKWARD compatibility mechanism: the new schema (v2) knows how to read data written with the old schema (v1), intelligently filling the gap thanks to the default value declared in the most recent schema.

### Teaching Note to Add to the Workshop

> **💡 What to understand:** in Avro, a field's default value is **not** used by the producer (who must always explicitly provide all fields it knows), but rather by the **reader**, when it encounters data written with an earlier schema that did not yet know about this field. This is exactly what message `A-2001` demonstrates: written without `currency`, read with `currency=EUR` thanks to Avro schema resolution between the writer schema (v1) and the reader schema (v2).

### Step C4 — Translate This into an Operating Procedure

The group then formalizes the recommended sequence: design the new schema, test its compatibility, register the version, deploy the change-tolerant consumers first if necessary, then the producers, and finally monitor metrics and logs over a short window after going to production.

✅ **Objective achieved:** schema evolution is no longer seen as a simple Avro file modification, but as a controlled change operation close to a production runbook.

***

## Part D — Serialization, Deserialization Errors and Dead Letter Queue

### Teaching Objective

Deliberately trigger realistic errors to show what fails on the producer side and the consumer side, then illustrate an operational DLQ strategy that complements the DLQ already seen in Workshop 5 for Kafka Connect.

### Step D1 — Trigger a Serialization Error on the Producer Side

**Machine: VM1**

Relaunch the Avro producer and send an invalid message:
```bash
/opt/schema-registry/bin/kafka-avro-console-producer \
  --topic avro-orders \
  --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  --property schema.registry.url=http://10.18.0.5:8081 \
  --property value.schema="$(tr -d '\n' < /home/kafka/schema-lab/order-v2-backward.avsc)"
```

```json
{"order_id":"A-1003","customer_id":"C-44","amount":"INVALID_AMOUNT"}
```

### Expected Result

The producer must refuse the message because `amount` expects a `double`, not a string; this illustrates the first safety net, even before the data enters Kafka.

### Step D2 — Simulate a Deserialization Error on the Python Consumer Side

**Machine: VM2**

```bash
python3 -m venv /home/kafka/schema-lab/venv
source /home/kafka/schema-lab/venv/bin/activate
pip install confluent-kafka[avro] fastavro
```

Then create a demonstration consumer:

```bash
cat > /home/kafka/schema-lab/consumer_with_dlq.py <<'PY'
from confluent_kafka import Producer
from confluent_kafka.deserializing_consumer import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

schema_registry_conf = {'url': 'http://10.18.0.5:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
value_deserializer = AvroDeserializer(schema_registry_client)

consumer_conf = {
    'bootstrap.servers': '10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096',
    'group.id': 'orders-avro-consumer-lab',
    'auto.offset.reset': 'earliest',
    'value.deserializer': value_deserializer,
    'key.deserializer': None
}

producer = Producer({'bootstrap.servers': '10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096'})
consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe(['avro-orders'])

try:
    while True:
        msg = consumer.poll(3.0)
        if msg is None:
            break
        if msg.error():
            raise msg.error()
        print(msg.value())
except Exception as e:
    payload = '{"error":"deserialization_failed","reason":"%s"}' % str(e).replace('"', "'")
    producer.produce('avro-orders-dlq', value=payload.encode('utf-8'))
    producer.flush()
finally:
    consumer.close()
PY
```
## Step D2 (Java version) — Simulate a Deserialization Error on the Consumer Side

### Teaching Objective

Reproduce, with a Java consumer using `KafkaAvroDeserializer`, the same broken deserialization scenario as the Python version, then redirect the faulty message to a DLQ (`avro-orders-dlq`), without stopping stream processing.

### Sub-step D2.1 — Prepare the Maven Project

**Machine: VM2**

```bash
mkdir -p /home/kafka/schema-lab/dlq-consumer/src/main/java/lab/avro
cd /home/kafka/schema-lab/dlq-consumer
```

Create the `pom.xml` file:

```bash
cat > pom.xml <<'XML'
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>lab.avro</groupId>
  <artifactId>dlq-consumer</artifactId>
  <version>1.0</version>
  <properties>
    <maven.compiler.source>17</maven.compiler.source>
    <maven.compiler.target>17</maven.compiler.target>
  </properties>
  <repositories>
    <repository>
      <id>confluent</id>
      <url>https://packages.confluent.io/maven/</url>
    </repository>
  </repositories>
  <dependencies>
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka-clients</artifactId>
      <version>3.7.0</version>
    </dependency>
    <dependency>
      <groupId>io.confluent</groupId>
      <artifactId>kafka-avro-serializer</artifactId>
      <version>7.7.0</version>
    </dependency>
    <dependency>
      <groupId>org.apache.avro</groupId>
      <artifactId>avro</artifactId>
      <version>1.11.3</version>
    </dependency>
  </dependencies>
  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.5.1</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
              <transformers>
                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                  <mainClass>lab.avro.DlqConsumer</mainClass>
                </transformer>
              </transformers>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
XML
```

### Sub-step D2.2 — Write the Consumer with DLQ Handling

```bash
cat > src/main/java/lab/avro/DlqConsumer.java <<'JAVA'
package lab.avro;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class DlqConsumer {

    public static void main(String[] args) {
        String bootstrap = "10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096";
        String schemaRegistryUrl = "http://10.18.0.5:8081";

        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", bootstrap);
        consumerProps.put("group.id", "orders-avro-consumer-lab-java");
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", KafkaAvroDeserializer.class.getName());
        consumerProps.put("schema.registry.url", schemaRegistryUrl);
        consumerProps.put("specific.avro.reader", "false");

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", bootstrap);
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());

        KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(consumerProps);
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        consumer.subscribe(Collections.singletonList("avro-orders"));

        try {
            while (true) {
                ConsumerRecords<String, Object> records = consumer.poll(Duration.ofSeconds(3));
                for (ConsumerRecord<String, Object> record : records) {
                    System.out.println(record.value());
                }
            }
        } catch (org.apache.kafka.common.errors.SerializationException e) {
            String reason = e.getMessage().replace('"', '\'');
            String payload = "{\"error\":\"deserialization_failed\",\"reason\":\"" + reason + "\"}";
            producer.send(new ProducerRecord<>("avro-orders-dlq", payload));
            producer.flush();
        } finally {
            consumer.close();
            producer.close();
        }
    }
}
JAVA
```
**This Java code has one single concrete goal**: show how a Kafka consumer should behave when it receives a message it cannot properly deserialize, instead of crashing and blocking the entire processing stream.

The problem it solves
Without this mechanism, if a single corrupted or malformed message arrives in the `avro-orders` topic (e.g. a raw JSON message instead of a real Avro message, like the one injected in D3), the consumer crashes with an exception and completely stops processing all subsequent messages — even perfectly valid ones. This is a classic production problem: a single toxic message blocks the entire chain.

What the code concretely does, step by step
It connects to Kafka and Schema Registry
The consumer uses `KafkaAvroDeserializer`, which automatically queries Schema Registry (10.18.0.5:8081) to retrieve the correct Avro schema matching each received message, in order to turn it into a readable object.

It reads messages in a loop (poll)
For each valid message, it simply prints it with `System.out.println(record.value())` — this is the expected normal behavior.

It intercepts deserialization errors
If a message does not respect the expected Avro format (like the raw message injected in D3), a `SerializationException` is thrown. Instead of letting this exception crash the whole program, the code catches it.

It isolates the faulty message in a Dead Letter Queue (DLQ)
Rather than losing the message or blocking the stream, the code produces a new event to the `avro-orders-dlq` topic, containing an explanation of the error (deserialization_failed + the exact reason). This allows Ops or application teams to later check this specific topic to understand what failed, without having had to stop the main processing.

The teaching objective in your workshop
This code illustrates an essential Kafka operating pattern: isolate errors without interrupting the service. This is exactly the same logic as the DLQ already seen with Kafka Connect in Workshop 5, but applied here directly at the Avro application level, on the consumer side, rather than the connector side.

### Sub-step D2.3 — Compile and Run

```bash
mvn clean package -q
java -jar target/dlq-consumer-1.0.jar
```

### Specific Java Point of Attention

Unlike the Python version, in Java, the `SerializationException` thrown by `KafkaAvroDeserializer` generally occurs **at the `poll()` call itself**, not inside the `for` loop, because deserialization happens before the record is returned to the caller. This is why the outer `catch` around `consumer.poll(...)` is essential: this is where the Avro deserialization error will actually be caught in most cases, rather than in the inner `try` inside the loop.

## Step D3 — Inject a Non-Avro Message into the Avro Topic

**Machine: VM1**

```bash
echo '{"order_id":"BROKEN","amount":"not-avro"}' | \
/opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --topic avro-orders
```

### Teaching Interest

This message is sent as raw JSON, **without going through the Avro serializer** — so without a magic header or Confluent schema ID at the start of the binary message. This is exactly the kind of real incident that happens in production when a misconfigured application writes directly to an Avro topic without using the correct serializer (configuration error, forgotten test script, incomplete migration). This test verifies that the Java consumer (D2) can detect and isolate this case, instead of simply crashing without leaving any usable trace.

## Step D4 — Run the Java Consumer with DLQ

**Machine: VM2**

```bash
# before launching
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --describe --group orders-avro-consumer-lab-java
  # If you see a CURRENT-OFFSET column already advanced (not 0), this confirms this group has already consumed part or all of the messages. RESET THEN LAUNCH

  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --group orders-avro-consumer-lab-java \
  --topic avro-orders \
  --reset-offsets --to-earliest --execute

cd /home/kafka/schema-lab/dlq-consumer
java -jar target/dlq-consumer-1.0.jar
```

Let it run for a few seconds, then stop with `Ctrl+C` once the DLQ message has been produced.

### Teaching Interest

This is the moment where we concretely observe the expected behavior: the consumer must normally read valid Avro messages (`A-1001`, `A-1002`, etc.), then encounter the `BROKEN` message injected in D3, trigger the `SerializationException` caught in the Java code, and produce a descriptive event in `avro-orders-dlq` instead of stopping abruptly. The group must observe that **the program keeps running** after the error — this is proof that the DLQ strategy works, unlike a naive consumer that would have crashed on this same message.

## Step D5 — Verify the Dead Letter Queue Content

**Machine: VM2**

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 10.118.0.5:9094 \
  --topic avro-orders-dlq \
  --from-beginning \
  --max-messages 1
```

### Expected Result

```json
{"error":"deserialization_failed","reason":"..."}
```

### Teaching Interest

This step closes the scenario's loop: it proves the faulty message did not simply disappear or block processing — it was **captured with usable context** (error type, precise reason). This is the information that would allow an Ops or application team, in real conditions, to understand the incident, potentially retrieve the original message, and decide whether to fix the source data or ignore this isolated case. Without this DLQ, the team would only have had a silent crash to investigate blindly in application logs.

## Step D6 — Explicit Link with Workshop 5

*(text-only step, no command)*

In Workshop 5, the DLQ was used on the Kafka Connect side to capture connector errors and preserve failure context in a dedicated topic. In this Workshop 5 bis, the same logic is transposed to the application-level Avro world: the DLQ is no longer just a connector mechanism, but a generic operating pattern for isolating corrupted, incompatible, or non-deserializable messages, without ever stopping the whole processing stream.

### Teaching Interest

This step has no command to run, but it has significant teaching value: it links two workshops by showing that the **DLQ pattern** (isolate rather than block) is a reusable architectural principle, whether implemented at a Kafka Connect connector level (Workshop 5) or directly in the application code of a Java consumer (Workshop 5 bis). The group should leave with the idea that this principle transcends the tool used — it is an operating discipline, not a feature specific to a single Kafka component.
***

## Part E — Monitoring and Operating Best Practices

### Teaching Objective

Define what to monitor around Schema Registry and Avro flows in order to quickly detect schema incidents, rather than discovering them only via a late application failure.

### Step E1 — Check Schema Registry Logs

**Machine: VM1**

```bash
tail -n 50 /home/kafka/schema-lab/schema-registry.log
```

### Expected Result

The logs must allow quick identification of schema registrations, compatibility rejections, and any service unavailability; these are the first traces to correlate with an application alert or a DLQ spike.

### Step E2 — List Registered Subjects and Versions

**Machine: VM1**

```bash
curl -s http://10.18.0.5:8081/subjects
curl -s http://10.18.0.5:8081/subjects/avro-orders-value/versions
```

### Expected Result

The group must be able to quickly answer three operating questions: how many subjects are registered, what versions exist for a given subject, and which version is the most recent.

### Step E3 — Monitor DLQ Volume

**Machine: VM2**

```bash
/opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  --topic avro-orders-dlq \
  --time -1
```

### Expected Result

A sudden increase in the number of messages in the DLQ should be interpreted as a sign of a schema, format, or broken application contract incident; this is a simple but very useful metric for Grafana and run alerts.

### Step E4 — Best Practices Checklist to Validate with the Group

- Define compatibility per critical subject, not just a default global configuration.
- Use the compatibility check before every producer or consumer deployment that impacts a schema.
- Separate business topics from DLQ topics to avoid any consumption confusion.
- Log deserialization errors with enough context to enable a targeted replay.
- Clean up test schemas and topics after labs so as not to pollute subsequent workshops, particularly Workshop 6 which starts with performance and integration measurements requiring a stable environment.

✅ **Objective achieved:** the group leaves with a concrete monitoring grid and operating habits aligned with the daily tools already stated in the questionnaire.

***

## Cleanup After the Workshop

This cleanup is important to link with Workshop 6: it must start without test topic pollution, without extra temporary processes, and without lab scripts still active, so that tuning tests and MongoDB/Elasticsearch integrations start from a clean base.

### Step N1 — Delete Test Topics

**Machine: VM1**

```bash
/opt/kafka/bin/kafka-topics.sh --delete   --bootstrap-server 10.18.0.5:9092   --topic avro-orders

/opt/kafka/bin/kafka-topics.sh --delete   --bootstrap-server 10.18.0.5:9092   --topic avro-orders-dlq
```

### Step N2 — Stop Schema Registry

**Machine: VM1**

```bash
pkill -f schema-registry-start
```

### Step N3 — Remove Temporary Lab Artifacts

**Machine: VM1**

```bash
rm -rf /home/kafka/schema-lab
```

### Step N4 — Final Check Before Moving to Workshop 6

**Machine: VM1**

```bash
/opt/kafka/bin/kafka-topics.sh --list   --bootstrap-server 10.18.0.5:9092 | grep avro-orders || true

ps aux | grep schema-registry | grep -v grep || true
```

### Expected Result

No `avro-orders*` topic should remain and no lab Schema Registry process should still be active; the environment thus becomes consistent again with Workshop 6's prerequisites.

***

## What the Client Will Have Learned

By the end of this Workshop 5 bis, the group will have handled Schema Registry as an administrable component, tested Backward/Forward/Full compatibilities, practiced safe schema evolution, observed the difference between a serialization error and a deserialization error, implemented a DLQ on the Avro stream side, and defined simple controls to monitor schema-related incidents. This workshop therefore addresses point by point the request made in the questionnaire and properly completes the teaching trajectory between Workshop 5 focused on Kafka Connect and Workshop 6 focused on tuning and integrations.
