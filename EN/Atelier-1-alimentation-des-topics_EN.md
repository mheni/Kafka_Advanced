# Workshop 1 (full update) — Feeding Kafka Topics with the JCDecaux API (Java + Python)

## Workshop Objective

Build a Java producer that queries the JCDecaux API and continuously publishes real bike station data into Kafka topics, with a complementary Python script for quick testing.

***

## Prerequisites

| Item | Detail |
| :-- | :-- |
| Kafka Cluster | 6 brokers + 5 controllers deployed (Workshop 0) |
| JCDecaux API Key | Obtained from developer.jcdecaux.com |
| Java | OpenJDK 17+ already installed on the VMs |
| Maven | To be installed to compile the Java producer |
| Execution machine | Can be one of the 3 VMs or an external host with network access to the cluster |

***

## Step 1 — Registration and API Key Retrieval

### Actions

1. Create an account at `https://developer.jcdecaux.com`
2. Retrieve the API key (`apiKey`) from the dashboard
3. List available contracts:
```bash
curl "https://api.jcdecaux.com/vls/v1/contracts?apiKey=YOUR_KEY"
```

### Expected Result

A JSON list of contracts (cities), for example:

```json
[{"name":"nancy","commercial_name":"vélOstan'lib"},{"name":"toulouse","commercial_name":"VélôToulouse"}]
```

✅ **Validation:** you should get an HTTP 200 code and a non-empty list. A 403 code means an invalid key.

***

## Step 2 — Test the API Manually

### Actions

```bash
curl "https://api.jcdecaux.com/vls/v1/stations?contract=nancy&apiKey=YOUR_KEY" | jq .
```

### Expected Result

A JSON array of stations with the fields `number`, `name`, `position`, `available_bikes`, `available_bike_stands`, `status`, `last_update`.

✅ **Validation:** the `last_update` field should correspond to a recent date (epoch timestamp in milliseconds) — confirming the feed is indeed "real-time".

***

## Step 3 — Create Kafka Topics

### Actions

```bash
/opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  --topic vls-stations-nancy \
  --partitions 6 \
  --replication-factor 3

/opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  --topic vls-stations-toulouse \
  --partitions 6 \
  --replication-factor 3
```

### Expected Result

```bash
/opt/kafka/bin/kafka-topics.sh --describe --bootstrap-server 10.18.0.5:9092 --topic vls-stations-nancy
```

Should show 6 partitions, each with 3 replicas distributed across the West/North racks, and a leader assigned for each partition.

✅ **Validation:** no partition should appear "under-replicated" at this stage (cluster at rest).

***

## Step 4 — Python Producer (Quick Test)

### Actions

Install dependencies:

```bash
sudo apt install -y python3-pip
pip3 install confluent-kafka requests
```

Create `jcdecaux_producer.py`:

```python
import requests
import json
import time
from confluent_kafka import Producer

API_KEY = "YOUR_KEY"
CONTRACT = "nancy"
TOPIC = "vls-stations-nancy"
BOOTSTRAP_SERVERS = "10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096"
POLL_INTERVAL_SECONDS = 60

producer_conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'client.id': 'jcdecaux-producer-python',
    'acks': 'all',
    'compression.type': 'snappy'
}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery error: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def fetch_stations():
    url = f"https://api.jcdecaux.com/vls/v1/stations?contract={CONTRACT}&apiKey={API_KEY}"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()

def main():
    while True:
        try:
            stations = fetch_stations()
            for station in stations:
                key = str(station["number"])
                value = json.dumps(station)
                producer.produce(TOPIC, key=key.encode("utf-8"), value=value.encode("utf-8"), callback=delivery_report)
            producer.flush()
            print(f"{len(stations)} stations published at {time.strftime('%H:%M:%S')}")
        except Exception as e:
            print(f"Error: {e}")
        time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
```

Run:

```bash
python3 jcdecaux_producer.py
```

### Expected Result

One log line every 60 seconds: `XX stations published at HH:MM:SS`, with a delivery confirmation message for each station.

✅ **Validation:** this script only serves to quickly validate the API → Kafka chain before moving to the final Java version.

***

## Step 5 — Java Producer (Production Version, Consistent with the Client Stack)

### 5.1 — Install Maven

```bash
sudo apt install -y maven
mvn -version
```

**Expected result:** display of the Maven version and detected JDK (17 or 21).

### 5.2 — Project Structure

```bash
mkdir -p ~/jcdecaux-kafka-producer/src/main/java/com/luxse/lab
cd ~/jcdecaux-kafka-producer
```

### 5.3 — `pom.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.luxse.lab</groupId>
    <artifactId>jcdecaux-kafka-producer</artifactId>
    <version>1.0.0</version>
    <packaging>jar</packaging>

    <properties>
        <maven.compiler.source>17</maven.compiler.source>
        <maven.compiler.target>17</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>4.2.0</version>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
            <version>2.17.0</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>2.0.13</version>
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
                                    <mainClass>com.luxse.lab.JCDecauxKafkaProducer</mainClass>
                                </transformer>
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

### 5.4 — Main Java Class

`src/main/java/com/luxse/lab/JCDecauxKafkaProducer.java`:

```java
package com.luxse.lab;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Future;

public class JCDecauxKafkaProducer {

    private static final String API_KEY = System.getenv().getOrDefault("JCDECAUX_API_KEY", "YOUR_KEY");
    private static final String CONTRACT = System.getenv().getOrDefault("JCDECAUX_CONTRACT", "nancy");
    private static final String TOPIC = System.getenv().getOrDefault("KAFKA_TOPIC", "vls-stations-nancy");
    private static final String BOOTSTRAP_SERVERS = System.getenv().getOrDefault(
            "KAFKA_BOOTSTRAP_SERVERS",
            "10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096"
    );
    private static final long POLL_INTERVAL_MS = 60_000L;

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Producer<String, String> producer;

    public JCDecauxKafkaProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("client.id", "jcdecaux-producer-java");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("compression.type", "snappy");
        props.put("enable.idempotence", "true");
        props.put("retries", "5");
        props.put("linger.ms", "50");
        this.producer = new KafkaProducer<>(props);
    }

    private String fetchStationsJson() throws Exception {
        String url = String.format(
                "https://api.jcdecaux.com/vls/v1/stations?contract=%s&apiKey=%s",
                CONTRACT, API_KEY
        );
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new RuntimeException("JCDecaux API error, HTTP code: " + response.statusCode());
        }
        return response.body();
    }

    private void publishStations() {
        try {
            String json = fetchStationsJson();
            JsonNode stations = objectMapper.readTree(json);
            int count = 0;
            for (JsonNode station : stations) {
                String key = station.get("number").asText();
                String value = station.toString();
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);

                Future<RecordMetadata> future = producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Send error for station " + key + ": " + exception.getMessage());
                    }
                });
                count++;
            }
            producer.flush();
            System.out.println(count + " stations published at " + java.time.LocalTime.now());
        } catch (Exception e) {
            System.err.println("Error during fetch/publish: " + e.getMessage());
        }
    }

    public void run() {
        System.out.println("Starting JCDecaux -> Kafka producer");
        System.out.println("Contract: " + CONTRACT + " | Topic: " + TOPIC + " | Bootstrap: " + BOOTSTRAP_SERVERS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Stopping producer, clean shutdown...");
            producer.close();
        }));

        while (true) {
            publishStations();
            try {
                Thread.sleep(POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public static void main(String[] args) {
        new JCDecauxKafkaProducer().run();
    }
}
```

### 5.5 — Compilation

```bash
cd ~/jcdecaux-kafka-producer
mvn clean package
```

### Expected Result

```
[INFO] BUILD SUCCESS
[INFO] Total time: XX s
```

A `target/jcdecaux-kafka-producer-1.0.0.jar` file should be generated.

✅ **Validation:** if `BUILD FAILURE`, check the Java version (`java -version` must be 17+) and Maven Central connectivity.

***

## Step 6 — Run the Java Producer

### Actions

```bash
export JCDECAUX_API_KEY="YOUR_KEY"
export JCDECAUX_CONTRACT="nancy"
export KAFKA_TOPIC="vls-stations-nancy"
export KAFKA_BOOTSTRAP_SERVERS="10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096"

java -jar target/jcdecaux-kafka-producer-1.0.0.jar
```

### Expected Result

```
Starting JCDecaux -> Kafka producer
Contract: nancy | Topic: vls-stations-nancy | Bootstrap: 10.18.0.5:9092,...
XX stations published at 10:15:32
XX stations published at 10:16:32
```

✅ **Validation:** no `Send error` or `API error` should appear; publications should repeat every 60 seconds.

***

## Step 7 — Verify Reception in Kafka

### Actions

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --topic vls-stations-nancy \
  --from-beginning \
  --property print.key=true
```

### Expected Result

A stream of JSON messages showing `station_number | {station JSON data}`, with a new batch of messages visible every minute.

✅ **Validation:** the number of messages should increase with each cycle; check via:

```bash
/opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --topic vls-stations-nancy
```

Offsets should increase between two successive runs of this command.

***

## Step 8 — Automate the Java Producer as a Systemd Service

### Actions

```bash
sudo mkdir -p /opt/kafka/apps
sudo cp target/jcdecaux-kafka-producer-1.0.0.jar /opt/kafka/apps/

sudo tee /etc/systemd/system/jcdecaux-producer-java.service <<EOF
[Unit]
Description=JCDecaux VLS Kafka Producer (Java)
After=network.target

[Service]
Type=simple
User=kafka
Environment=JCDECAUX_API_KEY=YOUR_KEY
Environment=JCDECAUX_CONTRACT=nancy
Environment=KAFKA_TOPIC=vls-stations-nancy
Environment=KAFKA_BOOTSTRAP_SERVERS=10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096
ExecStart=/usr/bin/java -jar /opt/kafka/apps/jcdecaux-kafka-producer-1.0.0.jar
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable jcdecaux-producer-java
sudo systemctl start jcdecaux-producer-java
```

### Expected Result

```bash
sudo systemctl status jcdecaux-producer-java
```

Should show `active (running)` in green, with logs visible via:

```bash
journalctl -u jcdecaux-producer-java -f
```

✅ **Validation:** the service should restart automatically in case of a temporary network outage (`Restart=always`), guaranteeing a continuous flow even without manual supervision.

```bash
/opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --topic vls-stations-nancy
```

***

## Step 9 — Repeat for a Second Contract (Optional, Topic Diversity)

Repeat steps 3, 5-8 with `JCDECAUX_CONTRACT=toulouse` and `KAFKA_TOPIC=vls-stations-toulouse`, changing the systemd service name (`jcdecaux-producer-java-toulouse`).

### Expected Result

Two independent Kafka streams active simultaneously, simulating a diversity of business topics comparable to the client's real environment.

***

## Final Workshop Validation Table

| Check | Command | Expected result |
| :-- | :-- | :-- |
| Java producer active | `systemctl status jcdecaux-producer-java` | `active (running)` |
| Messages received | `kafka-run-class.sh kafka.tools.GetOffsetShell` | Increasing offsets between 2 checks |
| Partition distribution | `kafka-topics.sh --describe` | 6 partitions, RF=3, none under-replicated |
| Visible throughput | Grafana / AKHQ dashboard | Non-zero throughput graph, updated every minute |
| Resilience | `systemctl stop jcdecaux-producer-java` then `start` | Automatic resumption of publishing without topic loss |

***

With this Java producer running continuously via systemd, your topics will naturally be filled and active for several hours or days before the training — exactly the condition required for the DRP and performance workshops (Workshops 2 and 4) to work on live data rather than empty topics.
