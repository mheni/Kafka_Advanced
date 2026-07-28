# Workshop 4 — Under-Replicated Partitions Diagnostic and Performance Benchmark

## Workshop Objective

This workshop covers the last two priorities confirmed in section H1 of the questionnaire: **under-replicated partitions diagnostics** and **performance benchmarking** (throughput, latency, consumer lag). It directly addresses the gap confirmed in F1 — tuning parameters have never been modified by the client, who explicitly admits not knowing whether they are actually useful.

## Client Context to Keep in Mind

| Questionnaire finding | Implication for the workshop |
| :-- | :-- |
| URPs observed only during planned maintenance/DRP, not in normal usage | The workshop must deliberately trigger URPs to study them, since they don't see them daily |
| `replica.lag.time.max.ms`, `leader.replication.throttled.rate` never modified | These parameters will be the core of the diagnostic |
| `compression.type`, `linger.ms`, `fetch.min.bytes` managed "by App team" | To be treated as a demonstration, not as a gap to fill for this admin/ops audience |
| Real topics up to 90 partitions, RF=3 | The benchmark must use a comparable load |

***

## Prerequisites

| Element | Machine |
| :-- | :-- |
| Active 6-broker/5-controller cluster | VM1, VM2, VM3 |
| Active JCDecaux producer | Producer VM |
| Topics `vls-stations-nancy` and `vls-stations-toulouse` already filled | — |

***

## Part A — Under-Replicated Partitions Diagnostic

### Step A1 — Create a High-Cardinality Test Topic

**Machine: VM1 (10.18.0.5)**

```bash
/opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  --topic perf-diagnostic-test \
  --partitions 90 \
  --replication-factor 3 \
  --config min.insync.replicas=2
```

### Expected Result

```
Created topic perf-diagnostic-test.
```

✅ **Validation:**

```bash
/opt/kafka/bin/kafka-topics.sh --describe \
  --bootstrap-server 10.18.0.5:9092 \
  --topic perf-diagnostic-test | head -5
```

Should show 90 partitions with `Isr` consistently containing 3 replicas — reproducing the size of their largest topic `bdl_port_events_injector_notifier` (90 partitions).

***

### Step A2 — Trigger URPs in a Controlled Way

**Machine: VM3 (10.128.0.5)**

```bash
sudo systemctl stop kafka-broker@5
```

### Expected Result

No error message — the service stops cleanly.

**Machine: VM1** — immediately observe the impact:

```bash
/opt/kafka/bin/kafka-topics.sh --describe \
  --bootstrap-server 10.18.0.5:9092 \
  --topic perf-diagnostic-test \
  --under-replicated-partitions
```

### Expected Result

```
Topic: perf-diagnostic-test  Partition: 3  Leader: 3  Replicas: 5,3,1  Isr: 3,1
Topic: perf-diagnostic-test  Partition: 12 Leader: 1  Replicas: 1,5,4  Isr: 1,4
...
```

✅ **Key reading point:** each displayed line = a partition whose `Isr` contains fewer elements than `Replicas`. The number of lines displayed corresponds to the number of partitions having broker5 in their replica list (~30 out of 90 partitions, if evenly distributed across 6 brokers).

***

### Step A3 — Automated Diagnostic Script

**Machine: VM1**

```bash
nano /home/kafka/diagnose_urp.sh
```

```bash
#!/bin/bash
# =============================================================
# Under-replicated partitions diagnostic script
# To be run from VM1
# =============================================================

set -euo pipefail

BOOTSTRAP="10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096"
KAFKA_BIN="/opt/kafka/bin"
REPORT="/home/kafka/urp_diagnostic_$(date +%Y%m%d_%H%M%S).txt"

echo "=== UNDER-REPLICATED PARTITIONS DIAGNOSTIC ===" | tee "$REPORT"
echo "Date: $(date)" | tee -a "$REPORT"

echo -e "\n--- 1. List of URP partitions ---" | tee -a "$REPORT"
$KAFKA_BIN/kafka-topics.sh --describe \
  --bootstrap-server $BOOTSTRAP \
  --under-replicated-partitions | tee -a "$REPORT"

URP_COUNT=$($KAFKA_BIN/kafka-topics.sh --describe \
  --bootstrap-server $BOOTSTRAP \
  --under-replicated-partitions | wc -l)
echo -e "\nTotal number of URP partitions: $URP_COUNT" | tee -a "$REPORT"

echo -e "\n--- 2. Faulty brokers (identified via Replicas vs Isr) ---" | tee -a "$REPORT"
$KAFKA_BIN/kafka-topics.sh --describe \
  --bootstrap-server $BOOTSTRAP \
  --under-replicated-partitions | \
  grep -oP 'Replicas: \K[0-9,]+' | tr ',' '\n' | sort -u | tee -a "$REPORT"

echo -e "\n--- 3. Offline partitions (no leader) ---" | tee -a "$REPORT"
$KAFKA_BIN/kafka-topics.sh --describe \
  --bootstrap-server $BOOTSTRAP \
  --unavailable-partitions | tee -a "$REPORT"

echo -e "\n--- 4. State of suspected broker (broker 5) ---" | tee -a "$REPORT"
ssh kafka@10.128.0.5 "systemctl is-active kafka-broker5" | tee -a "$REPORT" || echo "INACTIVE" | tee -a "$REPORT"

echo -e "\n--- 5. Current replica.lag.time.max.ms configuration ---" | tee -a "$REPORT"
$KAFKA_BIN/kafka-configs.sh \
  --bootstrap-server $BOOTSTRAP \
  --describe --entity-type brokers --entity-name 1 --all | grep -i "replica.lag" | tee -a "$REPORT"

echo -e "\n=== END OF DIAGNOSTIC - Report: $REPORT ===" | tee -a "$REPORT"
```

```bash
chmod +x /home/kafka/diagnose_urp.sh
./diagnose_urp.sh
```

### Expected Result

A report clearly identifying:

- The exact number of affected partitions
- That broker5 is systematically present in the `Replicas` of each URP partition
- That broker5 is `INACTIVE` — confirming the causal link
- The current value of `replica.lag.time.max.ms` (default value if never configured, consistent with their "Not set" answer in F1)

✅ **Teaching point:** this script turns a tedious manual observation into an actionable automated diagnostic — directly adaptable to their production cluster for a first level of automated troubleshooting.

***

### Step A4 — Restore and Observe Recovery

**Machine: VM3**

```bash
sudo systemctl start kafka-broker5
```

**Machine: VM1** — monitor the recovery:

```bash
watch -n 5 '/opt/kafka/bin/kafka-topics.sh --describe --bootstrap-server 10.18.0.5:9092 --topic perf-diagnostic-test --under-replicated-partitions | wc -l'
```

### Expected Result

The displayed number should gradually decrease from ~30 to 0, illustrating the ISR resynchronization live.

***

## Part B — Performance Benchmark

### Step B1 — Producer Throughput Test

**Machine: VM2 (10.118.0.5)** — chosen to avoid interfering with brokers hosted on VM1/VM3.

```bash
/opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic perf-diagnostic-test \
  --num-records 500000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props bootstrap.servers=10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  acks=all
```

### Expected Result

```
500000 records sent, 48523.6 records/sec (47.39 MB/sec), 12.4 ms avg latency, 340.0 ms max latency
```

✅ **What to read:** `records/sec` and `MB/sec` give the raw throughput, `avg latency` the average acknowledgment latency (with `acks=all`, this latency includes waiting for full ISR synchronization).

***

### Step B2 — Throughput Test with `acks=1` (Comparison)

**Machine: VM2**

```bash
/opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic perf-diagnostic-test \
  --num-records 500000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props bootstrap.servers=10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  acks=1
```

### Expected Result

```
500000 records sent, 89234.1 records/sec (87.14 MB/sec), 4.1 ms avg latency, 210.0 ms max latency
```

✅ **Expected comparison:** significantly higher throughput and reduced latency with `acks=1`, since the producer only waits for the leader's acknowledgment, not the whole ISR. **Mandatory discussion point:** remind everyone that the client uses `acks=all` in production for their critical topics — this performance vs durability tradeoff must be explicitly justified, not just measured.

***

### Step B3 — Consumer Lag Test Under Load

**Machine: VM3 (10.128.0.5)**

```bash
/opt/kafka/bin/kafka-consumer-perf-test.sh \
  --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  --topic perf-diagnostic-test \
  --messages 500000 \
  --group perf-test-consumer-group
```

### Expected Result

```
start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec
2026-07-13 10:40:00, 2026-07-13 10:40:08, 488.28, 61.03, 500000, 62500.0
```

**Machine: VM1** — check the lag while the test runs (in a 2nd terminal, if you immediately re-run the test):

```bash
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --describe --group perf-test-consumer-group
```

### Expected Result

`LAG` columns visible per partition — should trend toward 0 at the end of the test if the consumer keeps up with the producer's pace.

***

## Part C — Tuning Parameters Never Configured (F1)

### Step C1 — Baseline Without Tuning

Reuse test B1 (`acks=all`) as the baseline, note the throughput obtained (e.g., 48,523 records/sec).

### Step C2 — Adjust `num.network.threads` and `num.io.threads`

**Machine: VM1** — these parameters are already set to 8/8 for the client, so test a **reduced** value to illustrate the negative impact of under-configuration:

```bash
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --alter --entity-type brokers --entity-name 1 \
  --add-config num.io.threads=4
```

### Expected Result

```
Completed updating config for broker: 1.
```

Re-run test B1: throughput should **drop significantly** (e.g., 15,000 records/sec instead of 48,000), concretely illustrating why their current value of 8 threads is justified and should not be reduced.

**Machine: VM1** — restore the original value:

```bash
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --alter --entity-type brokers --entity-name 1 \
  --add-config num.io.threads=8
```

***

### Step C3 — Test `replica.lag.time.max.ms`

**Machine: VM1** — this parameter controls the delay before a lagging replica is removed from the ISR:

```bash
sudo nano /opt/kafka/config/kraft-lab/broker1.properties
replica.lag.time.max.ms=5000
sudo systemctl restart kafka-broker@1.service
```

Reproduce Step A2 (stopping broker5) and observe: with a reduced value (5000ms instead of the default 30000ms), the broker will be removed from the ISR **faster**, which can speed up detection but also generate more false positives in case of a simple transient network latency.

✅ **Discussion point:** this parameter is a tradeoff between detection responsiveness and tolerance to temporary fluctuations — exactly the type of tradeoff the client wanted to understand in F1.

***

## Results Summary Table to Complete

| Test | Configuration | Measured throughput | Average latency |
| :-- | :-- | :-- | :-- |
| B1 | acks=all, baseline | ___ records/sec | ___ ms |
| B2 | acks=1 | ___ records/sec | ___ ms |
| C2 | num.io.threads=2 (degraded) | ___ records/sec | ___ ms |
| C2 | num.io.threads=8 (restored) | ___ records/sec | ___ ms |

***

## Cleanup After the Workshop

**Machine: VM1**

```bash
/opt/kafka/bin/kafka-topics.sh --delete \
  --bootstrap-server 10.18.0.5:9092 \
  --topic perf-diagnostic-test
# delete this line from broker1.properties
replica.lag.time.max.ms=5000
```

***

This workshop wraps up the 4 priority topics identified in the questionnaire (DRP, security, URP, performance), demonstrating at each step that parameters never touched by the client (F1) have a measurable and justifiable impact, turning their "lack of knowledge" into concrete operational understanding.
