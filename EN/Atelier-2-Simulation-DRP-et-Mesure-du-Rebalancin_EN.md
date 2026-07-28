# Workshop 2 — DRP Simulation and Rebalancing Measurement

## Workshop Objective

Reproduce the client's real DRP scenario (stopping the West region, observing cluster behavior, measuring rebalancing time), leveraging the JCDecaux flow set up in Workshop 1 as a realistic background load, then test optimizations to reduce the observed recovery time (~1h in production).

## Topology Reminder

| VM | IP | Role | Rack |
| :-- | :-- | :-- | :-- |
| VM1 | 10.18.0.5 | Broker 1 (9092), Broker 2 (9093), Controller 1 (9192), Controller 2 (9193) | West |
| VM2 | 10.118.0.5 | Broker 3 (9094), Broker 4 (9095), Controller 3 (9194), Controller 4 (9195) | North |
| VM3 | 10.128.0.5 | Broker 5 (9096), Broker 6 (9097), Controller 5 (9196) | West/North |

**Simulated scenario:** complete failure of the West region → stopping VM1 (brokers 1, 2 + controllers 1, 2) and broker 5 on VM3 if configured on the West rack.

***

## Prerequisites Before Starting

| Check | Where | Command |
| :-- | :-- | :-- |
| Healthy cluster | Any VM | `kafka-metadata-quorum.sh describe --status` |
| Active JCDecaux producer | Producer execution VM | `systemctl status jcdecaux-producer-java` |
| Topics filled | VM1 | `kafka-run-class.sh kafka.tools.GetOffsetShell` |

***

## Step 1 — Baseline State Verification

**Machine: VM1 (10.18.0.5)**

```bash
/opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-controller 10.18.0.5:9192 \
  describe --status
```

### Expected Result

```
ClusterId: k7F3nQzXTHmR9vK2wLpQAg
LeaderId: 103
LeaderEpoch: 5
HighWatermark: 1245
MaxFollowerLag: 0
MaxFollowerLagTimeMs: 0
CurrentVoters: [101,102,103,104,105]
CurrentObservers: []
```

✅ **Validation:** `MaxFollowerLag: 0` confirms all controllers are synchronized before starting the test.

**Machine: VM1**

```bash
/opt/kafka/bin/kafka-topics.sh --describe \
  --bootstrap-server 10.18.0.5:9092 \
  --topic vls-stations-nancy
```

### Expected Result

All partitions should show `Isr: [1,3,5]` (or equivalent) with **as many replicas in the Isr as the replication factor (3)** — no under-replicated partition.

***

## Step 2 — Automated Rebalancing Measurement Script

**Machine: VM2 (10.118.0.5)** — chosen because it stays active throughout the test (North region).

Create the `drp_test.sh` script:

```bash
nano /home/kafka/drp_test.sh
```

```bash
#!/bin/bash
# =============================================================
# DRP simulation and rebalancing time measurement script
# To be run from VM2 (North region - stays active)
# =============================================================

set -euo pipefail

KAFKA_BIN="/opt/kafka/bin"
BOOTSTRAP_SURVIVING="10.118.0.5:9094,10.118.0.5:9095"
TOPIC="vls-stations-nancy"
LOG_FILE="/home/kafka/drp_test_$(date +%Y%m%d_%H%M%S).log"
CHECK_INTERVAL=10

log() {
    echo "[$(date '+%H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

check_urp_count() {
    $KAFKA_BIN/kafka-topics.sh --describe \
        --bootstrap-server $BOOTSTRAP_SURVIVING \
        --under-replicated-partitions 2>/dev/null | wc -l
}

check_offline_partitions() {
    $KAFKA_BIN/kafka-topics.sh --describe \
        --bootstrap-server $BOOTSTRAP_SURVIVING \
        --unavailable-partitions 2>/dev/null | wc -l
}

wait_for_full_recovery() {
    local start_time=$1
    log "Waiting for return to a healthy state (0 URP, 0 offline)..."
    while true; do
        urp=$(check_urp_count)
        offline=$(check_offline_partitions)
        log "Under-replicated: $urp | Offline: $offline"
        if [[ "$urp" -eq 0 && "$offline" -eq 0 ]]; then
            end_time=$(date +%s)
            duration=$((end_time - start_time))
            log "=========================================="
            log "CLUSTER HEALTHY - Rebalancing complete"
            log "Total rebalancing duration: ${duration} seconds ($((duration/60)) min $((duration%60)) sec)"
            log "=========================================="
            break
        fi
        sleep $CHECK_INTERVAL
    done
}

log "=========================================="
log "STARTING DRP TEST"
log "=========================================="

log "Step 1: Checking initial cluster health"
$KAFKA_BIN/kafka-topics.sh --describe --bootstrap-server $BOOTSTRAP_SURVIVING --topic $TOPIC | tee -a "$LOG_FILE"

log "Step 2: READY to simulate the West region failure."
log ">>> Now manually run the stop_west_region.sh script on VM1 <<<"
log "Press ENTER once the West region is stopped to start the measurement..."
read -r

START_TIME=$(date +%s)
log "Timer started at $(date '+%H:%M:%S')"

log "Step 3: Monitoring the cluster in degraded mode"
for i in 1 2 3; do
    urp=$(check_urp_count)
    offline=$(check_offline_partitions)
    log "Check $i - Under-replicated: $urp | Offline: $offline"
    sleep 5
done

log "Step 4: READY to restore the West region."
log ">>> Now manually run the restore_west_region.sh script on VM1 <<<"
log "Press ENTER once the West region has been restarted..."
read -r

log "Step 5: Monitoring rebalancing until full recovery"
wait_for_full_recovery $START_TIME

log "Full log available at: $LOG_FILE"
```

Make the script executable:

```bash
chmod +x /home/kafka/drp_test.sh
```

***

## Step 3 — West Region Shutdown Script

**Machine: VM1 (10.18.0.5)**

Create `stop_west_region.sh`:

```bash
nano /home/kafka/stop_west_region.sh
```

```bash
#!/bin/bash
# =============================================================
# Simulates the West region failure
# To be run on VM1 only
# =============================================================

echo "[$(date '+%H:%M:%S')] Stopping West region brokers (broker1, broker2)..."
sudo systemctl stop kafka-broker1
sudo systemctl stop kafka-broker@2

echo "[$(date '+%H:%M:%S')] Stopping West region controllers (controller1, controller2)..."
sudo systemctl stop kafka-controller@1
sudo systemctl stop kafka-controller@2

echo "[$(date '+%H:%M:%S')] West region stopped. Service status:"
systemctl is-active kafka-broker1 kafka-broker@2 kafka-controller@1 kafka-controller@2 || true
```

```bash
chmod +x /home/kafka/stop_west_region.sh
```

### Expected Execution Result

```
[10:20:15] Stopping West region brokers (broker1, broker2)...
[10:20:16] Stopping West region controllers (controller1, controller2)...
[10:20:17] West region stopped. Service status:
inactive
inactive
inactive
inactive
```

✅ **Validation:** all 4 services should show `inactive`, confirming the complete shutdown of the simulated region.

***

## Step 4 — Observe Behavior During the Outage

**Machine: VM2 (10.118.0.5)** — in a second terminal, in parallel with the main script

```bash
watch -n 5 '/opt/kafka/bin/kafka-topics.sh --describe \
  --bootstrap-server 10.118.0.5:9094 \
  --topic vls-stations-nancy'
```

### Expected Result

Display refreshed every 5 seconds showing:

- Partitions whose leader was on broker1/2/5 switch to a leader on broker3/4/6
- The `Isr` column temporarily shrinks (e.g., `Isr: [3,4]` instead of `[1,3,4]`)
- Possibly partitions marked `Leader: none` very briefly during election

**Machine: VM2** — check consumer lag in parallel

```bash
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server 10.118.0.5:9094 \
  --describe --all-groups
```

### Expected Result

A temporary increase in `LAG` on active consumers, confirming the impact observed by the client in D4 (*"Consumer lag increases temporarily"*).

***

## Step 5 — West Region Restoration Script

**Machine: VM1 (10.18.0.5)**

Create `restore_west_region.sh`:

```bash
nano /home/kafka/restore_west_region.sh
```

```bash
#!/bin/bash
# =============================================================
# Restores the West region after DRP simulation
# To be run on VM1 only
# =============================================================

echo "[$(date '+%H:%M:%S')] Restarting West region controllers..."
sudo systemctl start kafka-controller@1
sudo systemctl start kafka-controller@2
sleep 10

echo "[$(date '+%H:%M:%S')] Restarting West region brokers..."
sudo systemctl start kafka-broker@1
sudo systemctl start kafka-broker@2

echo "[$(date '+%H:%M:%S')] West region restarted. Service status:"
systemctl is-active kafka-broker1 kafka-broker@2 kafka-controller@1 kafka-controller@2
```

```bash
chmod +x /home/kafka/restore_west_region.sh
```

### Expected Result

```
[10:35:02] Restarting West region controllers...
[10:35:12] Restarting West region brokers...
[10:35:13] West region restarted. Service status:
active
active
active
active
```

✅ **Validation:** all 4 services show `active`. **This is the moment when rebalancing/resynchronization begins**, and the main script's timer keeps running until a healthy state is reached.

***

## Step 6 — Full Test Execution (Chronological Sequence)

| Order | Machine | Action |
| :-- | :-- | :-- |
| 1 | VM2 | Launch `./drp_test.sh` |
| 2 | VM1 | On script instruction, run `./stop_west_region.sh` |
| 3 | VM2 | Press ENTER in `drp_test.sh` to start the timer |
| 4 | VM2 (2nd terminal) | Observe with `watch` (Step 4) |
| 5 | VM1 | On script instruction, run `./restore_west_region.sh` |
| 6 | VM2 | Press ENTER in `drp_test.sh` to monitor recovery |

### Expected Result at End of Test

```
[10:35:13] CLUSTER HEALTHY - Rebalancing complete
[10:35:13] Total rebalancing duration: 187 seconds (3 min 7 sec)
```

> **Teaching note:** on a lab cluster with little data (unlike production with multi-GB topics), rebalancing will be **significantly faster** than the hour observed in production. The lab's goal is to understand the **mechanism**, not to reproduce the same order of magnitude in time.

## Step 6-bis — Repeat the exercise with a script requested by your trainer and run the same pipeline

```bash
/home/kafka/drp_test_volume.sh vls-stations-nancy 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096
```

***

## Step 7 — Test with Preferred Leader Election (Optimization)

**Machine: VM1 or VM2** (any active node)

After restoration (Step 5), instead of waiting for natural re-election, immediately force the return to preferred leaders:

```bash
/opt/kafka/bin/kafka-leader-election.sh \
  --bootstrap-server 10.118.0.5:9094 \
  --election-type preferred \
  --all-topic-partitions
```

### Expected Result

```
Successfully completed leader election (PREFERRED) for partitions vls-stations-nancy-0, vls-stations-nancy-1, ...
```

Immediately run a verification:

```bash
/opt/kafka/bin/kafka-topics.sh --describe \
  --bootstrap-server 10.118.0.5:9094 \
  --topic vls-stations-nancy
```

### Expected Result

Leaders should be redistributed immediately to their preferred broker (the first one defined in the replica list), without waiting for natural rebalancing.

✅ **Comparison to document:** re-run the full test (Steps 1 to 6) a second time, this time running the Step 7 command right after restoration, and compare the total time obtained with the first run.

***

## Step 8 — Optimization Test via Configuration (Throttling)

**Machine: VM1** — before running a 3rd test, adjust the configuration on the fly on the surviving brokers:

```bash
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server 10.118.0.5:9094 \
  --alter --entity-type brokers --entity-name 3 \
  --add-config leader.replication.throttled.rate=52428800

/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server 10.118.0.5:9094 \
  --alter --entity-type brokers --entity-name 3 \
  --add-config follower.replication.throttled.rate=52428800
```

### Expected Result

```
Completed updating config for broker: 3.
```

Re-run the full DRP test (Step 6) a 3rd time and compare the resulting rebalancing time.

✅ **Final validation:** record the 3 measured durations (baseline / with forced preferred leader election / with adjusted throttling) in a table to objectively show the gain from each optimization.

# Part D — Deep Dive Leader & Synchronization (Production Troubleshooting)

This extension of Workshop 2 reuses the same 6-broker / 5-controller KRaft cluster already deployed for the DRP scenarios, which allows adding production-level troubleshooting without setting up a new separate infrastructure. The content directly addresses incidents already reported by the team, notably rebalancing delays after regional outages, topic synchronization issues, and errors related to leader changes.

## Learning Objectives

By the end of this part, participants should be able to:

- Reproduce a case where a newly created topic is not immediately healthy across the entire cluster after a partial failure.
- Correctly interpret the `Leader`, `Replicas`, and `Isr` columns of a `kafka-topics.sh --describe` during and after an incident.
- Understand the difference between leader election, return of the preferred leader, and ISR resynchronization after brokers are restored.
- Prepare the ground for more targeted troubleshooting scenarios that will be covered in a follow-up workshop, notably `NOT_LEADER_OR_FOLLOWER`.

## Prerequisites

The lab cluster must already be operational with 6 brokers spread across 3 VMs and 5 KRaft controllers, as per Workshop 0. Systemd services must be available to cleanly stop and restart brokers during the regional outage simulation.

Quick check before starting:

```bash
/opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  describe --status

/opt/kafka/bin/kafka-topics.sh --describe \
  --bootstrap-server 10.18.0.5:9092 \
  --topic test-drp-lab
```

The quorum should be healthy and the `test-drp-lab` topic should already show leaders and replicas correctly distributed across the cluster's brokers.

## D1 — Unsynchronized Topic After a Partial Outage

This scenario directly addresses the incident reported by the client: a newly created topic that is not immediately synchronized across all nodes. The goal is not to reproduce corruption, but a realistic transient state where the topology is nominal at the metadata level while replication has not yet fully converged.

### D1.1 — Simulate a Partial Regional Outage

In the lab architecture, VM1 hosts brokers 1 and 2, which allows simulating a partial outage by stopping these two brokers as if part of the West region became unavailable. This logic is consistent with the DRP tests described in the questionnaire, where an entire region is cut off and then reintegrated to measure the reconstruction of leaders and ISRs.

```bash
# On VM1
sudo systemctl stop kafka-broker@1
sudo systemctl stop kafka-broker@2
```

Check from another VM that the cluster remains partially operational:

```bash
/opt/kafka/bin/kafka-topics.sh --describe \
  --bootstrap-server 10.118.0.5:9094 \
  --topic test-drp-lab
```

The cluster should remain available thanks to the replicas present on the other brokers, which well reflects the behavior already observed in production where applications mostly continue to operate during the failover, despite a temporary performance degradation and increased consumer lag.

### D1.2 — Create a Topic During the Outage

Now create a new topic while two brokers are offline:

```bash
/opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server 10.118.0.5:9094,10.128.0.5:9096 \
  --topic test-sync-issue \
  --partitions 6 \
  --replication-factor 3
```

Describe the topic immediately after creation:

```bash
/opt/kafka/bin/kafka-topics.sh --describe \
  --bootstrap-server 10.118.0.5:9094 \
  --topic test-sync-issue
```

In this scenario, the important point to observe is not just the successful creation of the topic, but the actual state of its partitions, leaders, replicas, and ISR while part of the cluster has not yet returned. This echoes the client's need to dig deeper into situations where a Kafka object appears to exist but has not yet fully converged across the whole topology.

### D1.3 — Reintegrate the Stopped Brokers

Restart the VM1 brokers:

```bash
# On VM1
sudo systemctl start kafka-broker@1
sudo systemctl start kafka-broker@2
```

Check the return of the brokers then describe the topic again:

```bash
/opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  describe --status

/opt/kafka/bin/kafka-topics.sh --describe \
  --bootstrap-server 10.18.0.5:9092 \
  --topic test-sync-issue
```

This phase helps explain that a broker returning does not mean an immediate return to an optimal state: replication must first catch up, the ISR must be rebuilt, and leaders may then eventually be rebalanced according to the chosen strategy. It is precisely this gap between "service restored" and "cluster back to optimal" that stands out strongly in the client's DRP observations.

### D1.4 — Detailed Diagnostic to Comment on During the Session

When analyzing the `--describe` output, have the group comment on:

- `Leader`: the broker currently responsible for reads/writes for the partition.
- `Replicas`: the theoretical list of brokers that should host the partition.
- `Isr`: the replicas actually synchronized at that moment.

If `Isr` is smaller than `Replicas`, this means replication has not yet fully converged, even though the topic is visible and usable. This diagnostic is fundamental to explaining why a cluster may appear "back" after an incident, while it continues to resynchronize its partitions in the background for several minutes, or even longer for large volumes.

### D1.5 — Guided Rebalancing with `kafka-reassign-partitions.sh`

The questionnaire indicates that this command is used only occasionally, making it a good practical topic in this troubleshooting context. The idea here is not to systematically impose a manual reassignment, but to show how to use it when the observed topology is not the expected one after restoration.

**Step 1 — Create the description file for the topic to move:**

```bash
cat > /home/kafka/test-sync-issue-topics.json <<'EOF'
{
  "topics": [
    {"topic": "test-sync-issue"}
  ],
  "version": 1
}
EOF
```

**Step 2 — Generate a reassignment proposal:**

```bash
/opt/kafka/bin/kafka-reassign-partitions.sh \
  --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  --topics-to-move-json-file /home/kafka/test-sync-issue-topics.json \
  --broker-list "1,2,3,4,5,6" \
  --generate
```

The command displays two JSON blocks on screen: `Current partition replica assignment` and `Proposed partition reassignment configuration`. Only the second block should be kept for the next step.

**Step 3 — Copy the "Proposed" block into a dedicated file:**

⚠️ **Caution point**: use `cat` with redirection rather than `nano`, to avoid any residual content from a previous file that would corrupt the final JSON:

```bash
cat > /home/kafka/test-sync-issue-reassignment.json <<'EOF'
{"version":1,"partitions":[{"topic":"test-sync-issue","partition":0,"replicas":,"log_dirs":["any","any","any"]},{"topic":"test-sync-issue","partition":1,"replicas":,"log_dirs":["any","any","any"]},{"topic":"test-sync-issue","partition":2,"replicas":,"log_dirs":["any","any","any"]},{"topic":"test-sync-issue","partition":3,"replicas":,"log_dirs":["any","any","any"]},{"topic":"test-sync-issue","partition":4,"replicas":,"log_dirs":["any","any","any"]},{"topic":"test-sync-issue","partition":5,"replicas":,"log_dirs":["any","any","any"]}]}[1][2][3][4][5][6]
EOF
```

**Step 4 — Validate JSON syntax with Python before execution:**

```bash
python3 -m json.tool < /home/kafka/test-sync-issue-reassignment.json
```

If the JSON is reformatted without error, the file is ready to use. Any parsing error at this stage (invalid character, missing brace) must be corrected before proceeding — running `--execute` directly on a corrupted file produces a Jackson error that is hard to interpret on the Kafka side.

**Step 5 — Execute the reassignment:**

```bash
/opt/kafka/bin/kafka-reassign-partitions.sh \
  --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  --reassignment-json-file /home/kafka/test-sync-issue-reassignment.json \
  --execute
```

**Step 6 — Check progress:**

```bash
/opt/kafka/bin/kafka-reassign-partitions.sh \
  --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  --reassignment-json-file /home/kafka/test-sync-issue-reassignment.json \
  --verify
```

**Step 7 — Visually confirm the result:**

```bash
/opt/kafka/bin/kafka-topics.sh --describe \
  --bootstrap-server 10.18.0.5:9092 \
  --topic test-sync-issue
```

Compare the `Replicas` column with the applied proposal, and verify that the `Isr` fully covers the expected replicas, confirming complete synchronization after reassignment.

This sub-section introduces an important discipline for production: not confusing the administrative visibility of a topic with the actually optimal distribution of replicas and leaders after an incident.

## D2 — Leader-Related Errors During and After DRP

The client explicitly requested adding scenarios around leader-related errors and preferred leader election after broker restoration. This section directly builds on observations already reported: increased leader elections, leader redistribution, higher consumer lag, and ISR reconstruction when a region returns.

### D2.1 — Identify a Partition Whose Leader Is in the West Region

Use the test topic already created in the environment:

```bash
/opt/kafka/bin/kafka-topics.sh --describe \
  --bootstrap-server 10.18.0.5:9092 \
  --topic test-drp-lab
```

Choose a partition whose `Leader` is broker 1 or 2, i.e., a VM1 broker in the lab topology. This choice is important because it allows triggering a visible and understandable leader change without multiplying variables.

### D2.2 — Stop the Leader Broker

Stop the broker holding the observed leader, for example broker 1:

```bash
sudo systemctl stop kafka-broker@1
```

Describe the topic again:

```bash
/opt/kafka/bin/kafka-topics.sh --describe \
  --bootstrap-server 10.118.0.5:9094 \
  --topic test-drp-lab
```

This step should reveal a new leader for at least one partition, as well as a potentially degraded transient replication state depending on load and timing. This is exactly the type of behavior the team already observes during its DRP tests, with significant leader redistribution and progressive ISR reconstruction.

### D2.3 — Observe the Impact on the Producer and Consumer Side

In one terminal, launch a continuous console producer:

```bash
/opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --topic test-drp-lab
```

In a second terminal, launch a consumer:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 10.118.0.5:9094 \
  --topic test-drp-lab \
  --from-beginning
```

During and right after stopping the leader, have the group note the possible symptoms: increased latency, producer-side retries, possible metadata refresh messages, and temporary consumer-side lag. The teaching goal is to help participants relate an error message or increased lag not to a global cluster failure, but to a specific leader change and replica reconstruction event.

### D2.4 — Restart the Broker and Observe Resynchronization

```bash
sudo systemctl start kafka-broker@1
```

Then monitor:

```bash
watch -n 5 "/opt/kafka/bin/kafka-topics.sh --describe --bootstrap-server 10.18.0.5:9092 --topic test-drp-lab"
```

This step is essential to show that the broker's return does not automatically trigger an immediate return of leadership to it. Replication must first catch up and the ISR must be rebuilt, which allows concretely linking the concepts of leader election, catch-up replica, and return to the preferred state.

### D2.5 — Force a Return to the Preferred Leader

The questionnaire shows explicit interest in `kafka-leader-election.sh --election-type preferred` after broker restoration. This command therefore fits naturally here as a direct continuation of the DRP scenario.

Example targeted at one partition:

```bash
/opt/kafka/bin/kafka-leader-election.sh \
  --bootstrap-server 10.18.0.5:9092 \
  --topic test-drp-lab \
  --partition 0 \
  --election-type preferred
```

Or on several partitions by preparing a JSON file adapted to the exercise's needs. After the election, describe the topic again to check whether leadership has returned to the preferred replica once replication is healthy again.

### D2.6 — Guided Discussion Point

This discussion should bring out three ideas:

- An automatic leader election after a failure restores availability, but not necessarily the preferred leader distribution.
- A broker's return does not mean its partitions immediately become leaders or complete ISRs again.
- Keeping `unclean.leader.election.enable=false` protects data consistency but can lengthen certain recovery times, which corresponds to the choice already made in production by the team.

## D3 — Transition to More Targeted Incidents

This final sequence serves as a bridge to a future, more specialized troubleshooting workshop, without overly burdening Workshop 2. It also links DRP observations to finer-grained application errors that often occur right after a leader change or a stale metadata phase.

### D3.1 — Introduction to `NOT_LEADER_OR_FOLLOWER`

The `NOT_LEADER_OR_FOLLOWER` error typically appears when a client still tries to talk to a broker that is no longer leader — or no longer a valid replica for the targeted partition — due to a recent topology change. This phenomenon is directly linked to DRP scenarios, broker maintenance, and client-side metadata refresh.

### D3.2 — What Will Be Covered in the Follow-Up Workshop

Announce that the next workshop will separately cover in depth:

- Frequent broker connection errors, already mentioned in the questionnaire as a recurring incident.
- Consumer offsets to reset after reloading a SQL Server source, a topic consistent with the Kafka Connect and JDBC environment requested by the client.
- Controlled reproduction of a client-side `NOT_LEADER_OR_FOLLOWER` to understand how metadata refreshes and why these errors are often transient.

## Expected Results

By the end of this Part D, participants will have reproduced two families of incidents very close to their operational reality: incomplete synchronization of a topic after an incident, and the visible effects of a leader change during a DRP scenario. They will also have a clear thread linking symptoms observed in AKHQ or client logs, diagnostic CLI commands, and corrective actions such as reassignment or preferred leader election.

## Cleanup

Delete the demonstration topic if needed:

```bash
/opt/kafka/bin/kafka-topics.sh --delete \
  --bootstrap-server 10.18.0.5:9092 \
  --topic test-sync-issue
```

Verify that all brokers stopped for the demonstration are back up before moving on to the next workshop:

```bash
sudo systemctl status kafka-broker@1 kafka-broker@2

/opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  describe --status
```

The cluster must be back to a healthy state before opening the next workshop, to avoid a degraded lab state polluting subsequent teaching observations.

***

## Summary Table to Complete During the Workshop

| Scenario | Rebalancing duration | Comment |
| :-- | :-- | :-- |
| Baseline (no optimization) | ___ sec |  |
| With forced preferred leader election | ___ sec |  |
| With adjusted throttling | ___ sec |  |

***

## Cleanup After the Workshop

**Machine: VM1**

```bash
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server 10.118.0.5:9094 \
  --alter --entity-type brokers --entity-name 3 \
  --delete-config leader.replication.throttled.rate,follower.replication.throttled.rate
```

**All VMs** — restore the VMware snapshot taken after Workshop 0, to start with a clean cluster for the next group of participants.

***

This test faithfully reproduces the 8 steps of the DRP procedure documented by the client, while isolating each optimization variable (preferred leader election, throttling) to objectively show their real impact on recovery time — the central question raised in D6/D7 of the questionnaire.
