## Issues to Fix

1. `/home/kafka/logs` likely already exists (it's the mount point for `sda-sdd` per your `df -h` output) — creating/chowning it is harmless but unnecessary; skip it if it already belongs to `kafka:kafka`.
2. The two `Environment=` lines must be on **separate lines** in the systemd unit file, not on the same line — systemd will fail to parse them combined.
3. This only fixes controller **1002**. Since the incident could hit any controller, you should apply this to all 5 (1001-1005) before tonight.

## Corrected Steps

**1. Check logs directory ownership (skip mkdir if already correct)**

```bash
ls -ld /home/kafka/logs
```

**2. Edit the systemd service file**

```bash
sudo nano /etc/systemd/system/kafka-controller1002.service
```

**3. Add these as two separate lines under `[Service]`**

```ini
Environment="KAFKA_HEAP_OPTS=-Xmx4G -Xms4G"
Environment="KAFKA_JVM_PERFORMANCE_OPTS=-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -Xlog:gc*:file=/home/kafka/logs/controller1002-gc.log:time,uptime:filecount=10,filesize=100M"
```

**4. Reload and restart**

```bash
sudo systemctl daemon-reload
sudo systemctl restart kafka-controller1002
```

**5. Verify the change was applied**

```bash
ps aux | grep java | grep controller1002 | grep -o '\-Xmx[0-9]*G\|\-Xlog:gc[^ ]*'
```

**6. Confirm the controller rejoined the quorum before moving to the next one**

```bash
/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server <host>:9092 describe --status
```

**7. Repeat steps 2-6 for controllers 1001, 1003, 1004, 1005**, restarting one at a time and re-checking quorum health between each restart — never restart all controllers simultaneously.

