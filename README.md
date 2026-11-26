# redborder-spooler

High-performance traffic capture, disk-backed spooling, and accelerated catch-up replay daemon for the **redBorder** platform.

---

## Overview

During software release updates, system upgrades, or maintenance windows, core ingestion services (`x2kafka`, `http2k`, Kafka, Druid, etc.) are stopped and restarted repeatedly. External network devices (routers, switches, firewalls, and remote probes) continuously emit live telemetry.

**`redborder-spooler`** (`rb-spooler`) ensures **zero data loss**:
1. **Passively Captures Telemetry**: Ingests incoming **NetFlow (v5/v9/IPFIX)**, **sFlow (v5)**, **Syslog (UDP port 514)**, and **HTTP data** across all network interfaces without interfering with running collectors. Includes a dedicated **TCP Syslog spooler** (port 514/10514) supporting standard streams and RFC 6587 octet counting.
2. **Spools to Time-Partitioned Storage**: Writes datagrams and payloads into UTC-partitioned hourly directories (`YYYY-MM-DD/HH/`) protected by CRC32 checksums, minute-level indexes (`udp.idx`), template snapshots (`templates.snap`), and framed WAL files (`udp.wal`, `http.wal`, `syslog.wal`).
3. **Accelerated Catch-Up Replay**: Replays buffered traffic from any specific Date + Hour at accelerated speed (e.g., `5x`, `10x`, `20x`, or unthrottled `max`), preserving the **Manager's exact UTC arrival timerange** and original **`sensor_ip`** via the companion `RBRP` envelope.

---

## Architecture

```mermaid
flowchart TD
    subgraph Ingestion["1. Capture Layer (Manager UTC Clock)"]
        UDP_IN["Incoming UDP (NetFlow 2055, sFlow 6343)"]
        HTTP_IN["Incoming HTTP (POST /rbdata/...)"]

        UDP_TAP["AF_PACKET BPF Capture (Passive Tap)\nOR Direct UDP Socket Listener"]
        HTTP_PROXY["HTTP Tee / Spooling Proxy\n(Stamps Manager UTC 'timestamp')"]

        UDP_IN --> UDP_TAP
        HTTP_IN --> HTTP_PROXY
        UDP_TAP --> DEDUP["LVS-DR Dedup Ring (20ms sliding window)"]
    end

    subgraph Storage["2. Time-Partitioned UTC Storage (/var/spool/redborder-spooler)"]
        DEDUP --> WRITER["Hourly Segment Writer + Template Tracker"]
        HTTP_PROXY --> WRITER

        WRITER --> B1["2026-11-24/08/ (UTC)\n├── templates.snap\n├── udp.wal (CRC32)\n├── udp.idx (minute index)\n├── http.wal\n└── meta.json"]
        WRITER --> B2["2026-11-24/09/ (UTC)\n├── templates.snap\n├── udp.wal\n├── udp.idx\n├── http.wal\n└── meta.json"]

        RETENTION["Retention Engine (MaxAge, MaxStorageMB, MinFreeDiskPct)"] -.->|Prunes oldest hours| B1
    end

    subgraph Replay["3. Point-in-Time Accelerated Replay Engine"]
        B1 & B2 -->|--from '2026-11-24 08:00' --speed 10x| READER["UTC Time-Range Reader"]
        READER -->|1. Pre-transmit templates.snap\n2. Paced RBRP Envelope (CaptureTimeUTC + ExporterIP)| X2K_UDP["x2kafka UDP (:2055, :6343)"]
        READER -->|Concurrent HTTP POST Replay (with original UTC timestamp)| X2K_HTTP["x2kafka / http2k HTTP (:8080)"]
    end
```

---

## Key Features

- **Multi-Interface Passive Tap**: Uses Linux raw `AF_PACKET` sockets (`ifindex = 0` / `"any"`) with kernel-level BPF filters to monitor all physical, bonded (`bond0`), and VLAN interfaces simultaneously without port conflicts or binding collisions.
- **Cluster Mode (`keepalived` + LVS Direct Routing)**:
  - Includes a 20ms sliding-window deduplication ring that inspects packet direction (`PACKET_HOST` vs `PACKET_OUTGOING`).
  - When the VRRP Master forwards a packet to a Backup node via `lvs_method DR`, the Master cancels the staged frame locally, ensuring **zero duplicate recordings across the cluster**.
  - During a full cluster release update (when all collectors are down), the VRRP Master automatically captures **100% of the VIP traffic**.
- **Replay Loop Self-Protection (Zero Duplication)**:
  - Both `AF_PACKET` raw socket capture and direct socket listeners automatically ignore loopback traffic (`127.0.0.0/8`, `::1`) and packets carrying the `RBRP` replay envelope.
  - The HTTP spooler automatically ignores requests carrying `X-Redborder-Replay: true`.
  - Even if `replay` is run while the daemon is actively capturing on `any`, **replayed packets are never re-recorded or duplicated**.
- **Template Snapshotting (`templates.snap`)**:
  - Automatically identifies and caches NetFlow v9 (FlowSet IDs `0`, `1`) and IPFIX (Set IDs `2`, `3`) template sets per exporter.
  - Saves a cumulative snapshot at the start of every hourly bucket.
  - When replaying from any historical hour, `rb-spooler` **pre-transmits `templates.snap` first**, guaranteeing 100% immediate flow decoding without waiting for external routers to re-send templates.
- **Manager UTC Timeline Preservation (`RBRP` Replay Envelope)**:
  - External router clocks are never trusted for event `"timestamp"` (routers often have drifting NTP or wrong timezone offsets; sFlow v5 lacks wall-clock timestamps altogether).
  - Every packet is stamped with the **Manager's UTC clock** at capture time.
  - On localhost replay, packets are wrapped in a 28-byte `"RBRP"` header (`Magic + ManagerCaptureTimeUTC + OriginalExporterIP`).
  - When `x2kafka` receives `"RBRP"`, it restores the true historical capture time and original `sensor_ip`—filling in maintenance gaps seamlessly in Druid without causing artificial 10x traffic spikes.
- **HTTP Spooler & Reverse Proxy**:
  - Accepts `POST /rbdata/...` bulk metrics from remote forwarders (`k2http` / `rbforwarder`).
  - Stamps `"timestamp"` (if missing), logs the request body to `http.wal`, and returns `202 Accepted` immediately so remote clients never time out or drop connections during updates.
- **Automated Retention Management**:
  - Periodically enforces `max_age` (e.g. 72h), `max_storage_mb` (disk cap), and `min_free_disk_pct` (host OS protection via `statfs`).
  - **Guards the active current hour**: The running hour is strictly protected from eviction.

---

## Configuration (`/etc/redborder-spooler/config.yaml`)

```yaml
spool_dir: "/var/spool/redborder-spooler"
meta_sync_interval: "5s"           # Periodic flush of active hour meta.json / udp.idx (default: 5s)

capture:
  interfaces:
    - "any"
  udp_ports:
    - 2055    # NetFlow / IPFIX
    - 6343    # sFlow
    - 514     # Syslog (passive tap)
  mode: "af_packet"                # "af_packet" (passive tap) or "socket" (direct UDP bind)
  lvs_dr_dedup_window_ms: 20       # Cluster mode LVS-DR PACKET_OUTGOING cancellation window
  socket_buffer_bytes: 33554432    # 32MB socket receive buffer
  http:
    enabled: true
    listen_addr: ":18080"
    upstream_url: ""               # Optional upstream URL for continuous live teeing
  syslog_tcp:
    enabled: false                 # Enable dedicated TCP Syslog spooler
    listen_addr: ":10514"
    upstream_addr: ""              # Optional upstream forwarder address

retention:
  max_age: "72h"                   # Keep 3 days of historical hourly buckets
  max_storage_mb: 20480            # 20 GB spool limit
  min_free_disk_pct: 15.0          # Prune oldest hours if disk free space < 15%
  check_interval: "5m"

replay:
  target_host: "127.0.0.1"
  default_speed: 10.0              # 10x accelerated replay
  use_rbrp_envelope: true          # Send 28-byte RBRP envelope to localhost
  http_target_url: "http://127.0.0.1:8080"
```

---

## Nginx Integration for http2k (HTTP Ingestion)

In redBorder clusters, HTTP bulk telemetry is received by Nginx on port 443 and proxied to `http2k` (:7980). To ensure zero dropped events during service updates or maintenance windows, Nginx can integrate with `rb-spooler` (:18080) in two modes:

1. **Option 1: Failover Backup Upstream (`backup`) [Recommended]**:
   - Zero disk writes on spooler during normal operation.
   - When `http2k` is stopped, Nginx seamlessly forwards POST requests to `rb-spooler` using `proxy_next_upstream ... non_idempotent;`.
   - Spooled events are replayed once `http2k` is back online.
2. **Option 2: Live Traffic Mirroring (`mirror`)**:
   - Uses `ngx_http_mirror_module` to clone every incoming live request to `rb-spooler` in parallel, maintaining a continuous rolling historical archive.

For complete Nginx configuration snippets, RFC 7230 failover explanations, and verification steps, see **[docs/nginx-http2k.md](docs/nginx-http2k.md)**.

---

## Syslog & rsyslog Integration

`redborder-spooler` buffers Syslog telemetry across maintenance windows via two distinct mechanisms:
- **UDP Port 514 (Passive)**: Captured via `AF_PACKET` kernel tap concurrently with `rsyslog` without socket conflicts. When replayed, RBRP encapsulation is bypassed for port 514 to deliver raw logs.
- **TCP Port 514/10514 (TCPSyslogSpooler)**: A dedicated spooler supporting both traditional newline-delimited streams and RFC 6587 octet counting. Drops replayed streams to prevent feedback loops, with optional asynchronous upstream forwarding.

For full architecture details, maintenance mode cutover instructions, and testing with `logger`, see **[docs/rsyslog-integration.md](docs/rsyslog-integration.md)**.


---

## CLI Commands

### 1. Run Recording Daemon
```bash
# Start background spooler (captures UDP + HTTP + runs retention loop)
rb-spooler record --config /etc/redborder-spooler/config.yaml
```

### 2. List Recorded Hourly Buckets
```bash
rb-spooler list --config /etc/redborder-spooler/config.yaml

# Output:
# Stored Traffic Buckets in /var/spool/redborder-spooler (2 hours):
# Available Data Range:   2026-11-24 10:32:44 UTC to 2026-11-24 11:45:12 UTC
#
# HOUR (UTC)          TRAFFIC PKTS  TRAFFIC BYTES HTTP REQS     SYSLOG MSGS   DISK SIZE     TEMPLATES   EXPORTERS
# -------------------------------------------------------------------------------------------------------------
# 2026-11-24/10       1450230       1240000000    12400         5200          1.3 GB        18          5
# 2026-11-24/11       980100        820000000     8900          4100          892.4 MB      18          5
# -------------------------------------------------------------------------------------------------------------
# Total: 2430330 traffic pkts (1.9 GB payload) | 21300 HTTP reqs | 9300 Syslog msgs | Disk: 2.2 GB across 2 buckets
#
# (EXPORTERS indicates the count of unique source IPs / network devices sending traffic)

# Verbose listing (shows exact exporter IP addresses per hour):
rb-spooler list -v

# Output with -v:
# HOUR (UTC)          TRAFFIC PKTS  TRAFFIC BYTES HTTP REQS     SYSLOG MSGS   DISK SIZE     TEMPLATES   EXPORTERS
# -------------------------------------------------------------------------------------------------------------
# 2026-11-24/10       1450230       1240000000    12400         5200          1.3 GB        18          3
#    └─ Exporters: 10.1.50.20, 10.1.50.21, 10.0.10.1
# 2026-11-24/11       980100        820000000     8900          4100          892.4 MB      18          2
#    └─ Exporters: 10.1.50.20, 10.1.50.21
# -------------------------------------------------------------------------------------------------------------
# Total: 2430330 traffic pkts (1.9 GB payload) | 21300 HTTP reqs | 9300 Syslog msgs | Disk: 2.2 GB across 2 buckets
```

### 3. Replay Traffic (Catch-Up Mode)
```bash
# Replay UDP flow traffic starting from a specific Date + Hour at 10x speed to localhost (default --type traffic)
rb-spooler replay --from "2026-11-24 10:00" --speed 10x

# Replay with traffic UDP ports filter (e.g. only NetFlow 2055 and sFlow 6343)
rb-spooler replay --from "2026-11-24 10:00" --ports 2055,6343 --speed 10x

# Replay Syslog records (UDP to 514 UDP and TCP to 514 TCP) to rsyslog
rb-spooler replay --from "2026-11-25 08:00" --type syslog --syslog-target 127.0.0.1:514

# Replay HTTP events only (e.g. to redborder http2k proxy)
rb-spooler replay --from "2026-11-25 08:00" --type http --http-target http://127.0.0.1:7980

# Replay all traffic (UDP, HTTP, and Syslog) non-interactively (-y / --yes)
rb-spooler replay --from "2026-11-25 08:00" --type all --http-target http://127.0.0.1:7980 --syslog-target 127.0.0.1:514 -y

# Replay directly to another manager or remote collector IP
rb-spooler replay --from "2026-11-24 10:00" --speed 10x --target 10.0.209.20

# Replay an exact maintenance window at 20x speed
rb-spooler replay --from "2026-11-24 10:15" --to "2026-11-24 10:45" --speed 20x

# Replay unthrottled (maximum wire speed burst)
rb-spooler replay --from "2026-11-24 10:00" --speed 0

# Replay catch-up and continuously follow live incoming traffic
rb-spooler replay --from "2026-11-24 10:00" --speed 10x --follow --target 10.0.209.20
```

> **Note on Pre-Flight Action Summary & Confirmation**:
> By default, `replay` prints a pre-flight summary with the time range, replay type (`UDP`, `HTTP`, or `ALL`), target destination(s), speed, and estimated payload (packet/request counts), then prompts for confirmation (`Proceed with replay? [y/N]: `). In automated scripts, pass `-y` or `--yes` to proceed without interactive confirmation.


### 4. Check Spool Metrics & Status
```bash
rb-spooler status --config /etc/redborder-spooler/config.yaml

# Output:
# redborder-spooler Status:
#   Daemon State:          RUNNING (pid: 24102, uptime: 3h 12m)
#   Control Socket:        /run/redborder-spooler/control.sock
#   Recording Listeners:
#     TRAFFIC:     [ENABLED ]  af_packet (ports 2055,6343)   (NetFlow / IPFIX / sFlow capture)
#     SYSLOG_UDP:  [ENABLED ]  af_packet (port 514)          (Passive UDP Syslog capture)
#     SYSLOG_TCP:  [DISABLED]  :10514                        (RFC 6587 / newline TCP Syslog spooler)
#     HTTP:        [DISABLED]  :18080                        (HTTP event ingest spooler)
#
#   Spool Directory:       /var/spool/redborder-spooler
#   Stored Hourly Buckets: 2
#   First Available Data:  2026-11-24 10:32:44 UTC
#   Latest Available Data: 2026-11-24 11:45:12 UTC
#   Total Traffic Packets: 2430330 (2060000000 bytes, 1.9 GB payload)
#   Total HTTP Requests:   21300
#   Total Syslog Messages: 9300
#   Total Disk Usage:      2.2 GB (2362232012 bytes)
#   Retention Max Age:     72h0m0s
#   Retention Max Storage: 20480 MB
#   Min Free Disk %:       15.0%
```

### 5. Dynamic Runtime Control (`ctl`)
Control recording components dynamically via UNIX domain socket without modifying config or restarting:
```bash
# Check live daemon state and recording listeners
rb-spooler ctl status

# Dynamically enable/disable individual components during maintenance
rb-spooler ctl enable syslog_tcp
rb-spooler ctl disable syslog_tcp

rb-spooler ctl enable http
rb-spooler ctl disable http

rb-spooler ctl disable traffic   # Pauses flow packet capture
rb-spooler ctl enable traffic    # Resumes flow packet capture

# Toggle all listeners at once
rb-spooler ctl enable all
rb-spooler ctl disable all
```

### 6. Export Recorded Traffic to Archive
```bash
# Export all buckets from a time range into a compressed tar.gz bundle
rb-spooler export --from "2026-11-24 10:00" --to "2026-11-24 12:00" --output /tmp/traffic-backup.tar.gz

# Export with custom spool directory override
rb-spooler export --spool-dir /custom/spool --from "2026-11-24 10:00" --output /tmp/traffic.tar.gz
```

### 7. Import Traffic Archive on Another Manager
```bash
# Import traffic archive into local spool directory
rb-spooler import --input /tmp/traffic-backup.tar.gz

# Import and overwrite existing hourly files
rb-spooler import --input /tmp/traffic-backup.tar.gz --force
```

### 7. Convert Recorded Data to Standard PCAP
```bash
# Export recorded UDP traffic to standard PCAP format for Wireshark / tcpdump analysis
rb-spooler pcap --from "2026-11-24 10:00" --to "2026-11-24 11:00" --output /tmp/capture.pcap

# Inspect with tcpdump
tcpdump -nn -r /tmp/capture.pcap
```

### 8. Manually Prune Old Storage
```bash
rb-spooler prune --config /etc/redborder-spooler/config.yaml
```

---

## Storage & Disk Performance Sizing

### 1. Disk I/O Overhead: Very Low
* **Pure Sequential Append**: During live recording, the daemon only performs sequential appends to `udp.wal` (no random seeks, no database locks, no B-tree indexing).
* **Linux Page Cache**: The kernel coalesces writes into large 128 KB – 2 MB I/O blocks.
* Even under heavy load of **20,000 packets/sec (~26 MB/s)**:
  * NVMe SSDs (1,500–3,500 MB/s) experience less than **1% write load**.
  * Standard SATA hard drives (150–200 MB/s sequential) experience less than **15% write load**.
* Disk throughput or IOPS is rarely a bottleneck.

### 2. Capacity Sizing Guidelines

Each UDP record incurs a 56-byte header + CRC32, averaging **~1.3 KB to 1.5 KB per packet**:

| Ingestion Rate | Estimated Flows/sec | Write Throughput | Hourly Storage | 24-Hour Storage |
|---|---|---|---|---|
| **Moderate** (1,000 pkts/s) | ~30,000 flows/s | ~1.3 MB/s (10 Mbps) | **~4.7 GB / hr** | ~112 GB / day |
| **High** (5,000 pkts/s) | ~150,000 flows/s | ~6.5 MB/s (52 Mbps) | **~23 GB / hr** | ~550 GB / day |
| **Very High** (20,000 pkts/s) | ~600,000 flows/s | ~26 MB/s (208 Mbps) | **~93 GB / hr** | ~2.2 TB / day |

### 3. Built-in Retention Safeguards

To prevent disk exhaustion on high-volume managers, the retention engine continuously enforces three thresholds:
1. **`max_storage_mb` (Hard Capacity Limit)**: Caps the total spool size (e.g. `20480` = 20 GB). The oldest completed hourly buckets are automatically deleted once the threshold is crossed.
2. **`min_free_disk_pct` (Emergency Protection)**: Defaults to `15.0%`. If free disk space on the partition falls below this percentage, older buckets are pruned aggressively to protect system health.
3. **`max_age` (Time Window)**: Deletes buckets older than the configured duration (e.g. `72h` or `12h`).

### 4. Production Best Practices
* **Dedicated Mount Point**: Mount `/var/spool/redborder-spooler` on its own disk or partition to ensure recording traffic never contends with the root `/` or logging `/var/log` partitions.
* **Maintenance Buffer Tuning**: If the spooler is deployed primarily to cover service restarts and upgrade maintenance windows, configure `max_age: "12h"` or `"24h"` with an appropriate `max_storage_mb` ceiling to keep the spool compact.

---

## Release Update Runbook

When performing a software update on a redBorder node or cluster:

1. **Before stopping services**: Ensure `redborder-spooler` is active:
   ```bash
   sudo systemctl start redborder-spooler
   ```
2. **Execute release update**: Stop, upgrade, and restart `x2kafka`, `http2k`, `kafka`, and other services as needed. All wire traffic arriving on the interfaces is safely spooled to disk.
3. **Verify services are healthy**: Confirm `x2kafka` and `kafka` are running normally.
4. **Trigger Catch-Up Replay**: Replay the recorded window at accelerated speed (the spooler can stay running—its built-in loopback/RBRP filters automatically prevent re-recording replayed packets):
   ```bash
   rb-spooler replay --from "2026-11-24 08:00" --speed 10x
   ```
5. Once replay completes, all historical traffic is indexed in Druid at its true UTC timestamp, and real-time operations continue seamlessly.

---

## Building & Packaging

### Local Build & Test
```bash
# Build binary
make build

# Run unit and race tests
make test
```

### RPM Build with Mock
The repository follows standard redBorder packaging practices using `mock`:

```bash
# Build Source RPM (SRPM) and Binary RPM using mock
make rpm

# Or build SRPM only
make srpm
```

RPM packages are generated under `packaging/rpm/pkgs/`.

---

## License

GNU AGPLv3. See [LICENSE](LICENSE) for details.
