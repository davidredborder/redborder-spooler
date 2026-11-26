# Nginx Configuration for http2k (HTTP Telemetry Ingestion)

In redBorder clusters, HTTP event telemetry (such as JSON bulk metrics sent by `k2http`, `rbforwarder`, or custom HTTP exporters) is received on port 443 by Nginx, which terminates TLS and forwards requests to `http2k` running on port 7980.

During maintenance windows, service upgrades, or `http2k` restarts, external forwarders continuously send live HTTP POST requests. If `http2k` is stopped, standard Nginx configurations return `502 Bad Gateway`, causing remote forwarders to drop connections or fail.

Integrating Nginx with `rb-spooler` (listening on `:17980`) provides zero-data-loss ingestion.

---

## Architecture Overview

```
                      ┌───────────────────────────────────────────────┐
                      │              redBorder Node                   │
                      │                                               │
Remote Exporters      │   Nginx (:443)                                │
 (k2http / Probes) ───┼─► ├─► Primary:  http2k (:7980)                │
   [HTTP POST]        │   └─► Backup/Mirror: rb-spooler (:17980)
                      │                            │                  │
                      │                            ▼                  │
                      │                   Spool: /var/spool/.../      │
                      │                            http.wal           │
                      └───────────────────────────────────────────────┘
```

---

## Option 1: Failover Backup Upstream (Recommended for Maintenance & Upgrades)

### Behavior
* **Normal Operation**: 100% of traffic goes to `http2k` (:7980). **Zero disk writes** on `rb-spooler`.
* **During Maintenance / Upgrades**: When `http2k` is stopped, Nginx automatically and transparently forwards incoming HTTP POST requests to `rb-spooler` (:17980).
* `rb-spooler` responds with `202 Accepted` immediately and writes requests to `http.wal`.
* **Post-Maintenance**: An administrator replays the spooled HTTP requests back to `http2k`.

### Nginx Configuration (`/etc/nginx/conf.d/http2k.conf`)

```nginx
upstream http2k {
    server 127.0.0.1:7980 max_fails=3 fail_timeout=5s;
    server 127.0.0.1:17980 backup;   # Failover to rb-spooler when http2k is down
}

server {
    listen 443 ssl http2;
    listen [::]:443 ssl http2;

    server_name http2k.redborder.cluster http2k.service.redborder.cluster;

    ssl_certificate /etc/nginx/ssl/http2k.crt;
    ssl_certificate_key /etc/nginx/ssl/http2k.key;
    ssl_session_timeout 10m;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers EECDH+CHACHA20:EECDH+AES128:RSA+AES128:EECDH+AES256:RSA+AES256:!MD5;
    ssl_prefer_server_ciphers on;

    client_max_body_size 512m;

    location / {
        proxy_pass http://http2k;
        proxy_redirect http://http2k /;

        # =====================================================================
        # CRITICAL DIRECTIVE FOR HTTP POST FAILOVER:
        # RFC 7230 and default Nginx behavior forbid retrying failed POST requests
        # to backup upstreams because POST is non-idempotent. Adding 'non_idempotent'
        # allows Nginx to seamlessly retry POST requests against the backup server.
        # =====================================================================
        proxy_next_upstream error timeout http_502 http_503 non_idempotent;
        proxy_next_upstream_tries 3;
        proxy_next_upstream_timeout 10s;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
        proxy_pass_request_headers on;

        proxy_connect_timeout  10s;
        proxy_send_timeout     90s;
        proxy_read_timeout     90s;

        proxy_buffer_size          128k;
        proxy_buffers              4 256k;
        proxy_busy_buffers_size    256k;
        proxy_temp_file_write_size 256k;
    }
}
```

### Verification & Testing
1. Test Nginx syntax and reload:
   ```bash
   sudo nginx -t && sudo systemctl reload nginx
   ```
2. Stop `http2k`:
   ```bash
   sudo systemctl stop http2k
   ```
3. Send a test POST request to Nginx:
   ```bash
   curl -k -i -X POST https://127.0.0.1/api/v1/events -H "Content-Type: application/json" -d '{"test":"failover"}'
   ```
   **Expected Response**: `HTTP/1.1 202 Accepted` from `rb-spooler`.
4. Inspect the active hour spool:
   ```bash
   rb-spooler list
   ```
   Verify `HTTP REQS` incremented.
5. Restart `http2k`:
   ```bash
   sudo systemctl start http2k
   ```
6. Replay spooled HTTP records back to `http2k`:
   ```bash
   rb-spooler replay --from "2026-11-25 08:00" --type http --http-target http://127.0.0.1:7980 -y
   ```

---

## Option 2: Live Traffic Mirroring (Continuous Dual Ingestion)

### Behavior
* Uses `ngx_http_mirror_module` to duplicate every incoming request in the background.
* `http2k` receives and processes the request normally.
* `rb-spooler` asynchronously receives a clone of the request, storing an ongoing rolling archive governed by `max_age` and `max_storage_mb`.
* If `http2k` crashes or experiences data corruption, past traffic can be replayed for any historical window.

### Nginx Configuration (`/etc/nginx/conf.d/http2k.conf`)

```nginx
upstream http2k_backend {
    server 127.0.0.1:7980;
}

upstream rb_spooler {
    server 127.0.0.1:17980;
}

server {
    listen 443 ssl http2;
    listen [::]:443 ssl http2;

    server_name http2k.redborder.cluster;

    ssl_certificate /etc/nginx/ssl/http2k.crt;
    ssl_certificate_key /etc/nginx/ssl/http2k.key;
    client_max_body_size 512m;

    location / {
        # Duplicate live traffic asynchronously to internal mirror location
        mirror /mirror_to_spooler;
        mirror_request_body on;

        proxy_pass http://http2k_backend;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
    }

    location = /mirror_to_spooler {
        internal;
        proxy_pass http://rb_spooler$request_uri;
        proxy_pass_request_body on;
        proxy_pass_request_headers on;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;

        # Short timeouts so spooler never slows down client requests
        proxy_connect_timeout 200ms;
        proxy_read_timeout 1s;
        proxy_send_timeout 1s;
        proxy_ignore_client_abort on;
    }
}
```

---

## Feature Comparison

| Feature | Option 1: Failover Backup (`backup`) | Option 2: Live Mirroring (`mirror`) |
|---|---|---|
| **Disk I/O** | Zero writes during normal operation; writes only during maintenance | Continuous rolling disk writes |
| **Spool Growth** | Minimal (proportional to maintenance duration) | Matches retention policy (`max_storage_mb`, `max_age`) |
| **Replay Action** | Mandatory after maintenance (`--type http`) | Only needed for disaster recovery or backfill |
| **Primary Use Case** | Node software updates, package upgrades, service restarts | Continuous audit trail, parallel testing, disaster recovery |
