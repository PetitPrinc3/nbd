# Production Setup Recommendations

NBD relays what it receives, with no authentication of the multicast feed. These recommendations assume a trusted multicast segment and a host patched according to your distribution's security advisories. See [SECURITY.md](SECURITY.md) for the threat model.

## Build

Use a TLS build unless NBD and the broker share a trusted segment. The default build and the released binaries have no TLS support.

```bash
cargo build --locked --release --features kafka-tls
```

Add `metrics-exporter` to get [metrics](#metrics). Without it, nothing counts dropped datagrams, but the hot path is lighter: see [PERFORMANCE.md](PERFORMANCE.md#hot-path).

## Host

### Privileges

NBD needs no root access and no Linux capability, provided that:

- provider ports are above 1023
- `net.core.rmem_max` allows `socket_buffer_size`
- the kernel is 5.7 or later, which allows `SO_BINDTODEVICE` without `CAP_NET_RAW`

### File layout

| Path | Owner | Mode | Notes |
|---|---|---|---|
| `/opt/nbd/`, `/etc/nbd/tls/` | `root` | `0755` | No write access for group or others |
| `/opt/nbd/nbd` | `root` | `0755` | |
| `/opt/nbd/config.toml` | `root` | `0644` | No literal secret |
| CA and client certificates | `root` | `0644` | |
| Private key, keytab, secret files | Service user | `0400` | |

A secret file must be readable by its owner only, so the service user has to own it. See [File permissions](CONFIGURATION.md#file-permissions).

### systemd

```ini
[Unit]
Description=Nothing But Data
Wants=network-online.target
After=network-online.target

[Service]
ExecStart=/opt/nbd/nbd --config-file /opt/nbd/config.toml
Restart=on-failure
RestartSec=5

DynamicUser=true
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
ReadOnlyPaths=/opt/nbd
CapabilityBoundingSet=
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

`Restart=on-failure` brings NBD back after it gives up on a [producer recovery](#producer-recovery).

`DynamicUser=true` can't own secret files, since the UID is allocated at each run. To supply secrets, either:

- replace it with a static user (`User=nbd`, `Group=nbd`) that owns the secret files, or
- keep it and use `{ env = ... }` sources fed by `EnvironmentFile=`, a root-owned `0600` file read by systemd.

`LoadCredential=` looks like a better fit for `DynamicUser=true`, with a `{ file = ... }` source pointing at `/run/credentials/<unit>/<name>`, but it hasn't been tested with NBD.

These directives further reduce the attack surface. They haven't been tested with NBD, so validate them in staging:

```ini
RestrictAddressFamilies=AF_INET AF_INET6
LimitCORE=0
MemoryDenyWriteExecute=true
LockPersonality=true
RestrictRealtime=true
RestrictSUIDSGID=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
SystemCallFilter=@system-service
```

`LimitCORE=0` keeps credentials out of core dumps. `RestrictAddressFamilies` also applies to name resolution: a `broker` given as a hostname may need `AF_UNIX` and `AF_NETLINK`. Kerberos runs `kinit` through a shell and needs a writable ticket cache.

### Kernel

NBD exits at startup if the kernel grants less than `socket_buffer_size`. The limit is `net.core.rmem_max`, usually 212,992 bytes. Raise it before raising the buffer:

```bash
sudo tee /etc/sysctl.d/99-nbd-sysctl.conf <<'EOF'
net.core.rmem_max = 16777216
net.core.netdev_max_backlog = 10000
EOF
sudo sysctl --system
```

`net.core.rmem_default` has no effect on NBD, and raising it enlarges every other socket on the host.

## Network

### Multicast ingress

The multicast segment is the trust boundary: anyone who can send to a group can inject messages, and UDP source addresses can be spoofed.

- Keep producers and NBD on a dedicated, non-routed VLAN or subnet
- Enable IGMP/MLD snooping on the switches
- Apply ingress ACLs at every router or firewall facing an untrusted zone
- Allow inbound UDP on the provider ports
- If nothing arrives, check `rp_filter` on the interface

On Linux, each socket is bound to its group and interface, with `IP_MULTICAST_ALL` disabled. This limits cross-talk between feeds on a multi-homed host. It doesn't authenticate anything.

### Kafka egress

NBD derives `security.protocol` from the configuration:

| `[kafka.tls]` | `[kafka.auth]` | `security.protocol` |
|---|---|---|
| No | No | `plaintext` |
| Yes | No | `ssl` |
| Yes | Yes | `sasl_ssl` |
| No | Yes | `sasl_plaintext`, refused without `allow_insecure_transport = true` |

Prefer `SCRAM-SHA-512`, Kerberos or OAuth 2.0 to `PLAIN`. With `plaintext`, keep NBD and the broker on a trusted segment or in a VPN/IPsec tunnel, and restrict the broker listener to expected sources.

Allow outbound TCP to the broker, and to every broker it advertises.

### Metrics endpoint

The endpoint is plain HTTP, unauthenticated, and answers on any path. It exposes groups, ports, topics, counters and the state of the producer. Keep it on loopback. If it must be remote, set `allow_remote_metrics = true` and restrict the port to your monitoring network.

## Tuning

### Delivery and latency

The defaults favor latency, except `acks = "all"` with idempotence, which keeps delivery guarantees and [producer recovery](#producer-recovery) working. There is no universally right balance: it depends on whether a late message is worth more than a lost one for your feed.

| Parameter | Trade-off |
|---|---|
| `message_timeout` | The default of 100 ms turns a longer broker pause into lost messages. Seconds survive the pause, at the price of more queueing. |
| `acks`, `idempotence` | `1` or `0` lower latency, but a broker failure can lose messages. They also require `idempotence = false`, which leaves the supervisor little to detect. |
| `linger` | `0` sends immediately, with more requests. Higher values improve throughput and compression, and add that delay. |
| `compression` | `none` costs the least CPU. `lz4` saves bandwidth for some CPU. `gzip` is the most expensive. |
| `parallel_requests` | More concurrent requests per connection, at most 5 with idempotence. |

Keep `verbosity` at `info` or above: `debug` and `trace` log every delivered message.

### Receive buffer

`socket_buffer_size` is the margin of a provider when NBD falls behind. A larger buffer survives longer stalls and uses more kernel memory. Size it for `peak rate × longest stall`, plus a margin for the kernel's per-packet overhead: 100 MB/s with a 20 ms stall needs more than 2 MB.

### Producer queue

The queue absorbs what the broker doesn't take right away. It is bounded by `queue_size` and `queue_weight`, and the lower limit wins. A larger queue survives longer stalls and uses more memory.

With the defaults, the limits are equal for 1 KiB messages. With 1,300-byte datagrams, the queue is full at about 78,700 messages, or one second of traffic at 100 MB/s, so raise `queue_weight` first.

When the queue is full, datagrams are dropped. Messages older than `message_timeout` fail, which happens first with the default of 100 ms.

### Memory

| Item | Size |
|---|---|
| Receive buffer | 6.55 MB per provider, allocated at startup |
| Kernel socket buffer | Up to `socket_buffer_size` per provider |
| `librdkafka` queue | Up to `queue_weight` KiB, shared by all providers |

## Secrets

| Source | Exposure | Rotation |
|---|---|---|
| `{ file = ... }` | Readable by the service user | Replace the file |
| `{ env = ... }` | `/proc/<pid>/environ`, for the life of the process | Restart |
| Literal | The configuration file, which must then be `0600` or `0400` | Edit the file |

Prefer `{ file = ... }`.

Secrets and certificates are read at startup and at each producer rebuild, and a rebuild only follows a fatal error. To apply a rotation, restart NBD, and keep the old credentials valid until then.

## Configuration changes

At startup, NBD records every non-secret setting of `[kafka]`. A producer rebuild refuses any difference with this reference, an improvement included.

| Compared: a change is refused | Not compared |
|---|---|
| `broker` | SASL `username` and `password` |
| Presence of `[kafka.tls]` and `[kafka.auth]` | OAuth `client_secret` |
| `allow_insecure_transport` | TLS `key_password` |
| Paths of `ca_file`, `cert_file`, `key_file` | Content of certificate, key, keytab and secret files |
| SASL `mechanism` | Form of a secret source |
| Kerberos `principal`, `service_name`, path of `keytab` | |
| OAuth `token_endpoint_url`, `client_id`, `scope`, `extensions` | |
| Timeouts, retries, `compression`, `idempotence`, `linger`, `acks` | |
| `queue_size`, `queue_weight`, `parallel_requests` | |

This trades availability for visibility. An edit of the file can't silently change the Kafka connection, but an edit left pending blocks the next recovery: the rebuild is refused, and datagrams are lost for up to `recovery_timeout`, until NBD exits and restarts with the new file.

Restart NBD after every edit of `[kafka]`:

```bash
nbd --check-config-file /opt/nbd/config.toml && sudo systemctl restart nbd
```

A restart has no reference to compare with and accepts the file. Protecting the file is the control that matters. The check only makes a change slower and visible.

## Producer recovery

The supervisor checks the producer every 500 ms, and immediately when a submission reports a fatal error. It then rebuilds the producer from the configuration file and swaps it in, without restarting the daemon. The first attempt is immediate, unless the previous recreation is less than 60 s old. Failed attempts are spaced by 1 s, 2 s, 4 s, up to 60 s. After `recovery_timeout` of failures, NBD shuts down and exits with status `1`.

Only fatal errors trigger a rebuild. `librdkafka` reconnects to a slow or unreachable broker by itself, and these failures show up as delivery errors. It raises fatal errors for idempotent producers: with idempotence disabled, the supervisor has little to detect.

Datagrams are lost during a recovery: submissions are rejected, and messages queued or in flight on the faulted producer are purged when it is destroyed.

A rebuild fails if:

- the configuration file can't be read, or fails to parse in any section
- a file permission check fails
- `[kafka]` is invalid, the broker doesn't resolve, or a secret can't be resolved
- `[kafka]` differs from the startup reference, see [Configuration changes](#configuration-changes)
- `librdkafka` rejects the TLS or SASL material

The configuration file and every file it references must therefore stay readable and valid for the life of the daemon. Don't use read-once-then-delete secret files. A DNS outage fails the rebuild while it lasts: use an IP address in `broker` if that matters more to you than the hostname in the certificate.

Only `[kafka]` is validated and used during a rebuild. The other sections are parsed, not reloaded.

A rebuild doesn't probe the broker, so it succeeds even if the broker is down. Once the new producer is in place, the supervisor spends up to 10 s retiring the old one, and doesn't watch the new one meanwhile.

## Monitoring

### Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `nbd_udp_packets_total` | Counter | `group` | Non-empty datagrams received |
| `nbd_udp_bytes_total` | Counter | `group` | Payload bytes received |
| `nbd_udp_empty_packets_total` | Counter | `group` | Empty datagrams received, not forwarded |
| `nbd_kafka_sent_total` | Counter | `topic` | Messages confirmed by the broker |
| `nbd_errors_listeners_total` | Counter | `group` | Socket errors |
| `nbd_errors_full_queue_total` | Counter | `group` | Datagrams dropped on a full producer queue |
| `nbd_errors_sender_total` | Counter | `group` | Datagrams rejected for another reason, including during a recovery |
| `nbd_errors_kafka_transmission_total` | Counter | `topic` | Delivery failures |
| `nbd_errors_kafka_fatal_total` | Counter | | Fatal producer errors |
| `nbd_kafka_recreation_total` | Counter | | Successful producer recreations |
| `nbd_kafka_producer_generation` | Gauge | | Generation of the current producer, `0` at startup |

The `group` label is `<group>:<port>`. The three producer series exist from startup, the others appear with their first occurrence.

### Alerts

| Signal | Meaning |
|---|---|
| `nbd_errors_kafka_fatal_total`, `nbd_kafka_recreation_total` | A recovery. It is also when replaced secret files and certificates take effect |
| `error` logs | Failed rebuilds and refused changes. No metric counts them |
| `nbd_errors_sender_total` | Datagrams lost, for instance during a recovery |
| `nbd_errors_full_queue_total` | Producer queue saturated |
| `nbd_errors_kafka_transmission_total` | Broker slow or unreachable |
| Repeated restarts | NBD gave up on a recovery |
| `UdpRcvbufErrors` | Kernel drops |

NBD can't see the datagrams dropped by the kernel. Compare the rate of your feed with `nbd_udp_packets_total`, and watch the kernel counters:

```bash
nstat -az UdpRcvbufErrors   # host-wide
cat /proc/net/udp           # per socket: last column
```

### Logs

Logs go to the standard output. Delivery failures are limited to one line per second and per topic, socket errors to one line per second and per provider, and rejected datagrams aren't logged. Without `metrics-exporter`, what isn't logged isn't counted.

A refused change logs, at each attempt:

```text
Changes in the configuration file were refused (broker, linger). They can be manually re-applied by restarting NBD.
```

## Shutdown

On `SIGTERM` or Ctrl+C, the listeners and the supervisor stop together. NBD then waits for the listeners to drain, for the supervisor, and for the producer to be closed. Each wait is bounded by 5 s, 10 s for the producer. A listener waits for its in-flight messages, so a `message_timeout` above 5 s can make a shutdown against a stalled broker end with an error.
