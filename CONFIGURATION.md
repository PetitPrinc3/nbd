# Configuration

NBD reads one TOML file. To validate a file without starting the daemon:

```bash
nbd --check-config-file config.toml
```

See [PRODUCTION.md](PRODUCTION.md) for how to choose these values.

## Validation

| Case | Result |
|---|---|
| Invalid value | Refused: NBD exits with an error |
| Unknown key, in any section | Refused |
| Section or mechanism of a feature missing from the binary | Refused |
| Two providers with the same `group` and `port` | Refused |
| Missing optional parameter | Default value, logged at `warn` |

Validation stops at the first section or provider that fails, so fixing a file can take several passes.

`--check-config-file` checks the file permissions, validates the file, resolves the broker address and submits the settings to `librdkafka`. It doesn't contact the broker or create the multicast sockets, so these errors only appear at startup:

- `socket_buffer_size` above `net.core.rmem_max`
- a provider or metrics port that can't be bound, or a group that can't be joined
- `linger` not lower than `message_timeout`
- an unreachable broker, or TLS and SASL material rejected by `librdkafka`

## Example

```toml
[nbd]
socket_buffer_size = 204800
verbosity = "info"

[kafka]
broker = "192.168.1.100:9092"
connection_timeout = 5000
recovery_timeout = 600000
message_timeout = 6000
message_retries = 2
compression = "lz4"

# Requires the metrics-exporter feature.
[metrics]
interface = "127.0.0.1"
port = 9000

[[provider]]
topic = "market-data-feed"
group = "239.255.0.1"
port = 20001
interface = "eth0"

[[provider]]
topic = "order-events"
group = "239.255.0.2"
port = 20002
interface = "eth1"
```

## Parameters

### `[nbd]`

Optional.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `socket_buffer_size` | integer | `204800` | Receive buffer of each socket, in bytes (`SO_RCVBUF`). 1 to 1 GiB. Must not exceed `net.core.rmem_max`. |
| `verbosity` | string | `"info"` | `trace`, `debug`, `info`, `warn` or `error`. `RUST_LOG` takes precedence. Applied once the configuration is validated and the metrics exporter started, and never by `--check-config-file`. |

### `[kafka]`

Required.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `broker` | string | required | One address, `<ip>:<port>` or `<hostname>:<port>`. Must resolve. |
| `connection_timeout` | integer | `2000` | Timeout of the broker check at startup, in ms. Not `0`. |
| `recovery_timeout` | integer | `600000` | Time spent failing to rebuild the producer before exiting, in ms. Not `0`. Warns below `10000`. |
| `message_timeout` | integer | `100` | `message.timeout.ms`, retries included. `1` to `300000`, greater than `linger`. Warns below `10`. |
| `message_retries` | integer | `2` | `message.send.max.retries`. Not `0`. |
| `compression` | string | `"none"` | `compression.type`: `none`, `lz4`, `gzip` or `snappy`. |
| `acks` | string | `"all"` | `all`, `-1`, `1` or `0`. |
| `idempotence` | boolean | `true`, or `false` if `acks` is `1` or `0` | `enable.idempotence`. `true` requires `acks` to be `all` or `-1`. |
| `linger` | integer | `1` | `linger.ms`. |
| `queue_size` | integer | `100000` | `queue.buffering.max.messages`. `1` to `2147483647`. |
| `queue_weight` | integer | `100000` | `queue.buffering.max.kbytes`, in KiB. `1` to `2147483647`. |
| `parallel_requests` | integer | `5` | `max.in.flight.requests.per.connection`. `1` to `5` with idempotence, at least `1` without. |
| `allow_insecure_transport` | boolean | `false` | Accepts `[kafka.auth]` without `[kafka.tls]`, and a `token_endpoint_url` that isn't `https://`. |

### `[kafka.tls]`

Requires the `kafka-tls` feature.

```toml
[kafka.tls]
ca_file      = "/etc/nbd/tls/ca.pem"
cert_file    = "/etc/nbd/tls/client.pem"
key_file     = "/etc/nbd/tls/client.key"
key_password = { file = "/run/secrets/tls_key_password" }
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ca_file` | string | required | CA certificate (PEM). The file must exist. |
| `cert_file` | string | | Client certificate (PEM), for mutual TLS. Requires `key_file`. |
| `key_file` | string | | Client private key (PEM). Requires `cert_file`. |
| `key_password` | secret | | Password of the private key. Not empty. |

NBD enforces `enable.ssl.certificate.verification = true` and `ssl.endpoint.identification.algorithm = https`: the broker certificate must chain to `ca_file` and match the host of `broker`. A broker reached by IP address needs that address in its certificate.

### `[kafka.auth]`

Enables SASL, in one of three forms. Without `[kafka.tls]`, it requires `allow_insecure_transport = true`.

#### Credentials

`PLAIN` is always available. `SCRAM-SHA-256` and `SCRAM-SHA-512` require the `kafka-tls` feature.

```toml
[kafka.auth]
username  = { env = "KAFKA_USER" }
password  = { file = "/run/secrets/kafka_password" }
mechanism = "SCRAM-SHA-512"
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `username` | secret | required | SASL username. Not empty. |
| `password` | secret | required | SASL password. Not empty. |
| `mechanism` | string | `"PLAIN"` | `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512`. |

#### Kerberos

Requires the `kafka-auth-gssapi` feature. `librdkafka` runs `kinit` through a shell with `principal` and `keytab`, hence the restrictions below.

```toml
[kafka.auth.gssapi]
principal    = "nbd@EXAMPLE.COM"
keytab       = "/etc/nbd/nbd.keytab"
service_name = "kafka"
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `principal` | string | required | `sasl.kerberos.principal`. At most 255 characters among `[a-zA-Z0-9._/@-]`, not starting with `-`. |
| `keytab` | string | required | `sasl.kerberos.keytab`. Absolute path to an existing file, without `"`, `$`, `` ` ``, `\` or a line break. |
| `service_name` | string | required | `sasl.kerberos.service.name`, usually `kafka`. Same rules as `principal`. |

#### OAuth 2.0 / OIDC

Requires the `kafka-auth-oauth` feature.

```toml
[kafka.auth.oauth]
token_endpoint_url = "https://auth.example.com/oauth/token"
client_id          = "nbd-client"
client_secret      = { env = "KAFKA_OAUTH_SECRET" }
scope              = "kafka"
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `token_endpoint_url` | string | required | `sasl.oauthbearer.token.endpoint.url`. `https://` unless `allow_insecure_transport = true`. |
| `client_id` | string | required | `sasl.oauthbearer.client.id`. Not empty. |
| `client_secret` | secret | required | `sasl.oauthbearer.client.secret`. Not empty. |
| `scope` | string | | `sasl.oauthbearer.scope`. |
| `extensions` | string | | `sasl.oauthbearer.extensions`, as comma-separated `key=value` pairs. |

### `[[provider]]`

Repeatable. At least one is required.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `topic` | string | required | Kafka topic. 1 to 249 characters among `[a-zA-Z0-9._-]`, except `.` and `..`. |
| `group` | IP address | required | Multicast group, IPv4 or IPv6. |
| `port` | integer | required | UDP port. Not `0`. Ports below 1024 need `CAP_NET_BIND_SERVICE`. |
| `interface` | string | required | Interface name on Linux (`"eth0"`), which must exist. Index on other systems (`"2"`, not `"0"`). At most 15 characters among `[a-zA-Z0-9._-]`. An IP address is refused. |

Datagrams can be up to 65,527 bytes, the maximum UDP payload over IPv6.

### `[metrics]`

Requires the `metrics-exporter` feature. Without this section, the exporter doesn't start.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `interface` | IP address | required | Listening address. Must belong to the host. |
| `port` | integer | `9000` | Listening port. |
| `allow_remote_metrics` | boolean | `false` | Accepts an `interface` that isn't a loopback address, `0.0.0.0` included. |

## Secret sources

A parameter of type secret takes one of three forms:

```toml
password = "my-secret"                           # literal
password = { env = "MY_SECRET_VAR" }             # environment variable
password = { file = "/run/secrets/my_secret" }   # file, surrounding whitespace trimmed
```

See [Secrets](PRODUCTION.md#secrets) for how to choose between them.

## File permissions

On Unix, NBD checks these files at startup, with `--check-config-file`, and at every producer rebuild. A violation is an error.

| File | Group and others |
|---|---|
| Configuration file with a literal `password`, `key_password` or `client_secret` | No access |
| Any other configuration file | No write access |
| `{ file = ... }` source of a `password`, `key_password` or `client_secret` | No access |
| `key_file`, `keytab` | No access |
| `ca_file`, `cert_file` | No write access |

Each of these files must also not be a symbolic link, and its parent directory must not be writable by group or others. This refuses files in a sticky directory such as `/tmp`, and the entries of a Kubernetes secret volume, which are symlinks.

On Windows, these checks only log a warning.
