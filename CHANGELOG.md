# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/PetitPrinc3/nbd/compare/v0.1.1-beta...HEAD)

Breaking: the configuration now fails closed, and files written for `0.1.x` are refused.

### Migrating from 0.1.x

Run `nbd --check-config-file` on your file, then:

| `0.1.x` | `0.2.0-beta` |
|---|---|
| `provider.message_size` | Remove it |
| `provider.interface` as an IP address or an integer | Interface name (`"eth0"`) on Linux, index as a string (`"2"`) elsewhere |
| `provider.topic`, `port` or `interface` not set, `port = 0` | Set them: required |
| `provider.topic` of 250 to 254 characters | 249 at most |
| Two providers on the same `group` and `port` | Remove the duplicate |
| `kafka.compression` not set | The default is now `none`. Set `lz4` to keep the former default |
| `kafka.compression = "zstd"` | Use `lz4`, `gzip`, `snappy` or `none` |
| `message_retries = 0`, invalid `acks` or `compression`, any value that was replaced by a default | Fix it: now an error |
| `connection_timeout = 0`, `message_timeout = 0` or above `300000`, `queue_size = 0`, `parallel_requests = 0` | Fix it: now an error |
| `idempotence = true` with `acks` set to `1` or `0` | Remove it, or set it to `false` |
| `parallel_requests` above 5 with idempotence | Lower it, or disable idempotence |
| `kafka.broker` that doesn't resolve | Now an error, at startup and at every producer rebuild |
| `[kafka.auth]` without `[kafka.tls]` | Add TLS, or set `allow_insecure_transport = true` |
| OAuth `token_endpoint_url` that isn't `https://` | Use HTTPS, or set `allow_insecure_transport = true` |
| Kerberos `keytab` relative, or with `"`, `$`, `` ` ``, `\` or a line break | Use a plain absolute path |
| Kerberos `principal` or `service_name` outside `[a-zA-Z0-9._/@-]`, or starting with `-` | Fix it: now an error |
| No `[metrics]` section | The exporter no longer starts: add the section |
| `[metrics]` without `interface`, or with an address not on the host | Set a valid `interface`: required |
| `metrics.interface` not on loopback | Set `allow_remote_metrics = true` and firewall the port |
| `[metrics]` in a binary without `metrics-exporter` | Remove the section |
| Unknown keys outside `[kafka]`, or inside a secret source | Remove them: now errors |
| A secret source with both `env` and `file` | Keep one |

On the host:

- Linux 5.1 or later is required, and 5.7 or later to run without `CAP_NET_RAW`, since sockets are now bound to their interface (`SO_BINDTODEVICE`). This includes `--check-config-file`.
- `socket_buffer_size` can't exceed `net.core.rmem_max`: NBD refuses to start instead of getting a smaller buffer. It is also limited to 1 GiB.
- See [File permissions](CONFIGURATION.md#file-permissions) for the new rules. They refuse files in `/tmp` and Kubernetes secret volumes, among others.

In your monitoring:

- `nbd_errors_kafka_total` is renamed `nbd_errors_kafka_transmission_total`.
- The `group` label is now `<group>:<port>`.

### Added

- Self-healing Kafka producer: `ManagedProducer`, supervised by a task that replaces the producer after a fatal error. Replaced producers are destroyed on the `nbd-kafka-reaper` thread.
- `kafka.recovery_timeout` (default `600000` ms), `kafka.queue_weight` (default `100000` KiB), `kafka.allow_insecure_transport` and `metrics.allow_remote_metrics`.
- Metrics: `nbd_errors_kafka_fatal_total`, `nbd_kafka_recreation_total`, `nbd_kafka_producer_generation`.
- `--check-config-file` submits the settings to `librdkafka`, without creating a client.
- Release pipeline: a `verify` job gates the builds, and releases come with `SHA256SUMS` and build provenance attestations.
- CI: `cargo vet`, Dependabot, a daily audit run, configuration tests on every feature combination, a build with Rust 1.95 (Ferrocene 26.05).
- API: `ManagedProducer`, `RecoveryPolicy`, `build_producer`, `producer_factory`, `wait_for_retired_producers`, `ProducerConfig` (`configure`, `check`, `erase_secrets`, `posture`), `SecurityPosture`, `SecretSource::erase`, `Provider::create_socket`, `Provider::join`, `UnwindShield`, `NbdError::Recovery`.

### Changed

- Configuration: invalid values are errors, every section rejects unknown keys, and `provider.topic`, `port`, `interface` and `metrics.interface` are required.
- Configuration: `kafka.compression` defaults to `none`, and `zstd` is no longer accepted.
- The exporter only starts with a `[metrics]` section.
- Metrics: `nbd_errors_kafka_total` is renamed, and the `group` label is `<group>:<port>`.
- Listeners: sockets are bound to their interface (`SO_BINDTODEVICE`) and group on Linux, with `IP_MULTICAST_ALL` disabled, and groups are joined by interface index. `SO_REUSEPORT` is no longer set.
- Configuration: on Linux, an interface is checked by binding a temporary socket to it and reading back its index (`SO_BINDTOIFINDEX`).
- Listeners: sockets are created before the connection to Kafka, and groups are joined after it.
- Listeners: the receive buffer is a fixed 6.55 MB per provider.
- Logs: socket errors and delivery failures are limited to one line per second, per provider and per topic.
- Shutdown also stops the supervisor and closes the producer, with one log line per stage instead of `NBD exited gracefully.`
- The daemon exits with an error, after its graceful shutdown, if the supervisor fails or gives up.
- `BenchSink` and `IPerfSink` reject a timestamp later than the reception time.
- `kafka-auth-oauth` enables `kafka-tls`.
- `build.rs` fails the build on an invalid `SOURCE_DATE_EPOCH`.
- Benchmarks take their interface from `NBD_TEST_INTERFACE`, and no longer raise the Windows timer resolution. `stress`, `spike` and the `multi_channel` tests fail when they receive nothing.
- Release assets are renamed `nbd-<tag>-linux-x86_64[-with-metrics]` and `nbd-<tag>-windows[-with-metrics].exe`.
- API: `RawConfig::from_path` takes a `safe_output` argument.
- API: `Interface` is a validated name or index, built with `Interface::try_from`. `Interface::index` takes the IP family. `ProviderConfig` no longer has `message_size`.
- API: `NbdError::Network` and `NbdError::TaskPanic` carry an `UnwindShield`, so that `NbdError` is `UnwindSafe`.
- API: `Provider::get_socket` is renamed `Provider::socket` (ANSSI `LANG-NAMING`).
- Rust `1.97.1` to `1.99.1`. New dependency: `arc-swap`. `chrono`, a build dependency, is built without its default features.
- Documentation rewritten: shorter `README.md`, new `CONFIGURATION.md` and `PRODUCTION.md`. The ANSSI matrix reports the validation of dependencies as partially compliant.

### Removed

- `provider.message_size`.
- The defaults of `provider.topic`, `provider.port` and `provider.interface`.
- The `workflow_dispatch` trigger of the release pipeline.

### Fixed

- The default-value warning of `kafka.connection_timeout` named it `nbd.kafka.connection_timeout`.
- Trailing `]` in `BUILD_FEATURES` for binaries compiled without any feature.

### Security

- SASL without TLS and a non-HTTPS OAuth token endpoint are refused unless `allow_insecure_transport = true`.
- TLS certificate and hostname verification are set explicitly.
- A metrics `interface` outside loopback is refused unless `allow_remote_metrics = true`.
- File checks: the configuration file is always checked, the CA and client certificate too, symbolic links are refused, and parent directories must not be writable by group or others.
- Kerberos: `keytab` and `principal` are restricted, since `librdkafka` passes them to `kinit` through a shell. `service_name` follows the same alphabet.
- A producer rebuild refuses any change to the non-secret settings of `[kafka]` (`SecurityPosture`). Secrets and file contents aren't compared, so that they can be rotated. A restart applies the file.
- TOML errors no longer print the offending line in the daemon, in rebuilds, or from `--check-config-file` when its output isn't a terminal. A type error still quotes the refused value.
- Secrets are erased explicitly once used, on top of the zeroization on drop.
- Secret sources reject unknown keys: `{ env = ..., file = ... }` used to take the variable and skip the file and its permission check.
- `cargo vet`: `arc-swap`, `metrics`, `metrics-exporter-prometheus`, `secrecy`, `smallvec` and the `tracing` crates are covered by local reviews. 46 crates remain exempted, including `chrono` and `rdkafka`.
- `clippy` denies `unwrap`, `expect`, `panic!`, `unreachable!`, `todo!`, `unimplemented!`, unchecked arithmetic, and unchecked indexing and slicing in `src/`.
- `NbdError` is checked at compile time to be `Send`, `Sync`, `UnwindSafe` and `RefUnwindSafe`.
- CI: actions pinned by commit, read-only permissions by default, no persisted checkout credentials, pinned audit tools, `--locked` builds.

## [0.1.1-beta](https://github.com/PetitPrinc3/nbd/compare/v0.1.0-beta...v0.1.1-beta) - 2026-09-22

### Added

- Replace hard-coded Kafka producer configuration with fully configurable `[kafka]` parameters: `compression`, `idempotence`, `linger`, `acks`, `queue_size`, and `parallel_requests`.
- TLS support for the Kafka producer via the `kafka-tls` optional feature (`rdkafka/ssl`): configurable CA, certificate, and key files with optional encrypted key password.
- SASL authentication support for the Kafka producer:
  - Credential-based SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) — always available.
  - Kerberos GSSAPI via the `kafka-auth-gssapi` optional feature (`rdkafka/gssapi`).
  - OAuth 2.0 / OIDC via the `kafka-auth-oauth` optional feature (`rdkafka/curl`).
- `SecretSource` type: secrets (passwords, tokens) can be supplied as a literal value, a shell environment variable (`{ env = "VAR" }`), or a file path (`{ file = "/path/to/secret" }`).
- File-permission enforcement for secret-bearing files on Unix: NBD refuses to start if a secret file is world- or group-readable.
- Automatic `security.protocol` selection (`plaintext`, `ssl`, `sasl_plaintext`, `sasl_ssl`) based on the presence of `kafka.tls` and `kafka.auth` sections.
- Explicit `drop` of `kafka_config` and `config.kafka` after producer creation to minimize secret retention in memory.
- GitHub Actions release pipeline (`release.yml`): compiles Linux and Windows binaries (standard and `with_metrics` variants) on every version tag push, and uploads them to the corresponding GitHub Release.

### Changed

- Errors that cause the process to exit are now logged through `tracing` (with the usual timestamp/level formatting) using their `Display` representation instead of the raw `Debug` dump previously printed by the Rust runtime — e.g. TOML parsing errors now keep their line/column context and source snippet instead of showing the internal error struct.

## [0.1.0-beta](https://github.com/PetitPrinc3/nbd/releases/tag/v0.1.0-beta) - 2026-09-11

### Added

- High-performance UDP multicast ingress using `socket2` and `tokio`.
- Zero-copy buffer management via `bytes::BytesMut` (split-and-freeze pattern).
- Non-blocking ingestion with asynchronous sink delivery confirmation and backpressure handling.
- Asynchronous Kafka egress using `librdkafka` and the `rdkafka` crate.
- Two-stage configuration validation supporting TOML.
- Graceful shutdown via `CancellationToken` with in-flight message draining.
- Command-line interface (`clap`) with `--config-file`, `--check-config-file`, and `--about`.
- Cross-platform support for Linux and Windows (Tier 1 targets).
- Prometheus metrics exporter (`metrics-exporter` feature) via HTTP.
- Comprehensive integration test suite (benchmarks, multi-channel stress tests).
- GitHub Actions CI/CD pipelines (`build.yml`, `audit.yml`).
- Strict supply-chain security policy via `cargo-deny`.
- ANSSI Secure Rust compliance tracking (`SECURITY.md`).
- Performance review and benchmark methodology (`PERFORMANCE.md`).
- Systemd service unit with advanced security hardening (`DynamicUser`, `ProtectSystem=strict`, etc.).
