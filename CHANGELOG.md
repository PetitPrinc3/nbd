# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/PetitPrinc3/nbd/compare/v0.1.1-beta...HEAD)

### Added

- Self-healing Kafka producer: the production sink is now a `ManagedProducer`, supervised by a task that replaces the `librdkafka` producer with a fresh one when it raises a fatal error. Listeners keep their handle and no restart is needed. The replaced producers are destroyed on a dedicated `nbd-kafka-reaper` thread, off the hot path.
- `kafka.recovery_timeout` parameter (default `600000` ms; `0` is replaced by the default, values below `10000` ms trigger a warning): how long NBD keeps failing to rebuild the producer before exiting with an error. Attempts are spaced out with an exponential back-off (1 s to 60 s).
- Prometheus metrics (`metrics-exporter` feature): `nbd_errors_kafka_fatal_total`, `nbd_kafka_recreation_total` and `nbd_kafka_producer_generation`.
- Security posture of the Kafka connection (`SecurityPosture`, `ProducerConfig::posture`): the broker, the paths of the TLS files and the SASL mechanism NBD started with, kept without any secret. A producer rebuild is checked against it, see *Security* below.
- Public API: `ManagedProducer` (with `close`), `RecoveryPolicy`, `build_producer`, `producer_factory` (which takes the `SecurityPosture` to enforce), `wait_for_retired_producers`, `ProducerConfig` (with `erase_secrets` and `posture`), `SecurityPosture`, `SecretSource::erase`, `UnwindShield` and the `NbdError::Recovery` variant.

### Changed

- **Metrics:** `nbd_errors_kafka_total` is renamed `nbd_errors_kafka_transmission_total`. Update the dashboards and alerts that use the old name.
- Kafka delivery failure warnings are limited to one per topic and per second, so that a broker outage cannot flood the logs. `nbd_errors_kafka_transmission_total` still counts every failure.
- The Kafka producer is built by `build_producer` (extracted from `main.rs`).
- Shutdown now also stops the supervisor and closes the producer (`ManagedProducer::close`), which waits for it to be released by the listeners and then destroyed. Each stage is bounded to 5 seconds like the listeners drain (twice for the producer). The final `NBD exited gracefully.` log line is replaced by one line per stage (`All listeners were stopped gracefully.`, `Supervisor task stopped gracefully.`, `All producers were stopped gracefully.`).
- A replaced producer is retired by the supervisor, which waits for the listeners to release it and for its destruction (5 seconds each at most) before it resumes watching the new producer.
- **API:** `NbdError::Network` and `NbdError::TaskPanic` now carry an `UnwindShield<std::io::Error>` and an `UnwindShield<tokio::task::JoinError>`, so that `NbdError` is `UnwindSafe` and `RefUnwindSafe`. Build them with `NbdError::from(error)` and get the error back with `UnwindShield::into_inner`.
- **API:** `Provider::get_socket` is renamed `Provider::socket`, following the Rust API Guidelines naming of getters (ANSSI `LANG-NAMING`).
- `providers[].message_size` is now consistently capped to 65,507 bytes (the UDP maximum), as the warning has always announced. Before, such a value was replaced by the default (1,500) or, when it also exceeded `nbd.socket_buffer_size`, left unbounded.
- `BenchSink` and `IPerfSink` reject a packet whose timestamp is later than its reception time with `NbdError::InvalidPacket`, instead of computing a latency from it.
- The daemon exits with an error, after its graceful shutdown, if the producer supervisor fails or gives up.
- Upgrade `Rust` from `1.97.1` to `1.99.1`.
- New dependency: `arc-swap`.
- Documentation: every public item of the library now has a `cargo doc` comment, and the module documentation describes the configuration behaviors that were not written down (replacement of invalid values by defaults, unknown keys, default configuration path, `RUST_LOG`, coverage of the permission check). The README has a reference of the validation rules, an accurate description of the CI/CD pipelines (including the fact that the released binaries are built without TLS or SASL support), and the instructions of the kernel tuning no longer depend on a file that isn't in the repository. `SECURITY.md` documents the runtime behavior of the producer recovery, the explicit secret erasure and the security posture check, and its compliance matrix is reviewed against the code. `PERFORMANCE.md` states which version its figures come from.

### Fixed

- The default-value warning of the `kafka.connection_timeout` parameter named it `nbd.kafka.connection_timeout`.
- Removed an unnecessary trailing `]` in the `BUILD_FEATURES` string for binaries compiled without any feature.

### Security

- Secrets are now erased explicitly once used, in addition to being zeroized on drop: the text of the configuration file once parsed, the literal secrets of the raw configuration once resolved, the temporary copies made by `build_producer`, and the validated `ProducerConfig` once the Kafka producer is created (at startup, with `--check-config-file`, and at each producer recreation). Copies held by `librdkafka`, by its client configuration, and by a TOML parse error are not covered.
- The library and the daemon now deny, through `clippy`, `unwrap`, `expect`, `panic!`, `unreachable!`, `todo!`, `unimplemented!`, unchecked arithmetic, and unchecked indexing and string slicing. The remaining `unwrap` calls, in `BenchSink` and `IPerfSink`, are replaced by error returns, and unchecked arithmetic on buffer sizes, default ports and back-off delays is replaced by saturating operations. The crates of `tests/` are not covered.
- `NbdError` is checked at compile time to be `Send`, `Sync`, `UnwindSafe` and `RefUnwindSafe`.
- A Kafka producer rebuild is refused when the `[kafka]` section it re-reads would weaken the connection compared with the one NBD started with (or with the last one that was applied): `[kafka.tls]` removed, `ca_file` changed, client certificate or key removed, SASL removed or replaced by a weaker mechanism. The refusal is logged at `error` level and counts as a failed rebuild attempt, retried with the usual back-off until `kafka.recovery_timeout`. A change of broker, or any other change of the TLS settings, is applied with a `warn` log. Changes of credentials, of file contents and of the other `[kafka]` parameters are not compared. This notifies and slows down a change of the configuration file made behind the operator's back, it doesn't replace the protection of the integrity of the file (see `SECURITY.md`).

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
