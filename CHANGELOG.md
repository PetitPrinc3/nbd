# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/PetitPrinc3/nbd/compare/v0.1.1-beta...HEAD)

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