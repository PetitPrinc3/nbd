# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/PetitPrinc3/nbd/compare/v0.1.0-beta...HEAD)

- Replace hard-coded configuration values for the kafka client with configurable variables.

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