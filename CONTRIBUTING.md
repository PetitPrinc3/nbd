# Contributing

NBD has a single maintainer, so reviews are best-effort. Changes should keep NBD's data path free of payload copies, keep the configuration failing closed, and stay compliant with the [ANSSI Secure Rust Guide](https://anssi-fr.github.io/rust-guide/).

## Setup

The toolchain is selected by `rust-toolchain.toml`. The code must also build with Rust 1.95, the compiler of Ferrocene 26.05.

On Debian/Ubuntu:

```bash
sudo apt-get install -y cmake libssl-dev libsasl2-dev libcurl4-openssl-dev
```

| Package | Needed by |
|---|---|
| `cmake`, a C/C++ compiler | `librdkafka` |
| `libssl-dev` | `kafka-tls`, `kafka-auth-gssapi`, `kafka-auth-oauth` |
| `libsasl2-dev` | `kafka-auth-gssapi`. At run time, Kerberos also needs `kinit` and `libsasl2-modules-gssapi-mit` |
| `libcurl4-openssl-dev` | `kafka-auth-oauth` |

On Windows, you need the Visual Studio Build Tools (C++ workload) and CMake. The CI doesn't build the TLS and SASL features there.

## Checks

```bash
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
cargo test --test config --all-features
cargo test --doc --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features --document-private-items
```

The CI goes further, on every combination of features and with the supply chain tools:

```bash
cargo install --locked cargo-hack@0.6.45 cargo-audit@0.22.2 cargo-deny@0.20.2 cargo-vet@0.10.3 cargo-outdated@0.19.0

cargo hack clippy --locked --feature-powerset --all-targets -- -D warnings
cargo hack test --locked --feature-powerset --test config
cargo audit
cargo deny check
cargo vet --locked
cargo outdated --exit-code 1
```

`src/` denies `unwrap`, `expect`, `panic!`, `unreachable!`, `todo!`, `unimplemented!`, unchecked arithmetic, and unchecked indexing and slicing. Use `NbdError`, `.get()`, and checked or saturating operations.

A new dependency must pass `cargo vet`. Review it and run `cargo vet certify` rather than adding an exemption, and update `deny.toml` if it brings a native build tool.

## Tests

The tests use in-memory sinks and need no broker. There is no unit test. The end-to-end harness lives outside this repository, see [PERFORMANCE.md](PERFORMANCE.md#end-to-end).

| Test | Duration | Channels | Nominal rate | Required `net.core.rmem_max` | Purpose |
|---|---|---|---|---|---|
| `config` | Instant | — | — | — | Validation, permissions, secret leaks |
| `simple_latency` | Instant | 1 | 3 packets | 64 KiB | Latency smoke test |
| `stress` | 5 min | 1 | 1 kHz | 2 MiB | Sustained throughput |
| `spike` | 10 s | 1 | 1 MHz | 2 MiB | Burst resilience |
| `multi_channel_stress` | 5 min | 5 | 100 kHz/channel | 2 MiB | Concurrency |
| `multi_channel_iperf_stress` | 5 min | 5 | External | 250 MiB | External traffic generator |

```bash
export NBD_TEST_INTERFACE=eth0
cargo test --test simple_latency -- --nocapture
cargo test --release --test stress -- --nocapture
```

`NBD_TEST_INTERFACE` must be the interface that carries your multicast route. It defaults to `lo` on Linux and to index `1` elsewhere. `stress`, `spike` and the two `multi_channel` tests fail when they receive nothing.

Run `simple_latency` before a pull request. Run the benchmarks if your change touches `providers.rs`, `sinks.rs` or `kafka.rs`, and add the results to the pull request, see [Reproducing the benchmarks](PERFORMANCE.md#reproducing-the-benchmarks). A bare `cargo test --all-features` runs everything, in more than 15 minutes.

## CI

| Workflow | Trigger | Jobs |
|---|---|---|
| `build.yml` | Push and pull request to `main` | Linux release build with all features, Ferrocene 26.05 build, Windows `config` tests and build |
| `audit.yml` | Same, and daily at 06:00 UTC | `cargo fmt`, `cargo audit`, `cargo deny`, `cargo vet`, `clippy` and `config` tests on every feature combination. `cargo outdated` is skipped on the daily run |
| `release.yml` | Tag `vX.Y.Z` or `vX.Y.Z-suffix` | `cargo fmt`, `clippy`, `config` tests, `cargo audit`, `cargo deny`, `cargo vet`, then Linux and Windows binaries with and without metrics, `SHA256SUMS`, attestations, GitHub Release |

## Documentation

Update the documentation in the same pull request as the change.

| Change | Update |
|---|---|
| Parameter, default or validation rule | `CONFIGURATION.md`, doc comments of `config.rs`, `tests/config.rs` |
| Secrets, files, or what a rebuild re-reads or compares | `PRODUCTION.md`, `SECURITY.md`, doc comments of `kafka.rs` and `config.rs` |
| Metric | `PRODUCTION.md` |
| Hot path, or a default that affects latency | `PERFORMANCE.md`, including which figures are no longer current |
| CI, `deny.toml`, `supply-chain/` | This file, and the matrix of `SECURITY.md` if it is used as evidence |
| Anything user-visible | `CHANGELOG.md` |

## Layout

| Path | Content |
|---|---|
| `src/main.rs` | CLI, logging, shutdown |
| `src/config.rs` | Configuration, secrets, security posture |
| `src/providers.rs` | Multicast sockets and receive loop |
| `src/sinks.rs` | `MessageSink` and its Kafka, bench and iperf implementations |
| `src/kafka.rs` | Producer builder, `ManagedProducer`, supervisor |
| `src/errors.rs` | `NbdError`, `UnwindShield` |
| `src/args.rs`, `src/about.rs` | CLI arguments, `--about` |
| `tests/` | Configuration tests and benchmarks |
| `supply-chain/` | `cargo-vet` audits, trusted publishers, exemptions |
| `build.rs` | Build date, architecture, features |

## Pull requests

1. Create a feature branch from `main`.
2. Add tests for new behavior.
3. Update the documentation.
4. Describe what the change does and how it was tested.
5. Make sure `build.yml` and `audit.yml` pass.

Report vulnerabilities as described in [SECURITY.md](SECURITY.md#reporting-a-vulnerability), not in an issue.
