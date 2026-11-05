# Contributing to Nothing But Data (NBD)

First off, thanks for taking the time to contribute! NBD is a "no-bullshit" daemon focused on sub-millisecond latency, strict security, and stability.

As a single-maintainer project, PR reviews and issue responses are best-effort. Please ensure your contributions align with the project's core principles: zero-copy data paths, strict ANSSI compliance, minimal unnecessary overhead, and very low latency.

## Development Environment Setup

1. **Rust Toolchain**: NBD is pinned to Rust **1.99.1**. Ensure you have `rustup` installed; the toolchain will be selected automatically via `rust-toolchain.toml`.
2. **System Dependencies (Linux/macOS)**: You need a C/C++ compiler and `cmake` for `librdkafka` compilation (pulled via `rdkafka-sys`). Optionally:
   - `libsasl2-dev` (Debian/Ubuntu) — required to build the `kafka-auth-gssapi` feature (Kerberos GSSAPI).
   - `libcurl4-openssl-dev` (Debian/Ubuntu) — required to build the `kafka-auth-oauth` feature (OAuth 2.0/OIDC).
3. **System Dependencies (Windows)**: Building `rdkafka-sys` with the `cmake-build` feature additionally requires Perl and NASM (for the bundled OpenSSL build) and the Visual Studio Build Tools (C++ workload). If `cmake` fails to locate a dependency, check the [`rust-rdkafka` build notes](https://github.com/fede1024/rust-rdkafka) first.
4. **Testing Infrastructure**: A local Kafka broker (or Docker/Podman for `testcontainers`) is recommended for end-to-end testing. The end-to-end harness lives outside this repository (see [PERFORMANCE.md](PERFORMANCE.md#end-to-end)). The benchmarks of `tests/` use in-memory mock sinks (`BenchSink`, `IPerfSink`) and need no external broker.

## Workflow and Quality Gates

Before submitting a Pull Request, you must ensure all CI/CD checks pass locally. We enforce strict quality gates:

### 1. Formatting

```bash
cargo fmt
```

### 2. Linting

We maintain a strict zero-warning policy. A quick local check:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

The CI goes further and lints every combination of the optional features (`cargo install cargo-hack` first if you want to reproduce it):

```bash
cargo hack clippy --feature-powerset --all-targets -- -D warnings
```

The library and the daemon (`src/lib.rs`, `src/main.rs`) also deny, through `clippy`, the constructs that can panic or overflow silently: `unwrap`, `expect`, `panic!`, `unreachable!`, `todo!`, `unimplemented!`, unchecked arithmetic (`+`, `-`, `*`, `/`...), and unchecked indexing and string slicing. Use `Result` and `NbdError`, `.get()`, and checked or saturating operations instead. These lints are enforced by `clippy`, not by `cargo build`, and don't apply to the crates of `tests/`.

### 3. Testing

Run the fast checks below before every PR:

```bash
cargo test --test config --all-features   # configuration validity and secret handling
cargo test --doc --all-features           # the examples of the API documentation
cargo test --test simple_latency -- --nocapture   # latency smoke test
cargo test --lib --all-features
```

A change to `config.rs`, `kafka.rs`, `providers.rs` or `sinks.rs` deserves a new test in `tests/` when it can be written without a broker.

The suite also ships longer-running benchmarks (`stress`, `spike`, `multi_channel_stress`) and one that depends on an external `iperf` process (`multi_channel_iperf_stress`). These are **not** required to pass a PR — run them manually if your change touches the hot path (`providers.rs`, `sinks.rs`, `kafka.rs`):

```bash
cargo test --test stress -- --nocapture                   # 5 min sustained load
cargo test --test spike -- --nocapture                    # 10 s burst
cargo test --test multi_channel_stress -- --nocapture     # 5-channel concurrency
```

> **Heads up**: running a bare `cargo test --all-features` with no filter will attempt every test above, including the multi-minute stress tests and the `iperf`-dependent one. Expect it to take 10+ minutes and to fail unless `iperf` is already running separately.

### 4. Supply Chain & Security Auditing

We use `cargo-deny`, `cargo-audit` and `cargo-outdated` to enforce license, registry, freshness and vulnerability policies. The CI fails on any of them, an outdated dependency included:

```bash
cargo deny check
cargo audit
cargo outdated --exit-code 1
```

## Pull Request Process

1. Fork the repository and create a feature branch from `main`.
2. If you introduce new features, write corresponding tests (unit or integration).
3. Update the documentation (`README.md`, `SECURITY.md`, `CHANGELOG.md`, the `cargo doc` comments, etc.) if you change user-facing behavior, configuration schemas, or compliance boundaries. A change to what a producer rebuild re-reads or checks (`ManagedProducer`, `SecurityPosture`) or to the way secrets are handled must be reflected in `SECURITY.md` too. Check the API documentation with `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features --document-private-items`, and the Markdown links with a link checker of your choice.
4. Keep the PR description clear: what does this fix/add, and how was it tested?
5. Ensure the GitHub Actions pipelines (`audit.yml` and `build.yml`) pass.
6. If your change touches the ingestion, buffering, or egress path (`providers.rs`, `sinks.rs`, `kafka.rs`), include your test results along with internal-process and end-to-end latency evaluations (`stress`/`spike` output). This isn't required for documentation, configuration, or CLI-only changes.

## Vulnerability Reporting

Please **do not** report security vulnerabilities through public GitHub issues. Refer to the [Security Policy](SECURITY.md#reporting-a-vulnerability) for instructions on emailing the maintainer directly.
