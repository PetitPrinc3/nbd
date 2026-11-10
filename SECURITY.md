# Security

## Reporting a vulnerability

Report security issues by [email](mailto:gavrochebackups@gmail.com), not in a public issue. Include a description, reproduction steps if possible, and the affected version or commit.

NBD has a single maintainer, so response times are best-effort. Only the latest commit on `main` is supported: NBD is pre-1.0 (`0.2.0-beta`), with no LTS branch and no backports.

## Threat model

NBD sits between an unauthenticated UDP multicast feed and a Kafka broker. It assumes a controlled network where both are trusted infrastructure.

| Threat | What NBD does | What remains yours |
|---|---|---|
| Datagrams injected on the multicast segment | Nothing: they are relayed as-is | Segmentation, ACLs, validating messages downstream |
| Flood of the multicast segment | Memory stays bounded: the kernel buffer and the Kafka queue fill up, then datagrams are dropped | Upstream rate limiting, monitoring the drops |
| Eavesdropping on the Kafka link | TLS. Credentials are refused without TLS unless explicitly allowed | Building with `kafka-tls` |
| Rogue broker | With TLS, certificate and hostname verification are enforced | Without TLS, nothing authenticates the broker |
| Tampering with the configuration | Permission checks. Non-secret `[kafka]` changes are refused until a restart | The integrity of the files: a restart accepts their content |
| Theft of secrets from the process | Erasure of NBD's own copies | Protecting the process: `librdkafka` keeps its own copies |
| Compromised dependency or build | `cargo-audit`, `cargo-deny`, `cargo-vet`, pinned actions, attested releases | Verifying attestations, reviewing exempted crates |

Consumers of the Kafka topics should treat messages as untrusted input.

The configuration file and the files it references are read again at every producer rebuild, so they are part of the trust boundary for the whole life of the process. See [Configuration changes](PRODUCTION.md#configuration-changes).

The daemon's Rust code is memory-safe, with `unsafe` forbidden. The C code of `librdkafka` runs in the same process, outside that guarantee.

[PRODUCTION.md](PRODUCTION.md) describes how to set up the host and the network accordingly.

## Secrets

- `SecretString` requires an explicit `.expose_secret()`: `Display` doesn't compile and `Debug` prints `[REDACTED]`. The `config` tests check this with a canary value.
- NBD doesn't rely on destructors. `ProducerConfig::erase_secrets` overwrites the credentials with zeros once `build_producer` returns, whatever its result, at startup, at each rebuild and with `--check-config-file`.
- The text of the configuration file and of secret files is zeroized once used. `SecretString`'s zeroization on drop is a second layer.
- An erased configuration can't be reused, so each rebuild starts from the file.
- TOML errors omit the offending line in the daemon, in producer rebuilds, and in `--check-config-file` when `stdout` isn't a terminal.

## Build and release integrity

- `Cargo.lock` is tracked and every CI build uses `--locked`.
- `cargo audit` and `cargo deny check` run on every push and pull request to `main`, every day, and before a release. `deny.toml` restricts sources to `crates.io` and bans `bindgen` and unvetted native build tools.
- `cargo vet` requires every crate to be covered by an audit, a trusted publisher or an exemption. That doesn't make them all reviewed:
  - `cargo vet` reports 46 of the 168 third-party crates as exempted, including 2 of the 17 direct dependencies: `chrono` (build only) and `rdkafka`. 9 more are partially audited: a delta audit on top of an exempted version
  - 69 entries trust a publisher rather than a review
  - the 8 local audits are recorded as made by the maintainer with Claude Opus 5.5. They are full reviews of `arc-swap`, `metrics` and `secrecy`, a review of `metrics-exporter-prometheus` limited to the features NBD uses, and delta reviews of `smallvec`, `tracing`, `tracing-attributes` and `tracing-subscriber`
- Actions are pinned by commit, workflows default to read-only permissions and don't persist checkout credentials, and audit tools are pinned. Dependabot proposes updates weekly.
- `release.yml` re-runs the formatting, lint, configuration test and supply chain gates before building. Binaries are published with `SHA256SUMS` and build provenance attestations. `SHA256SUMS` itself isn't attested.
- The release profile keeps `overflow-checks = true` and sets `strip = "symbols"`.

## Limitations

- The default build and the released binaries are plaintext only.
- `allow_insecure_transport` is one switch for two risks: SASL without TLS, and a non-HTTPS OAuth token endpoint.
- There is no certificate revocation check: `ssl.crl.location` isn't configured.
- TLS versions and cipher suites are those of the OpenSSL build `librdkafka` links, and can't be configured.
- The metrics endpoint is unauthenticated plain HTTP.
- On Windows, permission checks only warn, and sockets aren't isolated per group or interface.
- File permission checks are a safety net, not a boundary. They don't check the owner of a file, the directories above its parent, or a `username` file. Only the last path component is checked for symbolic links, and nothing stops a file being swapped between the check and its use.
- Secrets and the content of files are trusted at runtime. A replaced CA, key or credential is applied at the next rebuild without a message, and a restart accepts whatever the file contains.
- A refused configuration change costs availability, up to `recovery_timeout`, and only the `error` logs show it.
- Datagrams are lost during a producer recovery. The recovery has no automated test: it was exercised by hand against a mock cluster.
- Credentials are partly wiped, not entirely. NBD can't reach the copies `librdkafka` keeps for authentication, the plain `String`s of its `ClientConfig`, the values of `{ env = ... }` secrets in the process environment, the copy of the file text held by a `toml` parse error, or intermediate copies in parsing libraries. This narrows the window against an attacker who can read process memory, it doesn't close it.
- A secret can reach a terminal or the logs through a TOML error. On a terminal, `--check-config-file` shows the offending line. In every mode, a type error quotes the refused value, for instance a secret written under a key that expects a number.
- The `clippy` denials on panics and arithmetic are enforced in CI, not by `cargo build`. They don't cover `tests/`, `build.rs`, or dependencies.
- A Kafka error code unknown to `librdkafka` 2.12.1 crashes NBD. `librdkafka` passes a broker's unknown code to the delivery report, and `rdkafka-sys` reads it as a Rust enum, which is undefined behaviour ([rust-rdkafka#127](https://github.com/fede1024/rust-rdkafka/issues/127)). Recent Kafka versions define 38 such codes, among them `READ_ONLY_TOPIC` when producing to a mirror topic. A broker can trigger it, and so can anyone on the path without TLS.
- With `kafka-auth-oauth`, the token endpoint is verified against the system trust store (OpenSSL's default paths, or the Windows certificate stores), not `ca_file`, and libcurl honours the `HTTPS_PROXY` and `NO_PROXY` environment variables. A build without a system libcurl uses the bundled curl 8.22.0, which lacks the fixes of curl 8.23.0.
- OpenSSL comes from the build system (pkg-config, `OPENSSL_DIR` or vcpkg) and is linked dynamically when a shared library exists. `openssl-sys` accepts OpenSSL 1.1.0 to 4.x, end-of-life lines included. Use a supported line, 3.5 LTS (until 2030-04-08) or 4.0 (until 2027-05-14), or a distribution build that backports security fixes.
- Dependency review is incomplete, see [Build and release integrity](#build-and-release-integrity).

## Roadmap

1. Add authentication to the metrics endpoint (static bearer token or mTLS).

## ANSSI compliance matrix

Self-assessment against the 60 rules and recommendations of the [ANSSI Secure Rust Guide](https://anssi-fr.github.io/rust-guide/checklist.html): 26 compliant, 1 partially compliant, 33 not applicable. This isn't a certification.

- ✅ Compliant
- ⚠️ Partially compliant
- ➖ Not applicable

Several rules rely on `clippy` lints declared with `#![deny(...)]` in `src/main.rs` and `src/lib.rs`. The CI enforces them with `cargo hack clippy --locked --feature-powerset --all-targets -- -D warnings`. `tests/` and `build.rs` only get clippy's default lints: the tests use `unwrap`, `expect` and, to set environment variables, `unsafe`.

### Development environment

| Rule | Requirement | Status | Evidence |
|---|---|---|---|
| `DENV-STABLE` | Stable toolchain | ✅ | `rust-toolchain.toml` pins stable `1.99.1`. The CI also builds with 1.95, the compiler of Ferrocene 26.05. |
| `DENV-TIERS` | Tier 1 targets only | ✅ | `x86_64-pc-windows-msvc` and `x86_64-unknown-linux-gnu`. |
| `DENV-CARGO-LOCK` | Track `Cargo.lock` | ✅ | Tracked, and CI builds use `--locked`. |
| `DENV-CARGO-OPTS` | Default values for critical profile variables | ✅ | `[profile.dev]` and `[profile.test]` aren't defined. `[profile.release] overflow-checks = true` deviates in the hardening direction. |
| `DENV-CARGO-ENVVARS` | Default compiler environment variables | ✅ | `RUSTC`, `RUSTC_WRAPPER` and `RUSTFLAGS` aren't set by the CI, `build.rs` or a `.cargo/config.toml`. `release.yml` sets `SOURCE_DATE_EPOCH`, read by `build.rs` only. |
| `DENV-FORMAT` | Use `rustfmt` | ✅ | `cargo fmt -- --check` in `audit.yml` and `release.yml`. |
| `DENV-LINTER` | Use a linter | ✅ | `clippy` on every feature combination in `audit.yml` and `release.yml`. |
| `DENV-AUTOFIX` | Review automatic fixes | ✅ | No `cargo fix` or `clippy --fix` runs in CI. |

### Third-party libraries

| Rule | Requirement | Status | Evidence |
|---|---|---|---|
| `LIBS-VETTING-DIRECT` | Validate direct dependencies | ✅ | `cargo vet --locked` tracks validation and `deny.toml` restricts sources. Of the 17 direct dependencies, 7 are covered by audits (8 local, 1 imported) and 8 by trusted publishers. |
| `LIBS-VETTING-TRANSITIVE` | Validate transitive dependencies | ⚠️ | Same tools. `cargo vet` reports 46 of 168 third-party crates as exempted and 9 as partially audited, through the exemptions of `supply-chain/config.toml`. |
| `LIBS-OUTDATED` | Check for outdated dependencies | ✅ | `cargo outdated --exit-code 1` on every push and pull request to `main`. Skipped on the daily run, not a gate of `release.yml`. |
| `LIBS-AUDIT` | Check for known vulnerabilities | ✅ | `cargo audit` in `audit.yml`, including daily, and in `release.yml`. Informational advisories are left to `cargo deny`. |

### Naming

| Rule | Requirement | Status | Evidence |
|---|---|---|---|
| `LANG-NAMING` | Naming conventions | ✅ | Standard Rust conventions, checked by `clippy`. No `get_*` function in `src/`. |

### Integer operations

| Rule | Requirement | Status | Evidence |
|---|---|---|---|
| `LANG-ARITH` | Guard against overflow | ✅ | `clippy::arithmetic_side_effects` is denied with no `#[allow]` in `src/`, so each operation states a policy (`saturating_*`, `checked_*`, lossless conversions). `overflow-checks = true` in release turns the rest into a panic. |

### Error handling

| Rule | Requirement | Status | Evidence |
|---|---|---|---|
| `LANG-ERRWRAP` | Custom `Error` type | ✅ | `NbdError` wraps third-party errors. `std::io::Error` and `JoinError` go through `UnwindShield`, a wrapper around `AssertUnwindSafe`. A compile-time assertion keeps `NbdError` `Send + Sync + UnwindSafe + RefUnwindSafe`. |
| `LANG-LIMIT-PANIC` | Limit `panic!` | ✅ | `clippy::panic`, `unreachable`, `todo` and `unimplemented` are denied, with no `#[allow]` in `src/`. No `assert!` in `src/`. |
| `LANG-LIMIT-PANIC-SRC` | Limit panic-capable functions | ✅ | `clippy::unwrap_used` and `expect_used` are denied. Poisoned mutexes are recovered or reported. `build.rs` returns its errors. |
| `LANG-ARRINDEXING` | Validate indexing | ✅ | `clippy::indexing_slicing` and `string_slice` are denied. The benchmark sinks check lengths and use `.get(..)`. |

### Unsafe code

| Rule | Requirement | Status | Evidence |
|---|---|---|---|
| `UNSAFE-NOUB` | No undefined behavior | ✅ | No `unsafe` in `src/`. |
| `LANG-UNSAFE` | No `unsafe` blocks | ✅ | `#![forbid(unsafe_code)]` in both `src/main.rs` and `src/lib.rs`, which are separate crates. |
| `LANG-UNSAFE-ENCP` | Encapsulate unsafe features | ➖ | No `unsafe` code. |

### Memory management

| Rule | Requirement | Status | Evidence |
|---|---|---|---|
| `MEM-NO-LEAK` | No memory leaks | ✅ | No leaking API is called. The log-throttling table has one entry per topic, and retired producers go to the reaper at the pace of the recreations. |
| `MEM-FORGET` | No `mem::forget` | ✅ | `clippy::mem_forget` is denied. |
| `MEM-FORGET-LINT` | Lint for `mem::forget` | ✅ | Same. |
| `MEM-LEAK`, `MEM-MANUALLYDROP`, `MEM-NORAWPOINTER`, `MEM-INTOFROMRAWALWAYS`, `MEM-INTOFROMRAWONLY`, `MEM-UNINIT` | Raw pointers, `ManuallyDrop`, uninitialized memory | ➖ | Not used. |

### Foreign function interface

| Rule | Requirement | Status | Evidence |
|---|---|---|---|
| All 21 `FFI-*` rules | Safe FFI boundary | ➖ | NBD defines no `extern` boundary. `librdkafka` is used through `rdkafka`. `deny.toml` restricts the native build tools. |

### Standard library traits

| Rule | Requirement | Status | Evidence |
|---|---|---|---|
| `LANG-SYNC-TRAITS`, `LANG-CMP-INV`, `LANG-CMP-DEFAULTS`, `LANG-CMP-DERIVE`, `MEM-MUT-REC-RC` | Custom `Send`/`Sync`, comparison traits, recursive reference counting | ➖ | No manual `Send`/`Sync`, comparison traits are derived, no recursive `Rc`/`Arc` type. |

### Drop

| Rule | Requirement | Status | Evidence |
|---|---|---|---|
| `LANG-DROP` | Justify `Drop` | ✅ | `ProducerCell` is the only implementation: `rd_kafka_destroy` blocks, so it hands the client to the reaper thread rather than block a Tokio worker. |
| `LANG-DROP-NO-PANIC` | `Drop` must not panic | ✅ | It does an `Option::take` and a channel send. If the reaper is gone, the calling thread destroys the producer. |
| `LANG-DROP-NO-CYCLE` | No `Drop` type in a reference cycle | ✅ | Nothing reachable from a cell refers back to its container. The factory captures a `PathBuf` and a `SecurityPosture`. |
| `LANG-DROP-SEC` | Don't rely only on `Drop` for security | ✅ | `ProducerCell::drop` enforces no security property. Credentials are [erased explicitly](#secrets), and producers are retired explicitly by `retire_cell` and `ManagedProducer::close`. This doesn't cover the copies listed under [Limitations](#limitations). |
