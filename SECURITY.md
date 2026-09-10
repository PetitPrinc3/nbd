# Security

**Nothing But Data** (NBD) sits directly on the path between an unauthenticated UDP multicast feed and a Kafka broker. That position is exactly why this document exists: a daemon whose entire job is to move bytes with sub-millisecond latency can't afford chatty security theater — but "no bullshit" doesn't mean "no security posture" either.

This document covers NBD's trust model, deployment hardening recommendations, and a rule-by-rule [ANSSI Secure Rust Guide](https://anssi-fr.github.io/rust-guide/) compliance matrix with the actual code evidence behind each status, not just a checklist of green checkmarks.

## Table of Contents

- [Security Model and Trust Boundaries](#security-model-and-trust-boundaries)
- [Reporting a Vulnerability](#reporting-a-vulnerability)
- [Deployment Recommendations](#deployment-recommendations)
  - [Multicast Ingress and Network Segmentation](#multicast-ingress-and-network-segmentation)
  - [Kafka Broker Connectivity](#kafka-broker-connectivity)
  - [Metrics Endpoint Exposure](#metrics-endpoint-exposure)
  - [Systemd Hardening](#systemd-hardening)
  - [Kernel Tuning](#kernel-tuning)
  - [Configuration and Secrets Handling](#configuration-and-secrets-handling)
  - [Logging](#logging)
- [ANSSI Compliance Matrix](#anssi-compliance-matrix)
- [Known Limitations](#known-limitations)
- [Future Hardening Roadmap](#future-hardening-roadmap)

## Security Model and Trust Boundaries

NBD's job is to relay, not to authenticate. Concretely:

- **UDP multicast ingress is trusted by design.** NBD cannot — at the UDP layer — verify the source, integrity, or authenticity of the datagrams it receives. Anyone able to send to a subscribed multicast group (including via spoofed source addresses, since UDP checks nothing) can inject arbitrary payloads that NBD will forward to Kafka as-is.
- **Kafka egress is currently trusted at the network level, not the protocol level.** See [Kafka Broker Connectivity](#kafka-broker-connectivity) — this is the single most important thing in this document.
- **The daemon itself is memory-safe by construction and runs unprivileged** (no root required, all Linux capabilities dropped under the provided systemd unit). See the [ANSSI Compliance Matrix](#anssi-compliance-matrix) for exactly how far that guarantee currently extends.

None of this is a flaw so much as a design decision consistent with NBD's target environment: a controlled, already-segmented production network where the multicast feed and the Kafka broker live on trusted infrastructure. But it does mean the security of an NBD deployment lives almost entirely at the network layer, not inside the daemon. Read everything below as "how to build the trusted network NBD assumes it's running on."

## Reporting a Vulnerability

Report security issues privately by [email](mailto:gavrochebackups@gmail.com) rather than opening a public issue. Include a description, reproduction steps if possible, and the affected version or commit hash.

This is a single-maintainer project with no dedicated security mailing list or bounty program, so response times are best-effort, not SLA-backed. Only the latest commit on `main` is supported — NBD is pre-1.0 (`0.1.0`), so there's no LTS branch and no backported patches.

## Deployment Recommendations

### Multicast Ingress and Network Segmentation

NBD subscribes to whatever multicast group its configuration points at and relays whatever arrives, with no source authentication, no payload integrity check, and no rate limiting beyond what the OS-level receive buffer imposes. Adding cryptographic verification to a sub-millisecond UDP relay would defeat the point of the tool, so this is intentional — but it means **the multicast segment itself is the trust boundary**, not NBD.

- Keep multicast producers and NBD on a dedicated, non-routed VLAN or subnet; don't bridge it to general corporate or user networks.
- Enable IGMP snooping on switches in the path. The README already recommends this for correct operation — it's also a security control, since it limits which ports can inject traffic into the group.
- Apply ingress ACLs at every router/firewall between an untrusted zone and the multicast segment, even if you don't expect traffic to cross that boundary today.
- UDP source addresses are trivially spoofable — don't rely on source-IP allowlists as your only control; pair them with physical/L2 segmentation.
- Size `socket_buffer_size` generously (see [PERFORMANCE.md](PERFORMANCE.md) and [Kernel Tuning](#kernel-tuning) below). An undersized buffer makes an ordinary traffic burst — malicious or not — indistinguishable from a packet-loss denial of service.

### Kafka Broker Connectivity

> [!WARNING]
> As shipped, the `[kafka]` configuration section exposes only `broker`, `connection_timeout`, `message_timeout`, and `message_retries`. There is **no configuration surface for TLS or SASL** (`security.protocol`, `ssl.*`, `sasl.*`). `src/main.rs` only ever sets `bootstrap.servers`, the timeout/retry values, `compression.type=lz4`, `acks=all`, and `enable.idempotence=true` on the `librdkafka` client — every one of those is compatible with, and defaults to, a **plaintext, unauthenticated `PLAINTEXT` connection**. No code path currently sets anything else.

Practical implications:

- Traffic between NBD and the broker is not encrypted in transit.
- There is no client authentication, so any host that can reach the broker's port can produce as NBD.

Until TLS/SASL configuration is added to `[kafka]` (see [Future Hardening Roadmap](#future-hardening-roadmap)), treat the NBD-to-broker link as **at least as sensitive as the multicast feed itself**, and mitigate at the network layer:

- keep NBD and the broker on the same trusted, non-routable segment, or a dedicated VPN/IPsec tunnel if they must cross an untrusted network;
- restrict the broker's listener to expected source IPs via firewall rules;
- if your Kafka cluster enforces `SASL_SSL` for every other producer, NBD currently can't join it directly — plan for a local TLS-terminating sidecar (`stunnel`, `envoy`, etc.) if that's a hard requirement today.

### Metrics Endpoint Exposure

The optional Prometheus exporter (`metrics-exporter` feature) serves plain HTTP, unauthenticated, on `[metrics].interface:port`. It leaks operational metadata — multicast group addresses as labels, topic names, packet/byte counters — that isn't secret exactly, but is still reconnaissance-useful. The README already recommends restricting it to trusted monitoring subnets or loopback; treat that as a hard requirement, since there is no `Authorization` check of any kind in front of it.

### Systemd Hardening

The [systemd unit in the README](README.md#systemd-service) already applies solid baseline hardening (`DynamicUser`, `ProtectSystem=strict`, `NoNewPrivileges`, `CapabilityBoundingSet=`, `PrivateTmp`). NBD needs nothing more than network sockets and a config file, so a few extra directives are worth layering on for a production rollout:

```ini
RestrictAddressFamilies=AF_INET AF_INET6
MemoryDenyWriteExecute=true
LockPersonality=true
RestrictRealtime=true
RestrictSUIDSGID=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
SystemCallFilter=@system-service
```

These aren't in the baseline unit because they need validation against your specific kernel/distro — some `SystemCallFilter` presets can be overly aggressive with `tokio`/`mio`'s epoll usage. Test in staging before rolling to production.

### Kernel Tuning

Covered in [README/Kernel Tuning](README.md#kernel-tuning). The recommended `net.core.rmem_max` / `rmem_default` / `netdev_max_backlog` sysctls matter for reliability under load, which is a security property too: a saturated receive buffer looks identical to the early stage of a UDP flood, and undersized buffers make it cheaper for a noisy or hostile sender on the same multicast segment to cause packet loss.

### Configuration and Secrets Handling

- `config.toml` currently carries no secret material — there's no credential field in the schema yet (see [Known Limitations](#known-limitations)). Once TLS/SASL support lands, treat the config file like any other credentials file: `chmod 600`, owned by the `DynamicUser`-allocated runtime user, never committed to version control.
- `nothing_but_data --check-config-file` validates configuration without starting the daemon — use it in CI/CD or a pre-deploy step rather than validating by pointing a live run at production multicast groups.
- Nothing in the current logging paths prints secret material, because there isn't any yet (e.g. `info!("Connecting to the Kafka broker at {} ...", config.kafka.broker)` only ever logs a host:port).

### Logging

- `verbosity` is dynamically reloadable and defaults to `info`. `debug`/`trace` log per-packet details — don't run those levels persistently in production, both for performance (extra log I/O isn't free on a sub-millisecond-latency path) and to avoid pushing payload-adjacent metadata into log aggregation systems that may not be secured as tightly as the Kafka pipeline itself.
- Panics are routed to `tracing::error!` via the custom panic hook in `main.rs`. Make sure your log pipeline treats panic-derived error logs as pageable alerts, not noise.

## ANSSI Compliance Matrix

**Status legend:** 
- ✅ Compliant ;
- ⚠️ Partially compliant / needs attention ;
- ➖ Not applicable to NBD's current design.

Of the 60 rules and recommendations in the current ANSSI Secure Rust Guide checklist: **23 are fully compliant, 37 don't apply** (mostly the unsafe-memory and FFI rule families, moot as long as NBD contains no `unsafe` code and defines no foreign-function boundary of its own), **and none are currently flagged**. See [Known Limitations](#known-limitations) for the *deployment*-level gaps that remain — those live outside what this matrix measures.

### Development Environment

| Rule                 | Requirement                                              | Status | Evidence                                                                                                                                                                     |
| -------------------- | -------------------------------------------------------- | ------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DENV-STABLE`        | Use a stable compilation toolchain                       | ✅      | `rust-toolchain.toml` pins `channel = "1.97.1"` — a stable release, never `nightly`/`beta`.                                                                                  |
| `DENV-TIERS`         | Exclusive use of Tier 1 `rustc` targets                  | ✅      | Targets restricted to `x86_64-pc-windows-msvc` and `x86_64-unknown-linux-gnu`, both Tier 1.                                                                                  |
| `DENV-CARGO-LOCK`    | Track `Cargo.lock` in version control                    | ✅      | `Cargo.lock` is not excluded by `.gitignore` (only `target/`, `debug/`, etc. are) and pins the exact dependency graph.                                                       |
| `DENV-CARGO-OPTS`    | Keep default values for critical cargo profile variables | ✅      | `[profile.release] overflow-checks = true` deviates from Rust's release default (`false`) — but in the *hardening* direction, not the weakening one the rule guards against. |
| `DENV-CARGO-ENVVARS` | Keep default compiler environment variables              | ✅      | No custom `RUSTFLAGS` or build env vars observed in `build.rs`, the CI workflows, or `Cargo.toml`.                                                                           |
| `DENV-FORMAT`        | Use `rustfmt`                                            | ✅      | Enforced as a gate in `audit.yml` (`cargo fmt -- --check`).                                                                                                                  |
| `DENV-LINTER`        | Use a linter regularly                                   | ✅      | `cargo clippy -- -D warnings` in `audit.yml` turns every clippy warning into a build failure on every push/PR to `main`.                                                     |
| `DENV-AUTOFIX`       | Manually review automatic fixes                          | ✅      | No `cargo fix`/`clippy --fix` runs unattended in CI, which is the right default.                                                                                             |

### Third-Party Libraries

| Rule | Requirement | Status | Evidence |
|---|---|---|---|
| `LIBS-VETTING-DIRECT` | Validate direct dependencies | ✅ | `deny.toml`'s `[sources]` restricts crates to `crates.io` only (`unknown-registry = "deny"`, `unknown-git = "deny"`, `allow-git = []`), enforced via `cargo deny check`. |
| `LIBS-VETTING-TRANSITIVE` | Validate transitive dependencies | ✅ | The same `[sources]` restriction applies to the whole resolved graph, not just direct deps — `cargo-deny` walks transitively by construction. |
| `LIBS-OUTDATED` | Check for outdated dependencies | ✅ | `audit.yml` runs `cargo outdated --exit-code 1`, which fails the job the moment a dependency is out of date rather than just reporting it. |
| `LIBS-AUDIT` | Check for known vulnerabilities | ✅ | `cargo audit` runs in `audit.yml` and fails the job on any RUSTSEC match. |

### Naming

| Rule | Requirement | Status | Evidence |
|---|---|---|---|
| `LANG-NAMING` | Respect naming conventions | ✅ | Types and functions (`NbdError`, `ProviderConfig`, `socket_buffer_size`, …) follow standard `snake_case`/`PascalCase` conventions; `clippy -D warnings` additionally catches most naming-lint violations. |

### Integer Operations

| Rule         | Requirement                      | Status | Evidence                                                                             |
| ------------ | -------------------------------- | ------ | ------------------------------------------------------------------------------------ |
| `LANG-ARITH` | Guard against overflow/underflow | ✅      | `overflow-checks = true` turns any overflow into a panic rather than a silent wrap.  |

### Error Handling

| Rule                   | Requirement                                                      | Status | Evidence                                                                                                                                                                                                                                                                                                      |
| ---------------------- | ---------------------------------------------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `LANG-ERRWRAP`         | Custom `Error` type wrapping all errors                          | ✅      | `NbdError` (`src/errors.rs`) wraps every third-party error type via `thiserror`'s `#[from]` (`std::io::Error`, `toml::de::Error`, `rdkafka::error::KafkaError`, `tokio::task::JoinError`); the module doc comment explicitly cites this rule.                                                                 |
| `LANG-LIMIT-PANIC`     | Limit `panic!` use                                               | ✅      | No direct `panic!()`/`unreachable!()`/`todo!()` in library code. `.unwrap()` does appear in `src/sinks.rs`, but exclusively inside `BenchSink::submit` and `IPerfSink::submit` — the two benchmark/test sinks.                                                                                                |
| `LANG-LIMIT-PANIC-SRC` | Limit panic-capable functions (`unwrap`, `expect`, raw indexing) | ✅      | Same call sites, all in `BenchSink`/`IPerfSink`, and all guarded by a preceding `payload.len()` check (see `LANG-ARRINDEXING`). The remaining exposure is `duration_since(UNIX_EPOCH).unwrap()`, which only fails under a misconfigured system clock set before 1970 and lives exclusively inside test sinks. |
| `LANG-ARRINDEXING`     | Validate indexing or use `.get()`                                | ✅      | `IPerfSink::submit` checks `payload.len() < 12` and `BenchSink::submit` checks `payload.len() < 8` — both return `NbdError::InvalidPacket` before any slicing happens.                                                                                                                                        |

### Unsafe Code — Generalities

| Rule               | Requirement                 | Status | Evidence                                                                                                                                                                                                                                   |
| ------------------ | --------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `UNSAFE-NOUB`      | No undefined behavior       | ✅      | No `unsafe` blocks exist anywhere in `src/`.                                                                                                                                                                                               |
| `LANG-UNSAFE`      | Don't use `unsafe` blocks   | ✅      | `#![forbid(unsafe_code)]` is declared at the top of **both** `src/main.rs` and `src/lib.rs`. Since `main.rs` and `lib.rs` compile as separate crates, both need the attribute independently for the guarantee to cover the whole codebase. |
| `LANG-UNSAFE-ENCP` | Encapsulate unsafe features | ➖      | No `unsafe` code exists to encapsulate.                                                                                                                                                                                                    |

### Memory Management

| Rule                                                                                                             | Requirement                                                       | Status | Evidence                                                                                                                                        |
| ---------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- | ------ | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `MEM-NO-LEAK`                                                                                                    | No memory leaks                                                   | ✅      | No leaking API is called anywhere, and the lint that would catch the most common cause (`mem::forget`) is active crate-wide (see `MEM-FORGET`). |
| `MEM-FORGET`                                                                                                     | Do not use `mem::forget`                                          | ✅      | `#![deny(clippy::mem_forget)]` is declared at the top of both `src/main.rs` and `src/lib.rs`.                                                   |
| `MEM-FORGET-LINT`                                                                                                | Use the clippy lint for `mem::forget`                             | ✅      | Same as above — enforced for both the binary and library crates.                                                                                |
| `MEM-LEAK`, `MEM-MANUALLYDROP`, `MEM-NORAWPOINTER`, `MEM-INTOFROMRAWALWAYS`, `MEM-INTOFROMRAWONLY`, `MEM-UNINIT` | Various raw-pointer / `ManuallyDrop` / uninitialized-memory rules | ➖      | None of these APIs are used anywhere in the codebase.                                                                                           |

### Foreign Function Interface

| Rule | Requirement | Status | Evidence |
|---|---|---|---|
| All 21 `FFI-*` rules (`FFI-SAFEWRAPPING` through `FFI-CAPI`) | Safe FFI boundary design | ➖ | NBD defines no `extern "C"` boundary of its own — confirms the README's "FFI: N/A (no C FFI exposed)" line. The only native code in the dependency tree is `librdkafka` (pulled in via `rdkafka-sys`, built with `cmake`); its FFI safety is `rdkafka`'s responsibility, not NBD's. NBD's exposure to that native code is instead governed by the supply-chain rules above — `deny.toml` explicitly allow-lists the `cc`/`cmake`/`pkg-config` build-time wrappers rather than trusting them implicitly. |

### Standard Library Trait Implementations

| Rule | Requirement | Status | Evidence |
|---|---|---|---|
| `LANG-SYNC-TRAITS`, `LANG-CMP-INV`, `LANG-CMP-DEFAULTS`, `LANG-CMP-DERIVE`, `LANG-DROP`, `LANG-DROP-NO-PANIC`, `LANG-DROP-NO-CYCLE`, `LANG-DROP-SEC`, `MEM-MUT-REC-RC` | Custom `Send`/`Sync`, comparison traits, `Drop`, `Rc`/`Arc` cycles | ➖ | No manual `unsafe impl Send`/`Sync`, no custom `PartialEq`/`Ord`, no custom `Drop` implementation, and no reference-counted cycles observed anywhere — `Provider` only holds straightforward `Arc<str>` ownership. |

## Known Limitations

What's left is deployment-level limitations:

- **No TLS/SASL configuration surface for the Kafka connection** (see [Kafka Broker Connectivity](#kafka-broker-connectivity)) — the most significant remaining gap.
- **The Prometheus metrics endpoint is unauthenticated plaintext HTTP** (see [Metrics Endpoint Exposure](#metrics-endpoint-exposure)).

## Future Hardening Roadmap

Roughly in priority order:

1. Add a `security.protocol` / `ssl.*` / `sasl.*` configuration surface to `[kafka]`, and consider failing closed (refuse to start) if an operator hasn't explicitly opted into `PLAINTEXT`.
2. Decide the fate of the commented-out `metrics-exporter` block in `FutureProducer::submit`: at minimum, restore `nbd_kafka_sent_total` (no timestamp math involved, nothing risky about it); for `nbd_e2e_latency`, either reintroduce the computation with `checked_sub`/`saturating_sub` and skip-on-failure instead of panicking, or drop the metric from the README until it's reimplemented.