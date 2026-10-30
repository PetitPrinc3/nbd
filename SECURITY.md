# Security

**Nothing But Data** (NBD) sits directly on the path between an unauthenticated UDP multicast feed and a Kafka broker. That position is exactly why this document exists: a daemon whose entire job is to move bytes with sub-millisecond latency can't afford chatty security theater — but "no bullshit" doesn't mean "no security posture" either.

This document covers NBD's trust model, deployment hardening recommendations, and a rule-by-rule [ANSSI Secure Rust Guide](https://anssi-fr.github.io/rust-guide/) compliance matrix with the actual code evidence behind each status, not just a checklist of green checkmarks.

## Table of Contents

- [Security Model and Trust Boundaries](#security-model-and-trust-boundaries)
- [Reporting a Vulnerability](#reporting-a-vulnerability)
- [Deployment Recommendations](#deployment-recommendations)
  - [Multicast Ingress and Network Segmentation](#multicast-ingress-and-network-segmentation)
  - [Kafka Broker Connectivity](#kafka-broker-connectivity)
  - [Kafka Producer Recovery](#kafka-producer-recovery)
  - [Configuration Changes at Runtime (Security Posture)](#configuration-changes-at-runtime-security-posture)
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
- **Kafka egress can be secured at the protocol level** via optional TLS and SASL features. See [Kafka Broker Connectivity](#kafka-broker-connectivity) for the feature matrix. The default build still uses plaintext — this is intentional for fully trusted environments, but must be explicitly hardened for any cross-network deployment.
- **The `[kafka]` configuration is a runtime input, not only a startup input.** When the Kafka producer hits a fatal error, NBD rebuilds it by re-reading the `[kafka]` section of the configuration file and re-resolving its secrets (see [Kafka Producer Recovery](#kafka-producer-recovery)). Whoever can modify that file, or a file it references, can change the broker, the credentials and the TLS and SASL settings of a running daemon, at the next producer recreation and without any restart. NBD refuses a rebuild that lowers the security level of the connection (see [Configuration Changes at Runtime](#configuration-changes-at-runtime-security-posture)), but that check is a tripwire with stated limits, not an access control. The integrity of the configuration files is therefore part of the trust boundary for the whole life of the process, not just at deploy time.
- **The daemon itself is memory-safe by construction and runs unprivileged** (no root required, all Linux capabilities dropped under the provided systemd unit). Its code is also held, through `clippy`, to a set of lints that rule out the usual sources of panics and silent integer overflows (`unwrap`, `expect`, raw indexing, unchecked arithmetic, ...). That is a code-quality guarantee enforced in CI, not a runtime one: it doesn't make the daemon immune to a panic from a dependency. See the [ANSSI Compliance Matrix](#anssi-compliance-matrix) for exactly how far these guarantees currently extend.

None of this is a flaw so much as a design decision consistent with NBD's target environment: a controlled, already-segmented production network where the multicast feed and the Kafka broker live on trusted infrastructure. On the multicast side, the security boundary is entirely at the network layer — NBD cannot authenticate UDP senders. On the Kafka side, optional TLS and SASL features now allow hardening at the protocol level (see [Kafka Broker Connectivity](#kafka-broker-connectivity)), but only if the binary is built and configured accordingly. The same goes for the configuration: the files that matter at startup keep mattering for as long as the daemon runs. Read everything below as "how to build the trusted network NBD assumes it's running on," and how to layer protocol-level hardening on top of it where needed.

## Reporting a Vulnerability

Report security issues privately by [email](mailto:gavrochebackups@gmail.com) rather than opening a public issue. Include a description, reproduction steps if possible, and the affected version or commit hash.

This is a single-maintainer project with no dedicated security mailing list or bounty program, so response times are best-effort, not SLA-backed. Only the latest commit on `main` is supported — NBD is pre-1.0 (`0.2.0-beta`), so there's no LTS branch and no backported patches.

## Deployment Recommendations

### Multicast Ingress and Network Segmentation

NBD subscribes to whatever multicast group its configuration points at and relays whatever arrives, with no source authentication, no payload integrity check, and no rate limiting beyond what the OS-level receive buffer imposes. Adding cryptographic verification to a sub-millisecond UDP relay would defeat the point of the tool, so this is intentional — but it means **the multicast segment itself is the trust boundary**, not NBD.

- Keep multicast producers and NBD on a dedicated, non-routed VLAN or subnet; don't bridge it to general corporate or user networks.
- Enable IGMP snooping on switches in the path. The README already recommends this for correct operation — it's also a security control, since it limits which ports can inject traffic into the group.
- Apply ingress ACLs at every router/firewall between an untrusted zone and the multicast segment, even if you don't expect traffic to cross that boundary today.
- UDP source addresses are trivially spoofable — don't rely on source-IP allowlists as your only control; pair them with physical/L2 segmentation.
- Size `socket_buffer_size` generously (see [PERFORMANCE.md](PERFORMANCE.md) and [Kernel Tuning](#kernel-tuning) below). An undersized buffer makes an ordinary traffic burst — malicious or not — indistinguishable from a packet-loss denial of service.

### Kafka Broker Connectivity

TLS and SASL authentication are available as **optional compile-time features** rather than always-on defaults. The table below summarises the feature matrix:

| Feature flag          | What it enables                                       | `security.protocol` auto-selected  |
| --------------------- | ----------------------------------------------------- | ---------------------------------- |
| *(none)*              | Plaintext, unauthenticated (`PLAINTEXT`)              | `plaintext`                        |
| `kafka-tls`           | TLS encryption only (`SSL`)                           | `ssl`                              |
| `kafka-auth-*`        | SASL authentication only, no encryption               | `sasl_plaintext`                   |
| `kafka-tls` + `kafka-auth-*` | TLS encryption + SASL authentication (`SASL_SSL`) | `sasl_ssl`                   |

NBD selects the `security.protocol` automatically based on the presence of `[kafka.tls]` and `[kafka.auth]` sections in the configuration, and warns at startup if `kafka.auth` is configured without `kafka.tls` (credentials in plaintext). A binary built without `kafka-tls` refuses a configuration that has a `[kafka.tls]` section, rather than silently ignoring it, and the `SCRAM-*` mechanisms are only available with `kafka-tls`.

> [!IMPORTANT]
> The **default build** (no feature flags) still produces a `PLAINTEXT` connection — no encryption, no authentication. This is intentional for environments that run NBD on a fully trusted, air-gapped network segment. For any deployment that crosses untrusted links, build with `--features kafka-tls` (minimum) and configure `[kafka.tls]`.
>
> **The binaries attached to the GitHub Releases are default builds** (`release.yml` builds them without `kafka-tls` or `kafka-auth-*`): plaintext, with SASL limited to `PLAIN`. Don't deploy them as they are on a link you don't trust. Build from source with the features you need.

Practical guidance:

- **Plaintext deployment** (trusted segment only): keep NBD and the broker on the same trusted, non-routable segment or a dedicated VPN/IPsec tunnel if they must cross an untrusted network. Restrict the broker's listener to expected source IPs via firewall rules.
- **TLS deployment** (`kafka-tls` feature): provide `[kafka.tls]` with at minimum a `ca_file` pointing to the broker's CA certificate. Add `cert_file`/`key_file` for mutual TLS.
- **SASL authentication**: if your Kafka cluster enforces `SASL_SSL`, build with both `kafka-tls` and the appropriate auth feature (`kafka-auth-gssapi` for Kerberos, `kafka-auth-oauth` for OAuth 2.0/OIDC, or no extra flag for PLAIN/SCRAM credentials).

These settings are applied again, from the configuration file as it is at that moment, every time the producer is rebuilt after a fatal error. See [Kafka Producer Recovery](#kafka-producer-recovery).

### Kafka Producer Recovery

When `librdkafka` raises a *fatal* error on the Kafka producer, NBD doesn't wait for an operator: a supervisor builds a new producer and swaps it in, while the listeners keep using the same handle (the mechanism is described in [README/Kafka Producer Resilience](README.md#kafka-producer-resilience)). That's good for availability, but it changes what NBD does *after* startup, so several things that used to be true only at startup now hold for the whole life of the daemon:

- **The configuration is read again at runtime.** Each recreation re-reads `config.toml` from the path given at startup, re-validates the `[kafka]` section, re-resolves its secrets (`{ env = ... }`, `{ file = ... }`) and re-applies the TLS and SASL settings. Only `[kafka]` is used: providers, `[nbd]` and `[metrics]` are not reloaded. A change to the broker, TLS or SASL settings takes effect at the next recreation, with no restart.
  - Treat `config.toml` and every file it references (`ca_file`, `cert_file`, `key_file`, `keytab`, secret files) as **integrity-critical for the whole runtime**: root-owned and not writable by the service account, not only correct at deploy time. The `ReadOnlyPaths=/opt/nbd` of the [provided systemd unit](README.md#systemd-service) already does this for the files kept under that directory.
  - They must also stay **readable**. If the configuration file or a secret file is removed, moved or made unreadable, the rebuild fails, and once `kafka.recovery_timeout` (10 minutes by default) has elapsed NBD exits with an error. Don't use read-once-then-delete secret files with `{ file = ... }` sources, and keep `{ env = ... }` variables defined for as long as the process runs.
- **The permission checks run again** on every rebuild (`chmod 600` on Unix, see [Configuration and Secrets Handling](#configuration-and-secrets-handling)). A file that became group- or world-readable after startup makes the next recreation fail rather than silently accepting the weakened permissions.
- **The validation warnings are muted during a rebuild, the security posture check is not.** The validation runs with logging disabled so that a recovery loop doesn't repeat the startup warnings. Instead, the rebuilt configuration is compared with the one in force, and a change that lowers the security of the connection (removal of `[kafka.tls]`, weaker SASL mechanism, ...) is refused. Anything that comparison doesn't cover is applied without a message, SASL credentials sent without TLS included. See [Configuration Changes at Runtime](#configuration-changes-at-runtime-security-posture) for the exact rules.
- **The broker is not probed again.** At startup, NBD checks that the broker answers (`fetch_metadata`) before accepting traffic. A recreated producer is not checked: building a producer doesn't wait for the broker, so a rebuild succeeds even if the broker is unreachable, or if an edited configuration points at the wrong one. The symptom is a rise of delivery errors (`nbd_errors_kafka_transmission_total`), not a recovery failure.
- **Messages are lost during a recovery.** While the producer is faulted, submissions are rejected and the datagrams dropped (a UDP feed can't be replayed). Messages already in flight on the faulted producer may not be delivered, and a retired producer is destroyed without being flushed.
- **The faulted producer is retired explicitly, and it takes time.** Once the new producer is in place, the supervisor waits up to 5 s for the listeners to release the old one, then up to 5 s for the `nbd-kafka-reaper` thread to confirm its destruction. During that wait, which can reach about 10 s, the supervisor doesn't watch the new producer: a fatal error on it is handled afterwards, not immediately. If the listeners don't release the old producer in time, NBD logs a warning and the producer is destroyed when the last listener lets go of it. In every case the credentials held by the old `librdkafka` client stay in memory until that destruction.
- **Shutdown closes the producer explicitly.** On `SIGTERM`/Ctrl+C, once the listeners are drained and the supervisor stopped, NBD calls `ManagedProducer::close`: it swaps in an empty producer, retires the current one and waits for its destruction, each stage bounded by 5 s. A submission made after that is rejected with `NbdError::Recovery`. If the listeners don't release the producer in time, NBD logs a warning and shutdown goes on; if the reaper thread doesn't confirm the destruction in time, `close` fails, NBD logs an error and exits with a non-zero status. Either way, a producer that refuses to terminate cannot block the exit of the process for more than about 10 s.
- **Only fatal errors trigger it.** A slow or unreachable broker doesn't: `librdkafka` reconnects by itself and the failures show up as delivery errors. `librdkafka` raises fatal errors for idempotent producers, which is the default (`idempotence = true` with `acks = "all"`); if you disable idempotence, the supervisor has little left to detect.

Operational guidance:

- Alert on `nbd_errors_kafka_fatal_total` and `nbd_kafka_recreation_total` (`metrics-exporter` feature), and watch `nbd_kafka_producer_generation`. A recreation is an availability event, and also the moment a runtime change of the `[kafka]` configuration takes effect: when it happens unexpectedly, check the modification time of `config.toml` and of the files it references.
- Alert on the `error` logs too: no metric counts the failed rebuild attempts, including the ones refused by the [security posture check](#configuration-changes-at-runtime-security-posture).
- When NBD gives up (`kafka.recovery_timeout` elapsed), it exits with a non-zero status after a graceful shutdown. The provided systemd unit (`Restart=on-failure`, `RestartSec=5`) restarts it, and a restart re-reads the whole configuration and probes the broker again, so a repeated restart is a signal worth alerting on too.
- Keep `kafka.recovery_timeout` at its default unless you have a reason not to: below `10000` ms NBD warns, and a short-lived failure to read the configuration or a secret file is then enough to make the daemon exit.

### Configuration Changes at Runtime (Security Posture)

At startup NBD records the **security posture** of its Kafka connection: the broker address, the *paths* of the TLS files (CA, client certificate, client key) and the *name* of the SASL mechanism. It holds no secret and no file content. Every producer rebuild compares the posture of the `[kafka]` section it has just re-read with the one in force, and refuses what lowers the security of the connection:

| Change found in the file                                                                                       | Outcome                                          |
| -------------------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| `[kafka.tls]` removed                                                                                          | **Refused**, `error` log                         |
| `ca_file` path changed                                                                                         | **Refused**, `error` log                         |
| Client certificate or client key removed (when the reference has one)                                          | **Refused**, `error` log                         |
| `[kafka.auth]` removed                                                                                         | **Refused**, `error` log                         |
| Weaker SASL mechanism (`GSSAPI` = `OAUTHBEARER` > `SCRAM-SHA-512` > `SCRAM-SHA-256` > `PLAIN`)                | **Refused**, `error` log                         |
| `[kafka.tls]` added, or any other change of the TLS paths (a certificate or key swapped for another path)     | Applied, `warn` log                              |
| Stronger SASL mechanism                                                                                        | Applied, `warn` log                              |
| `kafka.broker` changed (even when the reference is plaintext)                                                  | Applied, `warn` log                              |
| Credentials, content of the TLS and secret files, switch between `GSSAPI` and `OAUTHBEARER`, other `[kafka]` parameters | Applied, **no message**                |

How it behaves:

- **A refusal is a failed rebuild attempt.** NBD logs each refused change at `error` level, then the attempt fails like any other: the producer stays faulted (the datagrams are dropped meanwhile), and the supervisor tries again with the usual back-off. The file is read again at each attempt, so restoring it lets the next one succeed. If it isn't restored, NBD exits once `kafka.recovery_timeout` has elapsed.
- **A restart accepts the file.** At startup there is no reference to compare with: the file is trusted, and its permissions and values are validated as usual. A change that was refused at runtime is applied by restarting NBD, which is what the error message tells the operator. In practice, someone who edited the file without being noticed gets their configuration applied once `kafka.recovery_timeout` (10 minutes by default) has elapsed and systemd has restarted NBD: later than without the check, and with `error` logs in between.
- **The reference only moves forward.** It is replaced by the new posture once a producer has been built with it, so a rebuild that tightened the connection raises the floor for the following ones, and an accepted change of broker doesn't warn again.
- **The check runs when a producer is rebuilt, not when the file is edited.** A modification made today is only looked at after the next fatal error of the producer, which can be months away. Nothing in NBD watches the file.

What it does not do:

- **It compares paths and names, not content.** Overwrite the CA file, the certificate or the key at the same path, or change the credentials, and nothing is noticed. Protect the files themselves (below).
- **It doesn't protect the destination.** The broker can change, with a warning. With TLS configured, the new broker must still present a certificate that the same CA signed (NBD leaves the certificate verification settings of `librdkafka` at their defaults), which limits the redirection. Without TLS, which is the default build, nothing verifies the new broker.
- **A plaintext reference can't be downgraded**, so for the default build the check adds a warning on a change of broker and nothing else.
- **It is not an access control.** Whoever controls the configuration file and can wait for NBD to restart ends up with their configuration. The check makes a downgrade slower, noisier and visible; the integrity of `config.toml` and of the files it references remains the control that matters.

Operational guidance:

- Keep the file and the files it references root-owned and read-only for the service, as described in [Kafka Producer Recovery](#kafka-producer-recovery). Note that the permission checks of [Configuration and Secrets Handling](#configuration-and-secrets-handling) don't cover all of them: `config.toml` is only checked when it holds a literal secret, and the CA and the client certificate are never checked.
- Alert on the `error` logs, which are the only trace of a refusal. Each refused change has its own line (for instance `The `kafka.tls` section is missing in the new configuration.`), followed by `Changes in the configuration file were refused.` and by the failed attempt (`An error occurred while recreating a connection with the Kafka broker`). Also watch the `warn` lines about a changed `kafka.broker` or `kafka.tls` configuration. No metric counts the refusals: while they last, `nbd_kafka_producer_generation` stays put, `nbd_kafka_recreation_total` doesn't move and `nbd_errors_sender_total` (rejected submissions) goes up.
- To lower the security of the connection on purpose (retiring TLS, moving to another CA), edit the file and **restart** NBD. Don't expect a runtime rebuild to accept it.

### Metrics Endpoint Exposure

The optional Prometheus exporter (`metrics-exporter` feature) serves plain HTTP, unauthenticated, on `[metrics].interface:port`. It leaks operational metadata — multicast group addresses as labels, topic names, packet/byte counters, and the health of the Kafka producer (fatal errors, recreations, current generation) — that isn't secret exactly, but is still reconnaissance-useful: it tells a scraper when the pipeline is degraded. The README already recommends restricting it to trusted monitoring subnets or loopback; treat that as a hard requirement, since there is no `Authorization` check of any kind in front of it.

### Systemd Hardening

The [systemd unit in the README](README.md#systemd-service) already applies solid baseline hardening (`DynamicUser`, `ProtectSystem=strict`, `NoNewPrivileges`, `CapabilityBoundingSet=`, `PrivateTmp`). `Restart=on-failure` also matters for the [Kafka producer recovery](#kafka-producer-recovery): NBD exits with a non-zero status when it can't rebuild its producer, and systemd brings it back. NBD needs nothing more than network sockets and a config file, so a few extra directives are worth layering on for a production rollout:

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

`LimitCORE=0` serves the secrets: with no core dump, a crash can't write the memory of the process, credentials included, to disk. These aren't in the baseline unit because they need validation against your specific kernel/distro — some `SystemCallFilter` presets can be overly aggressive with `tokio`/`mio`'s epoll usage. Test in staging before rolling to production.

### Kernel Tuning

Covered in [README/Kernel Tuning](README.md#kernel-tuning). The recommended `net.core.rmem_max` / `rmem_default` / `netdev_max_backlog` sysctls matter for reliability under load, which is a security property too: a saturated receive buffer looks identical to the early stage of a UDP flood, and undersized buffers make it cheaper for a noisy or hostile sender on the same multicast segment to cause packet loss.

### Configuration and Secrets Handling

- `config.toml` can now carry secret material when TLS or SASL authentication is configured — passwords, keys, OAuth tokens. The `SecretSource` type allows supplying them in three ways: as an inline literal, as a shell environment variable reference (`{ env = "VAR" }`), or as a path to a secrets file (`{ file = "/path/to/secret" }`). Inline literals are the least safe option — see below.
- **Unix file permission enforcement**: at startup (both `--config-file` and `--check-config-file`) and at every producer rebuild, NBD checks that the files holding secrets have no permission for the group and the others (`mode & 0o077 == 0`, i.e. `chmod 600`), and refuses to start (or to rebuild) if this check fails. The files covered are:
  - `config.toml` itself, only when it holds a literal `password`, `client_secret` or `key_password`;
  - a secrets file used by `{ file = ... }` for one of these three parameters;
  - the TLS `key_file` and the Kerberos `keytab`.

  The check is about secrecy, not about integrity, and it doesn't cover everything the producer reads: a `username` (even literal), the `ca_file` and the `cert_file` are not checked, and a `config.toml` without any literal secret is not checked either. Such a file can be world-writable without NBD saying a word, which matters because [the runtime rebuild reads it again](#kafka-producer-recovery). On Windows, the check emits a warning but does not block startup (the platform does not expose POSIX permission bits).
- **Explicit secret erasure**: NBD doesn't count on a destructor running to get rid of the credentials it holds. As soon as `build_producer` returns, whatever its result (a failed build or a failed broker check included), NBD calls `ProducerConfig::erase_secrets`, which overwrites with zeros the TLS key password, the SASL username and password and the OAuth client secret of the configuration. The same call is made on the temporary configuration of every producer rebuild and by `--check-config-file`. The text of the configuration file is zeroized as soon as it has been parsed, and so is the text of a secret file once it has been read. After the startup erasure, NBD also drops `config.kafka`. `SecretString` (zeroized on drop) remains as a second layer, not as the only one.
  - The erasure is irreversible: an erased configuration is filled with zeros and must not be used again. Each rebuild therefore starts from the configuration file, not from a kept configuration.
  - "Erased" doesn't mean "gone from the process". What NBD can't reach is listed under [Known Limitations](#known-limitations): the copies held by `librdkafka`, the strings of the `ClientConfig` handed to it, the values of `{ env = ... }` variables (they stay in the process environment), and the text kept by a TOML parse error.
- **Secrets are resolved again at every producer recreation** (see [Kafka Producer Recovery](#kafka-producer-recovery)): `{ env = ... }` and `{ file = ... }` sources are read again, so a rotated secret is picked up at the next recreation, and a secret that has disappeared makes the rebuild fail. During a rebuild, credentials live in NBD's memory again until the temporary configuration is erased, and the retired producer, which holds its own copy of the previous credentials, lingers until the reaper thread destroys it (see [Kafka Producer Recovery](#kafka-producer-recovery) for how long).
- `nbd --check-config-file` validates configuration (including secret resolution and file permissions) without starting the daemon — use it in CI/CD or a pre-deploy step rather than validating by pointing a live run at production infrastructure.
- `SecretString` (from the `secrecy` crate) requires an explicit `.expose_secret()` call to access the underlying string — accidental `Debug`/`Display` formatting of secrets is structurally prevented at compile time. Outside of the exception below, nothing in the current logging paths emits raw secret values.
- **One exception, at startup: TOML parse errors are logged with the offending line of the file.** `TOML parse error at line L, column C` is followed by the text of that line, so an error located on a line holding an inline literal secret (`password = ...`) writes that secret to the logs. Prefer `{ env = ... }` and `{ file = ... }` sources over inline literals, and check `--check-config-file` output before sharing it. During a producer rebuild, NBD reports an invalid configuration file with its line and column only, without the offending text. The zeroization of the file text doesn't extend to this case: the parse error returned by the `toml` crate keeps a full copy of the text it failed on, which is released but not wiped when the error is dropped.

### Logging

- `verbosity` is dynamically reloadable and defaults to `info`. `debug`/`trace` log per-packet details — don't run those levels persistently in production, both for performance (extra log I/O isn't free on a sub-millisecond-latency path) and to avoid pushing payload-adjacent metadata into log aggregation systems that may not be secured as tightly as the Kafka pipeline itself.
- Panics are routed to `tracing::error!` via the custom panic hook in `main.rs`. Make sure your log pipeline treats panic-derived error logs as pageable alerts, not noise. NBD's own code is checked in CI against `unwrap`, `expect`, `panic!`, raw indexing and unchecked arithmetic, so a panic is now more likely to come from a dependency, from the system, or from an overflow caught by `overflow-checks`, which is still worth an alert.
- **Delivery failure warnings are throttled** to one per topic and per second, so that a broker outage can't turn into a log flood that fills the disk or saturates the log pipeline. The price is visibility: the occurrences that aren't logged are only counted by `nbd_errors_kafka_transmission_total`, which needs the `metrics-exporter` feature. Without it, they're not counted anywhere.
- Producer recoveries are logged at `error` (the fatal error with its reason, each failed rebuild attempt, each change refused by the [security posture check](#configuration-changes-at-runtime-security-posture)), `warn` (delay before the next attempt, a retirement that could not be confirmed, a change of broker or of the TLS configuration accepted by the posture check) and `info` (successful recreation, with its duration and the number of errors it took). Route the `error` ones to your alerting along with panics: it is the only way to see a refused change, since no metric counts them. A failure to terminate the producer at shutdown is logged at `error` too.

## ANSSI Compliance Matrix

**Status legend:**
- ✅ Compliant ;
- ⚠️ Partially compliant / needs attention ;
- ➖ Not applicable to NBD's current design.

Of the 60 rules and recommendations in the current ANSSI Secure Rust Guide checklist: **27 are fully compliant, none is partially compliant, 33 don't apply** (mostly the unsafe-memory and FFI rule families, moot as long as NBD contains no `unsafe` code and defines no foreign-function boundary of its own).

Two rows deserve a word. The four `Drop` rules apply because the [Kafka producer recovery](#kafka-producer-recovery) introduced NBD's first custom `Drop` implementation. And `LANG-DROP-SEC` is marked ✅ because NBD erases its own copies of the credentials explicitly instead of counting on a destructor, with the limits stated in [Drop Implementations](#drop-implementations): that ✅ says that NBD doesn't *rely only* on `Drop`, not that no copy of a credential survives in the process.

A status describes the code, and the evidence column says what enforces it. Several rules below are enforced by `clippy` lints declared with `#![deny(...)]`, which `cargo clippy` honours but `cargo build` doesn't, and which do not apply to the integration tests in `tests/` (separate crates). The CI (on every push to `main` and every pull request against it) runs `cargo hack clippy --feature-powerset --all-targets -- -D warnings`, so the lints are checked on every feature combination, for the library and the binary. The integration tests in `tests/` and the build script (`build.rs`) are linted too, but with clippy's default lints only: the tests still use `unwrap` and `expect`, which is acceptable in test code, and `build.rs` calls `unwrap` once (see `LANG-LIMIT-PANIC-SRC`). See [Known Limitations](#known-limitations) for the *deployment*-level gaps that remain — those live outside what this matrix measures.

### Development Environment

| Rule                 | Requirement                                              | Status | Evidence                                                                                                                                                                     |
| -------------------- | -------------------------------------------------------- | ------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DENV-STABLE`        | Use a stable compilation toolchain                       | ✅      | `rust-toolchain.toml` pins `channel = "1.99.1"` — a stable release, never `nightly`/`beta`.                                                                                  |
| `DENV-TIERS`         | Exclusive use of Tier 1 `rustc` targets                  | ✅      | Targets restricted to `x86_64-pc-windows-msvc` and `x86_64-unknown-linux-gnu`, both Tier 1.                                                                                  |
| `DENV-CARGO-LOCK`    | Track `Cargo.lock` in version control                    | ✅      | `Cargo.lock` is not excluded by `.gitignore` (only `target/`, `debug/`, etc. are) and pins the exact dependency graph.                                                       |
| `DENV-CARGO-OPTS`    | Keep default values for critical cargo profile variables | ✅      | `[profile.release] overflow-checks = true` deviates from Rust's release default (`false`) — but in the *hardening* direction, not the weakening one the rule guards against. |
| `DENV-CARGO-ENVVARS` | Keep default compiler environment variables              | ✅      | No custom `RUSTFLAGS` or build env vars observed in `build.rs`, the CI workflows, or `Cargo.toml`.                                                                           |
| `DENV-FORMAT`        | Use `rustfmt`                                            | ✅      | Enforced as a gate in `audit.yml` (`cargo fmt -- --check`).                                                                                                                  |
| `DENV-LINTER`        | Use a linter regularly                                   | ✅      | `cargo hack clippy --feature-powerset --all-targets -- -D warnings` in `audit.yml` turns every clippy warning into a build failure on every push/PR to `main`, for every combination of features. Both crate roots also `deny` a set of restriction lints (see `LANG-LIMIT-PANIC`).                                                     |
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
| `LANG-NAMING` | Respect naming conventions | ✅ | Types and functions (`NbdError`, `ProviderConfig`, `socket_buffer_size`, …) follow standard `snake_case`/`PascalCase` conventions; `clippy -D warnings` additionally catches most naming-lint violations. The guide also asks for the Rust API guidelines' naming of getters (no `get_` prefix): `Provider::get_socket` was renamed `Provider::socket`, and no `get_*` function remains in `src/`. |

### Integer Operations

| Rule         | Requirement                      | Status | Evidence                                                                             |
| ------------ | -------------------------------- | ------ | ------------------------------------------------------------------------------------ |
| `LANG-ARITH` | Guard against overflow/underflow | ✅      | Two layers. Statically, `#![deny(clippy::arithmetic_side_effects)]` on both crate roots makes `cargo clippy` reject any arithmetic operator that could overflow, and no `#[allow]` overrides it in `src/`. The code therefore states a policy for each operation (`saturating_*`, `checked_*`, lossless conversions such as `u64::from`, ...): the generation counter, the error counter, the back-off delays, the latency computations of the benchmark sinks, the `message_size` cap. At run time, `[profile.release] overflow-checks = true` turns any overflow that the lint could not see (a dependency, for instance) into a panic rather than a silent wrap. |

### Error Handling

| Rule                   | Requirement                                                      | Status | Evidence                                                                                                                                                                                                                                                                                                      |
| ---------------------- | ---------------------------------------------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `LANG-ERRWRAP` | Custom `Error` type wrapping all errors | ✅ | `NbdError` (`src/errors.rs`) wraps every third-party error type: `toml::de::Error` and `rdkafka::error::KafkaError` through `thiserror`'s `#[from]`, `std::io::Error` and `tokio::task::JoinError` through hand-written `From` impls that put them in an `UnwindShield` (see the note below the table). The module doc comment explicitly cites this rule. |
| `LANG-LIMIT-PANIC` | Limit `panic!` use | ✅ | `#![deny(clippy::panic, clippy::unreachable, clippy::todo, clippy::unimplemented)]` on both crate roots, with no `#[allow]` in `src/`: no `panic!()`, `unreachable!()`, `todo!()` or `unimplemented!()` can be added without failing `cargo clippy`. Plain `assert!` macros are not covered by these lints; none is used in `src/`. |
| `LANG-LIMIT-PANIC-SRC` | Limit panic-capable functions (`unwrap`, `expect`, raw indexing) | ✅ | `#![deny(clippy::unwrap_used, clippy::expect_used)]` on both crate roots, and no `#[allow]` in `src/`: there is no `.unwrap()` or `.expect()` left in the library or the binary, including `BenchSink`/`IPerfSink`, which used to unwrap and now return `NbdError::InvalidPacket`. Poisoned mutexes are recovered or reported (`unwrap_or_else(\|e\| e.into_inner())` for the mutex that serializes the insertion of a topic in the log-throttling table and for the one that holds the security posture in `producer_factory`, an `InvalidPacket` error in the benchmark sinks). Two places are outside this scope: the integration tests in `tests/` (see above), and `build.rs`, which still calls `unwrap()` on `timestamp_opt` when `SOURCE_DATE_EPOCH` holds a timestamp out of range. That one is a build-time panic that stops the build, not the daemon. |
| `LANG-ARRINDEXING` | Validate indexing or use `.get()` | ✅ | `#![deny(clippy::indexing_slicing, clippy::string_slice)]` on both crate roots: raw `[i]`, `[a..b]` and string slicing don't compile under `cargo clippy`. `IPerfSink::submit` and `BenchSink::submit` check `payload.len()` first (`< 12` and `< 8`), then read the fields with `.get(..)` and a fallible `try_from`, returning `NbdError::InvalidPacket` on any failure. |

**A note on `UnwindShield`.** `std::io::Error` and `tokio::task::JoinError` are neither `UnwindSafe` nor `RefUnwindSafe` (both hold trait objects: `dyn Error + Send + Sync`, `dyn Any + Send`), which would make `NbdError` neither of them. `NbdError::Network` and `NbdError::TaskPanic` therefore hold them in an `UnwindShield`, a transparent wrapper around `std::panic::AssertUnwindSafe` (same `Display`, `Debug` and `source`), and a compile-time assertion in `src/errors.rs` fails the build if `NbdError` ever stops being `Send + Sync + UnwindSafe + RefUnwindSafe`. Two limits to keep in mind. The ANSSI guide has no rule on unwind safety, so this is not a compliance item, and it is not listed in the matrix. And `AssertUnwindSafe` is a statement by the author, not a proof: it holds here because these errors are created once and then only read.

### Unsafe Code — Generalities

| Rule               | Requirement                 | Status | Evidence                                                                                                                                                                                                                                   |
| ------------------ | --------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `UNSAFE-NOUB`      | No undefined behavior       | ✅      | No `unsafe` blocks exist anywhere in `src/`.                                                                                                                                                                                               |
| `LANG-UNSAFE`      | Don't use `unsafe` blocks   | ✅      | `#![forbid(unsafe_code)]` is declared at the top of **both** `src/main.rs` and `src/lib.rs`. Since `main.rs` and `lib.rs` compile as separate crates, both need the attribute independently for the guarantee to cover the whole codebase. |
| `LANG-UNSAFE-ENCP` | Encapsulate unsafe features | ➖      | No `unsafe` code exists to encapsulate.                                                                                                                                                                                                    |

### Memory Management

| Rule                                                                                                             | Requirement                                                       | Status | Evidence                                                                                                                                        |
| ---------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- | ------ | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `MEM-NO-LEAK` | No memory leaks | ✅ | No leaking API is called anywhere, and the lint that would catch the most common cause (`mem::forget`) is active crate-wide (see `MEM-FORGET`). The process-wide state added by the Kafka recovery is bounded: the log-throttling table holds one entry per configured topic, and retired producers are queued to the reaper thread at the pace of the recreations, which the back-off spaces out. |
| `MEM-FORGET` | Do not use `mem::forget` | ✅ | `clippy::mem_forget` is part of the `#![deny(...)]` set declared at the top of both `src/main.rs` and `src/lib.rs`. |
| `MEM-FORGET-LINT` | Use the clippy lint for `mem::forget` | ✅ | Same as above — enforced for both the binary and library crates, by `cargo clippy` in CI. |
| `MEM-LEAK`, `MEM-MANUALLYDROP`, `MEM-NORAWPOINTER`, `MEM-INTOFROMRAWALWAYS`, `MEM-INTOFROMRAWONLY`, `MEM-UNINIT` | Various raw-pointer / `ManuallyDrop` / uninitialized-memory rules | ➖      | None of these APIs are used anywhere in the codebase.                                                                                           |

### Foreign Function Interface

| Rule | Requirement | Status | Evidence |
|---|---|---|---|
| All 21 `FFI-*` rules (`FFI-SAFEWRAPPING` through `FFI-CAPI`) | Safe FFI boundary design | ➖ | NBD defines no `extern "C"` boundary of its own — confirms the README's "FFI: N/A (no C FFI exposed)" line. The only native code in the dependency tree is `librdkafka` (pulled in via `rdkafka-sys`, built with `cmake`); its FFI safety is `rdkafka`'s responsibility, not NBD's. NBD's exposure to that native code is instead governed by the supply-chain rules above — `deny.toml` bans the `cc`, `cmake` and `pkg-config` build tools except as dependencies of an explicit list of wrappers (and bans `bindgen` outright), rather than trusting them implicitly. |

### Standard Library Trait Implementations

| Rule | Requirement | Status | Evidence |
|---|---|---|---|
| `LANG-SYNC-TRAITS`, `LANG-CMP-INV`, `LANG-CMP-DEFAULTS`, `LANG-CMP-DERIVE`, `MEM-MUT-REC-RC` | Custom `Send`/`Sync`, comparison traits, cyclic reference-counted pointers with interior mutability | ➖ | No manual `unsafe impl Send`/`Sync` (`UnwindShield` and `NbdError` get their auto traits from their fields), no custom `PartialEq`/`Ord`, and no recursive type built on reference-counted pointers: `Provider` only holds straightforward `Arc<str>` ownership, and `ManagedProducer` shares one non-recursive `Arc<ProducerContainer>`. |

### Drop Implementations

| Rule | Requirement | Status | Evidence |
|---|---|---|---|
| `LANG-DROP` | Justify and document `Drop` implementations | ✅ | `impl Drop for ProducerCell` (`src/kafka.rs`) is NBD's only `Drop` implementation, and its doc comment says why it exists: dropping a `FutureProducer` ends in the blocking `rd_kafka_destroy`, so the cell hands the client to a dedicated reaper thread instead of blocking whichever thread — typically a Tokio worker on the ingestion path — releases the last reference. |
| `LANG-DROP-NO-PANIC` | `Drop` must not panic | ✅ | `ProducerCell::drop` only calls `retire_now`, which does an `Option::take` and sends the producer to the reaper thread over a channel. No `unwrap`/`expect` is possible (denied by lint). A failure to spawn the reaper thread is handled with a warning, and the result of the hand-over is ignored: if the thread is not there, the producer is simply destroyed by the calling thread, which costs latency but cannot panic. |
| `LANG-DROP-NO-CYCLE` | No `Drop` type embedded in a cycle of reference-counted pointers | ✅ | A `ProducerCell` lives in an `Arc` inside the `ArcSwap` of a `ProducerContainer`, itself behind an `Arc` shared by the `ManagedProducer` clones. Nothing reachable from a cell refers back to the container: the producer factory only captures a `PathBuf` and the `Mutex` holding the security posture (plain data), and the supervisor owns a `ManagedProducer` handle rather than being owned by a cell. |
| `LANG-DROP-SEC` | Do not rely only on `Drop` to ensure security | ✅ | `ProducerCell::drop` enforces no security property, and the credentials NBD holds don't depend on a destructor running. (1) **Explicit erasure**: `ProducerConfig::erase_secrets` is called right after `build_producer`, at startup and at every rebuild, and overwrites the credentials with zeros; the text of the configuration file and of secret files is zeroized as soon as it has been used. `SecretString`'s zeroization on drop is a second layer. (2) **Explicit retirement**: a replaced producer is retired by `retire_cell`, which waits for the reaper thread to confirm the destruction, and shutdown calls `ManagedProducer::close`, which does the same. `Drop` is only the fallback, for a producer that is still in use when the wait times out. (3) The causes of missing drops listed by the guide are ruled out: no cycle (`LANG-DROP-NO-CYCLE`), no `mem::forget` (denied by lint), no panic in `Drop`, and `panic = "unwind"` is the unchanged default of the release profile. **Limits of the ✅**: it says that NBD does not rely *only* on `Drop`. The copies that NBD cannot reach (the ones `librdkafka` keeps for authentication, which go away when the producer is destroyed; the strings of the `ClientConfig`; `{ env = ... }` values, which stay in the process environment) are covered by [Known Limitations](#known-limitations), not by this rule. |

## Known Limitations

What's left is mostly deployment-level limitations, plus the consequences of the [Kafka producer recovery](#kafka-producer-recovery) and the exact scope of the code-level guarantees claimed above:

- **Default build is `PLAINTEXT`**: the standard binary (no feature flags) connects to Kafka without TLS or authentication. Operators must explicitly build with the relevant feature flags (`kafka-tls`, `kafka-auth-*`) and supply the appropriate `[kafka.tls]` / `[kafka.auth]` configuration to enable encryption and authentication. This includes the binaries attached to the GitHub Releases, which are default builds (see [Kafka Broker Connectivity](#kafka-broker-connectivity)).
- **The Prometheus metrics endpoint is unauthenticated plaintext HTTP** (see [Metrics Endpoint Exposure](#metrics-endpoint-exposure)).
- **Unix-only file permission enforcement**: the `chmod 600` check for secret-bearing files is skipped on Windows — operators must enforce equivalent access controls manually.
- **The `[kafka]` configuration is trusted at runtime, not just at startup**: a producer recreation re-reads the configuration file and the files it references. The [security posture check](#configuration-changes-at-runtime-security-posture) refuses a rebuild that lowers the security level of the connection, but it compares paths and mechanism names only, accepts a change of broker (with a warning), and only runs when a producer is rebuilt. A replaced CA or key file, other credentials or a redirection to another broker are applied, the broker isn't probed afterwards, and a restart accepts whatever the file contains. Protect those files for the whole runtime, as described in [Kafka Producer Recovery](#kafka-producer-recovery).
- **A refused configuration change costs availability**: while the posture check refuses a rebuild, the producer stays faulted and the datagrams are dropped, up to `kafka.recovery_timeout`, after which NBD exits. No metric counts the refusals: only the `error` logs show them.
- **The permission checks don't cover every file the producer reads**: `config.toml` without a literal secret, the `ca_file`, the `cert_file` and the `username` are not checked (see [Configuration and Secrets Handling](#configuration-and-secrets-handling)).
- **Data is lost while the producer recovers**: datagrams submitted while the producer is faulted are dropped, and messages in flight on it may not be delivered. A retired producer is destroyed without being flushed. The supervisor also stays blind to the new producer for up to about 10 s while it retires the old one (see [Kafka Producer Recovery](#kafka-producer-recovery)).
- **Credentials are partly wiped, not entirely**: NBD overwrites with zeros the copies it owns (see [Configuration and Secrets Handling](#configuration-and-secrets-handling)), but cannot reach several others:
  - the copies `librdkafka` keeps for authentication and re-authentication, which exist for as long as a producer does and are not wiped by NBD;
  - the strings handed to `librdkafka` through `ClientConfig`, which are plain `String`s, released without zeroization when `build_producer` returns;
  - the values of `{ env = ... }` secrets, which stay in the environment of the process (`/proc/<pid>/environ`) for its whole life: removing a variable from the environment is an `unsafe` operation in edition 2024, which the crate forbids;
  - the copy of the configuration text kept by a `toml` parse error, and any intermediate copy made inside the parsing and string-handling libraries.

  Don't rely on this erasure against an attacker who can read the memory of the process (a core dump, `ptrace`, another process running as the same user): it narrows the window, it doesn't close it. The provided systemd unit doesn't disable core dumps: see `LimitCORE=0` in [Systemd Hardening](#systemd-hardening). Under `DynamicUser`, the only readers of `/proc/<pid>/environ` and `/proc/<pid>/mem` are the process itself and root, and `kernel.yama.ptrace_scope` set to 1 or more limits `ptrace` further.
- **Secrets can be logged by a TOML parse error at startup**: the error shows the offending line of the file, which may hold an inline literal secret (see [Logging](#logging)).
- **Panic and overflow protections are lint-level, not compiler-level**: the `#![deny(...)]` set (`unwrap`, `expect`, `panic!`, raw indexing, arithmetic side effects, ...) is enforced by `cargo clippy`, which the CI runs, not by `cargo build`; a local build of a change that violates it succeeds. The integration tests in `tests/` and the build script are outside its scope. And it only constrains NBD's own code: a panic raised by a dependency, or an overflow inside one, is not prevented (`overflow-checks` turns the latter into a panic).
- **`UnwindShield` is an assertion, not a proof**: `NbdError` is `UnwindSafe` because the two non-unwind-safe errors it carries are wrapped in `AssertUnwindSafe`, which relies on those errors being only read after creation. It is checked at compile time that `NbdError` has the property, not that the assertion is right.
- **Throttled failure logs**: Kafka delivery failures are logged at most once per topic and per second. Without the `metrics-exporter` feature, the other occurrences are not counted anywhere.

## Future Hardening Roadmap

Roughly in priority order:

1. Add authentication to the Prometheus metrics endpoint (e.g. a static bearer token or mTLS).
