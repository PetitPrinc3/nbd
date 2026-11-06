# Performance

How NBD spends its time on the data path, how it is measured, and the results. For sizing and tuning, see [PRODUCTION.md](PRODUCTION.md#tuning).

Performance depends on the combination of CPU cores, providers and packet rate, so measure NBD on your own hardware before going live. Under CPU saturation, the first symptom is packet loss in the kernel, then higher latency.

## Hot path

For each datagram, the task of a provider:

1. reads it into its preallocated `BytesMut` buffer, one copy from the kernel
2. splits it into a reference-counted `Bytes`, without copying
3. clones the `Arc<str>` of the topic, a reference count
4. loads the current producer from an `ArcSwap`, lock-free
5. calls `send_result`: `rdkafka` copies the topic name into a `CString`, and `librdkafka` copies the payload into its queue (`RD_KAFKA_MSG_F_COPY`). The `Bytes` is released right after, so the buffer can be reused before the delivery
6. tracks the delivery: a `oneshot` channel and its boxed sender in `rdkafka`, a boxed future in NBD, and a task node in a `FuturesUnordered`

NBD adds no payload copy of its own. A failed delivery copies the payload once more, in `rdkafka`.

NBD's code takes no lock on this path. `librdkafka` takes its own, and with `metrics-exporter`, each counter goes through the read lock of the metrics registry. That feature adds two labelled counters per datagram and one per delivery.

Off this path, the supervisor polls the producer every 500 ms, and retired producers are destroyed on the `nbd-kafka-reaper` thread, since `rd_kafka_destroy` blocks.

NBD uses the multi-threaded Tokio runtime, with one worker per core and no pinning. `librdkafka` adds a main thread and one thread per broker connection, and `rdkafka` a polling thread for delivery reports.

The release profile keeps `overflow-checks = true`. LTO and `codegen-units` keep their defaults, and the figures below don't cover other profiles.

## Results

> [!WARNING]
> These figures were last updated on 2026-09-10, for `0.1.0-beta`. Since then:
>
> - They were measured with `linger.ms = 0` and `lz4`, then hard-coded. The defaults are now `linger = 1` and `compression = "none"`, so the end-to-end figures can't be expected to hold with the default configuration.
> - `queue_weight` limits the queue to about 100 MB by default, instead of `librdkafka`'s 1 GiB.
> - The receive buffer of a provider is a fixed 6.55 MB, instead of 100 times `message_size`, and sockets are bound to their interface and group on Linux.
> - The Kafka sink is a `ManagedProducer`.
> - The tests no longer raise the resolution of the Windows timer, so the Windows figures can't be reproduced as they are.

Unless stated otherwise, the host is a Debian 13.1 LXC container with 8 vCPU cores and 16 GB RAM, on Proxmox.

### End-to-end

From UDP emission to the Kafka delivery acknowledgment, over 300 seconds, against a Redpanda mock broker provisioned by [Testcontainers](https://testcontainers.com/). Most of this latency was attributed to the broker. Accuracy is the share of messages received by NBD that reached the broker.

The harness is an external Python project, not part of this repository. It needs the `nbd_e2e_latency` histogram, commented out in `src/sinks.rs`.

| Providers | Bitrate (Mb/provider) | Flow (MB/s) | Accuracy (%) | p50 (µs) | p95 (µs) | p99 (µs) |
| :-------: | --------------------- | ----------- | ------------ | -------- | -------- | -------- |
|     1     | 1 000                 | 60.4        | 100          | 200      | 309      | 873      |
|     1     | 100 000               | 59.7        | 100          | 195      | 303      | 527      |
|     3     | 1 000                 | 150.9       | 100          | 466      | 2 515    | 4 106    |
|     3     | 100 000               | 151.8       | 100          | 474      | 1 720    | 4 379    |
|     5     | 1 000                 | 215.4       | 100          | 838      | 3 901    | 6 814    |
|     5     | 100 000               | 210.8       | 100          | 821      | 2 530    | 6 062    |

### Internal latency

These tests use in-memory sinks: `librdkafka` and `ManagedProducer` aren't part of what they measure. Accuracy is the share of sent messages that NBD processed.

#### `multi_channel_stress`

A self-contained Rust benchmark, with Tokio tasks as emitters, over 300 seconds. Its results depend on the precision of these emitters.

|  Providers  | Pulse (Hz) | Flow (MB/s) | Accuracy (%) | p50 (µs) | p95 (µs) | p99 (µs) |
| :---------: | ---------- | ----------- | ------------ | -------- | -------- | -------- |
|      1      | 1 000      | 1.3         | 100          | 35       | 64       | 91       |
|      1      | 10 000     | 12.9        | 100          | 92       | 227      | 332      |
|      1      | 100 000    | 84.9        | 100          | 435      | 849      | 1 150    |
|      3      | 10 000     | 38.9        | 100          | 240      | 597      | 874      |
|      3      | 100 000    | 91.6        | 100          | 901      | 2 020    | 2 465    |
|      5      | 1 000      | 6.5         | 100          | 61       | 140      | 202      |
|      5      | 5 000      | 12.9        | 100          | 94       | 220      | 323      |
|      5      | 10 000     | 64.7        | 100          | 398      | 985      | 1 402    |
|      5      | 100 000    | 87.6        | 100          | 1 181    | 2 954    | 3 668    |
| 5 (unicast) | 5 000      | 12.9        | 100          | 88       | 213      | 328      |

> [!NOTE]
> Windows results:
>
> |Providers|Pulse (Hz)|Flow (MB/s)|Accuracy (%)|p50 (µs)|p95 (µs)|p99 (µs)|
> |:-:|---|---|---|---|---|---|
> |1|100 000|25.3|100|971|2 002|2 493|
> |5|1 000|5.6|100|182|462|666|
> |5|5 000|11.2|100|283|821|1 126|
> |5|10 000|33.7|100|1 200|2 607|3 063|

#### `multi_channel_iperf_stress`

The traffic comes from external `iperf2` processes (`iperf3` doesn't support multicast), over 300 seconds. "Bitrate" is requested from `iperf`, "Flow" is measured by NBD.

| Providers | Bitrate (Mb/provider) | Flow (MB/s) | Accuracy (%) | p50 (µs) | p95 (µs) | p99 (µs) |
| :-------: | --------------------- | ----------- | ------------ | -------- | -------- | -------- |
|     1     | 10                    | 1.3         | 100          | 14       | 24       | 36       |
|     1     | 1 000                 | 64.2        | 100          | 4        | 5        | 6        |
|     1     | 100 000               | 64.5        | 100          | 4        | 5        | 6        |
|     3     | 100 000               | 174.0       | 100          | 5        | 8        | 9        |
|     3     | 1 000 000             | 173.8       | 100          | 5        | 7        | 9        |
|     5     | 10                    | 6.5         | 100          | 5        | 15       | 24       |
|     5     | 100                   | 65.5        | 100          | 5        | 7        | 10       |
|     5     | 250                   | 163.8       | 100          | 5        | 8        | 12       |
|     5     | 750                   | 245.9       | 100          | 5        | 9        | 11       |
|     5     | 1 000                 | 249.4       | 100          | 6        | 9        | 12       |
|    10     | 1 000                 | 287.5       | 99.99        | 33       | 5 390    | 30 798   |
|    20     | 13                    | 34.0        | 100          | 5        | 8        | 12       |
|    20     | 150                   | 282.5       | 98           | 46       | 201 434  | 378 398  |
|    50     | 15                    | 98.3        | 100          | 6        | 31       | 47       |

#### Network capacity

A gap between requested and measured flow, with accuracy near 100%, points at the test network rather than at NBD. This table compares the measured flow with the requested one, and with the same value capped at what a 2.5 Gb/s NIC sustains (312.5 MB/s).

| Providers | Bitrate (Mb/provider) | Measured Flow (MB/s) | Expected (MB/s) | Measured / Expected | Expected (2.5 Gb/s cap) | Measured / Expected (2.5 Gb/s cap) |
| :-------: | --------------------: | -------------------: | --------------: | ------------------: | ----------------------: | ---------------------------------: |
|     1     |                    10 |                  1.3 |            1.25 |             104.00% |                    1.25 |                            104.00% |
|     1     |                 1 000 |                 64.2 |             125 |              51.36% |                     125 |                             51.36% |
|     1     |               100 000 |                 64.5 |          12 500 |               0.52% |                   312.5 |                             20.64% |
|     3     |               100 000 |                174.0 |          37 500 |               0.46% |                   312.5 |                             55.68% |
|     3     |             1 000 000 |                173.8 |         375 000 |               0.05% |                   312.5 |                             55.62% |
|     5     |                    10 |                  6.5 |            6.25 |             104.00% |                    6.25 |                            104.00% |
|     5     |                   100 |                 65.5 |            62.5 |             104.80% |                    62.5 |                            104.80% |
|     5     |                   250 |                163.8 |          156.25 |             104.83% |                  156.25 |                            104.83% |
|     5     |                   750 |                245.9 |          468.75 |              52.46% |                   312.5 |                             78.69% |
|     5     |                 1 000 |                249.4 |             625 |              39.90% |                   312.5 |                             79.81% |
|    10     |                 1 000 |                287.5 |           1 250 |              23.00% |                   312.5 |                             92.00% |
|    20     |                    13 |                 34.0 |            32.5 |             104.62% |                    32.5 |                            104.62% |
|    20     |                   150 |                282.5 |             375 |              75.33% |                   312.5 |                             90.40% |
|    50     |                    15 |                 98.3 |           93.75 |             104.85% |                   93.75 |                            104.85% |

With the cap, multi-provider runs reach 55% to 105% of the ceiling. Single-provider runs plateau at about 64 MB/s: whether that comes from the single `iperf` stream or from the receive path of one provider hasn't been established.

Rows at about 104% are low-rate runs. The ratio 2²⁰ / 10⁶ = 1.0486 points at a binary-versus-decimal conversion between the `iperf` command line and the table, not at a real excess.

## Reproducing the benchmarks

See the [test table](CONTRIBUTING.md#tests). Use `--release`, the interface that carries your multicast route, and a `net.core.rmem_max` large enough for the test.

```bash
export NBD_TEST_INTERFACE=eth0

sudo sysctl -w net.core.rmem_max=2097152
cargo test --release --test multi_channel_stress -- --nocapture

# Build first, so that the listeners are up when iperf starts. They listen
# for 5 minutes on 239.255.0.1 to .5, ports 20001 to 20005.
sudo sysctl -w net.core.rmem_max=262144000
cargo test --release --no-run --test multi_channel_iperf_stress
cargo test --release --test multi_channel_iperf_stress -- --nocapture &
sleep 2
for i in 1 2 3 4 5; do
    iperf -c 239.255.0.$i -u -p $((20000 + i)) -b 100M -l 1300 -t 280 -T 1 &
done
wait
```

- Other rows of the tables come from editing `STRESS_TEST_CHANNEL_COUNT` and `STRESS_TEST_FRQ_MICROS` in the tests, and the `iperf` bitrate.
- Nominal rates aren't always reached: `spike` asks for 1 MHz and delivered about 100 kHz on a 2-core VM. The printed `Flow` is what counts.
- The benchmarks run on Tokio's single-threaded test runtime, while the daemon uses the multi-threaded one. In `multi_channel_stress`, emitters and listeners share one thread.
- `IPerfSink` compares the clocks of sender and receiver. With a remote `iperf`, latencies are only as good as the synchronization of the two hosts, and a sender ahead of the receiver gets its packets rejected.
- With `metrics-exporter`, `multi_channel_iperf_stress` also serves metrics on `127.0.0.1:9000`.
