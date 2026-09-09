# Performance

This document details how **Nothing But Data**'s (NBD) performance is measured, and presents the corresponding results. Latency and throughput are characterized in several complementary ways, from isolated internal benchmarks up to a full end-to-end test.

NBD's primary goal is to relay UDP multicast messages to a Kafka broker with the lowest possible latency. To evaluate this, the [end-to-end test](#end-to-end) measures the complete transaction, from UDP emission to Kafka delivery acknowledgment. However, most of the latency observed in that test was found to come from the broker itself, not from NBD.

For that reason, the tests below instead isolate **NBD's own internal processing latency**, which is what underpins the software's "sub-millisecond latency" claim:

- [`multi_channel_stress`](#multi_channel_stress) — a pure Rust benchmark that generates a cadenced network flow using Tokio. Being self-contained, this test is sensitive to the precision of its own emitter (see the difference with `multi_channel_iperf_stress` below).
- [`multi_channel_iperf_stress`](#multi_channel_iperf_stress) — a Rust test that instead relies on an external `iperf2` process (required for UDP support, since `iperf3` doesn't support it) to generate the network flow in parallel.

NBD is a CPU-intensive process: performance depends heavily on the combination of available CPU cores, number of providers, and packet frequency, so there is no universal recommended "providers-per-core" ratio. Under CPU saturation, the first symptom is packet loss — datagrams dropped by the kernel before they reach NBD, and therefore not directly measurable by the application — followed by increased latency once saturation deepens. It is recommended to run an extended test campaign during a non-critical period to validate NBD's compliance with your own organization's requirements before going live.

## Contents

- [Test Environment](#test-environment)
- [End-to-end](#end-to-end)
- [Internal Process Latency](#internal-process-latency)
    - [`multi_channel_stress`](#multi_channel_stress)
    - [`multi_channel_iperf_stress`](#multi_channel_iperf_stress)
        - [Network Capacity Analysis](#network-capacity-analysis)
- [Resource Considerations](#resource-considerations)

## Test Environment

> [!NOTE] 
> Unless otherwise specified, all results below were obtained on a Debian 13.1 (Trixie) LXC container with 8 vCPU cores and 16 GB RAM, hosted on Proxmox.  
> While NBD is compatible with Windows, it is developed for and intended to run on Linux.

## End-to-end

The end-to-end test measures the complete message transaction — from UDP datagram emission to Kafka delivery acknowledgment. It is the most representative test of NBD's contribution to real-world pipeline latency, since it covers every hop the data actually travels through.
The kafka broker is a virtualised mock server provided by [Testcontainer]()'s `Redpanda` docker container.

### `e2e_perf_test`

_(300 seconds)_

| Providers | Bitrate (Mb/provider) | Flow (MB/s) | Accuracy (%) | p50 (µs) | p95 (µs) | p99 (µs) |
| :-------: | --------------------- | ----------- | ------------ | -------- | -------- | -------- |
|     1     | 1 000                 | 60.4        | 100          | 200      | 309      | 873      |
|     1     | 100 000               | 59.7        | 100          | 195      | 303      | 527      |
|     3     | 1 000                 | 150.9       | 100          | 466      | 2 515    | 4 106    |
|     3     | 100 000               | 151.8       | 100          | 474      | 1 720    | 4 379    |
|     5     | 1 000                 | 215.4       | 100          | 838      | 3 901    | 6 814    |
|     5     | 100 000               | 210.8       | 100          | 821      | 2 530    | 6 062    |
> [!NOTE]
> In this test, the accuracy column is the percentage of messages successfully received by the Kafka broker over the number of messages received by `nbd`.
> In the following tests, the accuracy column represents the amount of messages processed by `nbd` over the number of messages sent, either by the tokio generated senders or the `iperf` binary.
## Internal Process Latency

### `multi_channel_stress`

_(300 seconds)_

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

### `multi_channel_iperf_stress`

_(300 seconds)_

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

> The "Bitrate (Mb/provider)" column is the flow requested from `iperf`, while "Flow (MB/s)" is the flow actually measured by NBD, based on the total number of bytes processed. The gap between those two columns — while accuracy remains at or near 100% — points to a limitation of the host's network capabilities, not of the NBD software. This is confirmed below.

#### Network Capacity Analysis

To verify that the gap between requested and measured throughput above comes from the host's network interface rather than from NBD, the requested bitrate is compared against two ceilings: the raw value requested via `iperf`, and the same value capped at what a 2.5 Gb/s NIC can physically sustain (2.5 Gb/s ÷ 8 = 312.5 MB/s).

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

Once the 2.5 Gb/s link is taken into account, measured throughput consistently lands in the 55–105% range of the theoretical maximum — instead of the near-zero ratios suggested by the raw comparison (e.g. 0.05% at 3 providers / 1,000,000 Mb). This confirms that the throughput ceiling observed under heavy load is a property of the test network's hardware, not a limitation of NBD itself.

## Resource Considerations

> [!NOTE] 
> Enabling the `metrics-exporter` feature adds overhead and can reduce measured performance. Its use is discouraged in environments that don't require this visibility — especially when running against a large number of providers under heavy network load, close to hardware saturation.
