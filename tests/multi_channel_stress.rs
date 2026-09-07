mod commons;
#[cfg(windows)]
use commons::NtSetTimerResolution;
use commons::show_report;

use nothing_but_data::{BenchSink, Interface, Provider, ProviderConfig};

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

#[cfg(windows)]
const WINDOWS_TIMER_TESTING_RESOLUTION_MS: f32 = 0.005;

const STRESS_TEST_DURATION_SECONDS: u64 = 5 * 60;
const STRESS_TEST_FRQ_MICROS: u64 = 10;
const STRESS_TEST_CHANNEL_COUNT: usize = 5;

/// This test aims at testing NBD's internal handling of a continuous network flow from multiple producers.
/// It doesn't take into consideration the transaction time with a Kafka broker (this is dealt with in the "e2e_multi_channel_stress" test).
#[tokio::test]
async fn multi_channel_stress_test() {
    println!(
        "Stress test will be running for {} seconds.",
        STRESS_TEST_DURATION_SECONDS
    );

    let tx_count = Arc::new(AtomicU64::new(0));

    #[cfg(windows)]
    let mut timer_resolution_change = false;

    #[cfg(windows)]
    let mut actual_100ns = 0;

    #[cfg(windows)]
    unsafe {
        let res = NtSetTimerResolution(
            (WINDOWS_TIMER_TESTING_RESOLUTION_MS * 10_000.0) as u32,
            1,
            &mut actual_100ns,
        );

        if res == 0 {
            println!(
                "Successfully reduced Windows' timer resolution to {} ms.",
                (actual_100ns as f32) / 10_000.0
            );
            timer_resolution_change = true;
        } else {
            println!("Failed to reduce Windows' timer resolution. Default is at 15,6ms.");
        }
    }

    let token = CancellationToken::new();
    let mut test_sinks: Vec<BenchSink> = Vec::new();

    for i in 1..(STRESS_TEST_CHANNEL_COUNT + 1) {
        let config_test = ProviderConfig {
            topic: "test".to_string(),
            group: format!("239.255.0.{i}").parse().unwrap(),
            port: 0,
            message_size: 2048,
            interface: Interface::V4(std::net::Ipv4Addr::UNSPECIFIED),
        };

        let mut provider = Provider::from(&config_test);
        provider.subscribe(&(2 * 1024 * 1024)).unwrap();

        let port = match provider.get_socket() {
            Ok(ref socket) => match socket.local_addr() {
                Ok(sockaddr) => match sockaddr.as_socket() {
                    Some(socketaddr) => socketaddr.port(),
                    None => 0,
                },
                Err(_) => 0,
            },
            Err(_) => 0,
        };

        assert_ne!(port, 0);

        let listener_token = token.clone();
        let listener_sink = BenchSink::default();

        let task_sink = listener_sink.clone();

        tokio::spawn(async move { provider.start_listener(task_sink, listener_token).await });

        let task_sink = listener_sink.clone();
        let task_token = token.clone();
        let mut task_interval =
            tokio::time::interval(Duration::from_micros(STRESS_TEST_FRQ_MICROS));
        task_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let task_socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();
        let task_tx_count = Arc::clone(&tx_count);

        let mut payload = vec![0xFFu8; 1300];

        let dst_addr: SocketAddr = format!("239.255.0.{i}:{port}").parse().unwrap();
        payload.fill(0xFF);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                _ = task_token.cancelled() => {
                    break;
                }
                _ = task_interval.tick() => {
                        let nanos = task_sink.epoch.elapsed().as_nanos() as u64;
                        payload[..8].copy_from_slice(&nanos.to_be_bytes());

                        match task_socket.send_to(&payload, dst_addr).await {
                            Ok(_) => {
                                task_tx_count.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_) => {}
                        }
                    }
                }
            }
        });

        test_sinks.push(listener_sink);

        println!("Spawned a listener on : 239.255.0.{i}:{port}");
    }

    tokio::time::sleep(std::time::Duration::from_secs(STRESS_TEST_DURATION_SECONDS)).await;
    token.cancel();

    let total_received_bytes: u64 = test_sinks
        .clone()
        .into_iter()
        .map(|s| s.received_bytes.load(Ordering::Relaxed))
        .sum::<usize>() as u64;

    let total_received_messages: u64 = test_sinks
        .clone()
        .into_iter()
        .map(|s| s.received_messages.load(Ordering::Relaxed))
        .sum::<u64>() as u64;

    let latencies = test_sinks
        .clone()
        .into_iter()
        .flat_map(|s| s.latencies.lock().unwrap().to_vec())
        .collect();

    show_report(
        &format!("{}-seconds-multi-stress-test", STRESS_TEST_DURATION_SECONDS),
        latencies,
        Some(tx_count.load(Ordering::Relaxed)),
        Some(total_received_messages),
    );

    println!(
        "Processed a total of {} bytes in {} secs. ({} bytes/s)",
        total_received_bytes,
        STRESS_TEST_DURATION_SECONDS,
        total_received_bytes / STRESS_TEST_DURATION_SECONDS
    );

    #[cfg(windows)]
    unsafe {
        if timer_resolution_change {
            if NtSetTimerResolution(5_000, 0, &mut actual_100ns) == 0 {
                println!("Successfully restored Windows' timer resolution.");
            } else {
                println!("Failed to restore Windows' timer resolution.");
            }
        };
    }
}
