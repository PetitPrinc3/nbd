mod commons;
#[cfg(windows)]
use commons::NtSetTimerResolution;
use commons::show_report;

use nbd::{BenchSink, Interface, Provider, ProviderConfig};

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

#[cfg(windows)]
const WINDOWS_TIMER_TESTING_RESOLUTION_MS: f32 = 0.005;

const STRESS_TEST_DURATION_SECONDS: u64 = 5 * 60;
const STRESS_TEST_FRQ_MICROS: u64 = 1000;

/// This test aims at testing NBD's internal handling of a continuous network flow from one producer.
/// It doesn't take into consideration the transaction time with a Kafka broker (this is dealt with in the "e2e_stress" test).
#[tokio::test]
async fn stress_test() {
    println!(
        "Stress test will be running for {} seconds.",
        STRESS_TEST_DURATION_SECONDS
    );

    let mut tx_count = AtomicU64::new(0);

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

    let config_test = ProviderConfig {
        topic: "test".to_string(),
        group: "239.255.0.1".parse().unwrap(),
        port: 0,
        message_size: 2048,
        interface: Interface::V4(std::net::Ipv4Addr::UNSPECIFIED),
    };

    let mut provider = Provider::from(&config_test);
    provider.subscribe(&(2 * 1024 * 1024)).unwrap();

    let token = CancellationToken::new();
    let listener_token = token.clone();
    let test_sink = BenchSink::default();

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

    let listener_sink = test_sink.clone();

    tokio::spawn(async move { provider.start_listener(listener_sink, listener_token).await });

    let dst_addr: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(239, 255, 0, 1)), port);
    let mut interval = tokio::time::interval(Duration::from_micros(STRESS_TEST_FRQ_MICROS));
    let sender_socket = UdpSocket::bind("0.0.0.0:20010").await;

    match sender_socket {
        Ok(_) => {}
        Err(ref e) => {
            println!("Failed to bind socket : {}", &e);
        }
    }

    let canceler_token = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(STRESS_TEST_DURATION_SECONDS)).await;
        canceler_token.cancel();
    });

    match sender_socket {
        Ok(socket) => {
            let sender_token = token.clone();

            let mut payload = vec![0xFFu8; 1300];
            payload.fill(0xFF);

            loop {
                interval.tick().await;

                let nanos = test_sink.epoch.elapsed().as_nanos() as u64;
                payload[..8].copy_from_slice(&nanos.to_be_bytes());

                tokio::select! {
                    _ = sender_token.cancelled() => {break}
                    Ok(_) = socket.send_to(&payload, dst_addr) => {tx_count.fetch_add(1, Ordering::Relaxed);}
                }
            }
        }
        Err(e) => {
            println!("Unexpected failure on sender socket : {}", e);
        }
    };

    let total_received_bytes: u64 = test_sink
        .received_bytes
        .load(Ordering::Relaxed)
        .try_into()
        .unwrap();

    show_report(
        &format!("{}-seconds-stress-test", STRESS_TEST_DURATION_SECONDS),
        test_sink.latencies.lock().unwrap().to_vec(),
        Some(*tx_count.get_mut()),
        Some(test_sink.received_messages.load(Ordering::Relaxed)),
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
