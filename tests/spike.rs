mod commons;
#[cfg(windows)]
use commons::NtSetTimerResolution;
use commons::show_report;

use nothing_but_data::{Interface, Provider, ProviderConfig};

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

#[cfg(windows)]
const WINDOWS_TIMER_TESTING_RESOLUTION_MS: f32 = 0.5;

const SPIKE_TEST_DURATION_SECONDS: u64 = 5;

#[tokio::test]
async fn spike_test() {
    println!(
        "Spike test will be running for {} seconds.",
        SPIKE_TEST_DURATION_SECONDS
    );

    let mut tx_count = AtomicU64::new(0);
    let rx_count = Arc::new(AtomicU64::new(0));
    let ro_count = Arc::new(AtomicUsize::new(0));

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

    let epoch = Instant::now();

    let config_test = ProviderConfig {
        topic: "test".to_string(),
        group: "239.255.0.1".parse().unwrap(),
        port: 0,
        message_size: 2048,
        interface: Interface::V4(std::net::Ipv4Addr::UNSPECIFIED),
    };

    let mut provider = Provider::from(&config_test);
    provider.subscribe(&(4 * 1024 * 1024)).unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::channel(10000);
    let token = CancellationToken::new();
    let listener_token = token.clone();

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

    tokio::spawn(async move { provider.start_listener(tx, listener_token).await });

    let receiver_token = token.clone();

    let dst_addr: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(239, 255, 0, 1)), port);
    let sender_socket = UdpSocket::bind("0.0.0.0:20010").await;

    assert_eq!(sender_socket.is_ok(), true);

    let canceler_token = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(SPIKE_TEST_DURATION_SECONDS)).await;
        canceler_token.cancel();
    });

    let task_rx_count = Arc::clone(&rx_count);
    let task_ro_count = Arc::clone(&ro_count);

    let latencies = tokio::spawn(async move {
        let mut lats = Vec::new();
        loop {
            let loop_rx_count = Arc::clone(&task_rx_count);
            let loop_ro_count = Arc::clone(&task_ro_count);
            tokio::select! {
                _ = receiver_token.cancelled() => break,
                Some(message) = rx.recv() => {
                    let ts_rx = Instant::now();
                    let ts_tx_ns = u64::from_be_bytes(message.payload[..8].try_into().unwrap());
                    let ts_tx = epoch + Duration::from_nanos(ts_tx_ns);
                    lats.push(ts_rx.duration_since(ts_tx));
                    loop_rx_count.fetch_add(1, Ordering::Relaxed);
                    loop_ro_count.fetch_add(message.payload.len(), Ordering::Relaxed);
                }
            }
        }
        lats
    });

    let mut interval = tokio::time::interval(Duration::from_micros(1));

    match sender_socket {
        Ok(socket) => {
            let sender_token = token.clone();
            let mut payload = vec![0xFFu8; 1300];
            payload.fill(0xFF);

            loop {
                interval.tick().await;

                let nanos = epoch.elapsed().as_nanos() as u64;
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

    show_report(
        &format!("{}-seconds-spike-test", SPIKE_TEST_DURATION_SECONDS),
        latencies.await.expect("Failed to compute test data."),
        Some(*tx_count.get_mut()),
        Some(rx_count.load(Ordering::Relaxed)),
    );

    let total_received_bytes = ro_count.load(Ordering::Relaxed) as u64;

    println!(
        "Processed a total of {} bytes in {} secs. ({} bytes/s)",
        total_received_bytes,
        SPIKE_TEST_DURATION_SECONDS,
        total_received_bytes / SPIKE_TEST_DURATION_SECONDS
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
