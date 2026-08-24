mod commons;
#[cfg(windows)]
use commons::NtSetTimerResolution;
use commons::show_report;

use nothing_but_data::{Interface, Provider, ProviderConfig};

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

#[cfg(windows)]
const WINDOWS_TIMER_TESTING_RESOLUTION_MS: f32 = 0.005;

const STRESS_TEST_DURATION_SECONDS: u64 = 60;
const STRESS_TEST_CHANNEL_COUNT: usize = 5;

#[tokio::test]
async fn multi_channel_stress_test() {
    println!(
        "Stress test will be running for {} seconds.",
        STRESS_TEST_DURATION_SECONDS
    );

    let epoch = Instant::now();

    let tx_count = Arc::new(AtomicU64::new(0));
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

    let mut dest_addrs: Vec<Arc<SocketAddr>> = Vec::new();

    let (tx, mut rx) = tokio::sync::mpsc::channel(STRESS_TEST_CHANNEL_COUNT * 10_000);
    let token = CancellationToken::new();

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

        let provider_tx = tx.clone();
        let listener_token = token.clone();

        dest_addrs.push(
            Arc::new(format!("239.255.0.{i}:{port}")
                .parse()
                .expect("Unable to convert string to SocketAddr.")),
        );

        tokio::spawn(async move { provider.start_listener(provider_tx, listener_token).await });

        println!("Spawned a listener on : 239.255.0.{i}:{port}");
    }

    let receiver_token = token.clone();

    let mut interval = tokio::time::interval(Duration::from_micros(100));
    let sender_socket = UdpSocket::bind("0.0.0.0:20030").await;

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

    match sender_socket {
        Ok(socket) => {
            let sender_token = token.clone();

            let mut payload = vec![0xFFu8; 1300];
            payload.fill(0xFF);

            loop {
                tokio::select! {
                    _ = sender_token.cancelled() => {
                        break;
                    }
                    _ = interval.tick() => {
                        for dst_addr in &dest_addrs {
                            let nanos = epoch.elapsed().as_nanos() as u64;
                            payload[..8].copy_from_slice(&nanos.to_be_bytes());
                            
                            match socket.send_to(&payload, **dst_addr).await {
                                Ok(_) => {
                                    tx_count.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(_) => {}
                            }

                        }
                    }
                }
            }
        }
        Err(e) => {
            println!("Unexpected failure on sender socket : {}", e);
        }
    };

    show_report(
        &format!("{}-seconds-stress-test", STRESS_TEST_DURATION_SECONDS),
        latencies.await.expect("Failed to compute test data."),
        Some(tx_count.load(Ordering::Relaxed)),
        Some(rx_count.load(Ordering::Relaxed)),
    );

    let total_received_bytes = ro_count.load(Ordering::Relaxed) as u64;

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