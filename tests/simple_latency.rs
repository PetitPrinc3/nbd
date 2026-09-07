#[cfg(windows)]
mod commons;
#[cfg(windows)]
use commons::NtSetTimerResolution;

use nothing_but_data::{BenchSink, Interface, Provider, ProviderConfig};

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

#[cfg(windows)]
const WINDOWS_TIMER_TESTING_RESOLUTION_MS: f32 = 0.5;

const TEST_MESSAGE_COUNT: usize = 3;

/// This test aims at testing NBD's internal latency for a small number of messages to validate the internal processing mechanism
#[tokio::test]
async fn minor_latency_test() {
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
        group: "239.255.0.1".parse().unwrap(), // multicast local, portable
        port: 0,                               // laisse l'OS choisir un port libre
        message_size: 2048,
        interface: Interface::V4(std::net::Ipv4Addr::UNSPECIFIED),
        // adapte les champs manquants/différents à ta vraie ProviderConfig
    };

    let mut provider = Provider::from(&config_test);
    provider.subscribe(&(64 * 1024)).unwrap();

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

    let test_sink = BenchSink::default();

    let token = CancellationToken::new();
    let listener_token = token.clone();
    let listener_sink = test_sink.clone();

    tokio::spawn(async move { provider.start_listener(listener_sink, listener_token).await });

    let dst_addr: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(239, 255, 0, 1)), port);

    let sender_socket = UdpSocket::bind("0.0.0.0:20000").await;

    let mut payload = vec![0xFFu8; 1300];
    payload.fill(0xFF);

    match sender_socket {
        Ok(socket) => {
            for _ in 0..TEST_MESSAGE_COUNT {
                let nanos = test_sink.epoch.elapsed().as_nanos() as u64;
                payload[..8].copy_from_slice(&nanos.to_be_bytes());
                match socket.send_to(&payload, dst_addr).await {
                    Ok(_) => {}
                    Err(e) => println!("Unexpected failure while sending data : {}", e),
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        Err(e) => {
            println!("Unexpected failure on sender socket : {}", e);
        }
    }

    token.cancel();

    for (idx, latency) in test_sink
        .latencies
        .lock()
        .unwrap()
        .to_vec()
        .iter()
        .enumerate()
    {
        println!("Latency n° {idx} : {latency:?}");
    }

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
