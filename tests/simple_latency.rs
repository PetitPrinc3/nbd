#[cfg(windows)]
mod commons;
#[cfg(windows)]
use commons::NtSetTimerResolution;

use nothing_but_data::{Interface, Provider, ProviderConfig};

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

#[cfg(windows)]
const WINDOWS_TIMER_TESTING_RESOLUTION_MS: f32 = 0.5;

#[tokio::test]
async fn minor_latency_test() {
    let test_message_count = 3;

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

    let (tx, mut rx) = tokio::sync::mpsc::channel(10);
    let mut horodatages: HashMap<u8, Instant> = HashMap::new();
    let token = CancellationToken::new();
    let listener_token = token.clone();

    tokio::spawn(async move { provider.start_listener(tx, listener_token).await });

    let dst_addr: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(239, 255, 0, 1)), port);

    let sender_socket = UdpSocket::bind("0.0.0.0:20020").await;

    match sender_socket {
        Ok(socket) => {
            for i in 0..test_message_count {
                horodatages.insert(i, Instant::now());
                match socket.send_to(&[i], dst_addr).await {
                    Ok(_) => match tokio::time::timeout(Duration::from_secs(1), rx.recv()).await {
                        Ok(message) => {
                            let i = &message.unwrap().payload[0];
                            let latence = horodatages.remove(i).unwrap().elapsed();
                            println!("Message n°{i} latency : {latence:?}");
                        }
                        Err(_) => {
                            println!("Timedout...")
                        }
                    },
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
