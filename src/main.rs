#![forbid(unsafe_code)]
//No Bullshit Daemon

use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinSet;

use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureRecord;
use rdkafka::util::Timeout;

mod config;
use config::Config;

mod providers;
use providers::Provider;

mod errors;
use errors::NbdError;

mod message;
use message::Message;

use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), NbdError> {
    let contenu = std::fs::read_to_string("config.toml")?;
    let config: Config = toml::from_str(&contenu)?;

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.nbd.verbosity));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let mut tasks = JoinSet::new();
    let (tx, mut rx) = mpsc::channel::<Message>(1000);

    for candidate in config.provider {
        let task_tx = tx.clone();
        tasks.spawn(async move {
            let mut provider: Provider = Provider::from_config(&candidate);
            match provider.subscribe(&config.nbd.network_buffer_size) {
                Ok(()) => {
                    match provider.start_listener(task_tx).await {
                        Ok(_) => {}
                        Err(e) => {
                            error!(
                                "Something went wrong while listening to {} : {}",
                                provider.group, e
                            );
                        }
                    };
                }
                Err(e) => {
                    error!(
                        "Something went wrong while subscribing to {} : {}",
                        provider.group, e
                    );
                }
            };
        });
    }

    //while tasks.join_next().await.is_some() {
    // essayer de redémarrer les producers, mais je n'ai pas compris comment ça s'orchestrait avec la consumer_task
    //};

    let consumer_task = tokio::spawn(async move {
        info!(
            "Connecting to the Kafka broker at {} ...",
            config.kafka.broker
        );

        (match ClientConfig::new()
            .set("bootstrap.servers", &config.kafka.broker)
            .set("message.timeout.ms", config.kafka.timeout.to_string())
            .set("message.send.max.retries", config.kafka.retries.to_string())
            .set("compression.type", "lz4")
            .set("acks", "1")
            .create::<rdkafka::producer::FutureProducer>()
        {
            Ok(producer) => {
                info!("Successfully connected to the Kafka broker.");
                while let Some(message) = rx.recv().await {
                    //println!("Got message @ {} on topic {} : {} ", message.timestamp, message.group, message.payload);
                    let record: FutureRecord<[u8], [u8]> =
                        FutureRecord::to(&message.topic).payload(message.payload.as_ref());

                    match producer
                        .send(record, Timeout::After(Duration::from_millis(100)))
                        .await
                    {
                        Ok(_) => {
                            debug!(
                                "Successfully sent some message on topic {} !",
                                &message.topic
                            );
                        }
                        Err((e, _)) => {
                            warn!(
                                "Failed to send one message on topic {} : {}",
                                &message.topic, e
                            )
                        }
                    };
                }
                Ok(())
            }
            Err(e) => Err(NbdError::Kafka(e)),
        }) as Result<(), NbdError>
    });

    consumer_task.await?
}
