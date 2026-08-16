#![forbid(unsafe_code)]
//No Bullshit Daemon

use std::path::PathBuf;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureRecord, Producer};
use rdkafka::util::Timeout;

use tracing::{debug, error, info, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt, reload};

use clap::Parser;

mod config;
use config::Config;

mod providers;
use providers::Provider;

mod errors;
use errors::NbdError;

mod message;
use message::Message;

mod args;
use args::Cli;

mod about;
use about::about;

mod checks;
use checks::config_check;

#[tokio::main]
async fn main() -> Result<(), NbdError> {
    let args = Cli::parse();

    if args.about {
        about();
        return Ok(());
    }

    let (filter_layer, reload_handle) = reload::Layer::new(
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
    );

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt::layer().compact())
        .init();

    std::panic::set_hook(Box::new(|info| {
        tracing::error!("panic : {info}");
    }));

    if let Some(config_path) = args.check_config_file {
        config_check(config_path)?;
        info!("The provided configuration file is valid and can be used as-is.");
        return Ok(());
    }

    let config_path: PathBuf = match args.config_file {
        Some(path) => {
            debug!("Config file submited through CLI arguments.");
            path
        }
        None => {
            error!("No config file specified, defaulting to `config.toml`.");
            PathBuf::from("./config.toml")
        }
    };

    let config: Config = config_check(config_path)?;

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(config.nbd.verbosity.to_string()));

    if let Err(e) = reload_handle.reload(filter) {
        eprintln!(
            "Failed to apply the requested log level for program execution : {}",
            e
        );
    }

    let mut tasks = JoinSet::new();

    let (tx, mut rx) = mpsc::channel::<Message>(1000);

    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    let cancel_token = CancellationToken::new();

    for candidate in config.provider {
        let task_tx = tx.clone();
        let task_tk = cancel_token.clone();

        tasks.spawn(async move {
            let mut provider: Provider = Provider::from_config(&candidate);
            match provider.subscribe(&config.nbd.socket_buffer_size) {
                Ok(()) => match provider.start_listener(task_tx, task_tk).await {
                    Ok(_) => {}
                    Err(e) => {
                        error!(
                            "Something went wrong while listening to {} : {}",
                            provider.group, e
                        );
                    }
                },
                Err(e) => {
                    error!(
                        "Something went wrong while subscribing to {} : {}",
                        provider.group, e
                    );
                }
            };
        });
    }

    let consumer_tk = cancel_token.clone();
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
                match producer
                    .client()
                    .fetch_metadata(None, Duration::from_secs(5))
                {
                    Ok(metadata) => {
                        info!(
                            "Successfully connected to the Kafka broker. (found {} brokers)",
                            metadata.brokers().len()
                        );
                        Ok(())
                    }
                    Err(e) => {
                        error!("Failed to reach the Kafka broker.");
                        consumer_tk.cancel();
                        Err(NbdError::Kafka(e))
                    }
                }?;

                while let Some(message) = rx.recv().await {
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
                    }
                }
                consumer_tk.cancel();
                Ok(())
            }
            Err(e) => {
                consumer_tk.cancel();
                Err(NbdError::Kafka(e))
            }
        }) as Result<(), NbdError>
    });

    #[cfg(unix)]
    tokio::select! {
        _ = ctrl_c => { info!("Received Ctrl+C signal, stopping NBD."); cancel_token.cancel(); drop(tx) }
        _ = sigterm.recv() => { info!("Received SIGTERM signal, stopping NBD."); cancel_token.cancel(); drop(tx) }
        _ = cancel_token.cancelled() => {}
    }

    #[cfg(windows)]
    tokio::select! {
        _ = ctrl_c => { info!("Received Ctrl+C signal, stopping NBD."); cancel_token.cancel(); drop(tx) }
        _ = cancel_token.cancelled() => {}
    }

    match task_termination(tasks, consumer_task).await {
        Ok(_) => {
            info!("NBD exited gracefully.");
            return Ok(());
        }
        Err(e) => {
            error!("Failed to stop gracefully : {}", e);
            return Err(e);
        }
    }

}

async fn task_termination(
    mut listener_tasks: tokio::task::JoinSet<()>,
    consumer_task: tokio::task::JoinHandle<Result<(), NbdError>>,
) -> Result<(), NbdError> {
    let listener_termination_status = tokio::time::timeout(Duration::from_secs(5), async {
        while listener_tasks.join_next().await.is_some() {}
    })
    .await;

    if listener_termination_status.is_err() {
        error!("Some providers failed to stop correctly.");
    }

    let consumer_termination_status =
        tokio::time::timeout(Duration::from_secs(5), consumer_task).await;

    if consumer_termination_status.is_err() {
        error!("The consumer task failed to stop correctly.");
    }

    Ok(())
}
