#![forbid(unsafe_code)]
#![deny(clippy::mem_forget)]
//! NBD daemon entry point.
//!
//! Orchestrates the full application lifecycle:
//! 1. Parse CLI arguments ([`Cli`])
//! 2. Initialize dynamic log filtering via `tracing_subscriber::reload`
//! 3. Install a custom panic hook that routes panics to structured `tracing::error!` logs
//! 4. Parse and validate the TOML configuration ([`RawConfig`] → [`Config`])
//! 5. Start the optional Prometheus metrics exporter (feature-gated)
//! 6. Pre-flight Kafka broker connectivity check via `fetch_metadata`
//! 7. Spawn one async listener task per provider in a [`JoinSet`]
//! 8. Monitor signals (Ctrl+C, SIGTERM on Unix) and task health for graceful shutdown
//! 9. Drain all in-flight tasks with a 5-second timeout via [`task_termination`]

use std::path::PathBuf;
use std::time::Duration;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use rdkafka::config::ClientConfig;
use rdkafka::producer::Producer;

use tracing::{debug, error, info, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt, reload};

#[cfg(feature = "metrics-exporter")]
use metrics_exporter_prometheus::PrometheusBuilder;

use clap::Parser;

use nothing_but_data::{Config, NbdError, Provider, RawConfig};

mod args;
use args::Cli;

mod about;
use about::about;

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
        let raw_config = RawConfig::from_path(&config_path)?;
        Config::try_from(raw_config)?;
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

    info!("Starting NBD...");

    let raw_config = RawConfig::from_path(&config_path)?;
    let config = Config::try_from(raw_config)?;

    #[cfg(feature = "metrics-exporter")]
    match PrometheusBuilder::new()
        .with_http_listener((config.metrics.interface, config.metrics.port))
        .set_buckets_for_metric(
            "nbd_e2e_latency",
            &[0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.1, 1, 10, 100],
        )
        .install()
    {
        Ok(_) => {
            info!(
                "Prometheus metrics exporter started at http://{}:{}",
                config.metrics.interface, config.metrics.port,
            );
            Ok(())
        }
        Err(e) => {
            error!(
                "Failed to start Prometheus metrics exporter on http://{}:{} : {}",
                config.metrics.interface, config.metrics.port, e,
            );
            Err(NbdError::Setup(format!(
                "Failed to start Prometheus metrics exporter on http://{}:{} : {}",
                config.metrics.interface, config.metrics.port, e,
            )))
        }
    }?;

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(config.nbd.verbosity.to_string()));

    if let Err(e) = reload_handle.reload(filter) {
        eprintln!(
            "Failed to apply the requested log level for program execution : {}",
            e
        );
    }

    let mut listener_tasks = JoinSet::<Result<(), NbdError>>::new();

    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    let cancel_token = CancellationToken::new();

    let providers: Vec<Provider> = config.provider.iter().map(Provider::from).collect();

    info!(
        "Connecting to the Kafka broker at {} ...",
        config.kafka.broker
    );

    let kafka_producer = match ClientConfig::new()
        .set("bootstrap.servers", &config.kafka.broker)
        .set(
            "message.timeout.ms",
            config.kafka.message_timeout.to_string(),
        )
        .set(
            "message.send.max.retries",
            config.kafka.message_retries.to_string(),
        )
        .set("compression.type", "lz4")
        .set("max.in.flight.requests.per.connection", "5")
        .set("queue.buffering.max.messages", "100000")
        .set("enable.idempotence", "true")
        .set("linger.ms", "0")
        .set("acks", "all")
        .create::<rdkafka::producer::FutureProducer>()
    {
        Ok(producer) => {
            match producer
                .client()
                .fetch_metadata(None, Duration::from_millis(config.kafka.connection_timeout))
            {
                Ok(metadata) => {
                    info!(
                        "Successfully connected to the Kafka broker. (found {} brokers)",
                        metadata.brokers().len()
                    );
                }
                Err(e) => {
                    error!("Failed to reach the Kafka broker.");
                    cancel_token.cancel();
                    return Err(NbdError::Kafka(e));
                }
            };

            producer
        }
        Err(e) => {
            error!("A fatal error occured : {}", e);
            cancel_token.cancel();
            return Err(NbdError::Kafka(e));
        }
    };

    for mut provider in providers {
        let task_tk = cancel_token.clone();
        let task_producer = kafka_producer.clone();

        listener_tasks.spawn(async move {
            match provider.subscribe(&config.nbd.socket_buffer_size) {
                Ok(()) => match provider.start_listener(task_producer, task_tk).await {
                    Ok(_) => {}
                    Err(e) => {
                        error!(
                            "Something went wrong while listening to {} : {}",
                            provider.group, e
                        );
                        return Err(e);
                    }
                },
                Err(e) => {
                    error!(
                        "Something went wrong while subscribing to {} : {}",
                        provider.group, e
                    );
                    return Err(e);
                }
            };

            Ok(())
        });
    }

    info!("NBD started successfully and is ready to accept incoming traffic.");

    #[cfg(unix)]
    tokio::select! {
        _ = ctrl_c => { info!("Received Ctrl+C signal, stopping NBD."); cancel_token.cancel(); }
        _ = sigterm.recv() => { info!("Received SIGTERM signal, stopping NBD."); cancel_token.cancel(); }
        _ = cancel_token.cancelled() => {}
        Some(res) = listener_tasks.join_next() => {
            match res {
                Ok(Ok(())) => {
                    debug!("A listener stopped gracefully.");
                },
                Ok(Err(e)) => {
                    error!("A fatal error occured with one or more listener : {}", e);
                    error!("NBD will now try to stop gracefully.");
                    cancel_token.cancel();
                }
                Err(e) => {
                    if e.is_panic() {
                        error!("NBD panicked while listening to the network traffic : {}", e);
                    } else {
                        warn!("A listener was cancelled : {}", e);
                    }
                    cancel_token.cancel();
                }
            }
        }
    }

    #[cfg(windows)]
    tokio::select! {
        _ = ctrl_c => { info!("Received Ctrl+C signal, stopping NBD."); cancel_token.cancel();  }
        _ = cancel_token.cancelled() => {}
        Some(res) = listener_tasks.join_next() => {
            match res {
                Ok(Ok(())) => {
                    debug!("A listener stopped gracefully.");
                },
                Ok(Err(e)) => {
                    error!("A fatal error occured with one or more listener : {}", e);
                    error!("NBD will now try to stop gracefully.");
                    cancel_token.cancel();
                }
                Err(e) => {
                    if e.is_panic() {
                        error!("NBD panicked while listening to the network traffic : {}", e);
                    } else {
                        warn!("A listener was cancelled : {}", e);
                    }
                    cancel_token.cancel();
                }
            }
        }
    }

    match task_termination(listener_tasks).await {
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

/// Drains all remaining listener tasks with a 5-second timeout.
///
/// Waits for every task in the [`JoinSet`] to complete. If any task is still
/// running after 5 seconds, returns [`NbdError::Termination`].
async fn task_termination(
    mut listener_tasks: tokio::task::JoinSet<Result<(), NbdError>>,
) -> Result<(), NbdError> {
    let listener_termination_status = tokio::time::timeout(Duration::from_secs(5), async {
        while listener_tasks.join_next().await.is_some() {}
    })
    .await;

    if listener_termination_status.is_err() {
        error!("Some providers failed to stop correctly.");
        return Err(NbdError::Termination(String::from(
            "Some providers failed to stop correctly.",
        )));
    }

    Ok(())
}
