#![forbid(unsafe_code)]
#![deny(clippy::mem_forget)]
//! NBD daemon entry point.
//!
//! Orchestrates the full application lifecycle:
//! 1. Initialize dynamic log filtering via `tracing_subscriber::reload`
//! 2. Parse CLI arguments ([`Cli`])
//! 3. Install a custom panic hook that routes panics to structured `tracing::error!` logs
//! 4. Parse and validate the TOML configuration ([`RawConfig`] → [`Config`])
//! 5. Start the optional Prometheus metrics exporter (feature-gated)
//! 6. Create the Kafka producer ([`build_producer`]) and run a pre-flight broker connectivity
//!    check via `fetch_metadata`
//! 7. Wrap the producer in a [`ManagedProducer`] and spawn its supervisor, which recreates the
//!    producer after a fatal error (see [`RecoveryPolicy`])
//! 8. Spawn one async listener task per provider in a [`JoinSet`]
//! 9. Monitor signals (Ctrl+C, SIGTERM on Unix) and the health of the listeners and of the
//!    supervisor for graceful shutdown
//! 10. Shut down in order, each stage with a 5-second timeout: drain the listeners via
//!     [`task_termination`], stop the supervisor, drop the managed producer, then wait for the
//!     retired producers to be destroyed via [`wait_for_retired_producers`]
//!
//! If a listener or the supervisor fails, or if a shutdown stage fails, the daemon exits with an
//! error once the shutdown sequence is over.

use std::path::PathBuf;
use std::time::Duration;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use rdkafka::producer::Producer;

use tracing::{debug, error, info, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt, reload};

#[cfg(feature = "metrics-exporter")]
use metrics_exporter_prometheus::PrometheusBuilder;

use clap::Parser;

use nbd::{
    Config, ManagedProducer, NbdError, Provider, RawConfig, RecoveryPolicy, build_producer,
    check_secrets, producer_factory, wait_for_retired_producers,
};

mod args;
use args::Cli;

mod about;
use about::about;

type LogReloadHandle = tracing_subscriber::reload::Handle<EnvFilter, tracing_subscriber::Registry>;

/// Application entry point and top-level error boundary.
///
/// Initializes the global [`tracing`] subscriber and log reload layer, delegates the
/// daemon lifecycle execution to [`run`], and formats any fatal [`NbdError`] through
/// structured logging before returning an appropriate [`ExitCode`].
#[tokio::main]
async fn main() -> std::process::ExitCode {
    let (filter_layer, reload_handle) = reload::Layer::new(
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
    );

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt::layer().compact())
        .init();

    match run(reload_handle).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!("{e}");
            std::process::ExitCode::FAILURE
        }
    }
}

/// Executes the core daemon lifecycle and task orchestration.
///
/// Handles CLI parsing, configuration loading, panic hooks, optional metrics
/// exporter initialization, Kafka connection checks, provider task spawning,
/// signal handling, and graceful shutdown.
///
/// Returns [`Ok(())`] upon clean exit or an [`NbdError`] if a fatal startup,
/// configuration, or runtime error occurs.
async fn run(reload_handle: LogReloadHandle) -> Result<(), NbdError> {
    let args = Cli::parse();

    if args.about {
        about();
        return Ok(());
    }

    std::panic::set_hook(Box::new(|info| {
        tracing::error!("panic : {info}");
    }));

    if let Some(config_path) = args.check_config_file {
        let raw_config = RawConfig::from_path(&config_path)?;
        check_secrets(&raw_config.kafka, &config_path)?;
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
    check_secrets(&raw_config.kafka, &config_path)?;
    let config = Config::try_from(raw_config)?;

    #[cfg(feature = "metrics-exporter")]
    match PrometheusBuilder::new()
        .with_http_listener((config.metrics.interface, config.metrics.port))
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

    let kafka_producer = match build_producer(&config.kafka) {
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
            return Err(e);
        }
    };

    let recovery_policy = RecoveryPolicy::new(Duration::from_millis(config.kafka.recovery_timeout));
    let producer_manager = ManagedProducer::new(kafka_producer);
    let factory = producer_factory(config_path);

    let mut supervisor_manager = producer_manager.clone();
    let supervisor_token = cancel_token.clone();

    let mut supervisor_task = tokio::spawn(async move {
        supervisor_manager
            .supervise(recovery_policy, factory, supervisor_token)
            .await
    });

    // Dropping unnecessary copies of the Kafka configuration.
    // This ensures that unnecessary copies of the secrets are not kept in memory.
    // The only remaining secrets become the ones stored by librdkafka and required for authentication and reauthentication.
    drop(config.kafka);

    for mut provider in providers {
        let task_tk = cancel_token.clone();
        let task_producer = producer_manager.clone();

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

    let mut process_error: Option<NbdError> = None;

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
                    process_error = Some(e);
                    cancel_token.cancel();
                }
                Err(e) => {
                    if e.is_panic() {
                        error!("NBD panicked while listening to the network traffic : {}", e);
                    } else {
                        warn!("A listener was cancelled : {}", e);
                    }
                    process_error = Some(NbdError::TaskPanic(e));
                    cancel_token.cancel();
                }
            }
        }
        res = &mut supervisor_task => {
            match res {
                Ok(Ok(())) => {
                    debug!("The supervisor stopped gracefully.");
                },
                Ok(Err(e)) => {
                    error!("A fatal error occured with the supervisor : {}", e);
                    error!("NBD will now try to stop gracefully.");
                    process_error = Some(e);
                    cancel_token.cancel();
                }
                Err(e) => {
                    if e.is_panic() {
                        error!("NBD panicked while supervising the Kafka producer : {}", e);
                    } else {
                        error!("The producer was cancelled : {}", e);
                        error!("NBD will now try to stop gracefully.");
                    }
                    process_error = Some(NbdError::TaskPanic(e));
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
                    process_error = Some(e);
                    cancel_token.cancel();
                }
                Err(e) => {
                    if e.is_panic() {
                        error!("NBD panicked while listening to the network traffic : {}", e);
                    } else {
                        warn!("A listener was cancelled : {}", e);
                    }
                    process_error = Some(NbdError::TaskPanic(e));
                    cancel_token.cancel();
                }
            }
        }
        res = &mut supervisor_task => {
            match res {
                Ok(Ok(())) => {
                    debug!("The supervisor stopped gracefully.");
                },
                Ok(Err(e)) => {
                    error!("A fatal error occured with the supervisor : {}", e);
                    error!("NBD will now try to stop gracefully.");
                    process_error = Some(e);
                    cancel_token.cancel();
                }
                Err(e) => {
                    if e.is_panic() {
                        error!("NBD panicked while supervising the Kafka producer : {}", e);
                    } else {
                        error!("The producer was cancelled : {}", e);
                        error!("NBD will now try to stop gracefully.");
                    }
                    process_error = Some(NbdError::TaskPanic(e));
                    cancel_token.cancel();
                }
            }
        }
    }

    let listener_termination = match task_termination(listener_tasks).await {
        Ok(_) => {
            info!("All listeners were stopped gracefully.");
            Ok(())
        }
        Err(e) => {
            error!("Failed to terminate some listeners : {}", e);
            Err(e)
        }
    };

    let supervisor_termination = if !supervisor_task.is_finished() {
        match tokio::time::timeout(Duration::from_secs(5), &mut supervisor_task).await {
            Ok(Ok(Ok(()))) => {
                info!("Supervisor task stopped gracefully.");
                Ok(())
            }
            Ok(Ok(Err(e))) => {
                warn!("The supervisor task failed : {}", e);
                Err(e)
            }
            Ok(Err(e)) => {
                warn!("The supervisor task failed : {}", e);
                Err(NbdError::TaskPanic(e))
            }
            Err(e) => {
                error!("Failed to terminate the supervisor task gracefully : {e}");
                Err(NbdError::Termination(String::from(
                    "Failed to terminate the supervisor task gracefully.",
                )))
            }
        }
    } else {
        Ok(())
    };

    drop(producer_manager);

    let producer_termination = match wait_for_retired_producers(Duration::from_secs(5)) {
        Ok(()) => {
            info!("All producers were stopped gracefully.");
            Ok(())
        }
        Err(e) => {
            error!("Failed to terminate some producers : {}", e);
            Err(e)
        }
    };

    match listener_termination {
        Ok(_) => match producer_termination {
            Ok(()) => match supervisor_termination {
                Ok(()) => match process_error {
                    Some(e) => Err(e),
                    None => Ok(()),
                },
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        },
        Err(e) => Err(e),
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
