#![forbid(unsafe_code)]
#![deny(
    clippy::mem_forget,
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::string_slice,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::todo,
    clippy::unimplemented
)]
//! NBD daemon entry point.
//!
//! Orchestrates the full application lifecycle:
//! 1. Initialize dynamic log filtering via `tracing_subscriber::reload`: `info`, or the level of
//!    the `RUST_LOG` environment variable
//! 2. Parse CLI arguments ([`Cli`]). `--about` prints the build information and exits;
//!    `--check-config-file` runs steps 3 and 4 on the given file, submits the settings to
//!    `librdkafka` without creating a client ([`check`](nbd::ProducerConfig::check)), erases the
//!    secrets and exits; without `--config-file`, the configuration file is `./config.toml`, with
//!    an error log
//! 3. Install a custom panic hook that routes panics to structured `tracing::error!` logs
//! 4. Parse the TOML configuration ([`RawConfig`]), check the permissions of its files
//!    ([`check_secrets`]), then validate it ([`Config`])
//! 5. Start the optional Prometheus metrics exporter (feature-gated), then apply the `verbosity` of
//!    the configuration to the log filter (`RUST_LOG`, when set, takes precedence)
//! 6. Create the socket of each provider ([`Provider::create_socket`]), which fails if the kernel
//!    grants a smaller receive buffer than configured
//! 7. Create the Kafka producer ([`build_producer`]), erase the secrets of the configuration
//!    (`ProducerConfig::erase_secrets`) whatever the outcome, and run a pre-flight broker
//!    connectivity check via `fetch_metadata`
//! 8. Wrap the producer in a [`ManagedProducer`] and spawn its supervisor, which recreates the
//!    producer after a fatal error (see [`RecoveryPolicy`]). The [`SecurityPosture`](nbd::SecurityPosture) of the
//!    configuration is handed to [`producer_factory`], so that a later rebuild cannot change it,
//!    and the configuration is then dropped
//! 9. Join the multicast groups ([`Provider::join`]) and spawn one async listener task per provider
//!    in a [`JoinSet`]
//! 10. Monitor signals (Ctrl+C, SIGTERM on Unix) and the health of the listeners and of the
//!     supervisor for graceful shutdown
//! 11. Shut down: one cancellation token stops the listeners and the supervisor together. The
//!     daemon then waits, each stage bounded by a 5-second timeout, for the listeners to drain
//!     ([`task_termination`]), for the supervisor, and for the managed producer to be closed
//!     ([`ManagedProducer::close`]), which waits for it to be released and destroyed (at most
//!     twice that timeout)
//!
//! If a listener or the supervisor fails, or if a shutdown stage fails, the daemon exits with an
//! error once the shutdown sequence is over.

use std::io::IsTerminal;
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
    check_secrets, producer_factory,
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
/// structured logging before returning an appropriate [`ExitCode`](std::process::ExitCode).
fn main() -> std::process::ExitCode {
    let (filter_layer, reload_handle) = reload::Layer::new(
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
    );

    if let Err(e) = tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt::layer().compact())
        .try_init()
    {
        eprintln!("Failed to initialize the logs: {e}");
        return std::process::ExitCode::FAILURE;
    };

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            tracing::error!("Failed to initialize runtime : {e}");
            return std::process::ExitCode::FAILURE;
        }
    };

    match runtime.block_on(run(reload_handle)) {
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
/// Returns `Ok(())` upon clean exit or an [`NbdError`] if a fatal startup,
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
        let raw_config = RawConfig::from_path(&config_path, !std::io::stdout().is_terminal())?;
        check_secrets(&raw_config.kafka, &config_path)?;

        let mut config = Config::try_from(raw_config)?;

        let check = config.kafka.check();
        config.kafka.erase_secrets();
        check?;

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

    let raw_config = RawConfig::from_path(&config_path, true)?;
    check_secrets(&raw_config.kafka, &config_path)?;
    let mut config = Config::try_from(raw_config)?;

    #[cfg(feature = "metrics-exporter")]
    if let Some(config_metrics) = config.metrics {
        match PrometheusBuilder::new()
            .with_http_listener((config_metrics.interface, config_metrics.port))
            .install()
        {
            Ok(_) => {
                info!(
                    "Prometheus metrics exporter started at http://{}:{}",
                    config_metrics.interface, config_metrics.port,
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    "Failed to start Prometheus metrics exporter on http://{}:{} : {}",
                    config_metrics.interface, config_metrics.port, e,
                );
                Err(NbdError::Setup(format!(
                    "Failed to start Prometheus metrics exporter on http://{}:{} : {}",
                    config_metrics.interface, config_metrics.port, e,
                )))
            }
        }?;
    }

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

    let mut providers: Vec<Provider> = config.provider.iter().map(Provider::from).collect();

    for provider in providers.iter_mut() {
        if let Err(e) = provider.create_socket(&config.nbd.socket_buffer_size) {
            error!(
                "Failed to create a listener socket for {} : {}",
                provider.group_label, e
            );
            return Err(e);
        }
    }

    info!(
        "Connecting to the Kafka broker at {} ...",
        config.kafka.broker
    );

    let producer_candidate = build_producer(&config.kafka);

    config.kafka.erase_secrets();

    let kafka_producer = match producer_candidate {
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

    let factory = producer_factory(config_path, config.kafka.posture());

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

    for provider in providers.iter() {
        if let Err(e) = provider.join() {
            error!("Failed to join {} : {}", provider.group_label, e);
            cancel_token.cancel();
            return Err(e);
        }
    }

    for mut provider in providers {
        let task_tk = cancel_token.clone();
        let task_producer = producer_manager.clone();

        listener_tasks.spawn(async move {
            match provider.start_listener(task_producer, task_tk).await {
                Ok(_) => {}
                Err(e) => {
                    error!(
                        "Something went wrong while listening to {} : {}",
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
                    process_error = Some(NbdError::from(e));
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
                    process_error = Some(NbdError::from(e));
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
                    process_error = Some(NbdError::from(e));
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
                    process_error = Some(NbdError::from(e));
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
                Err(NbdError::from(e))
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

    let producer_termination = match producer_manager.close(Duration::from_secs(5)).await {
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
