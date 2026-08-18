use std::cmp;
use std::net::Ipv4Addr;
use std::net::ToSocketAddrs;
use std::path::PathBuf;

use crate::config::{Config, Interface};
use crate::errors::NbdError;

use tracing::{debug, error, info, warn};

// Checks the nbd config
fn nbd_config_check(config: &Config) -> Result<(), NbdError> {
    if config.nbd.parallel_senders == 0 {
        error!(
            "The `nbd.parallel_senders` parameter can't be 0 and should at least be 1. A default value of 1000 is recommended."
        );
        return Err(NbdError::Config(String::from(
            "The `nbd.parallel_senders` parameter can't be 0.",
        )));
    } else if config.nbd.parallel_senders < 100 {
        warn!(
            "The `nbd.parallel_senders` parameter is low ({}). It is recommended to increase it to at least 100, depending on your infrastructure and server capabilities.",
            config.nbd.parallel_senders
        )
    }

    if config.nbd.parallel_receivers == 0 {
        error!(
            "The `nbd.parallel_receivers` parameter can't be 0 and should at least be 1. A default value of 1000 is recommended."
        );
        return Err(NbdError::Config(String::from(
            "The `nbd.parallel_receivers` parameter can't be 0.",
        )));
    } else if config.nbd.parallel_receivers < 1000 {
        warn!(
            "The `nbd.parallel_receivers` parameter is low ({}). It is recommended to increase it to at least 1000, depending on your infrastructure and server capabilities.",
            config.nbd.parallel_receivers
        )
    }

    if config.nbd.parallel_receivers <= config.nbd.parallel_senders {
        warn!(
            "The `nbd.parallel_receivers` parameter is lower than the `config.nbd.parallel_senders` which doesn't make sense  ({} <= {}). The software should be able to receive messages faster than it sends them as to be able to back pressure the network traffic.",
            config.nbd.parallel_receivers, config.nbd.parallel_senders,
        )
    };

    if config.nbd.socket_buffer_size == 0 {
        error!("The submitted nbd.socket_buffer_size is to low.");
        warn!(
            "Please increase the parameter's value to a realistic size based on the expected network traffic (a minimum of 100ko is recommended)."
        );
        Err(NbdError::Config(format!(
            "The submitted nbd.socket_buffer_size ({}) is to low.",
            config.nbd.socket_buffer_size
        )))
    } else if config.nbd.socket_buffer_size < 100 * 1024 {
        info!(
            "Your selected socket buffer size seems low. Be advised that the software will silently drop packets as soon as the buffer is filled."
        );
        Ok(())
    } else {
        Ok(())
    }
}

fn kafka_config_check(mut config: Config) -> Result<Config, NbdError> {
    // Checks kafka
    match config.kafka.broker.to_socket_addrs() {
        Ok(_) => {
            debug!("Valid ip/port combination.");
        }
        Err(_) => {
            error!(
                "The submitted `kafka.broker` parameter is not a valid <ip>:<port> combination."
            );
            warn!(
                "If you are using a hostname instead of an ip, this error can mean that the specified hostname doesn't resolve."
            );
        }
    };

    if config.kafka.connection_timeout < 500 {
        info!(
            "The `kafka.connection_timeout` parameter is low ({}ms). It is recommended to increase it to at least 500ms, depending on your infrastructure and network reliability.",
            config.kafka.connection_timeout
        );
    } else {
        debug!("The `kafka.connection_timeout` parameter is configured correctly.");
    };

    if config.kafka.message_timeout < 100 {
        info!(
            "The `kafka.message_timeout` parameter is low ({}ms). It is recommended to increase it to at least 100ms, depending on your infrastructure and network reliability.",
            config.kafka.message_timeout
        );
    } else {
        debug!("The `kafka.message_timeout` parameter is configured correctly.");
    };

    if config.kafka.message_retries < 1 {
        warn!(
            "The `kafka.message_retries` parameter cannot be 0. It was increased to the default value of `2`."
        );
        config.kafka.message_retries = 2;
    } else {
        debug!("The `kafka.message_retries` parameter is configured correctly.");
    };

    Ok(config)
}

pub fn kafka_topic_check(topic: &str, idx: usize) -> Result<(), NbdError> {
    if topic.len() > 254 || topic.is_empty() {
        return Err(NbdError::Config(format!(
            "The `providers.{}.topic` parameter's size is invalid as it should be between 1 and 254 chars. It will be replaced by a default value of `nbd-connector`.",
            idx
        )));
    } else {
        debug!("The `providers.{}.topic` parameter has a valid size.", idx)
    }

    if !topic
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.' || c == '_')
    {
        return Err(NbdError::Config(format!(
            "The `providers.{}.topic` parameter is invalid as it should only contain alphanumeric characters or '.-_'. It will be replaced by a default value of `nbd-connector`.",
            idx
        )));
    } else {
        debug!(
            "The `providers.{}.topic` parameter contains only valid characters.",
            idx
        )
    }

    Ok(())
}

fn provider_config_check(mut config: Config) -> Result<Config, NbdError> {
    if config.provider.is_empty() {
        error!("At least one provider is required.");
        return Err(NbdError::Config(String::from(
            "At least one provider is required.",
        )));
    }

    let mut idx: usize = 0;

    for provider in config.provider.iter_mut() {
        idx += 1;

        if provider.topic.is_empty() {
            warn!(
                "The `providers.{}.topic` parameter is unspecified and will be replaced by a default value of `nbd-connector`.",
                idx
            );
            provider.topic = String::from("nbd-connector");
        } else {
            match kafka_topic_check(&provider.topic, idx) {
                Ok(_) => {}
                Err(e) => {
                    warn!("{}", e);
                    provider.topic = String::from("nbd-connector");
                }
            }
        };

        if provider.port == 0 {
            let default_port: u16 = match u16::try_from(20000 + idx) {
                Ok(p) => p,
                Err(_) => {
                    error!(
                        "The `providers.{}.port` parameter is unspecified and the software failed to determine a default value.",
                        idx
                    );
                    Err(NbdError::Config(format!(
                        "The `providers.{}.port` parameter is unspecified and the software failed to determine a default value.",
                        idx
                    )))
                }?,
            };

            warn!(
                "The `providers.{}.port` parameter is unspecified and will be replaced by a default value of `{}`.",
                idx, default_port,
            );
            provider.port = default_port;
        } else if provider.port < 1024 {
            info!(
                "While the `provider.{}.port` parameter is specified, using a port lower than 1023 is not recommended (see https://en.wikipedia.org/wiki/List_of_TCP_and_UDP_port_numbers#Well-known_ports)",
                idx
            );
        } else if provider.port > 49151 {
            info!(
                "While the `provider.{}.port` parameter is specified, using a port higher than 49152 is not recommended (see https://en.wikipedia.org/wiki/List_of_TCP_and_UDP_port_numbers#Dynamic,_private_or_ephemeral_ports)",
                idx
            );
        } else {
            debug!("The `provider.{}.port` parameter is well configured.", idx);
        }

        if provider.message_size == 0 {
            let default_message_size = cmp::min(1500, config.nbd.socket_buffer_size);
            warn!(
                "The `providers.{}.message_size` parameter is unspecified and will be replaced by a default value of `{}`.",
                idx, default_message_size,
            );
            provider.message_size = default_message_size;
        } else if provider.message_size > config.nbd.socket_buffer_size {
            warn!(
                "The `providers.{}.message_size` parameter is bigger than the `nbd.socket_buffer_size` parameter ({} > {}).",
                idx, provider.message_size, config.nbd.socket_buffer_size,
            );
            warn!(
                "This is most likely a missconfiguration and will cause data loss when receiving packets longer than the `nbd.socket_buffer_size`."
            );
        } else {
            debug!(
                "The `providers.{}.message_size` parameter is configured correctly.",
                idx,
            );
        };

        if provider.group.is_ipv4() {
            match provider.interface {
                Some(Interface::V4(_)) => {
                    debug!(
                        "The `providers.{}.interface` parameter is configured correctly.",
                        idx
                    );
                }
                Some(Interface::V6(interface)) => {
                    warn!("The `providers.{}.interface` is invalid :", idx);
                    warn!(
                        "Impossible use of an Ipv6 interface ({}) for an Ipv4 group ({}). A default interface will be used (0.0.0.0).",
                        interface, provider.group
                    );
                    provider.interface = Some(Interface::V4(Ipv4Addr::UNSPECIFIED));
                }
                None => {
                    warn!(
                        "The `providers.{}.interface` is unspecified and will be replaced by the default Ipv4 interface (0.0.0.0).",
                        idx
                    );
                    provider.interface = Some(Interface::V4(Ipv4Addr::UNSPECIFIED));
                }
            }
        } else {
            match provider.interface {
                Some(Interface::V6(_)) => {
                    debug!(
                        "The `providers.{}.interface` parameter is configured correctly.",
                        idx
                    );
                }
                Some(Interface::V4(interface)) => {
                    warn!("The `providers.{}.interface` is invalid :", idx);
                    warn!(
                        "Impossible use of an Ipv4 interface ({}) for an Ipv6 group ({}). A default interface will be used (0).",
                        interface, provider.group
                    );
                    provider.interface = Some(Interface::V6(0));
                }
                None => {
                    warn!(
                        "The `providers.{}.interface` is unspecified and will be replaced by the default Ipv4 interface (0).",
                        idx
                    );
                    provider.interface = Some(Interface::V6(0));
                }
            }
        }
    }
    Ok(config)
}

pub fn config_check(path: PathBuf) -> Result<Config, NbdError> {
    if !path.exists() {
        //        Err(format!("The submited file does not exist ({}).", path))
        return Err(NbdError::Config(match path.to_str() {
            Some(p) => format!("The requested file doesn't exist ({}).", p),
            None => String::from("The requested file doesn't exist."),
        }));
    }

    let content = std::fs::read_to_string(path)?;

    let mut config: Config = toml::from_str(&content)?;

    nbd_config_check(&config)?;
    config = kafka_config_check(config)?;
    config = provider_config_check(config)?;

    Ok(config)
}
