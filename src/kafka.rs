use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;

use secrecy::ExposeSecret;

use crate::{AuthConfig, NbdError, ProducerConfig};

pub fn build_producer(config: &ProducerConfig) -> Result<FutureProducer, NbdError> {
    let mut kafka_config = ClientConfig::new();

    // TLS config application
    if let Some(ref tls_config) = config.tls {
        kafka_config.set("ssl.ca.location", tls_config.ca_file.display().to_string());
        if let Some(cert_file) = &tls_config.cert_file {
            kafka_config.set("ssl.certificate.location", cert_file.display().to_string());
        }
        if let Some(key_file) = &tls_config.key_file {
            kafka_config.set("ssl.key.location", key_file.display().to_string());
        }
        if let Some(key_password) = &tls_config.key_password {
            kafka_config.set("ssl.key.password", key_password.resolve()?.expose_secret());
        }
    };

    // Authentication config application
    kafka_config.set("security.protocol", config.security_protocol.to_string());

    if let Some(ref auth_config) = config.auth {
        match auth_config {
            AuthConfig::Credentials(credentials) => {
                kafka_config.set("sasl.mechanism", credentials.mechanism.as_str());
                kafka_config.set("sasl.username", credentials.username.expose_secret());
                kafka_config.set("sasl.password", credentials.password.expose_secret());
            }
            #[cfg(feature = "kafka-auth-gssapi")]
            AuthConfig::Kerberos(kerberos) => {
                kafka_config.set("sasl.mechanism", "GSSAPI");
                kafka_config.set("sasl.kerberos.principal", &kerberos.principal);
                kafka_config.set(
                    "sasl.kerberos.keytab",
                    kerberos.keytab.display().to_string(),
                );
                kafka_config.set("sasl.kerberos.service.name", &kerberos.service_name);
            }
            #[cfg(feature = "kafka-auth-oauth")]
            AuthConfig::OAuth(oauth) => {
                kafka_config.set("sasl.mechanism", "OAUTHBEARER");
                kafka_config.set("sasl.oauthbearer.method", "oidc");
                kafka_config.set("sasl.oauthbearer.client.id", &oauth.client_id);
                kafka_config.set(
                    "sasl.oauthbearer.client.secret",
                    oauth.client_secret.resolve()?.expose_secret(),
                );
                kafka_config.set(
                    "sasl.oauthbearer.token.endpoint.url",
                    &oauth.token_endpoint_url,
                );
                if let Some(scope) = &oauth.scope {
                    kafka_config.set("sasl.oauthbearer.scope", scope);
                }
                if let Some(extensions) = &oauth.extensions {
                    kafka_config.set("sasl.oauthbearer.extensions", extensions);
                }
            }
        }
    };

    Ok(kafka_config
        .set("bootstrap.servers", &config.broker)
        .set("message.timeout.ms", config.message_timeout.to_string())
        .set(
            "message.send.max.retries",
            config.message_retries.to_string(),
        )
        .set("compression.type", config.compression.to_string())
        .set(
            "max.in.flight.requests.per.connection",
            config.parallel_requests.to_string(),
        )
        .set(
            "queue.buffering.max.messages",
            config.queue_size.to_string(),
        )
        .set("enable.idempotence", config.idempotence.to_string())
        .set("linger.ms", config.linger.to_string())
        .set("acks", config.acks.to_string())
        .create::<rdkafka::producer::FutureProducer>()?)
}
