use nbd::{
    Config, Credentials, RawConfig, RawCredentials, SecretSource, SecurityMechanism, check_secrets,
};
use secrecy::{ExposeSecret, SecretString};
use std::io::Write;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

const CANARY_SECRET: &str = "SuperSecretCanaryValue_98765!";

/// This test checks that SecretSource::Literal never leaks by mistake in debug outputs.
#[test]
fn test_secrets_never_leaks() {
    let literal_secret = SecretSource::Literal(SecretString::from(CANARY_SECRET));

    let debug_literal = format!("{:?}", literal_secret);

    assert!(
        !debug_literal.contains(CANARY_SECRET),
        "Secret leaked from literal secret source SecretString."
    )
}

/// This test checks that credentials defined under [kafka.auth] with a user/password combination never leak by mistake in debug outputs.
#[test]
fn test_sasl_credentials_never_leaks() {
    let raw_creds = RawCredentials {
        username: SecretSource::Literal(SecretString::from(CANARY_SECRET)),
        password: SecretSource::Literal(SecretString::from(CANARY_SECRET)),
        mechanism: None,
    };
    let debug_raw_creds = format!("{:?}", raw_creds);

    let resolved_user = raw_creds.username.resolve().unwrap();
    let resolved_pass = raw_creds.password.resolve().unwrap();

    let creds = Credentials {
        username: resolved_user,
        password: resolved_pass,
        mechanism: SecurityMechanism::Plain,
    };
    let debug_creds = format!("{:?}", creds);

    assert_eq!(
        creds.username.expose_secret(),
        CANARY_SECRET,
        "Username doesn't have the intended value."
    );
    assert_eq!(
        creds.password.expose_secret(),
        CANARY_SECRET,
        "Password doesn't have the intended value."
    );

    assert!(
        !debug_raw_creds.contains(CANARY_SECRET),
        "Secrets leaked by raw credentials in debug mode."
    );

    assert!(
        !debug_creds.contains(CANARY_SECRET),
        "Secrets leaked by resolved credentials in debug mode."
    );
}

/// This test checks that credentials defined under [kafka.auth] with a user/password environment variables combination never leak by mistake in debug outputs.
#[test]
fn test_env_credentials_never_leaks() {
    let var_name = "NBD_TEST_SECRET_ENV_VAR";
    unsafe {
        std::env::set_var(var_name, CANARY_SECRET);
    }

    let raw_creds = RawCredentials {
        username: SecretSource::Env {
            env: var_name.to_string(),
        },
        password: SecretSource::Env {
            env: var_name.to_string(),
        },
        mechanism: None,
    };
    let debug_raw_creds = format!("{:?}", raw_creds);

    let resolved_user = raw_creds.username.resolve().unwrap();
    let resolved_pass = raw_creds.password.resolve().unwrap();

    let creds = Credentials {
        username: resolved_user,
        password: resolved_pass,
        mechanism: SecurityMechanism::Plain,
    };
    let debug_creds = format!("{:?}", creds);

    assert_eq!(
        creds.username.expose_secret(),
        CANARY_SECRET,
        "Username doesn't have the intended value."
    );
    assert_eq!(
        creds.password.expose_secret(),
        CANARY_SECRET,
        "Password doesn't have the intended value."
    );

    assert!(
        !debug_raw_creds.contains(CANARY_SECRET),
        "Secrets leaked by raw credentials in debug mode."
    );

    assert!(
        !debug_creds.contains(CANARY_SECRET),
        "Secrets leaked by resolved credentials in debug mode."
    );

    unsafe {
        std::env::remove_var(var_name);
    }
}

/// This test checks that credentials defined under [kafka.auth] with a user_file/password_file combination never leak by mistake in debug outputs.
#[test]
fn test_file_credentials_never_leaks() {
    let temp_dir = std::env::temp_dir();
    let secrets_file = temp_dir.as_path().join("nbd_canary_test_secrets.txt");
    let mut source_file = std::fs::File::create(&secrets_file).unwrap();
    writeln!(source_file, "{}", CANARY_SECRET).unwrap();

    let raw_creds = RawCredentials {
        username: SecretSource::File {
            file: secrets_file.clone(),
        },
        password: SecretSource::File {
            file: secrets_file.clone(),
        },
        mechanism: None,
    };
    let debug_raw_creds = format!("{:?}", raw_creds);

    let resolved_user = raw_creds.username.resolve().unwrap();
    let resolved_pass = raw_creds.password.resolve().unwrap();

    let creds = Credentials {
        username: resolved_user,
        password: resolved_pass,
        mechanism: SecurityMechanism::Plain,
    };
    let debug_creds = format!("{:?}", creds);

    assert_eq!(
        creds.username.expose_secret(),
        CANARY_SECRET,
        "Username doesn't have the intended value."
    );
    assert_eq!(
        creds.password.expose_secret(),
        CANARY_SECRET,
        "Password doesn't have the intended value."
    );

    assert!(
        !debug_raw_creds.contains(CANARY_SECRET),
        "Secrets leaked by raw credentials in debug mode."
    );

    assert!(
        !debug_creds.contains(CANARY_SECRET),
        "Secrets leaked by resolved credentials in debug mode."
    );

    std::fs::remove_file(&secrets_file).unwrap();
}

/// This test checks that a normal minimal config is correctly parsed.
#[test]
fn test_config_validity() {
    let valid_raw_toml_example = r#"
[nbd]
socket_buffer_size = 5000
verbosity = "info"

[kafka]
broker = "127.0.0.1:9092"
connection_timeout = 500
message_timeout = 100
message_retries = 2
compression = "none"
idempotence = true
linger = 1
acks = "all"
queue_size = 1000
parallel_requests = 5

[[provider]]
topic = "nbd-canary"
group = "230.0.0.1"
port = 20000
message_size = 2048
interface = "0.0.0.0"
    "#;

    let valid_raw_conf_example = toml::from_str::<RawConfig>(valid_raw_toml_example);
    assert!(valid_raw_conf_example.is_ok());

    let valid_conf_example = Config::try_from(valid_raw_conf_example.unwrap());
    assert!(valid_conf_example.is_ok());
}

/// This test tests the correct handling of tls configurations for the kafka client.
#[test]
#[cfg(feature = "kafka-tls")]
fn test_tls_config_validity() {
    let temp_dir = std::env::temp_dir();

    let secrets_file = temp_dir.as_path().join("nbd_canary_test_tls_secrets.txt");
    let mut source_file = std::fs::File::create(&secrets_file).unwrap();
    writeln!(source_file, "{}", CANARY_SECRET).unwrap();

    let valid_raw_toml_example = format!(
        r#"
[nbd]
socket_buffer_size = 5000
verbosity = "info"

[kafka]
broker = "127.0.0.1:9092"
connection_timeout = 500
message_timeout = 100
message_retries = 2
compression = "none"
idempotence = true
linger = 1
acks = "all"
queue_size = 1000
parallel_requests = 5

[kafka.tls]
ca_file = "{}"
cert_file = "{}"
key_file = "{}"
key_password = "some_fake_password"

[[provider]]
topic = "nbd-canary"
group = "230.0.0.1"
port = 20000
message_size = 2048
interface = "0.0.0.0"
    "#,
        secrets_file.display(),
        secrets_file.display(),
        secrets_file.display()
    );

    let conf_file = temp_dir.as_path().join("nbd_canary_test_tls_conf.txt");
    let mut source_file = std::fs::File::create(&conf_file).unwrap();
    writeln!(source_file, "{}", valid_raw_toml_example).unwrap();

    let valid_raw_conf_example = toml::from_str::<RawConfig>(&valid_raw_toml_example);
    assert!(valid_raw_conf_example.is_ok());

    assert!(check_secrets(&valid_raw_conf_example.as_ref().unwrap().kafka, &conf_file).is_ok());

    #[cfg(unix)]
    {
        std::fs::set_permissions(&secrets_file, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::set_permissions(&conf_file, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(check_secrets(&valid_raw_conf_example.as_ref().unwrap().kafka, &conf_file).is_ok());

        std::fs::set_permissions(&secrets_file, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(
            !check_secrets(&valid_raw_conf_example.as_ref().unwrap().kafka, &conf_file).is_ok()
        );

        std::fs::set_permissions(&conf_file, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(
            !check_secrets(&valid_raw_conf_example.as_ref().unwrap().kafka, &conf_file).is_ok()
        );

        std::fs::set_permissions(&secrets_file, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(
            !check_secrets(&valid_raw_conf_example.as_ref().unwrap().kafka, &conf_file).is_ok()
        );
    }

    let valid_conf_example = Config::try_from(valid_raw_conf_example.unwrap());
    assert!(valid_conf_example.is_ok());
    assert!(valid_conf_example.unwrap().kafka.tls.is_some());

    std::fs::remove_file(&conf_file).unwrap();
    std::fs::remove_file(&secrets_file).unwrap();
}

/// This test tests the correct handling of auth configurations for the kafka client.
#[test]
fn test_auth_config_validity() {
    let temp_dir = std::env::temp_dir();

    let secrets_file = temp_dir.as_path().join("nbd_canary_test_auth_secrets.txt");
    let mut source_file = std::fs::File::create(&secrets_file).unwrap();
    writeln!(source_file, "{}", CANARY_SECRET).unwrap();

    let valid_raw_toml_example = format!(
        r#"
[nbd]
socket_buffer_size = 5000
verbosity = "info"

[kafka]
broker = "127.0.0.1:9092"
connection_timeout = 500
message_timeout = 100
message_retries = 2
compression = "none"
idempotence = true
linger = 1
acks = "all"
queue_size = 1000
parallel_requests = 5

[kafka.auth]
username = "Dwight"
password = {{ file = '{}' }}
mechanism = "PLAIN"

[[provider]]
topic = "nbd-canary"
group = "230.0.0.1"
port = 20000
message_size = 2048
interface = "0.0.0.0"
    "#,
        secrets_file.display()
    );

    let conf_file = temp_dir.as_path().join("nbd_canary_test_auth_conf.txt");
    let mut source_file = std::fs::File::create(&conf_file).unwrap();
    writeln!(source_file, "{}", valid_raw_toml_example).unwrap();

    let valid_raw_conf_example = toml::from_str::<RawConfig>(&valid_raw_toml_example);

    assert!(valid_raw_conf_example.is_ok());

    #[cfg(unix)]
    {
        std::fs::set_permissions(&secrets_file, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::set_permissions(&conf_file, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(check_secrets(&valid_raw_conf_example.as_ref().unwrap().kafka, &conf_file).is_ok());

        std::fs::set_permissions(&secrets_file, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(
            !check_secrets(&valid_raw_conf_example.as_ref().unwrap().kafka, &conf_file).is_ok()
        );

        std::fs::set_permissions(&conf_file, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(
            !check_secrets(&valid_raw_conf_example.as_ref().unwrap().kafka, &conf_file).is_ok()
        );

        std::fs::set_permissions(&secrets_file, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(check_secrets(&valid_raw_conf_example.as_ref().unwrap().kafka, &conf_file).is_ok());
    }

    let valid_conf_example = Config::try_from(valid_raw_conf_example.unwrap());
    assert!(valid_conf_example.is_ok());
    assert!(valid_conf_example.unwrap().kafka.auth.is_some());

    std::fs::remove_file(&conf_file).unwrap();
    std::fs::remove_file(&secrets_file).unwrap();
}

/// This test tests the correct handling of auth configurations for the kafka client.
#[test]
fn test_auth_config_invalidity() {
    let temp_dir = std::env::temp_dir();

    let conf_file = temp_dir.as_path().join("nbd_canary_test_invalid_conf.txt");
    let mut source_file = std::fs::File::create(&conf_file).unwrap();

    let invalid_raw_toml_example = format!(
        r#"
[nbd]
socket_buffer_size = 5000
verbosity = "info"

[kafka]
broker = "127.0.0.1:9092"
connection_timeout = 500
message_timeout = 100
message_retries = 2
compression = "none"
idempotence = true
linger = 1
acks = "all"
queue_size = 1000
parallel_requests = 5

[kafka.auth]
username = "Dwight"
password = "Beets4ever!"
mechanism = "PLAIN"

[kafka.auth.gssapi]
principal = "office.com"
keytab = {}
service_name: "paper_sale"

[[provider]]
topic = "nbd-canary"
group = "230.0.0.1"
port = 20000
message_size = 2048
interface = "0.0.0.0"
    "#,
        conf_file.display()
    );

    writeln!(source_file, "{}", invalid_raw_toml_example).unwrap();

    let invalid_raw_conf_example = toml::from_str::<RawConfig>(&invalid_raw_toml_example);
    assert!(!invalid_raw_conf_example.is_ok());

    let invalid_raw_toml_example = format!(
        r#"
[nbd]
socket_buffer_size = 5000
verbosity = "info"

[kafka]
broker = "127.0.0.1:9092"
connection_timeout = 500
message_timeout = 100
message_retries = 2
compression = "none"
idempotence = true
linger = 1
acks = "all"
queue_size = 1000
parallel_requests = 5

[kafka.auth.oauth]
token_endpoint_url = "https://dunder-mifflin.com/auth"
client_id = "jim"
client_secret = "cece"

[kafka.auth.gssapi]
principal = "office.com"
keytab = {}
service_name: "paper_sale"

[[provider]]
topic = "nbd-canary"
group = "230.0.0.1"
port = 20000
message_size = 2048
interface = "0.0.0.0"
    "#,
        conf_file.display()
    );

    writeln!(source_file, "{}", invalid_raw_toml_example).unwrap();

    let invalid_raw_conf_example = toml::from_str::<RawConfig>(&invalid_raw_toml_example);
    assert!(!invalid_raw_conf_example.is_ok());

    let invalid_raw_toml_example = r#"
[nbd]
socket_buffer_size = 5000
verbosity = "info"

[kafka]
broker = "127.0.0.1:9092"
connection_timeout = 500
message_timeout = 100
message_retries = 2
compression = "none"
idempotence = true
linger = 1
acks = "all"
queue_size = 1000
parallel_requests = 5

[kafka.auth]
username = "Dwight"
password = "Beets4ever!"
mechanism = "PLAIN"

[kafka.auth.oauth]
token_endpoint_url = "https://dunder-mifflin.com/auth"
client_id = "jim"
client_secret = "cece"

[[provider]]
topic = "nbd-canary"
group = "230.0.0.1"
port = 20000
message_size = 2048
interface = "0.0.0.0"
    "#;

    writeln!(source_file, "{}", invalid_raw_toml_example).unwrap();

    let invalid_raw_conf_example = toml::from_str::<RawConfig>(invalid_raw_toml_example);
    assert!(!invalid_raw_conf_example.is_ok());

    std::fs::remove_file(&conf_file).unwrap();
}
