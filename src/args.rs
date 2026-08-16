use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "nbd", about = "Nothing But Data — the no bullshit daemon.")]
pub struct Cli {
    /// Path to the configuration file.
    #[arg(long, conflicts_with = "check_config_file")]
    pub config_file: Option<PathBuf>,

    /// Utility to check a configuration file before use.
    #[arg(long, conflicts_with = "about")]
    pub check_config_file: Option<PathBuf>,

    /// Shows the informations about the binary.
    #[arg(long, conflicts_with = "config_file")]
    pub about: bool,
}
