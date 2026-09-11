use std::io::{self, IsTerminal};

const RESET: &str = "\x1B[0m";
const BOLD: &str = "\x1B[1m";
const LIGHT: &str = "\x1B[2m";
const WHITE: &str = "\x1B[38;2;255;255;255m";
const ORANGE: &str = "\x1B[38;2;247;76;0m";

/// Prints project metadata, version, build date, architecture, and ASCII art logo.
///
/// Detects terminal capabilities via [`IsTerminal`](std::io::IsTerminal) and respects
/// the `NO_COLOR` environment variable. Renders 24-bit TrueColor output in capable
/// terminals, falling back to monochrome ASCII art otherwise.
///
/// Build-time metadata is injected by `build.rs` and Cargo:
/// `BUILD_DATE`, `BUILD_ARCH`, `CARGO_PKG_NAME`, `CARGO_PKG_VERSION`,
/// `CARGO_PKG_AUTHORS`, `CARGO_PKG_DESCRIPTION`, `CARGO_PKG_REPOSITORY`.
/// Enabled features are indicated at build time via via cfg!().
pub fn about() {
    let build_date = env!("BUILD_DATE");
    let build_arch = env!("BUILD_ARCH");
    let build_name = env!("CARGO_PKG_NAME");
    let build_vers = env!("CARGO_PKG_VERSION");
    let build_auth = env!("CARGO_PKG_AUTHORS");
    let build_desc = env!("CARGO_PKG_DESCRIPTION");
    let build_repo = env!("CARGO_PKG_REPOSITORY");
    let env_no_color = std::env::var("NO_COLOR").unwrap_or_default();

    let features = if cfg!(feature = "metrics-exporter") {
        " [+metrics-exporter]"
    } else {
        ""
    };

    if env_no_color.is_empty() && io::stdout().is_terminal() {
        println!("{RESET}{ORANGE}  .@@@@@@@.    {RESET}");
        println!(
            "{RESET}{ORANGE}.@@@{RESET}{WHITE}{BOLD}o{RESET}{ORANGE}@{RESET}{WHITE}{BOLD}o{RESET}{ORANGE}@{RESET}{WHITE}{BOLD}o{RESET}{ORANGE}@@@.{RESET}{WHITE}{BOLD}  {} {}{} ({}) built on {}{RESET}",
            build_name, build_vers, features, build_arch, build_date,
        );
        println!(
            "{RESET}{ORANGE}@@@@@{RESET}{WHITE}{BOLD}\\|/{RESET}{ORANGE}@@@@@{RESET}{WHITE}{LIGHT}  {}{RESET}",
            build_desc,
        );
        println!("{RESET}{ORANGE}@@@@@@{RESET}{WHITE}{BOLD}O{RESET}{ORANGE}@@@@@@{RESET}  / * * *");
        println!(
            "{RESET}{ORANGE}@@@@@@{RESET}{WHITE}{BOLD}|{RESET}{ORANGE}@@@@@@{RESET}{WHITE}  {}",
            build_auth,
        );
        println!(
            "{RESET}{ORANGE}'@@@@@{RESET}{WHITE}{BOLD}o{RESET}{ORANGE}@@@@@'{RESET}{WHITE}  {}",
            build_repo,
        );
        println!(
            "{RESET}{ORANGE}  '@@@@@@@'    {RESET}                                                  * * * /"
        );
    } else {
        println!("  .#######.    ");
        println!(
            ".## o o o ##.  {} {}{} ({}) built on {}",
            build_name, build_vers, features, build_arch, build_date,
        );
        println!("##   \\|/   ##  {}", build_desc,);
        println!("##    O    ##  / * * *");
        println!("##    |    ##  {}", build_auth,);
        println!("'##   o   ##'  {}", build_repo,);
        println!("  '#######'                                                      * * * /");
    }
}
