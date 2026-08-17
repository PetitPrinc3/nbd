use std::io::{self, IsTerminal};

const RESET: &str = "\x1B[0m";
const BOLD: &str = "\x1B[1m";
const LIGHT: &str = "\x1B[2m";
const WHITE: &str = "\x1B[38;2;255;255;255m";
const ORANGE: &str = "\x1B[38;2;247;76;0m";

pub fn about() {
    let build_date = env!("BUILD_DATE");
    let build_arch = env!("BUILD_ARCH");
    let build_name = env!("CARGO_PKG_NAME");
    let build_vers = env!("CARGO_PKG_VERSION");
    let build_auth = env!("CARGO_PKG_AUTHORS");
    let build_desc = env!("CARGO_PKG_DESCRIPTION");

    if io::stdout().is_terminal() {
        println!("{RESET}{ORANGE}  .@@@@@@@.    {RESET}");
        println!(
            "{RESET}{ORANGE}.@@@{RESET}{WHITE}{BOLD}o{RESET}{ORANGE}@{RESET}{WHITE}{BOLD}o{RESET}{ORANGE}@{RESET}{WHITE}{BOLD}o{RESET}{ORANGE}@@@.{RESET}{WHITE}{BOLD}  {} {} ({}) built on {}{RESET}",
            build_name, build_vers, build_arch, build_date,
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
            "{RESET}{ORANGE}'@@@@@{RESET}{WHITE}{BOLD}o{RESET}{ORANGE}@@@@@'{RESET}{WHITE}  https://github.com/PetitPrinc3/nbd"
        );
        println!(
            "{RESET}{ORANGE}  '@@@@@@@'    {RESET}                                                  * * * /"
        );
    } else {
        println!("  .#######.    ");
        println!(
            ".## o o o ##.  {} {} ({}) built on {}",
            build_name, build_vers, build_arch, build_date,
        );
        println!("##   \\|/   ##  {}", build_desc,);
        println!("##    O    ##  / * * *");
        println!("##    |    ##  {}", build_auth,);
        println!("'##   o   ##'  https://github.com/PetitPrinc3/nbd");
        println!("  '#######'                                                      * * * /");
    }
}
