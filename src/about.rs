use std::io::{self, IsTerminal};

const RESET: &str = "\x1B[0m";
const BOLD: &str = "\x1B[1m";
const LIGHT: &str = "\x1B[2m";
const WHITE: &str = "\x1B[38;2;255;255;255m";
const ORANGE: &str = "\x1B[38;2;247;76;0m";

pub fn about() {
    let build_date = env!("BUILD_DATE");

    if io::stdout().is_terminal() {
        println!("{RESET}{ORANGE}  .@@@@@@@.    {RESET}");
        println!(
            "{RESET}{ORANGE}.@@@{RESET}{WHITE}{BOLD}o{RESET}{ORANGE}@{RESET}{WHITE}{BOLD}o{RESET}{ORANGE}@{RESET}{WHITE}{BOLD}o{RESET}{ORANGE}@@@.{RESET}{WHITE}{BOLD}  nothing but data 1.0 (x64) built on {}{RESET}",
            build_date
        );
        println!(
            "{RESET}{ORANGE}@@@@@{RESET}{WHITE}{BOLD}\\|/{RESET}{ORANGE}@@@@@{RESET}{WHITE}{LIGHT}  the no bullshit daemon.{RESET}"
        );
        println!("{RESET}{ORANGE}@@@@@@{RESET}{WHITE}{BOLD}O{RESET}{ORANGE}@@@@@@{RESET}  / * * *");
        println!(
            "{RESET}{ORANGE}@@@@@@{RESET}{WHITE}{BOLD}|{RESET}{ORANGE}@@@@@@{RESET}{WHITE}  Arthur Reppelin `PetitPrinc3` (arthur.reppelin@gmail.com)"
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
            ".## o o o ##.  nothing but data 1.0 (x64) built on {}",
            build_date
        );
        println!("##   \\|/   ##  the no bullshit daemon.");
        println!("##    O    ##  / * * *");
        println!("##    |    ##  Arthur Reppelin `PetitPrinc3` (arthur.reppelin#gmail.com)");
        println!("'##   o   ##'  https://github.com/PetitPrinc3/nbd");
        println!("  '#######'                                                      * * * /");
    }
}
