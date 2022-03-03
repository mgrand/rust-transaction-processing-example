extern crate anyhow;
extern crate log;

use anyhow::{bail, Context, Result};
use log::{error, info};
use std::env;
use std::fs::File;
use std::io::{BufReader, Read};
use std::process::exit;

fn main() {
    env_logger::init();
    info!("Starting");
    if let Err(error) = run() {
        eprintln!("{}", error);
        error!("Exiting due to error: {}", error);
        exit(1);
    }
    info!("normal completion");
}

fn run() -> Result<()> {
    let _reader = process_command_line(env::args().collect())?;
    Ok(())
}

// Return a reader for the input.
fn process_command_line(args: Vec<String>) -> Result<Box<dyn Read>> {
    if args.len() == 2 {
        let file_name = &args[1];
        let file = File::open(file_name).with_context(|| format!("Error opening {}", args[0]))?;
        info!("Reading from {}", file_name);
        Ok(Box::new(BufReader::new(file)))
    } else {
        bail!("Expect exactly on file name on the command line");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process_command_line;
    use std::fs::{remove_file, File};
    use std::io::Write;

    #[test]
    fn process_command_line_wrong_arg_count() {
        if let Ok(_) = process_command_line(vec!["exe".to_string()]) {
            panic!("No error for zero args")
        }
        if let Ok(_) = process_command_line(vec![
            "exe".to_string(),
            "asdf".to_string(),
            "extra".to_string(),
        ]) {
            panic!("No error for two args")
        }
    }

    #[test]
    fn process_command_line_nonexistent_file() {
        if let Ok(_) = process_command_line(vec!["exe".to_string(), "bogus".to_string()]) {
            panic!("No error for zero args")
        }
    }

    const TRANSACTION_FILE_CONTENT: &str = r##"type, client, tx, amount
deposit, 1, 1, 1.0
deposit, 2, 2, 2.0
deposit, 1, 3, 2.0
withdrawal, 1, 4, 1.5
withdrawal, 2, 5, 3.0"##;

    #[test]
    fn process_command_line_good_file() -> Result<()> {
        let file_name = "cli_test_file";
        {
            let mut file = File::create(file_name)?;
            file.write_all(TRANSACTION_FILE_CONTENT.as_bytes())?;
        }
        let _file = process_command_line(vec!["exe".to_string(), file_name.to_string()])?;
        remove_file(file_name)?;
        Ok(())
    }
}
