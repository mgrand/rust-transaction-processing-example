extern crate anyhow;
extern crate log;

use anyhow::{bail, Context, Result};
use log::{error, info};
use serde::Deserialize;
use std::env;
use std::fs::File;
use std::io::{BufReader, Read};
use std::process::exit;
use rust_decimal::Decimal;

#[derive(Debug, Deserialize)]
struct InputTransaction {
    typ: String,
    client: String,
    tx: String,
    amount: Decimal
}

fn main() {
    env_logger::init();
    info!("Starting");
    if let Err(error) = run(process_input_transaction) {
        eprintln!("{}", error);
        error!("Exiting due to error: {}", error);
        exit(1);
    }
    info!("normal completion");
}

fn run(process: fn (&InputTransaction)-> ()) -> Result<()> {
    let reader = process_command_line(env::args().collect())?;
    let mut csv_reader = csv::Reader::from_reader(reader);
    let mut transaction_count = 0;
    let mut err_count = 0;
    for record_result in csv_reader.deserialize() {
        transaction_count += 1;
        match record_result {
            Ok(record) => {
                let tx: InputTransaction = record;
                process_input_transaction(&tx);
            }
            Err(error) => {
                error!("Error reading transaction: {}", error);
                err_count += 1;
            }
        }
    }
    info!("Processed {} transactions; {} had errors", transaction_count, err_count);
    Ok(())
}

fn process_input_transaction(tx: &InputTransaction) {

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
        fn do_it(file_name: &str) -> Result<()> {
            let _ = process_command_line(vec!["exe".to_string(), file_name.to_string()])?;
            Ok(())
        }
        with_test_file("test_file_cli", do_it)
    }

    fn with_test_file(file_name: &str, do_it: fn (file_name: &str)->Result<()>) -> Result<()>{
        {
            let mut file = File::create(file_name)?;
            file.write_all(TRANSACTION_FILE_CONTENT.as_bytes())?;
        }
        let result = do_it(file_name);
        let _ = remove_file(file_name);
        result
    }

    #[test]
    fn run_test() {

    }
}
