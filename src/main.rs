extern crate anyhow;
extern crate log;

use anyhow::{bail, Context, Result};
use log::{debug, error, info};
use rust_decimal::Decimal;
use rust_decimal::prelude::Zero;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{BufReader, Read};
use std::process::exit;
use std::str::FromStr;

#[derive(Debug, Deserialize)]
struct InputTransaction {
    #[serde(alias = "type")]
    typ: String,
    client: String,
    tx: String,
    amount: String,
}

#[derive(Debug, Serialize)]
struct Customer {
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,
    #[serde(skip)]
    transactions: Vec<InputTransaction>,
}

impl Customer {
    fn new() -> Self {
        Customer { available: Decimal::zero(), held: Decimal::zero(), total: Decimal::zero(), locked: false, transactions: vec![]}
    }
}

type CustomerMap = HashMap<u32, Customer>;

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
    let reader = process_command_line(env::args().collect())?;
    let mut customers = CustomerMap::new();
    organize_transactions_by_customer(&mut customers, add_customer_transaction, reader);
    compute_customer_state_from_transactions(&mut customers);
    write_customer_output(&customers)?;
    Ok(())
}

fn compute_customer_state_from_transactions(customers: &mut CustomerMap) {
    todo!()
}

fn write_customer_output(customers: &CustomerMap) -> Result<()> {
    todo!()
}

fn organize_transactions_by_customer(
    customers: &mut CustomerMap,
    process: fn(InputTransaction, &mut CustomerMap) -> Result<()>,
    reader: Box<dyn Read>,
) -> Result<()> {
    let mut csv_reader = csv::Reader::from_reader(reader);
    let mut transaction_count = 0;
    let mut err_count = 0;
    for record_result in csv_reader.deserialize() {
        transaction_count += 1;
        match record_result {
            Ok(tx) => {
                debug!("Processing transaction {:?}", tx);
                process(tx, customers)?;
            }
            Err(error) => {
                error!("Error reading transaction: {}", error);
                err_count += 1;
            }
        }
    }
    info!(
        "Processed {} transactions; {} had errors",
        transaction_count, err_count
    );
    Ok(())
}

fn add_customer_transaction(tx: InputTransaction, customers: &mut CustomerMap) -> Result<()> {
    let client_id= u32::from_str(tx.client.trim()).context("Client ID is not a valid integer")?;
    let customer = match customers.get_mut(&client_id) {
        Some(cust) => cust,
        None => {
            customers.insert(client_id, Customer::new());
            customers.get_mut(&client_id).unwrap()
        }
    };
    customer.transactions.push(tx);
    Ok(())
}

// Return a reader for the input.
fn process_command_line(args: Vec<String>) -> Result<Box<dyn Read>> {
    if args.len() == 2 {
        let file_name = &args[1];
        open_file_buffered(file_name)
    } else {
        bail!("Expect exactly on file name on the command line")
    }
}

fn open_file_buffered(file_name: &str) -> Result<Box<dyn Read>> {
    let file = File::open(file_name).with_context(|| format!("Error opening {}", file_name))?;
    info!("Reading from {}", file_name);
    Ok(Box::new(BufReader::new(file)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process_command_line;
    use ctor::ctor;
    use std::fs::{remove_file, File};
    use std::io::Write;

    #[ctor]
    fn init() {
        env_logger::init();
    }

    #[test]
    fn process_command_line_wrong_arg_count() {
        if let Ok(_) = process_command_line(vec!["exe".to_string()]) {
            panic!("No error for zero args")
        }
        if let Ok(_) = process_command_line(vec![
            "exe".to_string(),
            "apple".to_string(),
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

    const TRANSACTION_FILE_CONTENT: &str = r##"type,client,tx,amount
deposit, 1, 1, 1.0
deposit, 2, 2, 2.0
deposit, 1, 3, 2.0
withdrawal, 1, 4, 1.5
withdrawal, 2, 5, 3.0
badrecord, "##;

    #[test]
    fn process_command_line_good_file() -> Result<()> {
        fn do_it(file_name: &str) -> Result<()> {
            let _ = process_command_line(vec!["exe".to_string(), file_name.to_string()])?;
            Ok(())
        }
        with_test_file("test_file_cli", do_it)
    }

    fn with_test_file(file_name: &str, do_it: fn(file_name: &str) -> Result<()>) -> Result<()> {
        {
            let mut file = File::create(file_name)?;
            file.write_all(TRANSACTION_FILE_CONTENT.as_bytes())?;
        }
        let result = do_it(file_name);
        let _ = remove_file(file_name);
        result
    }

    static mut TRANSACTION_COUNT: usize = 0;

    #[test]
    fn run_test() -> Result<()> {
        fn increment_transaction_count(_: InputTransaction, _: &mut CustomerMap) -> Result<()> {
            unsafe {
                TRANSACTION_COUNT += 1;
            }
            Ok(())
        }
        fn do_it(file_name: &str) -> Result<()> {
            let mut customers = CustomerMap::new();
            let reader = open_file_buffered(file_name)?;
            organize_transactions_by_customer(&mut customers, increment_transaction_count, reader)?;
            Ok(())
        }
        with_test_file("test_file_run", do_it)?;
        let expected_transaction_count = TRANSACTION_FILE_CONTENT.lines().count() - 2; // 2 = 1 header record + 1 error record
        unsafe {
            assert_eq!(expected_transaction_count, TRANSACTION_COUNT);
        }
        Ok(())
    }
    
    #[test]
    fn add_customer_transaction_test() -> Result<()> {
        let tx1 = InputTransaction {
            typ: "deposit".to_string(),
            client: "1".to_string(),
            tx: "1".to_string(),
            amount: "1".to_string()
        };
        let tx2 = InputTransaction {
            typ: "deposit".to_string(),
            client: "2".to_string(),
            tx: "2".to_string(),
            amount: "1".to_string()
        };
        let tx3 = InputTransaction {
            typ: "deposit".to_string(),
            client: "1".to_string(),
            tx: "3".to_string(),
            amount: "1".to_string()
        };
        let mut customers = CustomerMap::new();
        add_customer_transaction(tx1, &mut customers)?;
        add_customer_transaction(tx2, &mut customers)?;
        add_customer_transaction(tx3, &mut customers)?;
        assert_eq!(2, customers.len());
        assert_eq!(2, customers.get(&1).unwrap().transactions.len());
        assert_eq!(1, customers.get(&2).unwrap().transactions.len());
        Ok(())
    }
}
