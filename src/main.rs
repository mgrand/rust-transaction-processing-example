extern crate anyhow;
extern crate log;

use anyhow::{bail, Context, Result};
use log::{debug, error, info, warn};
use rust_decimal::prelude::Zero;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{BufReader, Read};
use std::process::exit;
use std::str::FromStr;

#[derive(Clone, Debug, Deserialize)]
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
        Customer {
            available: Decimal::zero(),
            held: Decimal::zero(),
            total: Decimal::zero(),
            locked: false,
            transactions: vec![],
        }
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

const DEPOSIT: &'static str = "deposit";
const WITHDRAWAL: &'static str = "withdrawal";
const DISPUTE: &'static str = "dispute";
const RESOLVE: &'static str = "resolve";

const CHARGEBACK: &'static str = "chargeback";

fn compute_customer_state_from_transactions(customers: &mut CustomerMap) {
    for customer in customers.values_mut() {
        let transactions = customer.transactions.clone();
        for tx in transactions {
            match tx.typ.trim() {
                DEPOSIT => do_deposit(customer, &tx),
                WITHDRAWAL => do_withdrawal(customer, &tx),
                DISPUTE => do_dispute(customer, &tx),
                RESOLVE => do_resolve(customer, &tx),
                CHARGEBACK => do_chargeback(customer, &tx),
                _ => warn!("Ignoring transaction with unknown type {:?}", tx),
            }
        }
    }
}

// Used for deposit and withdrawal
fn change_balance(
    customer: &mut Customer,
    tx: &InputTransaction,
    f: fn(Decimal, Decimal) -> Option<Decimal>,
) {
    let amount = match Decimal::from_str(tx.amount.trim()) {
        Ok(amount) => amount,
        Err(_) => {
            error!("Bad amount in transaction {:?}; Ignoring transaction", tx);
            return;
        }
    };
    customer.total = match f(customer.total, amount) {
        Some(total) => total,
        None => {
            error!("Transaction caused overflow {:?}; ignoring transaction", tx);
            return;
        }
    };
    // abs of available should be less than or equal to abs of total, so it won't overflow if total didn't.
    customer.available = f(customer.available, amount).expect("available shouldn't overflow if total didn't");
}

fn do_deposit(customer: &mut Customer, tx: &InputTransaction) {
    change_balance(customer, tx, Decimal::checked_add)
}

fn do_withdrawal(customer: &mut Customer, tx: &InputTransaction) {
    change_balance(customer, tx, Decimal::checked_sub)
}

fn do_dispute(customer: &mut Customer, tx: &InputTransaction) {
    if let Some(tx) = find_disputed_transaction(customer, tx).map(|tx| tx.clone()) {
        dispute_transaction(customer, tx)
    };
}

fn find_disputed_transaction<'a>(
    customer: &'a Customer,
    tx: &InputTransaction,
) -> Option<&'a InputTransaction> {
    match u32::from_str(tx.tx.trim()) {
        Ok(tx_id) => match find_transaction(customer, tx_id) {
            Some(disputed_tx) => Some(disputed_tx),
            None => {
                info!("Ignoring {} because referenced transaction id does not exist for the specified customer: {}", 
                    tx.typ.trim(), tx_id);
                None
            }
        },
        Err(_) => {
            invalid_transaction_id(tx);
            None
        }
    }
}

fn dispute_transaction(customer: &mut Customer, tx: InputTransaction) {
    // I am assuming that only deposits can be disputed. Otherwise, people would be able to increase their available amount by disputing a withdrawal.
    if tx.typ == DEPOSIT {
        match Decimal::from_str(tx.amount.trim()) {
            Ok(amount) => {
                customer.held = customer.held.saturating_add(amount);
                customer.available = customer.available.saturating_sub(amount);
            }
            Err(_) => error!(
                "Unable to dispute transaction because it does not contain a valid amount {:?}",
                tx
            ),
        }
    } else {
        warn!(
            "Ignoring dispute of transaction that is not a deposit {:?}",
            tx
        )
    }
}

fn find_transaction(customer: &Customer, tx_id: u32) -> Option<&InputTransaction> {
    customer
        .transactions
        .iter()
        .find(|tx| match u32::from_str(tx.tx.trim()) {
            Ok(this_id) => this_id == tx_id,
            Err(_) => {
                invalid_transaction_id(tx);
                false
            }
        })
}

fn invalid_transaction_id(tx: &InputTransaction) {
    error!("Invalid transaction id in transaction: {:?}", tx)
}

fn do_resolve(customer: &mut Customer, tx: &InputTransaction) {
    if let Some(tx) = find_disputed_transaction(customer, tx).map(|tx| tx.clone()) {
        resolve_transaction(customer, tx)
    };
}

fn resolve_transaction(customer: &mut Customer, tx: InputTransaction) {
    // I am assuming that only deposits can be resolved, since I am assuming that only deposits can be disputed.
    if tx.typ == DEPOSIT {
        match Decimal::from_str(tx.amount.trim()) {
            Ok(amount) => {
                customer.held = customer.held.saturating_sub(amount);
                customer.available = customer.available.saturating_add(amount);
            }
            Err(_) => error!(
                "Unable to resolve transaction because it does not contain a valid amount {:?}",
                tx
            ),
        }
    } else {
        warn!(
            "Ignoring resolve of transaction that is not a deposit {:?}",
            tx
        )
    }
}

fn do_chargeback(customer: &mut Customer, tx: &InputTransaction) {
    if let Some(tx) = find_disputed_transaction(customer, tx).map(|tx| tx.clone()) {
        chargeback_transaction(customer, tx)
    };
}

fn chargeback_transaction(customer: &mut Customer, tx: InputTransaction) {
    // I am assuming that only deposits can be charged back, since I am assuming that only deposits can be disputed.
    if tx.typ == DEPOSIT {
        match Decimal::from_str(tx.amount.trim()) {
            Ok(amount) => {
                customer.held = customer.held.saturating_sub(amount);
                customer.total = customer.total.saturating_sub(amount);
                customer.locked = true;
            }
            Err(_) => error!(
                "Unable to charge back transaction because it does not contain a valid amount {:?}",
                tx
            ),
        }
    } else {
        warn!(
            "Ignoring charge back of transaction that is not a deposit {:?}",
            tx
        )
    }
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
    let client_id = u32::from_str(tx.client.trim()).context("Client ID is not a valid integer")?;
    let customer = match customers.get_mut(&client_id) {
        Some(customer) => customer,
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
            amount: "1".to_string(),
        };
        let tx2 = InputTransaction {
            typ: "deposit".to_string(),
            client: "2".to_string(),
            tx: "2".to_string(),
            amount: "1".to_string(),
        };
        let tx3 = InputTransaction {
            typ: "deposit".to_string(),
            client: "1".to_string(),
            tx: "3".to_string(),
            amount: "1".to_string(),
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

    #[test]
    fn customer_state_test() -> Result<()> {
        let mut customers = CustomerMap::new();
        add_customer_transaction(
            InputTransaction {
                typ: "deposit".to_string(),
                client: "1".to_string(),
                tx: "1".to_string(),
                amount: "1".to_string(),
            },
            &mut customers,
        )?;
        add_customer_transaction(
            InputTransaction {
                typ: "deposit".to_string(),
                client: "2".to_string(),
                tx: "2".to_string(),
                amount: "1".to_string(),
            },
            &mut customers,
        )?;
        add_customer_transaction(
            InputTransaction {
                typ: "deposit".to_string(),
                client: "1".to_string(),
                tx: "3".to_string(),
                amount: "3.5".to_string(),
            },
            &mut customers,
        )?;
        add_customer_transaction(
            InputTransaction {
                typ: "withdrawal".to_string(),
                client: "1".to_string(),
                tx: "4".to_string(),
                amount: "2".to_string(),
            },
            &mut customers,
        )?;
        compute_customer_state_from_transactions(&mut customers);
        let c1 = customers
            .get(&1)
            .expect("Expect to have a record for customer 1");
        assert_eq!(Decimal::from_str("2.5").unwrap(), c1.total, "expected total to be 2.5. Record is {:?}", c1);
        assert_eq!(Decimal::from_str("2.5").unwrap(), c1.available, "expected available to be 2.5. Record is {:?}", c1);
        assert_eq!(Decimal::zero(), c1.held);
        assert!(!c1.locked);
        Ok(())
    }
}
