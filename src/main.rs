extern crate anyhow;
extern crate log;

use anyhow::{bail, Context, Result};
use log::{error,info};
use std::env;
use std::fs::File;
use std::io::{BufReader, Read};
use std::process::exit;

fn main() {
    env_logger::init();
    info!("Starting");
    if let Err(error) = do_it() {
        eprintln!("{}", error);
        error!("Exiting due to error: {}", error);
        exit(1);
    }
    info!("normal completion");
}

fn do_it() -> Result<()> {
    let reader = process_command_line()?;
    Ok(())
}

// Return a reader for the input.
fn process_command_line() -> Result<Box<dyn Read>> {
    let args: Vec<String> = env::args().collect();
    if args.len() == 1 {
        let file = File::open(&args[0]).with_context(|| format!("Error opening {}", args[0]))?;
        Ok(Box::new(BufReader::new(file)))
    } else {
        bail!("Expect exactly on file name on the command line");
    }
}
