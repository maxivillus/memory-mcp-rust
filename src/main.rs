use memory_mcp_rust::backend::BackendCoordinator;
use memory_mcp_rust::migration::migrate;
use memory_mcp_rust::protocol::handle_line_with_coordinator;
use memory_mcp_rust::store::default_path;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

fn main() {
    if let Some(command) = std::env::args().nth(1) {
        if command == "migrate" {
            if let Err(error) = run_migration(std::env::args().skip(2).collect()) {
                eprintln!("memory-mcp-rust migration failed: {error}");
                std::process::exit(1);
            }
            return;
        }
        eprintln!("unknown command: {command}");
        std::process::exit(2);
    }

    let coordinator = match BackendCoordinator::open(default_path()) {
        Ok(coordinator) => coordinator,
        Err(error) => {
            eprintln!("failed to open memory backend: {error}");
            std::process::exit(1);
        }
    };
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(line) => line,
            Err(error) => {
                eprintln!("failed to read stdin: {error}");
                break;
            }
        };
        if let Some(response) = handle_line_with_coordinator(&line, &coordinator) {
            let encoded = match serde_json::to_vec(&response) {
                Ok(encoded) => encoded,
                Err(error) => {
                    eprintln!("failed to encode stdout response: {error}");
                    break;
                }
            };
            if let Err(error) = stdout
                .write_all(&encoded)
                .and_then(|_| stdout.write_all(b"\n"))
                .and_then(|_| stdout.flush())
            {
                eprintln!("failed to write stdout: {error}");
                break;
            }
        }
    }
}

fn run_migration(arguments: Vec<String>) -> Result<(), String> {
    let mut source = None;
    let mut destination = None;
    let mut index = 0;
    while index < arguments.len() {
        let value = &arguments[index];
        match value.as_str() {
            "--help" | "-h" => {
                println!("usage: memory-mcp-rust migrate --source LEGACY.db --target RUST.db");
                return Ok(());
            }
            "--source" | "--target" | "--destination" => {
                index += 1;
                let argument = arguments
                    .get(index)
                    .ok_or_else(|| format!("missing value for {value}"))?;
                let path = PathBuf::from(argument);
                if value == "--source" {
                    source = Some(path);
                } else {
                    destination = Some(path);
                }
                index += 1;
            }
            _ => return Err(format!("unknown migration argument: {value}")),
        }
    }
    let source = source.ok_or_else(|| "--source is required".to_owned())?;
    let destination = destination.ok_or_else(|| "--target is required".to_owned())?;
    let report = migrate(&source, &destination).map_err(|error| error.public_message())?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report).map_err(|error| error.to_string())?
    );
    Ok(())
}
