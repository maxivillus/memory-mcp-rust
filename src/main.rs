use memory_mcp_rust::backend::BackendCoordinator;
use memory_mcp_rust::protocol::handle_line_with_coordinator;
use memory_mcp_rust::store::default_path;
use std::io::{self, BufRead, Write};

fn main() {
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
