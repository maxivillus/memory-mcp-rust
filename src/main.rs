use memory_mcp_rust::protocol::handle_line;
use memory_mcp_rust::store::{default_path, Store};
use std::io::{self, BufRead, Write};

fn main() {
    let store = match Store::open(default_path()) {
        Ok(store) => store,
        Err(error) => {
            eprintln!("failed to open memory store: {error}");
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
        if let Some(response) = handle_line(&line, &store) {
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
