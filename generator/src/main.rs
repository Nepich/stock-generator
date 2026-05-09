use std::{path::PathBuf, sync::{Arc, Mutex}, thread};

use crate::{server::Server, types::QuoteGenerator};

mod errors;
mod server;
mod types;

fn main() {
    let data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data/tickets.txt");
    let server_generator = Arc::new(Mutex::new(QuoteGenerator::new(data_path.to_str().unwrap()).unwrap()));
    let generator = Arc::clone(&server_generator);
    thread::spawn(move || {
        loop {
            {
                let mut gener = generator.lock().unwrap();
                gener.generate();
            }
            thread::sleep(std::time::Duration::from_secs(1));
        }
    });
    let server = Server::new("127.0.0.1:8081", "127.0.0.1:34562", server_generator);
    server.run().unwrap();
    println!("Hello, world!");
}
