use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream, UdpSocket},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use crate::{
    errors::ServerError,
    types::{Command, QuoteGenerator},
};

pub struct Server {
    addr: String,
    udp_addr: String,
    generator: Arc<Mutex<QuoteGenerator>>,
}

impl Server {
    pub fn new(addr: &str, udp_addr: &str, generator: Arc<Mutex<QuoteGenerator>>) -> Self {
        Self {
            addr: addr.to_string(),
            udp_addr: udp_addr.to_string(),
            generator,
        }
    }

    pub fn run(&self) -> Result<(), ServerError> {
        let listener = TcpListener::bind(self.addr.clone())?;
        let socket = Arc::new(
            UdpSocket::bind(self.udp_addr.clone()).map_err(ServerError::UDPBindingFailed)?,
        );
        let ping_handlers = Arc::new(Mutex::new(HashMap::<String, Instant>::new()));

        Self::spawn_ping_listener(Arc::clone(&socket), Arc::clone(&ping_handlers));

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let generator = Arc::clone(&self.generator);
                    let socket = Arc::clone(&socket);
                    let ping_handlers = Arc::clone(&ping_handlers);
                    thread::spawn(move || {
                        if let Err(e) =
                            Self::handle_connection(stream, socket, generator, ping_handlers)
                        {
                            eprintln!("Error handling connection: {:?}", e);
                        }
                    });
                }
                Err(e) => eprintln!("Connection failed: {:?}", e),
            }
        }
        Ok(())
    }

    fn handle_connection(
        stream: TcpStream,
        socket: Arc<UdpSocket>,
        generator: Arc<Mutex<QuoteGenerator>>,
        ping_handlers: Arc<Mutex<HashMap<String, Instant>>>,
    ) -> Result<(), ServerError> {
        let mut writer = stream
            .try_clone()
            .map_err(ServerError::CloningStreamFailed)?;
        let mut reader = BufReader::new(stream);
        let _ = writer.write_all(b"Welcome to streamer!\nWhat stock do you intrested in?");
        let _ = writer.flush();
        let mut buf = String::new();
        loop {
            buf.clear();
            let response = match reader.read_line(&mut buf) {
                Ok(0) => return Ok(()),
                Ok(_) => {
                    let command = buf.trim();
                    if command.is_empty() {
                        let _ = writer.flush();
                        continue;
                    }
                    match Command::parse(command) {
                        Ok(Command::HelpCommand) => {
                            "To start streaming write STREAM udp://{host}:{port} {comma separated tickets}\nExample: STREAM udp://127.0.0.1:34562 AAPL,MSFT\n\nSend UDP PING packets from the client to keep the stream alive. The stream stops when pings stop.".to_string()
                        }
                        Ok(Command::UdpSendCommand { tickets, stream_addr }) => {
                            Self::stream_quotes(
                                Arc::clone(&socket),
                                Arc::clone(&generator),
                                stream_addr.clone(),
                                tickets,
                                Arc::clone(&ping_handlers),
                            );
                            "Streaming started".to_string()
                        }
                        Err(err) => format!("{:?}", err),
                    }
                }
                Err(_) => return Err(ServerError::StreamReadingError),
            };
            let _ = writer.write_all(response.as_bytes());
            let _ = writer.flush();
        }
    }

    fn stream_quotes(
        socket: Arc<UdpSocket>,
        generator: Arc<Mutex<QuoteGenerator>>,
        target_addr: String,
        tickets: Vec<String>,
        ping_handlers: Arc<Mutex<HashMap<String, Instant>>>,
    ) {
        thread::spawn(move || {
            loop {
                match ping_handlers.lock().unwrap().get(&target_addr).copied() {
                    Some(_) => {}
                    None => break,
                }
                for ticket in &tickets {
                    let quote = {
                        let gener = generator.lock().unwrap();
                        gener.get_quote(ticket)
                    };
                    if let Some(quote) = quote {
                        let encoded = quote.to_bytes();
                        if let Err(e) = socket.send_to(&encoded, &target_addr) {
                            eprintln!("Failed to send quote: {:?}", e);
                        }
                    } else {
                        eprintln!("No quote found for ticket: {}", ticket);
                    }
                }
                thread::sleep(Duration::from_millis(1000));
            }
        });
    }

    fn spawn_ping_listener(
        socket: Arc<UdpSocket>,
        ping_handlers: Arc<Mutex<HashMap<String, Instant>>>,
    ) {
        thread::spawn(move || {
            let mut buf = [0u8; 1024];
            let ping_timeout = Duration::from_secs(5);
            loop {
                match socket.recv_from(&mut buf) {
                    Ok((size, src_addr)) => {
                        let payload = String::from_utf8_lossy(&buf[..size]);
                        let addr = src_addr.to_string();

                        if payload.trim().eq_ignore_ascii_case("PING") {
                            ping_handlers
                                .lock()
                                .unwrap()
                                .entry(addr.clone())
                                .and_modify(|v| *v = Instant::now())
                                .or_insert(Instant::now());
                        } else {
                            let mut handlers = ping_handlers.lock().unwrap();
                            if let Some(last_ping) = handlers.get(&addr)
                                && last_ping.elapsed() > ping_timeout
                            {
                                handlers.remove(&addr);
                            }
                        }
                    }
                    Err(e) => eprintln!("UDP ping listener error: {:?}", e),
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fs::{File, remove_file};
    use std::io::Write;
    use std::net::UdpSocket;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    fn create_ticket_file(contents: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut path = std::env::temp_dir();
        path.push(format!("generator_tickets_test_{}.txt", nanos));
        let mut file = File::create(&path).expect("failed to create test ticket file");
        file.write_all(contents.as_bytes())
            .expect("failed to write test ticket file");
        path
    }

    #[test]
    fn stream_quotes_sends_udp_packet() {
        let path = create_ticket_file("AAPL\n");
        let generator = Arc::new(Mutex::new(
            QuoteGenerator::new(path.to_str().unwrap()).unwrap(),
        ));
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").unwrap());
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let target_addr = receiver.local_addr().unwrap().to_string();

        let ping_handlers = Arc::new(Mutex::new({
            let mut map = HashMap::new();
            map.insert(target_addr.clone(), Instant::now());
            map
        }));

        Server::stream_quotes(
            Arc::clone(&socket),
            Arc::clone(&generator),
            target_addr.clone(),
            vec!["AAPL".to_string()],
            Arc::clone(&ping_handlers),
        );

        let mut buf = [0u8; 1024];
        let (size, _) = receiver
            .recv_from(&mut buf)
            .expect("expected UDP quote packet");
        let packet = String::from_utf8_lossy(&buf[..size]);
        assert!(packet.starts_with("AAPL|"));

        ping_handlers.lock().unwrap().remove(&target_addr);
        thread::sleep(Duration::from_millis(1100));
        remove_file(path).unwrap();
    }
}
