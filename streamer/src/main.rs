use clap::Parser;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs, UdpSocket};
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(name = "streamer", about = "Quote Client")]
struct Args {
    /// TCP server address and port, for example 127.0.0.1:8081
    #[arg(long)]
    tcp_addr: String,

    /// Local UDP port to receive quote data
    #[arg(long)]
    udp_port: u16,

    /// Path to file with tickers, one ticker per line
    #[arg(long)]
    tickers: PathBuf,

    /// Server UDP port used for ping keepalive
    #[arg(long, default_value_t = 34562)]
    server_udp_port: u16,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let tickers = read_tickers(&args.tickers)?;
    if tickers.is_empty() {
        return Err("Tickers file is empty".into());
    }

    let tcp_addr = resolve_address(&args.tcp_addr)?;
    let mut tcp_stream = TcpStream::connect(tcp_addr)?;
    let local_udp_host = tcp_stream.local_addr()?.ip();
    let local_udp_addr = format!("udp://{}:{}", local_udp_host, args.udp_port);
    let command = build_stream_command(&local_udp_addr, &tickers);

    tcp_stream.write_all(command.as_bytes())?;
    tcp_stream.write_all(b"\n")?;
    tcp_stream.flush()?;

    println!("Sent to TCP server: {command}");

    let local_udp_socket = UdpSocket::bind(("0.0.0.0", args.udp_port))?;
    local_udp_socket.set_read_timeout(Some(Duration::from_secs(1)))?;

    let server_udp_addr = SocketAddr::new(tcp_addr.ip(), args.server_udp_port);
    let stop_flag = Arc::new(AtomicBool::new(false));
    let stop_ctrlc = Arc::clone(&stop_flag);
    let stop_ping = Arc::clone(&stop_flag);
    let ping_socket = local_udp_socket.try_clone()?;

    ctrlc::set_handler(move || {
        stop_ctrlc.store(true, Ordering::SeqCst);
    })?;

    let ping_handle = thread::spawn(move || {
        while !stop_ping.load(Ordering::SeqCst) {
            if let Err(err) = ping_socket.send_to(b"PING", server_udp_addr) {
                eprintln!("Failed to send ping: {err}");
            }
            thread::sleep(Duration::from_secs(2));
        }
    });

    println!("Listening for quotes on UDP port {}...", args.udp_port);
    let mut buffer = [0u8; 2048];

    while !stop_flag.load(Ordering::SeqCst) {
        match local_udp_socket.recv_from(&mut buffer) {
            Ok((size, src)) => {
                let message = String::from_utf8_lossy(&buffer[..size]);
                println!("[{}] {}", src, message.trim_end());
            }
            Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => continue,
            Err(ref err) if err.kind() == io::ErrorKind::TimedOut => continue,
            Err(err) => return Err(err.into()),
        }
    }

    println!("Shutting down...");
    ping_handle.join().expect("Ping thread join failed");
    Ok(())
}

fn resolve_address(addr: &str) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let mut addrs = addr.to_socket_addrs()?;
    addrs
        .next()
        .ok_or_else(|| format!("cannot resolve TCP address: {addr}").into())
}

fn read_tickers(path: &PathBuf) -> io::Result<Vec<String>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut tickers = Vec::new();
    for line_result in reader.lines() {
        let line = line_result?;
        let ticker = line.trim();
        if !ticker.is_empty() {
            tickers.push(ticker.to_string());
        }
    }
    Ok(tickers)
}

fn build_stream_command(local_udp_addr: &str, tickers: &[String]) -> String {
    let ticket_list = tickers.join(",");
    format!("STREAM {local_udp_addr} {ticket_list}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;

    #[test]
    fn build_stream_command_generates_expected_string() {
        let cmd = build_stream_command("udp://127.0.0.1:34562", &["AAPL".into(), "TSLA".into()]);
        assert_eq!(cmd, "STREAM udp://127.0.0.1:34562 AAPL,TSLA");
    }

    #[test]
    fn read_tickers_reads_non_empty_lines() {
        let mut path = std::env::temp_dir();
        path.push("streamer_tickers_test.txt");
        let mut file = File::create(&path).unwrap();
        writeln!(file, "AAPL\nGOOGL\nTSLA\n").unwrap();

        let tickers = read_tickers(&path).unwrap();
        assert_eq!(tickers, vec!["AAPL", "GOOGL", "TSLA"]);

        std::fs::remove_file(path).unwrap();
    }
}
