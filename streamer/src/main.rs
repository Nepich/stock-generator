use clap::Parser;
use std::io::{self, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs, UdpSocket};
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

    /// Comma-separated list of tickers, e.g. AAPL,MSFT,TSLA
    #[arg(long)]
    tickers: String,

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
    let tickers = parse_tickers(&args.tickers);
    if tickers.is_empty() {
        return Err("Tickers list is empty".into());
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

fn parse_tickers(tickers: &str) -> Vec<String> {
    tickers
        .split(',')
        .map(|ticket| ticket.trim().to_string())
        .filter(|ticket| !ticket.is_empty())
        .collect()
}

fn build_stream_command(local_udp_addr: &str, tickers: &[String]) -> String {
    let ticket_list = tickers.join(",");
    format!("STREAM {local_udp_addr} {ticket_list}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_stream_command_generates_expected_string() {
        let cmd = build_stream_command("udp://127.0.0.1:34562", &["AAPL".into(), "TSLA".into()]);
        assert_eq!(cmd, "STREAM udp://127.0.0.1:34562 AAPL,TSLA");
    }

    #[test]
    fn parse_tickers_parses_comma_separated_values() {
        let tickers = parse_tickers("AAPL, GOOG ,TSLA,, MSFT ");
        assert_eq!(tickers, vec!["AAPL", "GOOG", "TSLA", "MSFT"]);
    }
}
