use crate::errors::{ServerError, StockGeneratorError};

use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
};

#[derive(Debug, Clone)]
pub struct StockQuote {
    pub ticker: String,
    pub price: f64,
    pub volume: u32,
    pub timestamp: u64,
}

// Методы для сериализации/десериализации
impl StockQuote {
    pub fn to_string(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            self.ticker, self.price, self.volume, self.timestamp
        )
    }

    pub fn from_string(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.split('|').collect();
        if parts.len() == 4 {
            Some(StockQuote {
                ticker: parts[0].to_string(),
                price: parts[1].parse().ok()?,
                volume: parts[2].parse().ok()?,
                timestamp: parts[3].parse().ok()?,
            })
        } else {
            None
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(self.ticker.as_bytes());
        bytes.push(b'|');
        bytes.extend_from_slice(self.price.to_string().as_bytes());
        bytes.push(b'|');
        bytes.extend_from_slice(self.volume.to_string().as_bytes());
        bytes.push(b'|');
        bytes.extend_from_slice(self.timestamp.to_string().as_bytes());
        bytes
    }
}

pub struct QuoteGenerator {
    prices: HashMap<String, StockQuote>,
}

impl QuoteGenerator {
    pub fn new(path: &str) -> Result<Self, StockGeneratorError> {
        let mut prices = HashMap::new();
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let quote = line?.trim().to_string();
            if quote.is_empty() {
                continue;
            }
            let volume = match quote.as_str() {
                "AAPL" | "MSFT" | "TSLA" => 1000 + (rand::random::<f64>() * 5000.0) as u32,
                _ => 100 + (rand::random::<f64>() * 1000.0) as u32,
            };
            prices.entry(quote.clone()).or_insert(StockQuote {
                ticker: quote,
                price: rand::random_range(1000.0..=10000.0),
                volume,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            });
        }
        Ok(Self { prices })
    }

    pub fn generate(&mut self) {
        let len = self.prices.len();

        self.prices
            .iter_mut()
            .nth(rand::random_range(0..len))
            .map(|(_, quote)| {
                let volume = match quote.ticker.as_str() {
                    "AAPL" | "MSFT" | "TSLA" => 1000 + (rand::random::<f64>() * 5000.0) as u32,
                    _ => 100 + (rand::random::<f64>() * 1000.0) as u32,
                };
                quote.price *= 1.0 + rand::random_range(-0.03..=0.03);
                quote.volume = volume;
            });
    }

    pub fn get_quote(&self, ticket: &str) -> Option<StockQuote> {
        self.prices.get(ticket).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{File, remove_file};
    use std::io::Write;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

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
    fn quote_generator_loads_tickets_from_file() {
        let path = create_ticket_file("AAPL\nMSFT\nTSLA\n");
        let generator = QuoteGenerator::new(path.to_str().unwrap()).expect("failed to load quotes");

        assert!(generator.get_quote("AAPL").is_some());
        assert!(generator.get_quote("MSFT").is_some());
        assert!(generator.get_quote("TSLA").is_some());

        remove_file(path).expect("failed to remove test ticket file");
    }

    #[test]
    fn generate_keeps_quotes_within_expected_ranges() {
        let path = create_ticket_file("AAPL\nTSLA\n");
        let mut generator =
            QuoteGenerator::new(path.to_str().unwrap()).expect("failed to load quotes");
        let original_aapl = generator.get_quote("AAPL").expect("missing AAPL quote");

        generator.generate();

        let updated_aapl = generator
            .get_quote("AAPL")
            .expect("missing AAPL quote after generate");
        let ratio = updated_aapl.price / original_aapl.price;
        assert!(
            ratio >= 0.97 && ratio <= 1.03,
            "price changed more than 3%: {}",
            ratio
        );
        assert!(updated_aapl.volume >= 1000 && updated_aapl.volume <= 6000);

        remove_file(path).expect("failed to remove test ticket file");
    }

    #[test]
    fn stock_quote_to_bytes_and_from_string_roundtrip() {
        let quote = StockQuote {
            ticker: "AAPL".to_string(),
            price: 123.45,
            volume: 1000,
            timestamp: 1,
        };

        let bytes = quote.to_bytes();
        assert_eq!(bytes, b"AAPL|123.45|1000|1".to_vec());

        let parsed = StockQuote::from_string("AAPL|123.45|1000|1").expect("failed to parse quote");
        assert_eq!(parsed.ticker, "AAPL");
        assert!((parsed.price - 123.45).abs() < f64::EPSILON);
        assert_eq!(parsed.volume, 1000);
        assert_eq!(parsed.timestamp, 1);
    }

    #[test]
    fn command_parse_stream_command() {
        let command = Command::parse("STREAM udp://127.0.0.1:34562 AAPL,MSFT")
            .expect("failed to parse stream command");

        match command {
            Command::UdpSendCommand {
                tickets,
                stream_addr,
            } => {
                assert_eq!(tickets, vec!["AAPL".to_string(), "MSFT".to_string()]);
                assert_eq!(stream_addr, "127.0.0.1:34562");
            }
            _ => panic!("expected UdpSendCommand"),
        }
    }

    #[test]
    fn command_parse_invalid_udp_address_returns_error() {
        let err =
            Command::parse("STREAM http://127.0.0.1:34562 AAPL").expect_err("expected parse error");

        match err {
            ServerError::UDPAddrParsingError(_) => {}
            _ => panic!("expected UDPAddrParsingError"),
        }
    }
}

#[derive(Debug)]
pub enum Command {
    HelpCommand,
    UdpSendCommand {
        tickets: Vec<String>,
        stream_addr: String,
    },
}

impl Command {
    pub fn parse(command: &str) -> Result<Self, ServerError> {
        let mut parts = command.split_whitespace();
        match parts.next() {
            Some(cmd) => match cmd {
                "HELP" | "-H" | "-h" | "help" | "--help" => Ok(Self::HelpCommand),
                "STREAM" => {
                    let addr = parts.next();
                    let tickets = parts.next();
                    match (addr, tickets) {
                        (Some(addr), Some(tickets)) => {
                            let tickets: Vec<String> =
                                tickets.split(',').map(|s| s.trim().into()).collect();
                            if let Some(stream_addr) = addr.strip_prefix("udp://") {
                                return Ok(Self::UdpSendCommand {
                                    tickets,
                                    stream_addr: stream_addr.to_string(),
                                });
                            }
                            return Err(ServerError::UDPAddrParsingError(
                                "Invalid UDP address format".into(),
                            ));
                        }
                        (Some(_), None) => {
                            return Err(ServerError::UDPTicketParsingError(
                                "Ticket is not provided".into(),
                            ));
                        }
                        (None, Some(_)) => {
                            return Err(ServerError::UDPAddrParsingError(
                                "Address is not provided".into(),
                            ));
                        }
                        _ => {
                            return Err(ServerError::UDPAddrParsingError(
                                "Address and ticket are not provided".into(),
                            ));
                        }
                    }
                }
                _ => {
                    return Err(ServerError::UnknownCommandError(
                        "Unknown command! Use help or -h flag for description".into(),
                    ));
                }
            },
            None => {
                return Err(ServerError::UnknownCommandError(
                    "Unknown command! Use help or -h flag for description".into(),
                ));
            }
        }
    }
}
