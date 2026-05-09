use std::io;

#[derive(Debug)]
pub enum StockGeneratorError {
    NoSuchFile(std::io::Error),
}

impl From<io::Error> for StockGeneratorError {
    fn from(value: io::Error) -> Self {
        Self::NoSuchFile(value)
    }
}

#[derive(Debug)]
pub enum ServerError {
    TCPBindingFailed(std::io::Error),
    UDPBindingFailed(std::io::Error),
    CloningStreamFailed(std::io::Error),
    StreamReadingError,
    UDPAddrParsingError(String),
    UDPTicketParsingError(String),
    UnknownCommandError(String),
}

impl From<io::Error> for ServerError {
    fn from(value: io::Error) -> Self {
        Self::TCPBindingFailed(value)
    }
}
