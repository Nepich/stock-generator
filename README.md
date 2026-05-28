# Stock Streamer

A quote generator that reads from a file and serves quotes over a network connection.
The generator reads a list of stock tickers from a file, generates random quotes for those tickers,
and serves the quotes to clients that connect over TCP. Clients can request to stream quotes for specific tickers
over UDP. The server also expects clients to send periodic "PING" messages over UDP to keep the stream alive.
The generator updates the quotes every second, simulating real-time stock price changes.
The code includes error handling, command parsing, and unit tests to ensure the functionality of the quote generation and streaming.
The main components of the code are:
- `QuoteGenerator`: A struct that manages the stock quotes, including loading from a file
  and generating new quotes.
- `Server`: A struct that handles incoming TCP connections, processes client commands, and manages
  the streaming of quotes over UDP.
- `Command`: An enum that represents the different commands that clients can send to the server
  and includes a method for parsing command strings.
- Unit tests to verify the functionality of the quote generation, command parsing, and streaming logic
  ensuring that the quotes are generated correctly and that the commands are parsed as expected.

The code is structured to be modular and maintainable, with clear separation of concerns between the quote generation logic and the server handling logic.
The server listens for incoming TCP connections, processes client commands to start streaming quotes, and uses UDP to send the quotes to the clients while also expecting periodic "PING" messages to keep the stream alive.
## Example
```rust
// Start the server
cargo run --bin generator
```
Commands:
- To start streaming: `STREAM udp://{host}:{port} {comma separated tickers}`
- Example: `STREAM udp://127.0.0.1:34562 AAPL,GOOGL,MSFT`
- To get help: `HELP`

To start the client, you can use the `streamer` binary:
```rust
cargo run --bin streamer -- --tcp-addr --udp-port --tickers --server-udp-port
```
Example:
```rust
cargo run --bin streamer -- --tcp-addr 127.0.0.1:8081 --udp-port 34562 --tickers AAPL,GOOGL,MSFT --server-udp-port 34562
```

