use anyhow::{bail, Context, Result};

use std::io::Write;
use std::net::{TcpListener, TcpStream};

const PORT: u16 = 6379;

fn handle_connection(mut stream: TcpStream) -> Result<()> {
    // NOTE: for now we just ignore the payload and hard-code the response to PING
    let response = b"+PONG\r\n";
    stream
        .write_all(response)
        .context("failed to write response to PING")?;

    stream.flush().context("failed to flush a response")
}

fn main() -> Result<()> {
    println!("Binding to port {PORT}");

    let listener = TcpListener::bind(format!("127.0.0.1:{PORT}")).unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                handle_connection(stream)?;
            }
            Err(e) => bail!("error: {e}"),
        }
    }

    Ok(())
}
