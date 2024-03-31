use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::WriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::task;

const PORT: u16 = 6379;

enum Resp {
    Ping,
}

async fn write_resp(writer: &mut WriteHalf<'_>, resp: Resp) -> Result<()> {
    match resp {
        Resp::Ping => writer
            .write_all(b"+PONG\r\n")
            .await
            .context("failed to write response to PING")?,
    }

    writer.flush().await.context("failed to flush a response")
}

async fn handle_connection(mut stream: TcpStream) -> Result<()> {
    let (mut req, mut resp) = stream.split();

    let mut buf = [0; 1024];
    loop {
        let n = req.read(&mut buf).await.context("failed to read payload")?;

        if n == 0 {
            println!("no more bytes left to read, flushing and closing connection");
            break resp.flush().await.context("failed to flush a response");
        }

        println!("read {n} bytes from the connection: {:?}", &buf[..n]);

        // NOTE: for now we just ignore the payload and hard-code the response to PING
        write_resp(&mut resp, Resp::Ping).await?;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Binding TCP listener to port {PORT}");
    let listener = TcpListener::bind(format!("127.0.0.1:{PORT}"))
        .await
        .with_context(|| format!("failed to bind to port {PORT}"))?;

    loop {
        tokio::select! {
            // signal handling would be here

            conn = listener.accept() => {
                match conn {
                    Ok((stream, addr)) => {
                        println!("accepted new connection at {addr}");
                        task::spawn(async move {
                            if let Err(e) = handle_connection(stream).await {
                                eprintln!("task handling connection failed with {e}");
                            }
                        });
                    },
                    Err(e) => eprintln!("cannot get client: {e}"),
                }
            }
        }

        // cooperatively yield from the main loop
        task::yield_now().await;
    }
}
