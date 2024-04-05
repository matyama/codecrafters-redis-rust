use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::net::{TcpListener, TcpStream};
use tokio::task;

use redis_starter_rust::{DataReader, DataWriter, Store};

const PORT: u16 = 6379;

async fn handle_connection(mut stream: TcpStream, store: Arc<Store>) -> Result<()> {
    let (reader, writer) = stream.split();

    let mut reader = DataReader::new(reader);
    let mut writer = DataWriter::new(writer);

    loop {
        let Some(cmd) = reader.read_next().await? else {
            println!("flushing and closing connection");
            break writer.flush().await;
        };

        println!("executing {cmd:?}");
        let resp = cmd.exec(Arc::clone(&store)).await;

        // NOTE: for now we just ignore the payload and hard-code the response to PING
        writer.write(resp).await?;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("binding TCP listener to port {PORT}");
    let listener = TcpListener::bind(format!("127.0.0.1:{PORT}"))
        .await
        .with_context(|| format!("failed to bind to port {PORT}"))?;

    let store = Arc::new(Store::default());

    loop {
        tokio::select! {
            // signal handling would be here

            conn = listener.accept() => {
                match conn {
                    Ok((stream, addr)) => {
                        println!("accepted new connection at {addr}");
                        let store = Arc::clone(&store);
                        task::spawn(async move {
                            if let Err(e) = handle_connection(stream, store).await {
                                eprintln!("task handling connection failed with {e:?}");
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
