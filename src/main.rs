use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::net::{TcpListener, TcpStream};
use tokio::task;

use redis_starter_rust::{Config, DataReader, DataWriter, Store};

async fn handle_connection(
    mut stream: TcpStream,
    store: Arc<Store>,
    cfg: Arc<Config>,
) -> Result<()> {
    let (reader, writer) = stream.split();

    let mut reader = DataReader::new(reader);
    let mut writer = DataWriter::new(writer);

    loop {
        let Some(cmd) = reader.read_next().await? else {
            println!("flushing and closing connection");
            break writer.flush().await;
        };

        println!("executing {cmd:?}");
        let resp = cmd.exec(Arc::clone(&store), Arc::clone(&cfg)).await;

        // NOTE: for now we just ignore the payload and hard-code the response to PING
        writer.write(resp).await?;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cfg: Config = std::env::args()
        .try_into()
        .context("failed to parse program arguments")?;

    println!("using configuration: {cfg:?}");

    println!("binding TCP listener to port {}", cfg.port);
    let listener = TcpListener::bind(format!("127.0.0.1:{}", cfg.port))
        .await
        .with_context(|| format!("failed to bind to port {}", cfg.port))?;

    let store = Arc::new(Store::default());
    let cfg = Arc::new(cfg);

    loop {
        tokio::select! {
            // signal handling would be here

            conn = listener.accept() => {
                match conn {
                    Ok((stream, addr)) => {
                        println!("accepted new connection at {addr}");
                        let store = Arc::clone(&store);
                        let cfg = Arc::clone(&cfg);
                        task::spawn(async move {
                            if let Err(e) = handle_connection(stream, store, cfg).await {
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
