use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::net::TcpListener;
use tokio::task;

use redis_starter_rust::Instance;

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = std::env::args()
        .try_into()
        .context("failed to parse program arguments")?;

    println!("using configuration to init new instance: {cfg:?}");
    let instance = Arc::new(Instance::new(cfg));

    println!("binding TCP listener to {}", instance.addr());
    let listener = TcpListener::bind(instance.addr())
        .await
        .with_context(|| format!("failed to bind to {}", instance.addr()))?;

    loop {
        tokio::select! {
            // signal handling would be here

            conn = listener.accept() => {
                match conn {
                    Ok((stream, addr)) => {
                        println!("accepted new connection at {addr}");
                        let instance = Arc::clone(&instance);
                        task::spawn(async move {
                            if let Err(e) = instance.handle_connection(stream).await {
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
