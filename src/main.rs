use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::task;

use redis_starter_rust::Instance;

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = std::env::args()
        .try_into()
        .context("failed to parse program arguments")?;

    let (instance, replication) = Instance::new(cfg).await?;
    let instance = Arc::new(instance);

    println!("subscribing to replication via {replication}");
    let mut recv_write = replication.recv_write();

    let listener = instance.listen().await?;
    let replicator = instance.clone().spawn_replicator().await;

    loop {
        tokio::select! {
            // signal handling would be here

            result = &mut recv_write, if instance.is_replica() => match result {
                Ok((repl, cmd)) => {
                    println!("received write command {cmd:?}");
                    replicator.send(cmd).await?;
                    recv_write = repl.recv_write();
                },
                Err((repl, err)) => {
                    eprintln!("received unexpected replication message: {err:?}");
                    recv_write = repl.recv_write();
                },
            },

            conn = listener.accept() => {
                match conn {
                    Ok((stream, addr)) => {
                        println!("{instance}: accepted new connection at {addr}");
                        let instance = Arc::clone(&instance);
                        task::spawn(async move {
                            if let Err(e) = instance.handle_connection(stream).await {
                                eprintln!("task handling connection failed with {e:?}");
                            }
                        });
                    },
                    Err(e) => eprintln!("{instance} cannot get client: {e}"),
                }
            }
        }

        // cooperatively yield from the main loop
        task::yield_now().await;
    }
}
