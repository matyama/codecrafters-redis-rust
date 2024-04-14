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
    let mut repl_recv = replication.recv();

    let listener = instance.listen().await?;
    let replicator = instance.clone().spawn_replicator().await;

    loop {
        tokio::select! {
            // signal handling would be here

            result = &mut repl_recv, if instance.is_replica() => match result {
                Ok((mut repl, cmd, offset)) => {
                    println!("received command {cmd:?}");
                    if let Some(data) = replicator.exec(cmd).await? {
                        println!("replication response: {data:?}");
                        repl.resp(data).await?;
                    }
                    let old_offset = instance.shift_offset(offset);
                    println!("replica offset: {old_offset:?} -> {:?}", old_offset + offset);
                    repl_recv = repl.recv();
                },
                Err((repl, err)) => {
                    eprintln!("received unexpected replication message: {err:?}");
                    repl_recv = repl.recv();
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
