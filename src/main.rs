use std::{net::TcpListener, u16};

const PORT: u16 = 6379;

fn main() {
    println!("Binding to port {PORT}");

    let listener = TcpListener::bind(format!("127.0.0.1:{PORT}")).unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("accepted new connection");
            }
            Err(e) => {
                println!("error: {e}");
            }
        }
    }
}
