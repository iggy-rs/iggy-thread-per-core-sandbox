use std::{io::stdin, thread::available_parallelism};

use monoio::{
    io::{AsyncReadRentExt, Splitable},
    net::{TcpListener, TcpStream},
};

#[cfg(target_os = "linux")]
fn main() {
    println!("Running server with the io_uring driver");
    run();
}

#[cfg(target_os = "linux")]
fn run() {
    //use monoio::net::TcpListener;
    const ADDRESS: &str = "127.0.0.1:50000";
    let available_threads = available_parallelism().unwrap().get();
    println!("Available threads: {available_threads}");

    let mut threads = Vec::new();
    for cpu in 0..available_threads {
        let thread = std::thread::spawn(move || {
            monoio::utils::bind_to_cpu_set(Some(cpu)).unwrap();
            let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                .enable_timer()
                .build()
                .unwrap();
            rt.block_on(async move {
                /*
                let listener = TcpListener::bind(ADDRESS)
                    .unwrap_or_else(|_| panic!("[Server] Unable to bind to {ADDRESS}"));
                */
                println!("[Server] Bind ready with address {ADDRESS}");
                monoio::select! {
                    //res = listen(listener) => { res.unwrap()},
                    res = console_input() => { res.unwrap()}
                }
                loop {}
            });
        });
        threads.push(thread);
    }

    for thread in threads {
        thread.join().unwrap();
    }
}

async fn console_input() -> std::io::Result<()> {
    let stdin = stdin();
    let mut user_input = String::new();
    loop {
        stdin.read_line(&mut user_input)?;
        print!("User input: {user_input}");
        user_input.clear();
    }
}

async fn listen(listener: TcpListener) -> std::io::Result<()> {
    match listener.accept().await {
        Ok((stream, _)) => {
            println!("[Server] Accepted connection");
            monoio::spawn(async move {
                if let Err(e) = handle_connection(stream).await {
                    println!("Error handling connection: {e}")
                }
            });
        }
        Err(e) => {
            println!("[Server] Error accepting connection: {e}");
            return Err(e);
        }
    }
    Ok(())
}

async fn handle_connection(stream: TcpStream) -> Result<(), std::io::Error> {
    let (mut reader, _) = stream.into_split();
    let buf = Box::new([0u8; 4]);
    let (n, buf) = reader.read_exact(buf).await;
    let n = n?;
    println!(
        "[Server {:?}] Read {} bytes data",
        std::thread::current().id(),
        n
    );

    let shard_id = u32::from_le_bytes(*buf);
    println!(
        "[Server {:?}] Received shard id: {}",
        std::thread::current().id(),
        shard_id
    );
    Ok(())
}
