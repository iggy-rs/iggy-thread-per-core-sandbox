pub mod shard;

use futures::StreamExt;
use monoio::{
    io::{AsyncReadRentExt, Splitable},
    net::{TcpListener, TcpStream},
};
use rand::{thread_rng, Rng};
use shard::{Receiver, Shard};
use std::{
    io::{stdin, BufRead},
    rc::Rc,
    thread::available_parallelism,
    time::Duration,
};

#[cfg(target_os = "linux")]
fn main() {
    println!("Running server with the io_uring driver");
    run();
}

#[cfg(target_os = "linux")]
fn run() {
    use std::sync::Arc;

    use crate::shard::ShardMesh;

    //use monoio::net::TcpListener;
    const ADDRESS: &str = "127.0.0.1:50000";
    let available_threads = available_parallelism().unwrap().get();
    println!("Available threads: {available_threads}");

    let mesh = Arc::new(ShardMesh::<u32>::new(available_threads));

    let mut threads = Vec::new();
    for cpu in 0..available_threads {
        let mesh = mesh.clone();
        let thread = std::thread::spawn(move || {
            monoio::utils::bind_to_cpu_set(Some(cpu)).unwrap();
            let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                .enable_timer()
                .with_blocking_strategy(monoio::blocking::BlockingStrategy::ExecuteLocal)
                .build()
                .unwrap();

            let shard = mesh.shard(cpu);
            rt.block_on(async move {
                let receiver = shard.receiver().unwrap();
                /*
                let listener = TcpListener::bind(ADDRESS)
                    .unwrap_or_else(|_| panic!("[Server] Unable to bind to {ADDRESS}"));
                */
                println!("[Server] Bind ready with address {ADDRESS}");
                monoio::select! {
                    res = console_input(shard, available_threads) => { res.unwrap()},
                    _ = processing(receiver) => {}
                }
            });
        });
        threads.push(thread);
    }

    for thread in threads {
        thread.join().unwrap();
    }
}

async fn processing(mut receiver: Receiver<u32>) {
    loop {
        //let mut receiver = receiver.clone();
        if let Some(val) = receiver.next().await {
            let thread_id = std::thread::current().id();
            println!("[Server] Received data: {} on thread: {:?}", val, thread_id);
        }
    }
}

async fn console_input(shard: Shard<u32>, threads_count: usize) -> std::io::Result<()> {
    loop {
        //let mut stdin = stdin().lock();
        //let mut line = String::new();
        //stdin.read_line(&mut line).unwrap();
        //let shard_id = line.trim().parse::<usize>().unwrap();
        monoio::time::sleep(Duration::from_secs(2)).await;
        let shard_id: usize = thread_rng().gen_range(0..threads_count);
        println!("Sending data to shard: {}", shard_id);
            shard.send_to(shard_id, 69);
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
