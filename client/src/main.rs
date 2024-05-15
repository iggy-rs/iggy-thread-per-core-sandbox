use bytes::BufMut;
use monoio::io::AsyncReadRent;
use std::thread::available_parallelism;

#[cfg(target_os = "linux")]
fn main() {
    println!("Running server with the io_uring driver");
    run();
}

#[cfg(target_os = "linux")]
fn run() {
    const ADDRESS: &str = "127.0.0.1:50000";
    let available_threads = available_parallelism().unwrap().get();
    println!("Available threads: {available_threads}");

    let mut threads = Vec::new();
    use monoio::{io::AsyncWriteRentExt, net::TcpStream};

    for i in 1..=available_threads {
        let thread = std::thread::spawn(move || {
            let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                .enable_timer()
                .build()
                .unwrap();
            rt.block_on(async move {
                println!("[Client #{i}] Waiting 2 seconds for server ready");
                monoio::time::sleep(monoio::time::Duration::from_secs(2)).await;

                println!("[Client #{i}] Server is ready, will connect and send data");
                let mut conn = TcpStream::connect(ADDRESS)
                    .await
                    .expect("[Client] Unable to connect to server");
                loop {
                    let random_shard = rand::random::<u32>() % available_threads as u32;
                    let mut bytes = Vec::with_capacity(4);
                    bytes.put_u32_le(0);
                    bytes.put_u32_le(random_shard);
                    let (size, buf) = conn.write_all(bytes).await;
                    if size.is_err() {
                        println!("[Client #{i}] Connection closed");
                        break;
                    }

                    println!("[Client #{i}] Written {} bytes data", size.unwrap());
                    let (size, buf) = conn.read(buf).await;
                    if size.is_err() {
                        println!("[Client #{i}] Connection closed");
                        break;
                    }
                    let size = size.unwrap();
                    println!("[Client #{i}] Read {} bytes data", size);

                    if size == 0 {
                        continue;
                    }
                    let resp_value = u32::from_le_bytes(buf.try_into().unwrap());
                    println!(
                        "[Client #{i}] Received response from shard {random_shard}: {resp_value}"
                    )
                }
            });
        });
        threads.push(thread);
    }

    for thread in threads {
        thread.join().unwrap();
    }
}
