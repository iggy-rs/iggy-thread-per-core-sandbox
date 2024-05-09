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
    use monoio::{
        io::{AsyncReadRentExt, AsyncWriteRentExt},
        net::TcpStream,
    };

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
                    let buf: Vec<u8> = vec![97; 10];
                    let (r, buf) = conn.write_all(buf).await;
                    println!("[Client #{i}] Written {} bytes data", r.unwrap());
                    let (r, _) = conn.read_exact(buf).await;
                    println!("[Client #{i}] Read {} bytes data", r.unwrap());
                }
            });
        });
        threads.push(thread);
    }

    for thread in threads {
        thread.join().unwrap();
    }
}
