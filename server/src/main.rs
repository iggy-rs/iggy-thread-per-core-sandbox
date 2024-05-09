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
        io::{AsyncReadRentExt, AsyncWriteRentExt, Splitable},
        net::TcpListener,
    };
    for _ in 0..available_threads {
        let thread = std::thread::spawn(|| {
            let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                .enable_timer()
                .build()
                .unwrap();
            rt.block_on(async move {
                let listener = TcpListener::bind(ADDRESS)
                    .unwrap_or_else(|_| panic!("[Server] Unable to bind to {ADDRESS}"));
                println!("[Server] Bind ready with address {ADDRESS}");
                loop {
                    match listener.accept().await {
                        Ok((stream, _)) => {
                            println!("[Server] Accepted connection");
                            monoio::spawn(async move {
                                let (mut reader, mut writer) = stream.into_split();
                                loop {
                                    let buf = vec![0; 10];
                                    let (n, buf) = reader.read_exact(buf).await;
                                    let n = n.unwrap();
                                    println!(
                                        "[Server {:?}] Read {} bytes data",
                                        std::thread::current().id(),
                                        n
                                    );
                                    if n == 0 {
                                        break;
                                    }
                                    let (written, _) = writer.write_all(buf).await;
                                    let written = written.unwrap();
                                    println!(
                                        "[Server {:?}] Written {} bytes data",
                                        std::thread::current().id(),
                                        written
                                    );
                                    if written != n {
                                        println!("Failed to write all data");
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            println!("[Server] Error accepting connection: {e}");
                            continue;
                        }
                    }
                }
            });
        });
        threads.push(thread);
    }

    for thread in threads {
        thread.join().unwrap();
    }
}
