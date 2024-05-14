use futures::StreamExt;
use monoio::{
    io::{AsyncReadRentExt, Splitable},
    net::{TcpListener, TcpStream},
};
use server::{
    command::command::Command,
    shard::shard::{Receiver, Shard},
};
use std::{rc::Rc, thread::available_parallelism};

#[cfg(target_os = "linux")]
fn main() {
    println!("Running server with the io_uring driver");
    run();
}

#[cfg(target_os = "linux")]
fn run() {
    use server::shard::shard::ShardMesh;
    use std::{rc::Rc, sync::Arc};

    //use monoio::net::TcpListener;
    const ADDRESS: &str = "127.0.0.1:50000";
    let available_threads = available_parallelism().unwrap().get();
    println!("Available threads: {available_threads}");

    let mesh = Arc::new(ShardMesh::<(u32, Command)>::new(available_threads));

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

            let shard = Rc::new(mesh.shard(cpu));
            rt.block_on(async move {
                let receiver = shard.receiver().unwrap();
                let listener = TcpListener::bind(ADDRESS)
                    .unwrap_or_else(|_| panic!("[Server] Unable to bind to {ADDRESS}"));
                println!("[Server] Bind ready with address {ADDRESS}");
                monoio::select! {
                    res = listen(shard, listener) => { res.map_err(|err| println!("[Server] Error listening: {err}")).unwrap()},
                    _ = process_command(receiver) => {}
                }
            });
        });
        threads.push(thread);
    }

    for thread in threads {
        thread.join().unwrap();
    }
}

async fn process_command(mut receiver: Receiver<(u32, Command)>) {
    loop {
        if let Some((partition_id, command)) = receiver.next().await {
            let thread_id = std::thread::current().id();
            println!(
                "[Server] Received command {command} for partition {partition_id} on thread: {thread_id:?}");
            match command {
                Command::CreatePartition() => {
                    println!("[Server] Creating partition {partition_id}");
                }
                Command::SendToPartition(data) => {
                    println!(
                        "[Server] Sending data to partition {partition_id}, data: {data:?} bytes");
                }
                Command::ReadFromPartition() => {
                    println!("[Server] Reading from partition {partition_id}");
                }

            }
        }
    }
}

async fn listen(shard: Rc<Shard<(u32, Command)>>, listener: TcpListener) -> std::io::Result<()> {
    loop {
        let shard = shard.clone();
        match listener.accept().await {
            Ok((stream, _)) => {
                println!("[Server] Accepted connection");
                monoio::spawn(async move {
                    if let Err(e) = handle_connection(shard, stream).await {
                        println!("Error handling connection: {e}")
                    }
                });
            }
            Err(e) => {
                println!("[Server] Error accepting connection: {e}");
                return Err(e);
            }
        }
    }
}

async fn handle_connection(
    shard: Rc<Shard<(u32, Command)>>,
    stream: TcpStream,
) -> Result<(), std::io::Error> {
    let (mut reader, _) = stream.into_split();
    let buf = Box::new([0u8; 4]);
    let (n, buf) = reader.read_exact(buf).await;
    let n = n?;
    println!(
        "[Server {:?}] Read {} bytes data",
        std::thread::current().id(),
        n
    );

    let command_id = u32::from_le_bytes(*buf);
    let (n, buf) = reader.read_exact(buf).await;
    let _ = n?;
    let partition_id = u32::from_le_bytes(*buf);
    if command_id == 1 {
        let (n, buf) = reader.read_exact(buf).await;
        let _ = n?;
        let data_len = u32::from_le_bytes(*buf);

        let data = vec![0u8; data_len as usize];
        let (n, data) = reader.read_exact(data).await;
        let _ = n?;

        let command = Command::SendToPartition(data);
        shard.send_to(partition_id as usize, (partition_id, command));
        return Ok(());
    }
    let command = Command::from(command_id);
    shard.send_to(partition_id as usize, (partition_id, command));

    Ok(())
}
