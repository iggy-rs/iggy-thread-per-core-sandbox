use futures::StreamExt;
use monoio::{
    fs::OpenOptions,
    io::{AsyncReadRentExt, AsyncWriteRentExt},
    net::{TcpListener, TcpStream},
};
use server::{
    commands::command::Command,
    shards::{
        message::Message,
        shard::{Receiver, Shard},
    },
};
use std::path::Path;
use std::{rc::Rc, thread::available_parallelism};

const PARTITIONS_PATH: &str = "local_data/storage/partitions";
const READ_LENGTH_EXACT: usize = 104;

#[cfg(target_os = "linux")]
fn main() {
    println!("Running server with the io_uring driver");
    run();
}

#[cfg(target_os = "linux")]
fn run() {
    use server::shards::{message::Message, shard::ShardMesh};
    use std::{rc::Rc, sync::Arc};

    const ADDRESS: &str = "127.0.0.1:50000";
    let available_threads = available_parallelism().unwrap().get();
    println!("Available threads: {available_threads}");
    if !Path::new(PARTITIONS_PATH).exists() {
        std::fs::create_dir_all(PARTITIONS_PATH).unwrap();
    }

    let mesh = Arc::new(ShardMesh::<Message>::new(available_threads));
    let mut threads = Vec::new();
    for cpu in 0..available_threads {
        let mesh = mesh.clone();
        let thread = if cpu == 0 {
            // Create the thread responsible for tcp connections.
            std::thread::spawn(move || {
                monoio::utils::bind_to_cpu_set(Some(cpu)).unwrap();
                let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                    .enable_timer()
                    .with_blocking_strategy(monoio::blocking::BlockingStrategy::ExecuteLocal)
                    .build()
                    .unwrap();

                rt.block_on(async move {
                    let listener = TcpListener::bind(ADDRESS)
                        .unwrap_or_else(|_| panic!("[Server] Unable to bind to {ADDRESS}"));
                    println!("[Server] Bind ready with address {ADDRESS}");
                    let shard = Rc::new(mesh.shard(cpu));
                    listen(cpu, shard, listener, available_threads)
                        .await
                        .map_err(|err| println!("[Server] Error listening: {err}"))
                        .unwrap();
                });
            })
        } else {
            // Create threads responsbile for processing commands.
            std::thread::spawn(move || {
                monoio::utils::bind_to_cpu_set(Some(cpu)).unwrap();
                let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                    .enable_timer()
                    .with_blocking_strategy(monoio::blocking::BlockingStrategy::ExecuteLocal)
                    .build()
                    .unwrap();

                let shard = Rc::new(mesh.shard(cpu));
                rt.block_on(async move {
                    let receiver = shard.receiver().unwrap();
                    process_command(cpu, receiver).await;
                });
            })
        };
        threads.push(thread);
    }

    for thread in threads {
        thread.join().unwrap();
    }
}

async fn process_command(cpu: usize, mut receiver: Receiver<Message>) {
    loop {
        if let Some(message) = receiver.next().await {
            // Safety: File descriptor might not always be valid.
            // Given a case where the thread handling the connection might close the descriptor during panic.
            // Requires further validation whether this approach is completly safe.
            let sender = message.sender;
            let partition_id = message.partition_id;
            let command = message.command;
            let thread_id = std::thread::current().id();
            println!(
                "[Server {thread_id:?}] Received commands {command} for partition {partition_id}, CPU: #{cpu}");
            match command {
                Command::CreatePartition() => {
                    println!("[Server] Creating partition {partition_id}");
                    let path = format!("{PARTITIONS_PATH}/{partition_id}");
                    let file_exists = Path::new(&path).exists();
                    if file_exists {
                        sender.send(Box::new(420u32.to_le_bytes())).unwrap();
                        continue;
                    }
                    monoio::fs::File::create(&path).await.unwrap();
                    println!("[Server] Created partition {partition_id} at path: {path}");
                    // send the response
                    sender.send(Box::new(69u32.to_le_bytes())).unwrap();
                }
                Command::SendToPartition(data) => {
                    let stringify_data = std::str::from_utf8(&data).unwrap();
                    println!(
                        "[Server] Saving data to partition {partition_id}, data: {stringify_data}"
                    );
                    let path = format!("{PARTITIONS_PATH}/{partition_id}");
                    let file = OpenOptions::new()
                        .write(true)
                        .append(true)
                        .open(&path)
                        .await
                        .unwrap();
                    let stat = std::fs::metadata(&path).unwrap();
                    let len = stat.len();
                    file.write_all_at(data, len).await.0.unwrap();
                    // write the response
                    sender.send(Box::new(69u32.to_le_bytes())).unwrap();
                }
                Command::ReadFromPartition(offset) => {
                    println!("[Server] Reading from partition {partition_id}");
                    let path = format!("{PARTITIONS_PATH}/{partition_id}");
                    let file = OpenOptions::new().read(true).open(&path).await.unwrap();
                    let buf = Box::new([0u8; READ_LENGTH_EXACT]);
                    let (n, buf) = file.read_at(buf, offset).await;
                    let n = n.unwrap();
                    assert_eq!(n, READ_LENGTH_EXACT);
                    // Create a Box<[u8]> from the buffer.
                    let mut bytes = [0u8; READ_LENGTH_EXACT + 4 + 8];
                    bytes[..4].copy_from_slice(&69u32.to_le_bytes());
                    bytes[4..12].copy_from_slice(&n.to_le_bytes());
                    bytes[12..].copy_from_slice(buf.as_ref());
                    sender.send(Box::new(bytes)).unwrap();
                }
            }
        }
    }
}

async fn listen(
    cpu: usize,
    shard: Rc<Shard<Message>>,
    listener: TcpListener,
    available_threads: usize,
) -> std::io::Result<()> {
    loop {
        let shard = shard.clone();
        match listener.accept().await {
            Ok((stream, _)) => {
                println!("[Server] Accepted connection on CPU: #{cpu}");
                monoio::spawn(async move {
                    if let Err(e) = handle_connection(cpu, shard, stream, available_threads).await {
                        println!("Error handling connection on CPU: #{cpu}: {e}")
                    }
                });
            }
            Err(e) => {
                println!("[Server] Error accepting connection on CPU: #{cpu}: {e}");
                return Err(e);
            }
        }
    }
}

async fn handle_connection(
    cpu: usize,
    shard: Rc<Shard<Message>>,
    mut stream: TcpStream,
    available_threads: usize,
) -> Result<(), std::io::Error> {
    // Minus one to take into the account that one thread handling connections.
    let threads = available_threads - 1;
    println!("Available threads: {threads}");
    loop {
        let buf = Box::new([0u8; 4]);
        let (n, buf) = stream.read_exact(buf).await;
        let n = n?;
        println!(
            "[Server {:?}] Read {} bytes data on CPU: #{cpu}",
            std::thread::current().id(),
            n
        );

        let command_id = u32::from_le_bytes(*buf);
        let (n, buf) = stream.read_exact(buf).await;
        let _ = n?;
        let partition_id = u32::from_le_bytes(*buf);
        println!(
        "[Server {:?}] Received commands with ID: {command_id} for partition {partition_id} on CPU: #{cpu}",
        std::thread::current().id(),
    );
        // Offset by one to take into the account that one thread is only responsible for handling tcp connections.
        let shard_id = (partition_id as usize % threads) + 1;
        if command_id == 1 {
            let (n, buf) = stream.read_exact(buf).await;
            let _ = n?;
            let data_len = u32::from_le_bytes(*buf);

            let data = vec![0u8; data_len as usize];
            let (n, data) = stream.read_exact(data).await;
            let _ = n?;

            let stringify_data = std::str::from_utf8(&data).unwrap();
            println!(
            "[Server {:?}] Sending data to shards ID: {shard_id}, from CPU: #{cpu}, data: {stringify_data}",
            std::thread::current().id(),
        );

            let command = Command::SendToPartition(data);
            let (tx, rx) = local_sync::oneshot::channel();
            let message = Message::new(partition_id, command, tx);
            shard.send_to(shard_id, message);
            let recv = rx.await.unwrap();
            stream.write_all(recv).await.0.unwrap();
            continue;
        } else if command_id == 2 {
            let offset = stream.read_u64_le().await.unwrap();
            let command = Command::ReadFromPartition(offset);
            println!(
                "[Server {:?}] Sending commands {command} to shards ID: {shard_id} from CPU: #{cpu}",
                std::thread::current().id(),
            );
            let (tx, rx) = local_sync::oneshot::channel();
            let message = Message::new(partition_id, command, tx);
            shard.send_to(shard_id, message);
            let recv = rx.await.unwrap();
            stream.write_all(recv).await.0.unwrap();
            continue;
        }
        let command = Command::from(command_id);
        println!(
            "[Server {:?}] Sending commands {command} to shards ID: {shard_id} from CPU: #{cpu}",
            std::thread::current().id(),
        );
        let (tx, rx) = local_sync::oneshot::channel();
        let message = Message::new(partition_id, command, tx);
        shard.send_to(shard_id, message);
        let recv = rx.await.unwrap();
        stream.write_all(recv).await.0.unwrap();
    }
}
