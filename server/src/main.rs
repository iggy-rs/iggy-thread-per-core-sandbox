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
use std::{io::Cursor, path::Path};
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

    //use monoio::net::TcpListener;
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
                    res = listen(cpu, available_threads, shard, listener) => { res.map_err(|err| println!("[Server] Error listening: {err}")).unwrap()},
                    _ = process_command(cpu, receiver) => {}
                }
            });
        });
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
            let partition_id = message.partition_id;
            let command = message.command;
            let thread_id = std::thread::current().id();
            println!(
                "[Server] Received commands {command} for partition {partition_id} on thread: {thread_id:?}, CPU: #{cpu}");
            match command {
                Command::CreatePartition() => {
                    println!("[Server] Creating partition {partition_id}");
                    let path = format!("{PARTITIONS_PATH}/{partition_id}");
                    let file_exists = Path::new(&path).exists();
                    if file_exists {
                        let sender = message.sender;
                        sender.send(Box::new(420u32.to_le_bytes())).unwrap();
                        continue;
                    }
                    monoio::fs::File::create(&path).await.unwrap();
                    println!("[Server] Created partition {partition_id} at path: {path}");
                    // write the response
                    let sender = message.sender;
                    sender.send(Box::new(69u32.to_le_bytes())).unwrap();
                    continue;
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
                    let sender = message.sender;
                    sender.send(Box::new(69u32.to_le_bytes())).unwrap();
                    continue;
                }
                Command::ReadFromPartition(offset) => {
                    println!("[Server] Reading from partition {partition_id}");
                    let path = format!("{PARTITIONS_PATH}/{partition_id}");
                    let file = OpenOptions::new().read(true).open(&path).await.unwrap();
                    let buf = Box::new([0u8; READ_LENGTH_EXACT]);
                    let (n, buf) = file.read_at(buf, offset).await;
                    let n = n.unwrap();
                    assert_eq!(n, READ_LENGTH_EXACT);
                    let mut response_buf = Box::new([0u8; READ_LENGTH_EXACT + 4 + 8]);
                    response_buf[..4].copy_from_slice(&69u32.to_le_bytes());
                    response_buf[4..12].copy_from_slice(&n.to_le_bytes());
                    response_buf[12..].copy_from_slice(buf.as_ref());
                    // write the response
                    let sender = message.sender;
                    sender.send(response_buf).unwrap();
                }
            }
        }
    }
}

async fn listen(
    cpu: usize,
    available_threads: usize,
    shard: Rc<Shard<Message>>,
    listener: TcpListener,
) -> std::io::Result<()> {
    loop {
        let shard = shard.clone();
        match listener.accept().await {
            Ok((stream, _)) => {
                println!("[Server] Accepted connection on CPU: #{cpu}");
                monoio::spawn(async move {
                    if let Err(e) = handle_connection(cpu, available_threads, shard, stream).await {
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

fn create_hash(payload: &[u8]) -> usize {
    let clamp = 2usize.pow(32) as u128;
    (fastmurmur3::hash(payload) % clamp) as usize
}

async fn handle_connection(
    cpu: usize,
    threads: usize,
    shard: Rc<Shard<Message>>,
    mut stream: TcpStream,
) -> Result<(), std::io::Error> {
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
        let partition_name = format!("partition-{}", partition_id);
        let hash = create_hash(&partition_name.as_bytes());
        println!("partition_id: {}, hash: {}", partition_id, hash);
        let shard_id = hash % threads;
        println!(
        "[Server {:?}] Received commands with ID: {command_id} for partition {partition_id} on CPU: #{cpu}",
        std::thread::current().id(),
    );
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
            let (tx, rx) = futures::channel::oneshot::channel();
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
            let (tx, rx) = futures::channel::oneshot::channel();
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
        let (tx, rx) = futures::channel::oneshot::channel();
        let message = Message::new(partition_id, command, tx);
        shard.send_to(shard_id, message);
        let recv = rx.await.unwrap();
        stream.write_all(recv).await.0.unwrap();
    }
}
