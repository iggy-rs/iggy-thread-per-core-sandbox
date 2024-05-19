use futures::StreamExt;
use monoio::{
    fs::OpenOptions,
    io::{
        as_fd::{AsReadFd, AsWriteFd},
        AsyncReadRentExt, AsyncWriteRent, AsyncWriteRentExt,
    },
    net::{TcpListener, TcpStream},
};
use server::{
    command::command::Command,
    shard::{
        message::Message,
        shard::{Receiver, Shard},
    },
};
use std::{mem::ManuallyDrop, os::fd::AsRawFd};
use std::{
    os::fd::{FromRawFd, IntoRawFd},
    path::{self, Path},
};
use std::{rc::Rc, thread::available_parallelism};

const PARTITIONS_PATH: &str = "storage/partitions";

#[cfg(target_os = "linux")]
fn main() {
    println!("Running server with the io_uring driver");
    run();
}

#[cfg(target_os = "linux")]
fn run() {
    use server::shard::{message::Message, shard::ShardMesh};
    use std::{rc::Rc, sync::Arc};

    //use monoio::net::TcpListener;
    const ADDRESS: &str = "127.0.0.1:50000";
    let available_threads = available_parallelism().unwrap().get();
    println!("Available threads: {available_threads}");

    //TODO - Create a type for the message sent via channel, instead of using this triplet
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
                    res = listen(cpu, shard, listener) => { res.map_err(|err| println!("[Server] Error listening: {err}")).unwrap()},
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
            let fd = message.descriptor;
            let partition_id = message.partition_id;
            let command = message.command;
            let thread_id = std::thread::current().id();
            println!(
                "[Server] Received command {command} for partition {partition_id} on thread: {thread_id:?}, CPU: #{cpu}");
            match command {
                Command::CreatePartition() => {
                    let mut stream = 
                        TcpStream::from_std(unsafe { std::net::TcpStream::from_raw_fd(fd) })
                            .map_err(|err| {
                                println!("[Server] Error creating TcpStream from fd: {err}")
                            })
                            .unwrap();
                    println!("[Server] Creating partition {partition_id}");
                    /*
                    let path = format!("{PARTITIONS_PATH}/{partition_id}");
                    let file_exists = Path::new(&path).exists();
                    if file_exists {
                        stream
                            .write(Box::new(420u32.to_le_bytes()))
                            .await
                            .0
                            .unwrap();
                    }
                    monoio::fs::File::create(&path).await.unwrap();
                    println!("[Server] Created partition {partition_id} at path: {path}");
                    */
                    // write the response
                    //stream.write(Box::new(69u32.to_le_bytes())).await.0.unwrap();
                    stream
                        .write_all(Box::new(69u32.to_le_bytes()))
                        .await
                        .0
                        .unwrap();
                }
                Command::SendToPartition(data) => {
                    println!(
                        "[Server] Sending data to partition {partition_id}, data: {data:?} bytes"
                    );
                    /*
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
                    */
                    // write the response
                    //stream.write(Box::new(69u32.to_le_bytes())).await.0.unwrap();
                }
                Command::ReadFromPartition() => {
                    println!("[Server] Reading from partition {partition_id}");
                }
            }
        }
    }
}

async fn listen(
    cpu: usize,
    shard: Rc<Shard<Message>>,
    listener: TcpListener,
) -> std::io::Result<()> {
    loop {
        let shard = shard.clone();
        match listener.accept().await {
            Ok((stream, _)) => {
                println!("[Server] Accepted connection on CPU: #{cpu}");
                monoio::spawn(async move {
                    if let Err(e) = handle_connection(cpu, shard, stream).await {
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
) -> Result<(), std::io::Error> {
    let threads = available_parallelism().unwrap().get();
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
        "[Server {:?}] Received command with ID: {command_id} for partition {partition_id} on CPU: #{cpu}",
        std::thread::current().id(),
    );
        let shard_id = partition_id as usize % threads;
        if command_id == 1 {
            let (n, buf) = stream.read_exact(buf).await;
            let _ = n?;
            let data_len = u32::from_le_bytes(*buf);

            let data = vec![0u8; data_len as usize];
            let (n, data) = stream.read_exact(data).await;
            let _ = n?;

            println!(
            "[Server {:?}] Sending data to shard ID: {shard_id}, from CPU: #{cpu}, data: {data:?} bytes",
            std::thread::current().id(),
        );

            let command = Command::SendToPartition(data);
            let fd = unsafe { libc::dup(stream.as_raw_fd()) };
            let message = Message::new(partition_id, command, fd);
            shard.send_to(shard_id, message);
            continue;
        }
        let command = Command::from(command_id);
        let fd = unsafe { libc::dup(stream.as_raw_fd()) };
        println!(
            "[Server {:?}] Sending command {command} to shard ID: {shard_id} from CPU: #{cpu}",
            std::thread::current().id(),
        );
        let message = Message::new(partition_id, command, fd);
        shard.send_to(shard_id, message);
    }
}
