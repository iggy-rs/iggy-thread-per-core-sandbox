use futures::StreamExt;
use monoio::{
    io::{
        AsyncReadRentExt, AsyncWriteRentExt
    },
    net::{TcpListener, TcpStream},
};
use server::{
    command::command::Command,
    shard::shard::{Receiver, Shard},
};
use std::os::fd::{FromRawFd, IntoRawFd};
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

    //TODO - Create a type for the message sent via channel, instead of using this triplet
    let mesh = Arc::new(ShardMesh::<(u32, Command, i32)>::new(available_threads));

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

async fn process_command(cpu: usize, mut receiver: Receiver<(u32, Command, i32)>) {
    loop {
        if let Some((partition_id, command, fd)) = receiver.next().await {
            // Safety: File descriptor might not always be valid.
            // Given a case where the thread handling the connection might close the descriptor during panic.
            // Requires further validation whether this approach is completly safe.
            let mut stream = TcpStream::from_std(unsafe { std::net::TcpStream::from_raw_fd(fd) })
                .map_err(|err| println!("[Server] Error creating TcpStream from fd: {err}"))
                .unwrap();
            let thread_id = std::thread::current().id();
            println!(
                "[Server] Received command {command} for partition {partition_id} on thread: {thread_id:?}, CPU: #{cpu}");
            match command {
                Command::CreatePartition() => {
                    println!("[Server] Creating partition {partition_id}");
                    let recv_buf = Box::new([0u8; 14]);
                    let(n , recv_buf) = stream.read_exact(recv_buf).await;
                    n.unwrap();

                    let recv = std::str::from_utf8(&*recv_buf).unwrap();
                    assert_eq!(recv, "Hello, heaven!");
                    let resp_buf = Box::new(69u32.to_le_bytes());
                    stream.write_all(resp_buf).await.0.unwrap();
                }
                Command::SendToPartition(data) => {
                    println!(
                        "[Server] Sending data to partition {partition_id}, data: {data:?} bytes"
                    );
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
    shard: Rc<Shard<(u32, Command, i32)>>,
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
    shard: Rc<Shard<(u32, Command, i32)>>,
    mut stream: TcpStream,
) -> Result<(), std::io::Error> {
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

    let shard_id = partition_id as usize;
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
        let fd = stream.into_raw_fd();
        shard.send_to(partition_id as usize, (partition_id, command, fd));
        return Ok(());
    }
    let command = Command::from(command_id);
    let fd = stream.into_raw_fd();
    println!(
        "[Server {:?}] Sending command {command} to shard ID: {shard_id} from CPU: #{cpu}",
        std::thread::current().id(),
    );
    shard.send_to(shard_id, (partition_id, command, fd));
    Ok(())
}
