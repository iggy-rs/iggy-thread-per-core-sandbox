#[cfg(target_os = "linux")]
#[monoio::main(worker_threads = 1, driver = "io_uring")]
async fn main() {
    println!("Running client with the io_uring driver");
    run().await;
}

#[cfg(target_os = "linux")]
async fn run() {
    use bytes::BufMut;
    use monoio::{
        io::{AsyncReadRentExt, AsyncWriteRentExt},
        net::TcpStream,
    };
    use rand::Rng;
    const ADDRESS: &str = "127.0.0.1:50000";

    let mut conn = TcpStream::connect(ADDRESS)
        .await
        .expect("[Client] Unable to connect to server");

    println!("[Client] Connected to server");
    // Write down the steps
    // Create partition with random id
    let mut rng = rand::thread_rng();
    let partition_id = rng.gen_range(0..32);
    let mut buf = Vec::with_capacity(8);
    buf.put_u32_le(0);
    buf.put_u32_le(partition_id);
    conn.write_all(buf).await.0.unwrap();

    /*
    // Read the response
    let recv = conn.read_u32_le().await.unwrap();
    if recv == 420 {
        panic!("Partition already exists");
    }
    println!("Received response from server, for create partition command: {recv}");
    */
    // Send data to partition in a loop
    let data = b"Hello, World";
    let mut buf = Vec::with_capacity(12 + data.len());
    buf.put_u32_le(1);
    buf.put_u32_le(partition_id);
    buf.put_u32_le(data.len() as u32);
    buf.put(data.as_slice());
    conn.write_all(buf).await.0.unwrap();
    // Read response
    /*
    let recv = conn.read_u32_le().await.unwrap();
    println!("Received response from server: {recv}");
    */
}
