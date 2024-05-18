#[cfg(target_os = "linux")]
#[monoio::main(worker_threads = 1, driver = "io_uring")]
async fn main() {
    println!("Running server with the io_uring driver");
    run().await;
}

#[cfg(target_os = "linux")]
async fn run() {
    use bytes::BufMut;
    use monoio::{io::{AsyncReadRentExt, AsyncWriteRentExt}, net::TcpStream};
    const ADDRESS: &str = "127.0.0.1:50000";

    let mut conn = TcpStream::connect(ADDRESS)
        .await
        .expect("[Client] Unable to connect to server");
    println!("Connected to server");
    // Write down the steps
    // Create partition with random id
    let partition_id = rand::random::<u32>();
    let mut buf = Vec::with_capacity(8);
    buf.put_u32_le(0);
    buf.put_u32_le(partition_id);
    conn.write_all(buf).await.0.unwrap();
    // Send data to partition in a loop'
    loop {
        let data = b"Hello, World";
        let mut buf = Vec::with_capacity(8 + data.len());
        buf.put_u32_le(1);
        buf.put_u32_le(partition_id);
        buf.put(data.as_slice());
        conn.write_all(buf).await.0.unwrap();
        // Read response
        let recv = conn.read_u32_le().await.unwrap();
        println!("Received response from server: {recv}");
    }
}
