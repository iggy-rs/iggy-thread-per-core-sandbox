#[cfg(target_os = "linux")]
#[monoio::main(worker_threads = 1, driver = "io_uring", enable_timer = true)]
async fn main() {
    println!("Running client with the io_uring driver");
    run().await;
}
const DATA: &[u8; 13] = b"Hello, World!";

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
    // Create partition with random id
    let mut rng = rand::thread_rng();
    let partition_id = rng.gen_range(0..32);
    let mut buf = Vec::with_capacity(8);
    buf.put_u32_le(0);
    buf.put_u32_le(partition_id);
    conn.write_all(buf).await.0.unwrap();
    let response = conn.read_u32_le().await.unwrap();
    println!("[Client] Received response from server for create_partition commands: {response}");

    let mut offset: u64 = 0;
    loop {
        let mut buf = Vec::with_capacity(12 + DATA.len());
        // Here we will continuously send data to the server
        buf.put_u32_le(1);
        buf.put_u32_le(partition_id);
        buf.put_u32_le(DATA.len() as u32);
        buf.put(DATA.as_slice());
        conn.write_all(buf).await.0.unwrap();
        // receive response from server
        let response = conn.read_u32_le().await.unwrap();
        println!("[Client] Received response from server for send_data commands: {response}");

        let mut buf = Vec::with_capacity(16);
        // Here we will continuously fetch data from the server.
        buf.put_u32_le(2);
        buf.put_u32_le(partition_id);
        buf.put_u64_le(offset);
        conn.write_all(buf).await.0.unwrap();
        // receive response from server
        let response = conn.read_u32_le().await.unwrap();
        let data_len = conn.read_u64_le().await.unwrap();
        let data = vec![0; data_len as usize];
        let (n, data) = conn.read_exact(data).await;
        let n = n.unwrap();
        assert_eq!(n, data_len as usize);
        assert_eq!(data, DATA);
        let data = std::str::from_utf8(&data).unwrap();
        println!(
            "[Client] Received response from server for read_data commands: {response}, data: {data}");
        offset += 13;

        monoio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}
