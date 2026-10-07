#![cfg(feature = "tokio_io")]

use clickhouse_rs::{errors::Error, Client, ConnectionError, Options};
use std::{io, net::SocketAddr, str::FromStr};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};

struct Server {
    address: SocketAddr,
    task: JoinHandle<io::Result<()>>,
    query_received: oneshot::Receiver<()>,
    send_response: oneshot::Sender<()>,
    response_sent: oneshot::Receiver<()>,
}

async fn read_varint(stream: &mut TcpStream) -> io::Result<u64> {
    let mut value = 0;
    for shift in (0..64).step_by(7) {
        let byte = stream.read_u8().await?;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(io::Error::new(io::ErrorKind::InvalidData, "invalid varint"))
}

async fn read_string(stream: &mut TcpStream) -> io::Result<()> {
    let length = read_varint(stream).await?;
    let mut bytes = vec![0; length as usize];
    stream.read_exact(&mut bytes).await?;
    Ok(())
}

async fn server(response: &'static [u8], disconnect: bool) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (query_received_tx, query_received) = oneshot::channel();
    let (send_response, send_response_rx) = oneshot::channel();
    let (response_sent_tx, response_sent) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        assert_eq!(read_varint(&mut stream).await?, 0);
        read_string(&mut stream).await?;
        for _ in 0..3 {
            read_varint(&mut stream).await?;
        }
        for _ in 0..3 {
            read_string(&mut stream).await?;
        }
        // The native protocol's Hello packet advertises revision 0.
        stream.write_all(b"\x00\x0aClickHouse\x01\x00\x00").await?;
        assert_eq!(read_varint(&mut stream).await?, 1);
        query_received_tx.send(()).unwrap();
        send_response_rx.await.unwrap();
        stream.write_all(response).await?;
        if disconnect {
            stream.shutdown().await?;
        }
        response_sent_tx.send(()).unwrap();
        // Drain the query so this models EOF, rather than a TCP reset.
        stream.read_to_end(&mut Vec::new()).await?;
        Ok(())
    });
    Server {
        address,
        task,
        query_received,
        send_response,
        response_sent,
    }
}

async fn execute(response: &'static [u8], disconnect: bool) -> Result<(), Error> {
    let server = server(response, disconnect).await;
    let options = Options::from_str(&format!(
        "tcp://{}?compression=none&execute_timeout=2s",
        server.address
    ))
    .unwrap();
    let mut client = Client::connect(options).await.unwrap();
    let result = {
        let request = client.execute("SELECT 1");
        tokio::pin!(request);
        tokio::select! {
            _ = server.query_received => {},
            result = &mut request => panic!("query completed before response: {result:?}"),
        }
        server.send_response.send(()).unwrap();
        // Buffer the response and FIN before polling the client again.
        server.response_sent.await.unwrap();
        request.await
    };
    drop(client);
    server.task.await.unwrap().unwrap();
    result
}

#[tokio::test]
async fn execute_returns_connection_error_on_disconnect_before_completion() {
    assert!(matches!(
        execute(b"", true).await,
        Err(Error::Connection(ConnectionError::Broken))
    ));
}

#[tokio::test]
async fn execute_returns_connection_error_on_disconnect_after_progress() {
    assert!(matches!(
        execute(b"\x03\x00\x00", true).await,
        Err(Error::Connection(ConnectionError::Broken))
    ));
}

#[tokio::test]
async fn execute_preserves_completion_packet_before_disconnect() {
    assert!(execute(b"\x05", true).await.is_ok());
}

#[tokio::test]
async fn execute_preserves_progress_and_completion_packets_before_disconnect() {
    assert!(execute(b"\x03\x00\x00\x05", true).await.is_ok());
}

#[tokio::test]
async fn execute_returns_io_error_on_disconnect_with_incomplete_packet() {
    assert!(matches!(
        execute(b"\x03\x00", true).await,
        Err(Error::Io(error)) if error.kind() == io::ErrorKind::UnexpectedEof
    ));
}

#[tokio::test]
async fn execute_succeeds_after_end_of_stream_packet() {
    assert!(execute(b"\x05", false).await.is_ok());
}
