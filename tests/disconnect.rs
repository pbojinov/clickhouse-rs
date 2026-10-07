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
    request_received: oneshot::Receiver<()>,
    send_response: oneshot::Sender<()>,
    response_sent: oneshot::Receiver<()>,
}

enum Operation {
    Execute,
    Query,
    Ping,
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

async fn server(response: &'static [u8], disconnect: bool, command: u64) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (request_received_tx, request_received) = oneshot::channel();
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
        assert_eq!(read_varint(&mut stream).await?, command);
        request_received_tx.send(()).unwrap();
        send_response_rx.await.unwrap();
        stream.write_all(response).await?;
        if disconnect {
            stream.shutdown().await?;
        }
        response_sent_tx.send(()).unwrap();
        // Drain the request so this models EOF, rather than a TCP reset.
        stream.read_to_end(&mut Vec::new()).await?;
        Ok(())
    });
    Server {
        address,
        task,
        request_received,
        send_response,
        response_sent,
    }
}

async fn execute(response: &'static [u8], disconnect: bool) -> Result<(), Error> {
    request(Operation::Execute, response, disconnect)
        .await
        .map(|_| ())
}

async fn request(
    operation: Operation,
    response: &'static [u8],
    disconnect: bool,
) -> Result<usize, Error> {
    let command = match operation {
        Operation::Execute | Operation::Query => 1,
        Operation::Ping => 4,
    };
    let server = server(response, disconnect, command).await;
    let options = Options::from_str(&format!(
        "tcp://{}?compression=none&execute_timeout=2s&query_timeout=2s&ping_timeout=2s",
        server.address
    ))
    .unwrap();
    let mut client = Client::connect(options).await.unwrap();
    let result = {
        let request = async {
            match operation {
                Operation::Execute => client.execute("SELECT 1").await.map(|_| 0),
                Operation::Query => client
                    .query("SELECT 1")
                    .fetch_all()
                    .await
                    .map(|block| block.row_count()),
                Operation::Ping => client.ping().await.map(|_| 0),
            }
        };
        tokio::pin!(request);
        tokio::select! {
            _ = server.request_received => {},
            result = &mut request => panic!("request completed before response: {result:?}"),
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

#[tokio::test]
async fn ping_preserves_pong_before_disconnect() {
    assert!(request(Operation::Ping, b"\x04", true).await.is_ok());
}

#[tokio::test]
async fn query_preserves_data_and_completion_packets_before_disconnect() {
    // One UInt8 column named x, containing one row, followed by EndOfStream.
    assert_eq!(
        request(
            Operation::Query,
            b"\x01\x00\x00\x01\x01\x01x\x05UInt8\x07\x05",
            true,
        )
        .await
        .unwrap(),
        1,
    );
}

#[tokio::test]
async fn query_returns_io_error_on_disconnect_without_completion() {
    // The row is complete, but EndOfStream is missing.
    assert!(matches!(
        request(
            Operation::Query,
            b"\x01\x00\x00\x01\x01\x01x\x05UInt8\x07",
            true,
        )
        .await,
        Err(Error::Io(error)) if error.kind() == io::ErrorKind::UnexpectedEof
    ));
}

#[tokio::test]
async fn query_returns_io_error_on_disconnect_with_incomplete_data() {
    // The column declares one row but the UInt8 value is missing.
    assert!(matches!(
        request(Operation::Query, b"\x01\x00\x00\x01\x01\x01x\x05UInt8", true).await,
        Err(Error::Io(error)) if error.kind() == io::ErrorKind::UnexpectedEof
    ));
}

#[tokio::test]
async fn execute_preserves_server_exception_before_disconnect() {
    // Exception code 42 with empty name, message, and stack trace; no nested exception.
    assert!(matches!(
        execute(b"\x02\x2a\x00\x00\x00\x00\x00\x00\x00", true).await,
        Err(Error::Server(error)) if error.code == 42
    ));
}
