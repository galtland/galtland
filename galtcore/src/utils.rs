// SPDX-License-Identifier: AGPL-3.0-only

use std::io;
use std::string::FromUtf8Error;

use bytes::BytesMut;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, BufReader};

pub type ArcMutex<T> = std::sync::Arc<tokio::sync::Mutex<T>>;

// pub fn to_simple_error<T, E: std::fmt::Debug>(e: E) -> anyhow::Result<T> {
//     Err(anyhow::anyhow!("{:?}", e))
// }

pub async fn blake3_file_hash(filename: &str) -> Result<Vec<u8>, io::Error> {
    let mut hasher = blake3::Hasher::new();
    let f = File::open(filename).await?;
    let mut f = BufReader::with_capacity(8192, f); // TODO: check if makes any difference
    let mut bytes = BytesMut::with_capacity(8192);
    while f.read_buf(&mut bytes).await? > 0 {
        hasher.update(&bytes);
        bytes.clear(); // TODO: check if this is necessary
    }
    let hash = hasher.finalize();
    Ok(hash.as_bytes().to_vec())
}

#[cfg(not(any(target_os = "emscripten", target_os = "wasi", target_os = "unknown")))]
pub fn spawn<F, O: Send + 'static>(fut: F)
where
    F: std::future::Future<Output = O> + Send + 'static,
{
    tokio::spawn(fut);
}

#[cfg(any(target_os = "emscripten", target_os = "wasi", target_os = "unknown"))]
pub fn spawn<F>(fut: F)
where
    F: std::future::Future<Output = ()> + 'static,
{
    wspawn(fut);
}

pub fn wspawn<F>(fut: F)
where
    F: std::future::Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(fut);
}

pub fn spawn_and_log_error<F>(fut: F)
where
    F: std::future::Future<Output = Result<(), anyhow::Error>> + Send + 'static,
{
    spawn(async move {
        if let Err(e) = fut.await {
            log::error!("Spawn future error: {}\n{}", e, e.backtrace())
        }
    })
}

// No 'send' requirements
pub fn wspawn_and_log_error<F>(fut: F)
where
    F: std::future::Future<Output = Result<(), anyhow::Error>> + 'static,
{
    wspawn(async move {
        if let Err(e) = fut.await {
            log::error!("Spawn future error: {}\n{}", e, e.backtrace())
        }
    })
}

fn utf8_error(e: FromUtf8Error, context: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{context}: {e}"))
}

pub async fn read_to_string<T: futures::AsyncRead + Unpin + Send>(
    io: &mut T,
    max_size: usize,
    context: &str,
) -> Result<String, std::io::Error> {
    let s = libp2p::core::upgrade::read_length_prefixed(io, max_size).await?;
    String::from_utf8(s).map_err(|e| crate::utils::utf8_error(e, context))
}

async fn write_length_prefixed(
    socket: &mut (impl libp2p::futures::AsyncWrite + Unpin),
    data: impl AsRef<[u8]>,
) -> Result<usize, io::Error> {
    use libp2p::futures::AsyncWriteExt;

    let data = data.as_ref();
    let data_len = data.len();
    let mut written_bytes = 0;

    written_bytes += write_varint(socket, data_len).await?;
    socket.write_all(data).await?;
    written_bytes += data_len;

    Ok(written_bytes)
}

pub async fn write_limited_length_prefixed(
    socket: &mut (impl libp2p::futures::AsyncWrite + Unpin),
    data: impl AsRef<[u8]>,
    max_size: usize,
) -> Result<usize, io::Error> {
    let data = data.as_ref();
    let len = data.len();
    if len >= max_size {
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Error serializing response: {len} bytes is too much"),
        ))
    } else {
        write_length_prefixed(socket, data).await
    }
}

pub async fn write_varint(
    socket: &mut (impl libp2p::futures::AsyncWrite + Unpin),
    len: usize,
) -> Result<usize, io::Error> {
    use libp2p::futures::AsyncWriteExt;

    let mut len_data = unsigned_varint::encode::usize_buffer();
    let encoded_len = unsigned_varint::encode::usize(len, &mut len_data).len();
    socket.write_all(&len_data[..encoded_len]).await?;

    Ok(encoded_len)
}

#[allow(dead_code)]
pub fn measure<F: FnOnce() -> R, R>(prefix: &str, block: F) -> R {
    let now = instant::Instant::now();
    let r = block();
    log::debug!("{} took: {:?}", prefix, now.elapsed());
    r
}

#[allow(dead_code)]
pub fn measure_noop<F: FnOnce() -> R, R>(_prefix: &str, block: F) -> R {
    block()
}

pub fn send_error<E>(_: E) -> anyhow::Error {
    anyhow::anyhow!("expected receiver to not be dropped")
}
