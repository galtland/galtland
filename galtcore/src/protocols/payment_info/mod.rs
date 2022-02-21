pub mod handlers;
pub mod models;

use async_trait::async_trait;
use futures::{io, AsyncRead, AsyncWrite};
use libp2p::core::upgrade::{read_length_prefixed, read_varint};
use libp2p::core::ProtocolName;
use libp2p::request_response::RequestResponseCodec;
use serde::{Deserialize, Serialize};

use self::models::PaymentInfo;
use crate::utils;

#[derive(Debug, Clone)]
pub struct PaymentInfoProtocol();
#[derive(Clone)]
pub struct PaymentInfoCodec();
#[derive(Debug)]
pub struct PaymentInfoRequest {}
#[derive(Serialize, Deserialize, Debug)]
pub struct PaymentInfoResponse {
    pub payment_info: PaymentInfo,
}

pub type PaymentInfoResponseResult = Result<PaymentInfoResponse, String>;

impl ProtocolName for PaymentInfoProtocol {
    fn protocol_name(&self) -> &[u8] {
        "/payment-info/1".as_bytes()
    }
}

const MAX_SERIALIZED_SIZE: usize = 1_000_000;

#[async_trait]
impl RequestResponseCodec for PaymentInfoCodec {
    type Protocol = PaymentInfoProtocol;
    type Request = PaymentInfoRequest;
    type Response = PaymentInfoResponseResult;

    async fn read_request<T>(
        &mut self,
        _: &PaymentInfoProtocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let result = read_varint(io).await?;
        if result == 1 {
            Ok(PaymentInfoRequest {})
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Received wrong code {result}"),
            ))
        }
    }

    async fn read_response<T>(
        &mut self,
        _: &PaymentInfoProtocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let data = read_length_prefixed(io, MAX_SERIALIZED_SIZE).await?;
        match bincode::deserialize(&data) {
            Ok(r) => Ok(r),
            Err(e) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Error deserializing response {e:?}"),
                ))
            }
        }
    }

    async fn write_request<T>(
        &mut self,
        _: &PaymentInfoProtocol,
        io: &mut T,
        _: PaymentInfoRequest,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        utils::write_varint(io, 1usize).await?;
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &PaymentInfoProtocol,
        io: &mut T,
        r: PaymentInfoResponseResult,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        match bincode::serialize(&r) {
            Ok(r) => {
                let len = r.len();
                if len >= MAX_SERIALIZED_SIZE {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Error serializing response: {len} bytes is too much"),
                    ))
                } else {
                    utils::write_length_prefixed(io, r).await?;
                    Ok(())
                }
            }
            Err(e) => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Error serializing response {e:?}"),
            )),
        }
    }
}
