// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashSet;

use libp2p::kad::record::Key;
use libp2p::request_response::ResponseChannel;
use libp2p::swarm::{DialError, NetworkInfo};
use libp2p::{floodsub, Multiaddr, PeerId};
use tokio::sync::{mpsc, oneshot};

use super::protocols::kademlia_record::KademliaRecord;
use crate::protocols::delegated_streaming::{
    DelegatedStreamingRequest, DelegatedStreamingResponseResult,
};
use crate::protocols::galt_identify::{GaltIdentifyRequest, GaltIdentifyResponseResult};
use crate::protocols::media_streaming::{
    StreamingKey, StreamingRequest, StreamingResponseResult, WrappedStreamingResponseResult,
};
use crate::protocols::payment_info::PaymentInfoResponseResult;
use crate::protocols::simple_file_exchange::{SimpleFileRequest, SimpleFileResponse};
use crate::protocols::webrtc_signaling::{
    SignalingRequestOrResponse, WebRtcSignalingResponseResult,
};
use crate::utils;

pub enum NetworkBackendCommand {
    StartProvidingSimpleFile {
        key: Vec<u8>,
        filename: String,
        sender: oneshot::Sender<Result<(), libp2p::kad::AddProviderError>>,
    },
    StartProvidingStreaming {
        kad_key: libp2p::kad::record::Key,
        streaming_key: StreamingKey,
        sender: oneshot::Sender<Result<(), libp2p::kad::AddProviderError>>,
    },
    StopProviding {
        kad_key: libp2p::kad::record::Key,
        sender: oneshot::Sender<()>,
    },
    GetProviders {
        key: Key,
        sender: oneshot::Sender<Result<HashSet<PeerId>, libp2p::kad::GetProvidersError>>,
    },
    GetRecord {
        key: Key,
        sender: oneshot::Sender<anyhow::Result<KademliaRecord>>,
    },
    #[allow(dead_code)]
    GetPublishedFileName {
        key: Vec<u8>,
        sender: oneshot::Sender<Option<String>>,
    },
    PutRecord {
        record: KademliaRecord,
        sender: oneshot::Sender<anyhow::Result<()>>,
    },
    RemoveRecord {
        kad_key: libp2p::kad::record::Key,
        sender: oneshot::Sender<()>,
    },
    PublishGossip {
        data: Vec<u8>,
        // topic: IdentTopic,
        topic: floodsub::Topic,
        // sender: oneshot::Sender<Result<(), PublishError>>,
        sender: oneshot::Sender<()>,
    },
    RequestSimpleFile {
        file_request: SimpleFileRequest,
        peer: PeerId,
        sender: oneshot::Sender<
            Result<Result<SimpleFileResponse, String>, libp2p::request_response::OutboundFailure>,
        >,
    },
    RespondSimpleFile {
        response: Result<SimpleFileResponse, String>,
        channel: ResponseChannel<Result<SimpleFileResponse, String>>,
    },
    RequestStreamingData {
        params: RequestStreamingDataParams,
        sender: oneshot::Sender<
            Result<StreamingResponseResult, libp2p::request_response::OutboundFailure>,
        >,
    },
    RespondStreamingData {
        peer: PeerId,
        response: StreamingResponseResult,
        channel: ResponseChannel<WrappedStreamingResponseResult>,
    },
    RespondPaymentInfo {
        response: PaymentInfoResponseResult,
        channel: ResponseChannel<PaymentInfoResponseResult>,
    },
    RespondWebRtcSignaling {
        response: WebRtcSignalingResponseResult,
        channel: ResponseChannel<WebRtcSignalingResponseResult>,
    },
    RequestWebRtcSignaling {
        request: SignalingRequestOrResponse,
        peer: PeerId,
        sender: oneshot::Sender<
            Result<WebRtcSignalingResponseResult, libp2p::request_response::OutboundFailure>,
        >,
    },
    RequestDelegatedStreaming {
        request: DelegatedStreamingRequest,
        peer: PeerId,
        sender: oneshot::Sender<
            Result<DelegatedStreamingResponseResult, libp2p::request_response::OutboundFailure>,
        >,
    },
    RespondDelegatedStreaming {
        response: DelegatedStreamingResponseResult,
        channel: ResponseChannel<DelegatedStreamingResponseResult>,
    },
    RequestGaltIdentify {
        request: GaltIdentifyRequest,
        peer: PeerId,
        sender: oneshot::Sender<
            Result<GaltIdentifyResponseResult, libp2p::request_response::OutboundFailure>,
        >,
    },
    RespondGaltIdentify {
        response: GaltIdentifyResponseResult,
        channel: ResponseChannel<GaltIdentifyResponseResult>,
    },
    GetSwarmNetworkInfo {
        sender: oneshot::Sender<NetworkInfo>,
    },
    Dial {
        peer: PeerId,
        addresses: Vec<Multiaddr>,
        sender: oneshot::Sender<Result<(), DialError>>,
    },
}

#[derive(Debug)]
pub struct RequestStreamingDataParams {
    pub request: StreamingRequest,
    pub peer: PeerId,
}

#[derive(Clone)]
pub struct NetworkBackendClient {
    pub sender: mpsc::Sender<NetworkBackendCommand>,
}

impl NetworkBackendClient {
    pub fn new(sender: mpsc::Sender<NetworkBackendCommand>) -> NetworkBackendClient {
        Self { sender }
    }

    pub async fn start_providing_simple_file(
        &self,
        key: Vec<u8>,
        filename: String,
    ) -> anyhow::Result<Result<(), libp2p::kad::AddProviderError>> {
        log::debug!("start_providing_simple_file for {:?} {}", key, filename);
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::StartProvidingSimpleFile {
                key,
                filename,
                sender,
            })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        Ok(receiver.await?)
    }

    pub async fn start_providing_streaming(
        &self,
        kad_key: libp2p::kad::record::Key,
        streaming_key: StreamingKey,
    ) -> anyhow::Result<Result<(), libp2p::kad::AddProviderError>> {
        log::debug!("start_providing_streaming for {:?}", streaming_key);
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::StartProvidingStreaming {
                kad_key,
                streaming_key,
                sender,
            })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        Ok(receiver.await?)
    }

    pub async fn stop_providing(&self, kad_key: libp2p::kad::record::Key) -> anyhow::Result<()> {
        log::debug!("stop_providing {:?}", kad_key);
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::StopProviding { kad_key, sender })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        receiver.await?;
        Ok(())
    }

    pub async fn get_providers(
        &self,
        key: Key,
    ) -> anyhow::Result<Result<HashSet<PeerId>, libp2p::kad::GetProvidersError>> {
        log::debug!("get_providers {:?}", hex::encode(&key));
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::GetProviders { key, sender })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        Ok(receiver.await?)
    }

    #[allow(dead_code)]
    pub async fn get_published_file_name(&self, key: Vec<u8>) -> anyhow::Result<Option<String>> {
        log::debug!("get_published_file_name {:?}", key);
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::GetPublishedFileName { key, sender })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        Ok(receiver.await?)
    }

    pub async fn get_record(&self, key: Key) -> anyhow::Result<KademliaRecord> {
        log::debug!("get_record {:?}", key);
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::GetRecord { key, sender })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        receiver.await?
    }

    pub async fn put_record(&self, record: KademliaRecord) -> anyhow::Result<()> {
        log::debug!("put_record {:?}", record);
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::PutRecord { record, sender })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        receiver.await?
    }

    pub async fn remove_record(&self, kad_key: libp2p::kad::record::Key) -> anyhow::Result<()> {
        log::debug!("remove_record {:?}", kad_key);
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::RemoveRecord { kad_key, sender })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        receiver.await?;
        Ok(())
    }

    pub async fn publish_gossip(
        &self,
        data: Vec<u8>,
        // topic: IdentTopic,
        topic: floodsub::Topic,
    ) -> anyhow::Result<()> {
        // ) -> anyhow::Result<Result<(), PublishError>> {
        // log::debug!("publish_topic {:?} to {}", data, topic.to_string());
        log::debug!("publish_topic {:?} to {:?}", data, topic);
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::PublishGossip {
                data,
                topic,
                sender,
            })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        Ok(receiver.await?)
    }

    pub async fn request_simple_file(
        &self,
        file_request: SimpleFileRequest,
        peer: PeerId,
    ) -> anyhow::Result<
        Result<Result<SimpleFileResponse, String>, libp2p::request_response::OutboundFailure>,
    > {
        log::debug!("request_simple_file {:?}", file_request);
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::RequestSimpleFile {
                file_request,
                peer,
                sender,
            })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        Ok(receiver.await?)
    }

    pub async fn respond_simple_file(
        &self,
        response: Result<SimpleFileResponse, String>,
        channel: ResponseChannel<Result<SimpleFileResponse, String>>,
    ) -> anyhow::Result<()> {
        log::debug!("respond_simple_file");
        self.sender
            .send(NetworkBackendCommand::RespondSimpleFile { response, channel })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        Ok(())
    }

    pub async fn request_streaming_data(
        &self,
        request: StreamingRequest,
        peer: PeerId,
    ) -> anyhow::Result<Result<StreamingResponseResult, libp2p::request_response::OutboundFailure>>
    {
        log::debug!("request_streaming_data to peer {}", peer);
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::RequestStreamingData {
                params: RequestStreamingDataParams { peer, request },
                sender,
            })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        Ok(receiver.await?)
    }

    pub async fn respond_streaming_data(
        &self,
        peer: PeerId,
        response: StreamingResponseResult,
        channel: ResponseChannel<WrappedStreamingResponseResult>,
    ) -> anyhow::Result<()> {
        log::debug!("respond_streaming_data with success? {}", response.is_ok());
        self.sender
            .send(NetworkBackendCommand::RespondStreamingData {
                peer,
                response,
                channel,
            })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        Ok(())
    }

    pub async fn request_delegated_streaming(
        &self,
        request: DelegatedStreamingRequest,
        peer: PeerId,
    ) -> anyhow::Result<
        Result<DelegatedStreamingResponseResult, libp2p::request_response::OutboundFailure>,
    > {
        log::debug!("request_delegated_streaming to peer {}", peer);
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::RequestDelegatedStreaming {
                request,
                peer,
                sender,
            })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        Ok(receiver.await?)
    }

    pub async fn respond_delegated_streaming(
        &self,
        response: DelegatedStreamingResponseResult,
        channel: ResponseChannel<DelegatedStreamingResponseResult>,
    ) -> anyhow::Result<()> {
        log::debug!(
            "respond_delegated_streaming with success? {}",
            response.is_ok()
        );
        self.sender
            .send(NetworkBackendCommand::RespondDelegatedStreaming { response, channel })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        Ok(())
    }

    pub async fn request_galt_identify(
        &self,
        request: GaltIdentifyRequest,
        peer: PeerId,
    ) -> anyhow::Result<Result<GaltIdentifyResponseResult, libp2p::request_response::OutboundFailure>>
    {
        log::debug!("request_galt_identify to peer {}", peer);
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::RequestGaltIdentify {
                request,
                peer,
                sender,
            })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        Ok(receiver.await?)
    }

    pub async fn respond_galt_identify(
        &self,
        response: GaltIdentifyResponseResult,
        channel: ResponseChannel<GaltIdentifyResponseResult>,
    ) -> anyhow::Result<()> {
        log::debug!("respond_galt_identify with success? {}", response.is_ok());
        self.sender
            .send(NetworkBackendCommand::RespondGaltIdentify { response, channel })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        Ok(())
    }

    pub async fn respond_payment_info(
        &self,
        response: PaymentInfoResponseResult,
        channel: ResponseChannel<PaymentInfoResponseResult>,
    ) -> anyhow::Result<()> {
        log::debug!("respond_payment_info with success? {}", response.is_ok());
        self.sender
            .send(NetworkBackendCommand::RespondPaymentInfo { response, channel })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        Ok(())
    }

    pub async fn request_webrtc_signaling(
        &self,
        request: SignalingRequestOrResponse,
        peer: PeerId,
    ) -> anyhow::Result<
        Result<WebRtcSignalingResponseResult, libp2p::request_response::OutboundFailure>,
    > {
        log::debug!("request_webrtc_signaling to {peer}");
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::RequestWebRtcSignaling {
                request,
                peer,
                sender,
            })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        Ok(receiver.await?)
    }

    pub async fn respond_webrtc_signaling(
        &self,
        response: WebRtcSignalingResponseResult,
        channel: ResponseChannel<WebRtcSignalingResponseResult>,
    ) -> anyhow::Result<()> {
        log::debug!(
            "respond_webrtc_signaling with success? {}",
            response.is_ok()
        );
        self.sender
            .send(NetworkBackendCommand::RespondWebRtcSignaling { response, channel })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        Ok(())
    }

    pub async fn get_swarm_network_info(&self) -> anyhow::Result<NetworkInfo> {
        log::debug!("get_swarm_network_info");
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::GetSwarmNetworkInfo { sender })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        Ok(receiver.await?)
    }

    pub async fn dial(
        &self,
        peer: PeerId,
        addresses: Vec<Multiaddr>,
    ) -> anyhow::Result<Result<(), DialError>> {
        log::debug!("dial {peer}");
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::Dial {
                peer,
                addresses,
                sender,
            })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        Ok(receiver.await?)
    }
}
