// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::{HashMap, HashSet};

use libp2p::gossipsub::IdentTopic;
use libp2p::kad::record::Key;
use libp2p::request_response::ResponseChannel;
use libp2p::PeerId;
use tokio::sync::{mpsc, oneshot};

use super::protocols::kademlia_record::KademliaRecord;
use crate::protocols::payment_info::PaymentInfoResponseResult;
use crate::protocols::rtmp_streaming::{
    RTMPStreamingRequest, RTMPStreamingResponseResult, RtmpStreamingKey,
    WrappedRTMPStreamingResponseResult,
};
use crate::protocols::simple_file_exchange::{SimpleFileRequest, SimpleFileResponse};
use crate::protocols::PeerStatistics;

#[derive(Debug)]
pub enum NetworkBackendCommand {
    StartProvidingSimpleFile {
        key: Vec<u8>,
        filename: String,
        sender: oneshot::Sender<anyhow::Result<()>>,
    },
    StartProvidingRTMPStreaming {
        kad_key: libp2p::kad::record::Key,
        streaming_key: RtmpStreamingKey,
        sender: oneshot::Sender<anyhow::Result<()>>,
    },
    StopProviding {
        kad_key: libp2p::kad::record::Key,
        sender: oneshot::Sender<()>,
    },
    GetProviders {
        key: Vec<u8>,
        sender: oneshot::Sender<anyhow::Result<HashSet<PeerId>>>,
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
        topic: IdentTopic,
        sender: oneshot::Sender<anyhow::Result<()>>,
    },
    RequestSimpleFile {
        file_request: SimpleFileRequest,
        peer: PeerId,
        sender: oneshot::Sender<anyhow::Result<Result<SimpleFileResponse, String>>>,
    },
    RespondSimpleFile {
        response: Result<SimpleFileResponse, String>,
        channel: ResponseChannel<Result<SimpleFileResponse, String>>,
    },
    RequestRTMPData {
        params: RequestRTMPDataParams,
        sender: oneshot::Sender<
            Result<RTMPStreamingResponseResult, libp2p::request_response::OutboundFailure>,
        >,
    },
    RespondRTMPData {
        peer: PeerId,
        response: RTMPStreamingResponseResult,
        channel: ResponseChannel<WrappedRTMPStreamingResponseResult>,
    },
    RespondPaymentInfo {
        response: PaymentInfoResponseResult,
        channel: ResponseChannel<PaymentInfoResponseResult>,
    },
    GetPeerStatistics {
        sender: oneshot::Sender<HashMap<PeerId, PeerStatistics>>,
    },
    GetSwarmNetworkInfo {
        sender: oneshot::Sender<libp2p::core::network::NetworkInfo>,
    },
}

#[derive(Debug)]
pub struct RequestRTMPDataParams {
    pub request: RTMPStreamingRequest,
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
        &mut self,
        key: Vec<u8>,
        filename: String,
    ) -> anyhow::Result<()> {
        log::debug!("start_providing_simple_file for {:?} {}", key, filename);
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::StartProvidingSimpleFile {
                key,
                filename,
                sender,
            })
            .await?;
        tokio::task::yield_now().await;
        receiver.await?
    }

    pub async fn start_providing_rtmp_streaming(
        &mut self,
        kad_key: libp2p::kad::record::Key,
        streaming_key: RtmpStreamingKey,
    ) -> anyhow::Result<()> {
        log::debug!("start_providing_rtmp_streaming for {:?}", streaming_key);
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::StartProvidingRTMPStreaming {
                kad_key,
                streaming_key,
                sender,
            })
            .await?;
        tokio::task::yield_now().await;
        receiver.await?
    }

    pub async fn stop_providing(
        &mut self,
        kad_key: libp2p::kad::record::Key,
    ) -> anyhow::Result<()> {
        log::debug!("stop_providing {:?}", kad_key);
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::StopProviding { kad_key, sender })
            .await?;
        tokio::task::yield_now().await;
        receiver.await?;
        Ok(())
    }

    pub async fn get_providers(&self, key: Vec<u8>) -> anyhow::Result<HashSet<PeerId>> {
        log::debug!("get_providers {:?}", key);
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::GetProviders { key, sender })
            .await?;
        tokio::task::yield_now().await;
        receiver.await?
    }

    #[allow(dead_code)]
    pub async fn get_published_file_name(
        &mut self,
        key: Vec<u8>,
    ) -> anyhow::Result<Option<String>> {
        log::debug!("get_published_file_name {:?}", key);
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::GetPublishedFileName { key, sender })
            .await?;
        tokio::task::yield_now().await;
        Ok(receiver.await?)
    }

    pub async fn get_record(&mut self, key: Key) -> anyhow::Result<KademliaRecord> {
        log::debug!("get_record {:?}", key);
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::GetRecord { key, sender })
            .await?;
        tokio::task::yield_now().await;
        receiver.await?
    }

    pub async fn put_record(&mut self, record: KademliaRecord) -> anyhow::Result<()> {
        log::debug!("put_record {:?}", record);
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::PutRecord { record, sender })
            .await
            .expect("Command receiver not to be dropped.");
        tokio::task::yield_now().await;
        receiver.await?
    }

    pub async fn remove_record(&mut self, kad_key: libp2p::kad::record::Key) -> anyhow::Result<()> {
        log::debug!("remove_record {:?}", kad_key);
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::RemoveRecord { kad_key, sender })
            .await?;
        tokio::task::yield_now().await;
        receiver.await?;
        Ok(())
    }

    pub async fn publish_gossip(&mut self, data: Vec<u8>, topic: IdentTopic) -> anyhow::Result<()> {
        log::debug!("publish_topic {:?} to {}", data, topic.to_string());
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::PublishGossip {
                data,
                topic,
                sender,
            })
            .await?;
        tokio::task::yield_now().await;
        receiver.await?
    }

    pub async fn request_simple_file(
        &mut self,
        file_request: SimpleFileRequest,
        peer: PeerId,
    ) -> anyhow::Result<Result<SimpleFileResponse, String>> {
        log::debug!("request_simple_file {:?}", file_request);
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::RequestSimpleFile {
                file_request,
                peer,
                sender,
            })
            .await?;
        tokio::task::yield_now().await;
        receiver.await?
    }

    pub async fn respond_simple_file(
        &mut self,
        response: Result<SimpleFileResponse, String>,
        channel: ResponseChannel<Result<SimpleFileResponse, String>>,
    ) -> anyhow::Result<()> {
        log::debug!("respond_simple_file");
        self.sender
            .send(NetworkBackendCommand::RespondSimpleFile { response, channel })
            .await?;
        tokio::task::yield_now().await;
        Ok(())
    }

    pub async fn request_rtmp_streaming_data(
        &mut self,
        request: RTMPStreamingRequest,
        peer: PeerId,
    ) -> anyhow::Result<
        Result<RTMPStreamingResponseResult, libp2p::request_response::OutboundFailure>,
    > {
        log::debug!("request_rtmp_streaming_data to peer {}", peer);
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::RequestRTMPData {
                params: RequestRTMPDataParams { peer, request },
                sender,
            })
            .await?;
        tokio::task::yield_now().await;
        Ok(receiver.await?)
    }

    pub async fn respond_rtmp_streaming_data(
        &mut self,
        peer: PeerId,
        response: RTMPStreamingResponseResult,
        channel: ResponseChannel<WrappedRTMPStreamingResponseResult>,
    ) -> anyhow::Result<()> {
        log::debug!(
            "respond_rtmp_streaming_data with success? {}",
            response.is_ok()
        );
        self.sender
            .send(NetworkBackendCommand::RespondRTMPData {
                peer,
                response,
                channel,
            })
            .await?;
        tokio::task::yield_now().await;
        Ok(())
    }

    pub async fn respond_payment_info(
        &mut self,
        response: PaymentInfoResponseResult,
        channel: ResponseChannel<PaymentInfoResponseResult>,
    ) -> anyhow::Result<()> {
        log::debug!("respond_payment_info with success? {}", response.is_ok());
        self.sender
            .send(NetworkBackendCommand::RespondPaymentInfo { response, channel })
            .await?;
        tokio::task::yield_now().await;
        Ok(())
    }

    pub async fn get_peer_statistics(&mut self) -> anyhow::Result<HashMap<PeerId, PeerStatistics>> {
        log::debug!("get_peer_statistics");
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::GetPeerStatistics { sender })
            .await?;
        tokio::task::yield_now().await;
        Ok(receiver.await?)
    }

    pub async fn get_swarm_network_info(
        &mut self,
    ) -> anyhow::Result<libp2p::core::network::NetworkInfo> {
        log::debug!("get_swarm_network_info");
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(NetworkBackendCommand::GetSwarmNetworkInfo { sender })
            .await?;
        tokio::task::yield_now().await;
        Ok(receiver.await?)
    }
}
