// SPDX-License-Identifier: AGPL-3.0-only

use std::fmt::Write;
use std::pin::Pin;

use galtcore::cm::modules::simple_file::{GetSimpleFileInfo, PublishSimpleFileInfo};
use galtcore::cm::{self, SharedGlobalState};
use galtcore::daemons::gossip_listener::GossipListenerClient;
use galtcore::libp2p::futures::{self, StreamExt};
use galtcore::networkbackendclient::NetworkBackendClient;
use galtcore::protocols::media_streaming::StreamingKey;
use galtcore::protocols::NodeIdentity;
use galtcore::tokio::sync::oneshot;
use galtcore::tokio_stream::wrappers::ReceiverStream;
use galtcore::{tokio, utils};
use log::info;
use tonic::{Request, Response, Status};

pub mod sm {
    tonic::include_proto!("service_main");
}

pub struct Service {
    pub identity: NodeIdentity,
    pub gossip_listener_client: GossipListenerClient,
    pub network: NetworkBackendClient,
    pub shared_global_state: SharedGlobalState,
}

type ServiceStream<T> = Pin<
    Box<dyn galtcore::tokio_stream::Stream<Item = std::result::Result<T, Status>> + Send + 'static>,
>;

#[tonic::async_trait]
impl sm::service_server::Service for Service {
    type GetSimpleFileStream = ServiceStream<sm::GetSimpleFileResponse>;

    async fn add_simple_file(
        &self,
        request: Request<sm::AddSimpleFileRequest>,
    ) -> Result<Response<sm::AddSimpleFileResponse>, Status> {
        info!("add_simple_file: {:?}", request);
        let filename = request.into_inner().filename;
        let network = self.network.clone();
        let result: Result<Vec<u8>, String> = {
            tokio::spawn(async move {
                let hash = utils::blake3_file_hash(&filename)
                    .await
                    .map_err(|e| format!("Calculating hash: {}", e))?;
                let (sender, receiver) = oneshot::channel();
                let info = PublishSimpleFileInfo {
                    filename,
                    hash: hash.clone(),
                    sender,
                };
                cm::modules::simple_file::handle_publish(network, info)
                    .await
                    .map_err(|e| format!("Publish file: {}", e))?;
                let r = match receiver.await {
                    Ok(Ok(_)) => Ok(hash),
                    Ok(Err(e)) => Err(e),
                    Err(e) => Err(format!("Publish receive {:?}", e)),
                };
                r
            })
        }
        .await
        .expect("Publish file");
        let response = match result {
            Ok(hash) => sm::AddSimpleFileResponse {
                message: "Ok".to_string(),
                success: true,
                blake3: Some(hex::encode(hash)),
            },
            Err(s) => sm::AddSimpleFileResponse {
                message: s,
                success: false,
                blake3: None,
            },
        };
        Ok(Response::new(response))
    }

    async fn get_simple_file(
        &self,
        request: Request<sm::GetSimpleFileRequest>,
    ) -> Result<Response<Self::GetSimpleFileStream>, Status> {
        let hash = match hex::decode(request.into_inner().hash_hex) {
            Ok(h) => h,
            Err(e) => {
                return {
                    Err(Status::invalid_argument(format!(
                        "error converting key: {}",
                        e
                    )))
                }
            }
        };

        let network = self.network.clone();
        let (client_sender, client_receiver) = tokio::sync::mpsc::channel(10);
        tokio::spawn({
            async move {
                let (backend_sender, mut backend_receiver) = tokio::sync::mpsc::channel(10);
                cm::modules::simple_file::get(
                    network,
                    GetSimpleFileInfo {
                        hash,
                        sender: backend_sender,
                    },
                )
                .await?;
                loop {
                    tokio::task::yield_now().await;
                    match backend_receiver.recv().await {
                        Some(Ok(Ok(r))) => {
                            let r: Result<sm::GetSimpleFileResponse, Status> =
                                Ok(sm::GetSimpleFileResponse { data: r.data });
                            client_sender.send(r).await.map_err(utils::send_error)?
                        }
                        Some(Ok(Err(e))) => client_sender
                            .send(Err(Status::internal(e)))
                            .await
                            .map_err(utils::send_error)?,
                        Some(Err(e)) => client_sender
                            .send(Err(Status::internal(e.to_string())))
                            .await
                            .map_err(utils::send_error)?,
                        None => return Ok::<(), anyhow::Error>(()),
                    }
                }
            }
        });

        let mut stream = ReceiverStream::new(client_receiver);
        match stream.next().await {
            Some(Ok(r)) => {
                let stream =
                    futures::stream::once(async { Ok::<sm::GetSimpleFileResponse, Status>(r) })
                        .chain(stream);
                Ok(Response::new(Box::pin(stream)))
            }
            Some(Err(e)) => Err(Status::internal(format!("error: {}", e))),
            None => {
                let stream = futures::stream::empty::<Result<sm::GetSimpleFileResponse, Status>>();
                Ok(Response::new(Box::pin(stream)))
            }
        }
    }

    async fn get_network_info(
        &self,
        _: Request<sm::GetNetworkInfoRequest>,
    ) -> Result<Response<sm::GetNetworkInfoResponse>, Status> {
        let network = self.network.clone();
        let swarm_infos = network
            .get_swarm_network_infos()
            .await
            .map_err(|e| Status::unknown(e.to_string()))?;
        let gossip_info = self.gossip_listener_client.get_streaming_records().await;
        let peer_id = self.identity.peer_id;
        let org = &self.identity.org;

        let (blacklisted, last_peer_requests, our_org_peers, other_orgs_peers, peer_statistics) = {
            let p = self.shared_global_state.peer_control.lock().await;
            (
                p.blacklisted.clone(),
                p.last_peer_requests.clone(),
                p.our_org_peers.clone(),
                p.other_orgs_peers.clone(),
                p.peer_statistics.clone(),
            )
        };

        let format_output = || -> anyhow::Result<String> {
            let mut s = String::new();
            writeln!(s, "{org:?}\n")?;
            writeln!(s, "\nInfo:\n{:?}", swarm_infos.info)?;
            writeln!(s, "\nListen addresses:")?;
            for address in swarm_infos.listen_addresses {
                writeln!(s, "\t- {address}")?;
            }
            writeln!(s, "\nExternal addresses:")?;
            for address in swarm_infos.external_addresses {
                writeln!(s, "\t- {address:?}")?;
            }
            writeln!(s, "\nConnected peers:")?;
            for peer in swarm_infos.connected_peers {
                writeln!(s, "\t- {peer:?}")?;
            }
            if gossip_info.is_empty() {
                writeln!(s, "\nNo records gathered so far\n")?;
            } else {
                writeln!(s, "\nKnown Records:\n")?;
                for i in gossip_info {
                    writeln!(s, "\t{}", i.streaming_key)?;
                }
            }
            writeln!(s, "\nPeer control info\n")?;

            if !our_org_peers.is_empty() {
                writeln!(s, "\tOur peers:")?;
                for (p, addresses) in our_org_peers {
                    writeln!(s, "\t\t- {p:?}: {} addresses", addresses.len())?;
                }
            }

            if other_orgs_peers.iter().any(|(_, m)| !m.is_empty()) {
                writeln!(s, "\tOther organizations peers:")?;
                for (other_org, peer_map) in other_orgs_peers {
                    if !peer_map.is_empty() {
                        writeln!(s, "\t\t{:?}:", other_org)?;
                        for (p, addresses) in peer_map {
                            writeln!(s, "\t\t\t- {p:?}: {} addresses", addresses.len())?;
                        }
                    }
                }
            }

            if !blacklisted.is_empty() {
                writeln!(s, "\tBlacklisted:")?;
                for (p, instant) in blacklisted {
                    writeln!(s, "\t\t{p:?}: {instant:?}")?;
                }
            }

            if !last_peer_requests.is_empty() {
                writeln!(s, "\tLast peer requests:")?;
                for (p, requests) in last_peer_requests {
                    writeln!(s, "\t\t- {p:?}: {} requests", requests.len())?;
                }
            }

            if !peer_statistics.is_empty() {
                writeln!(s, "\tPeer statistics:")?;
                for (p, stats) in peer_statistics {
                    writeln!(s, "\t\t- {p:?}: {stats:?}")?;
                }
            }

            Ok(s)
        };
        match format_output() {
            Ok(info) => Ok(Response::new(sm::GetNetworkInfoResponse {
                info,
                peer_id: peer_id.into(),
            })),
            Err(e) => Err(Status::internal(format!("Error formatting output: {e}"))),
        }
    }

    async fn stream_publish_file(
        &self,
        request: Request<sm::StreamPublishFileRequest>,
    ) -> Result<Response<sm::StreamPublishFileResponse>, Status> {
        info!("stream_publish_file: {:?}", request);
        let r = request.into_inner();
        let video_file = r.video_file;
        let audio_file = r.audio_file;
        let video_stream_key = r.video_stream_key;

        let identity = self.identity.clone();
        let shared_global_state = &self.shared_global_state;
        let mut network = self.network.clone();

        let streaming_key = StreamingKey {
            video_key: video_stream_key,
            channel_key: identity.peer_id,
        };

        let result = nativecommon::webrtc::publish_local_file::publish3(
            video_file,
            audio_file,
            streaming_key,
            identity,
            shared_global_state,
            &mut network,
        )
        .await;

        let response = match result {
            Ok(_) => sm::StreamPublishFileResponse {
                message: "Ok".to_string(),
                success: true,
            },
            Err(e) => sm::StreamPublishFileResponse {
                message: format!("error publishing: {:?}", e),
                success: false,
            },
        };
        Ok(Response::new(response))
    }
}
