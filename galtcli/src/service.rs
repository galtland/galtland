// SPDX-License-Identifier: AGPL-3.0-only

use std::fmt::Write;
use std::pin::Pin;

use galtcore::daemons::cm::simple_file::{GetSimpleFileInfo, PublishSimpleFileInfo};
use galtcore::daemons::cm::ClientCommand;
use galtcore::daemons::gossip_listener::GossipListenerClient;
use galtcore::libp2p::futures::{self, StreamExt};
use galtcore::networkbackendclient::NetworkBackendClient;
use galtcore::tokio::sync::{mpsc, oneshot};
use galtcore::tokio_stream::wrappers::ReceiverStream;
use galtcore::{tokio, utils};
use log::info;
use tonic::{Request, Response, Status};

pub mod sm {
    tonic::include_proto!("service_main");
}

pub struct Service {
    pub commands: mpsc::Sender<ClientCommand>,
    pub gossip_listener_client: GossipListenerClient,
    pub network: NetworkBackendClient,
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
        let commands = self.commands.clone();
        let result: Result<Vec<u8>, String> = {
            tokio::spawn(async move {
                let hash = utils::blake3_file_hash(&filename)
                    .await
                    .map_err(|e| format!("Calculating hash: {}", e))?;
                let (sender, receiver) = oneshot::channel();
                let command = ClientCommand::PublishSimpleFile(PublishSimpleFileInfo {
                    filename,
                    hash: hash.clone(),
                    sender,
                });
                commands
                    .send(command)
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

        let (client_sender, client_receiver) = tokio::sync::mpsc::channel(10);
        tokio::spawn({
            let commands = self.commands.clone();
            async move {
                let (backend_sender, mut backend_receiver) = tokio::sync::mpsc::channel(10);
                if commands
                    .send(ClientCommand::GetSimpleFile(GetSimpleFileInfo {
                        hash,
                        sender: backend_sender,
                    }))
                    .await
                    .is_err()
                {
                    anyhow::bail!("commands receiver died")
                };
                loop {
                    match backend_receiver.recv().await {
                        Some(Ok(r)) => {
                            let r: Result<sm::GetSimpleFileResponse, Status> =
                                Ok(sm::GetSimpleFileResponse { data: r.data });
                            if client_sender.send(r).await.is_err() {
                                anyhow::bail!("receiver died")
                            };
                        }
                        Some(Err(e)) => {
                            if client_sender.send(Err(Status::internal(e))).await.is_err() {
                                anyhow::bail!("receiver died")
                            }
                        }
                        None => return Ok(()),
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
        let mut network = self.network.clone();
        let swarm_info = network.get_swarm_network_info().await;
        let gossip_info = self.gossip_listener_client.get_rtmp_records().await;
        let format_output = || -> anyhow::Result<String> {
            let mut s = String::new();
            writeln!(s, "\n{:?}\n", swarm_info)?;
            if gossip_info.is_empty() {
                writeln!(s, "\nEmpty Records\n")?;
            } else {
                writeln!(s, "\nKnown Records:\n")?;
                for i in gossip_info {
                    writeln!(
                        s,
                        "{}/{}",
                        i.streaming_key.app_name, i.streaming_key.stream_key
                    )?;
                }
            }
            Ok(s)
        };
        match format_output() {
            Ok(info) => Ok(Response::new(sm::GetNetworkInfoResponse { info })),
            Err(e) => Err(Status::internal(format!("Error formatting output: {e}"))),
        }
    }
}
