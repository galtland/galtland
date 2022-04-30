use std::time::Duration;

use anyhow::Context;
use itertools::Itertools;

use crate::cm::SharedGlobalState;
use crate::networkbackendclient::NetworkBackendClient;
use crate::protocols::galt_identify::{
    GaltIdentifyInfo, GaltIdentifyRequest, GaltIdentifyResponse, KnownPeer,
};
use crate::protocols::NodeIdentity;

pub async fn handle_respond(
    identity: &NodeIdentity,
    shared_state: SharedGlobalState,
    network: NetworkBackendClient,
    request: crate::protocols::galt_identify::RequestEvent,
) -> anyhow::Result<()> {
    match request.request {
        GaltIdentifyRequest::OfferInfo(info) => {
            let peer = request.peer;
            if !info.verify_sig(&peer) {
                let message = "Invalid org signature".to_string();
                network
                    .respond_galt_identify(Err(message.clone()), request.channel)
                    .await?;
                anyhow::bail!("{message}");
            }
            let mut new_connections = Vec::new();
            {
                let mut peer_control = shared_state.peer_control.lock().await;
                if info.org == identity.org {
                    // peer from our org
                    for k in info.our_org_peers {
                        if k.listen_addresses.is_empty() {
                            log::warn!(
                                "Ignoring our peer {} because there are no listen addresses",
                                k.peer
                            );
                        } else {
                            peer_control.add_our_org_peer(k.clone());
                            log::info!("Dialing our org peer {}", k.peer);
                            new_connections.push(network.dial(k.peer, k.listen_addresses));
                        }
                    }
                } else {
                    peer_control.add_org_peers(&info.our_org_peers, info.org.clone());
                }
                peer_control.add_peer_org(peer, info.org);
            }
            if network
                .respond_galt_identify(Ok(GaltIdentifyResponse {}), request.channel)
                .await
                .is_err()
            {
                log::warn!("Error sending GaltIdentifyResponse to {peer}")
            }

            for result in futures::future::join_all(new_connections).await {
                match result {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        log::warn!("Got dial error while contacting org peer: {e:?}")
                    }
                    Err(e) => {
                        log::warn!("Got unknown error while contacting org peer: {e:?}")
                    }
                };
            }
        }
    };
    Ok(())
}

pub(crate) async fn handle_establish_with_peer(
    identity: &NodeIdentity,
    shared_state: SharedGlobalState,
    network: NetworkBackendClient,
    peer: libp2p::PeerId,
) -> anyhow::Result<()> {
    let info = {
        let peer_control = shared_state.peer_control.lock().await;
        let other_peers = {
            let mut peers = peer_control
                .peer_statistics
                .iter()
                .filter(|(_, s)| {
                    s.latency.is_some() // we only share the peers that are currently connected
                        && s.identify_info.is_some()
                        &&s.org.is_some() && s.org.as_ref() != Some(&identity.org)
                })
                .collect_vec();
            peers.sort_by_key(|(_, s)| s.latency.unwrap());
            peers
                .into_iter()
                .map(|(peer, stats)| KnownPeer {
                    peer: *peer,
                    listen_addresses: stats
                        .identify_info
                        .as_ref()
                        .map(|i| i.listen_addrs.clone())
                        .unwrap(),
                })
                .take(crate::protocols::galt_identify::MAX_SHARED_PEERS_PER_LIST)
                .collect_vec()
        };
        let our_org_peers = {
            let mut peers = peer_control
                .peer_statistics
                .iter()
                .filter(|(_, s)| s.identify_info.is_some() && s.org.as_ref() == Some(&identity.org))
                .collect_vec();
            peers.sort_by_key(|(_, s)| s.latency.unwrap_or(Duration::MAX));
            peers
                .into_iter()
                .map(|(peer, stats)| KnownPeer {
                    peer: *peer,
                    listen_addresses: stats
                        .identify_info
                        .as_ref()
                        .map(|i| i.listen_addrs.clone())
                        .unwrap(),
                })
                .take(crate::protocols::galt_identify::MAX_SHARED_PEERS_PER_LIST)
                .collect_vec()
        };
        GaltIdentifyInfo {
            our_org_peers,
            org: identity.org.clone(),
            our_org_sig: identity.org_sig.to_vec(),
            other_peers,
        }
    };
    let request = GaltIdentifyRequest::OfferInfo(info);
    network
        .request_galt_identify(request, peer)
        .await
        .context("request_galt_identify first error")?
        .context("request_galt_identify second error")?
        .map_err(|s| anyhow::anyhow!("request_galt_identify last error {s}"))?;
    Ok(())
}
