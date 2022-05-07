use std::time::Duration;

use anyhow::Context;
use itertools::Itertools;

use crate::cm::SharedGlobalState;
use crate::networkbackendclient::NetworkBackendClient;
use crate::protocols::galt_identify::{
    GaltIdentifyInfo, GaltIdentifyRequest, GaltIdentifyResponse, KnownPeer,
};
use crate::protocols::NodeIdentity;
use crate::utils;

pub async fn handle_respond(
    identity: &NodeIdentity,
    shared_state: SharedGlobalState,
    network: NetworkBackendClient,
    request: crate::protocols::galt_identify::RequestEvent,
) -> anyhow::Result<()> {
    match request.request {
        GaltIdentifyRequest::OfferInfo(info) => {
            let peer = &request.peer;
            if !info.verify_sig(peer) {
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
                        let k = k.canonicalize();
                        let kpeer = k.peer;
                        if k.external_addresses.is_empty() {
                            log::warn!(
                                "Ignoring our peer {kpeer} because there are no listen addresses"
                            );
                        } else {
                            log::info!("Dialing our org peer {kpeer}");
                            new_connections.push(network.dial(kpeer, k.external_addresses.clone()));
                            peer_control.add_our_org_peer(k);
                        }
                    }
                } else {
                    peer_control.add_org_peers(
                        info.our_org_peers.into_iter().map(|p| p.canonicalize()),
                        info.org.clone(),
                    );
                }
                peer_control.add_peer_org(*peer, info.org);
                peer_control.add_external_address(*peer, info.external_addresses);
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
                        && !s.external_addresses.is_empty()
                        &&s.org.is_some() && s.org.as_ref() != Some(&identity.org)
                })
                .collect_vec();
            peers.sort_by_key(|(_, s)| s.latency.unwrap());
            peers
                .into_iter()
                .map(|(peer, stats)| KnownPeer {
                    peer: *peer,
                    external_addresses: stats.external_addresses.clone(),
                })
                .take(crate::protocols::galt_identify::MAX_SHARED_PEERS_PER_LIST)
                .collect_vec()
        };
        let our_org_peers = {
            let mut peers = peer_control
                .peer_statistics
                .iter()
                .filter(|(_, s)| {
                    !s.external_addresses.is_empty() && s.org.as_ref() == Some(&identity.org)
                })
                .collect_vec();
            peers.sort_by_key(|(_, s)| s.latency.unwrap_or(Duration::MAX));
            peers
                .into_iter()
                .map(|(peer, stats)| KnownPeer {
                    peer: *peer,
                    external_addresses: stats.external_addresses.clone(),
                })
                .take(crate::protocols::galt_identify::MAX_SHARED_PEERS_PER_LIST)
                .collect_vec()
        };
        let network_info = network.get_swarm_network_infos().await?;
        let mut external_addresses = network_info
            .external_addresses
            .iter()
            .filter_map(|e| {
                utils::canonical_address_for_peer_id(e.addr.clone(), &identity.peer_id)
                    .map(|addr| (e.score, addr))
                    .ok() // remove bad addresses
            })
            .collect_vec();
        // high score first
        external_addresses.sort_by(|a, b| b.cmp(a));
        let mut external_addresses = external_addresses
            .into_iter()
            .map(|(_, addr)| addr)
            .dedup()
            .take(crate::protocols::galt_identify::MAX_SHARED_ADDRESSES_PER_PEER) // get best addresses
            .collect_vec();
        if external_addresses.is_empty() {
            log::info!(
                "Fallbacking to send listen addresses because no external address is known (yet)"
            );
            external_addresses.extend(network_info.listen_addresses);
        }
        log::debug!("Sending external addresses: {external_addresses:?}");
        GaltIdentifyInfo {
            external_addresses,
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
