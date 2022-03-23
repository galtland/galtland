// SPDX-License-Identifier: AGPL-3.0-only

use libp2p::request_response::{RequestResponseEvent, RequestResponseMessage};
use libp2p::Swarm;
use log::{debug, warn};

use super::{PaymentInfoRequest, PaymentInfoResponseResult};
use crate::protocols::{ComposedBehaviour, InternalNetworkEvent};

pub(crate) fn handle_event(
    event: RequestResponseEvent<PaymentInfoRequest, PaymentInfoResponseResult>,
    swarm: &mut Swarm<ComposedBehaviour>,
) {
    match event {
        RequestResponseEvent::Message { peer, message } => match message {
            RequestResponseMessage::Request {
                request_id,
                request,
                channel,
            } => {
                log::debug!(
                    "RequestResponseMessage::Request {} {} {:?}",
                    request_id,
                    peer,
                    request
                );
                swarm
                    .behaviour_mut()
                    .event_sender
                    .send(InternalNetworkEvent::InboundPaymentInfoRequest {
                        peer,
                        request,
                        channel,
                    })
                    .expect("receiver to be receiving");
            }
            RequestResponseMessage::Response {
                request_id,
                response,
            } => {
                debug!("RequestResponseMessage::Response {} {}", request_id, peer);
                swarm
                    .behaviour_mut()
                    .state
                    .pending_payment_info_request
                    .remove(&request_id)
                    .expect("Request to still be pending")
                    .send(Ok(response))
                    .expect("Receiver not to be dropped") // FIXME: this happened, handle gracefully
            }
        },
        RequestResponseEvent::OutboundFailure {
            peer,
            request_id,
            error,
        } => {
            warn!(
                "RequestResponseEvent::OutboundFailure {} {}: {}",
                peer, request_id, error
            );
            if let Err(e) = swarm
                .behaviour_mut()
                .state
                .pending_payment_info_request
                .remove(&request_id)
                .expect("Request to still be pending")
                .send(Err(error))
            {
                log::warn!("Receiver dropped while trying to send: {e:?}")
            };
        }
        RequestResponseEvent::InboundFailure {
            peer,
            request_id,
            error,
        } => warn!(
            "RequestResponseEvent::InboundFailure {} {}: {}",
            peer, request_id, error
        ),
        RequestResponseEvent::ResponseSent { peer, request_id } => log::debug!(
            "RequestResponseEvent::ResponseSent {}: {}",
            peer,
            request_id
        ),
    }
}
