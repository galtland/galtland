// SPDX-License-Identifier: AGPL-3.0-only

use libp2p::request_response::ResponseChannel;
use libp2p::PeerId;

use crate::networkbackendclient::NetworkBackendClient;
use crate::protocols::payment_info::models::PaymentInfo;
use crate::protocols::payment_info::{
    PaymentInfoRequest, PaymentInfoResponse, PaymentInfoResponseResult,
};

pub struct RespondPaymentInfo {
    pub peer: PeerId,
    pub request: PaymentInfoRequest,
    pub channel: ResponseChannel<PaymentInfoResponseResult>,
}

pub(crate) async fn handle_respond(
    network: NetworkBackendClient,
    info: RespondPaymentInfo,
) -> anyhow::Result<()> {
    //FIXME: just a stub
    let payment_info = PaymentInfo {
        crypto_payment_methods: Vec::new(),
        fiat_payment_methods: Vec::new(),
    };
    network
        .respond_payment_info(Ok(PaymentInfoResponse { payment_info }), info.channel)
        .await
}
