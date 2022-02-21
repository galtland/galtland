// SPDX-License-Identifier: AGPL-3.0-only

use super::rtmp::handlers::RespondPaymentInfo;
use crate::networkbackendclient::NetworkBackendClient;
use crate::protocols::payment_info::models::PaymentInfo;
use crate::protocols::payment_info::PaymentInfoResponse;

pub async fn handle_respond(
    mut network: NetworkBackendClient,
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
