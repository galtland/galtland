// SPDX-License-Identifier: AGPL-3.0-only

use super::streaming::handlers::RespondPaymentInfo;
use crate::networkbackendclient::NetworkBackendClient;
use crate::protocols::payment_info::models::PaymentInfo;
use crate::protocols::payment_info::PaymentInfoResponse;

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
