use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct PaymentInfo {
    pub crypto_payment_methods: Vec<CryptoPaymentMethod>,
    pub fiat_payment_methods: Vec<FiatPaymentMethod>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum CryptoPaymentMethod {
    Bch(Bech32Address),
    Btc(Bech32Address),
    Ltc(Bech32Address),
    LightningNetwork(LnUrl),
    MoneroBase58(MoneroBase58Address),
    MoneroOpenAlias(OpenAliasAddress),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Bech32Address {
    address: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MoneroBase58Address {
    address: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LnUrl {
    address: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OpenAliasAddress {
    address: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum FiatPaymentMethod {
    FasterPayments(FasterPaymentsInfo),
    NationalBankTransfer(NationalBankTransferInfo),
    Paysera(PayseraInfo),
    Pix(PixInfo),
    Revolut(RevolutInfo),
    Sepa(SepaInfo),
    SepaInstant(SepaInfo),
    TransferwiseEur(TransferwiseEurInfo),
    TransferwiseUsd(TransferwiseUsdInfo),
    Uphold(UpholdInfo),
    UsMoneyOrder(UsMoneyOrderInfo),
    Zelle(ZelleInfo),
}

// loosely based on https://github.com/bisq-network/bisq/tree/master/core/src/main/java/bisq/core/payment

#[derive(Serialize, Deserialize, Debug)]
pub struct FasterPaymentsInfo {
    holder_name: String,
    sort_code: String,
    account_number: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NationalBankTransferInfo {
    bank_id: String,
    bank_name: String,
    branch_id: String,
    account_number: String,
    account_type: String,
    holder_tax_id: String,
    holder_name: String,
    country_code: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PayseraInfo {
    email: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PixInfo {
    pix_key: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RevolutInfo {
    user_name: String,
    account_id: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SepaInfo {
    bank_id: String,
    holder_name: String,
    iban: String,
    bic: String,
    country_code: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TransferwiseEurInfo {
    email: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TransferwiseUsdInfo {
    email: String,
    holder_name: String,
    beneficiary_address: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UpholdInfo {
    account_id: String,
    account_owner: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UsMoneyOrderInfo {
    postal_address: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ZelleInfo {
    email_or_mobile_number: String,
    holder_name: String,
}
