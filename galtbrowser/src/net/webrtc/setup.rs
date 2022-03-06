use std::rc::Rc;

use galtcore::tokio::sync::mpsc;
use js_sys::{Array, Object, Reflect};
use log::info;
use serde::{Deserialize, Serialize};
// use tinyjson::JsonValue;
use wasm_bindgen::{prelude::*, JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    window, Event, MediaStream, MediaStreamTrack, RtcConfiguration, RtcIceCandidate,
    RtcIceCandidateInit, RtcOfferOptions, RtcPeerConnection, RtcPeerConnectionIceEvent, RtcSdpType,
    RtcSessionDescriptionInit, RtcTrackEvent,
};

#[allow(unused_must_use)]
pub(crate) fn webrtc_initialize() -> (
    Rc<RtcPeerConnection>,
    mpsc::UnboundedReceiver<Option<MyRTCIceCandidateInit>>,
    mpsc::UnboundedReceiver<()>,
) {
    // Set up Ice Servers
    let ice_server_config_urls = Array::new();
    ice_server_config_urls.push(&JsValue::from("stun:stun.l.google.com:19302"));

    let ice_server_config = Object::new();
    Reflect::set(
        &ice_server_config,
        &JsValue::from("urls"),
        &JsValue::from(&ice_server_config_urls),
    );

    let ice_server_config_list = Array::new();
    ice_server_config_list.push(&ice_server_config);

    // Set up RtcConfiguration
    let mut peer_config: RtcConfiguration = RtcConfiguration::new();
    peer_config.ice_servers(&ice_server_config_list);

    let (ice_candidates_sender, ice_candidates_receiver) = mpsc::unbounded_channel();
    let (on_negotiation_needed_sender, on_negotiation_needed_receiver) = mpsc::unbounded_channel();

    // Setup Peer Connection
    let peer = match RtcPeerConnection::new_with_configuration(&peer_config) {
        Ok(peer) => {
            let peer = Rc::new(peer);
            {
                let callback: Closure<dyn FnMut(JsValue)> =
                    Closure::wrap(Box::new(move |e: JsValue| {
                        log::info!("callback: on negotiation needed {e:?}");
                        on_negotiation_needed_sender.send(());
                    }));
                peer.set_onnegotiationneeded(Some(callback.as_ref().unchecked_ref()));
                callback.forget();
            }

            {
                let callback: Closure<dyn FnMut(&Event)> = Closure::wrap(Box::new({
                    let peer = peer.clone();
                    move |_: &Event| {
                        log::info!(
                            "callback: on ice connection state change: {:?}",
                            peer.ice_connection_state()
                        );
                    }
                }));
                peer.set_oniceconnectionstatechange(Some(callback.as_ref().unchecked_ref()));
                callback.forget();
            }

            {
                let callback: Closure<dyn FnMut(&Event)> = Closure::wrap(Box::new({
                    let peer = peer.clone();
                    move |_: &Event| {
                        log::info!(
                            "callback: on ice gathering state change: {:?}",
                            peer.ice_gathering_state()
                        );
                    }
                }));
                peer.set_onicegatheringstatechange(Some(callback.as_ref().unchecked_ref()));
                callback.forget();
            }

            {
                let callback: Closure<dyn FnMut(&RtcPeerConnectionIceEvent)> =
                    Closure::wrap(Box::new(move |e: &RtcPeerConnectionIceEvent| {
                        let candidate: Option<MyRTCIceCandidateInit> =
                            e.candidate().as_ref().map(Into::into);
                        log::info!("callback: on ice candidate {candidate:?}");
                        ice_candidates_sender.send(candidate);
                    }));

                peer.set_onicecandidate(Some(callback.as_ref().unchecked_ref()));
                callback.forget();
            }

            {
                let callback: Closure<dyn FnMut(&Event)> = Closure::wrap(Box::new({
                    let peer = peer.clone();
                    move |_: &Event| {
                        log::info!(
                            "callback: on signaling state change: {:?}",
                            peer.signaling_state()
                        );
                    }
                }));
                peer.set_onsignalingstatechange(Some(callback.as_ref().unchecked_ref()));
                callback.forget();
            }

            {
                let callback: Closure<dyn FnMut(&RtcTrackEvent)> =
                    Closure::wrap(Box::new(move |e: &RtcTrackEvent| {
                        log::info!("callback: on track {e:?}");
                    }));

                peer.set_ontrack(Some(callback.as_ref().unchecked_ref()));
                callback.forget();
            }

            {
                let callback: Closure<dyn FnMut(JsValue)> =
                    Closure::wrap(Box::new(move |e: JsValue| {
                        log::info!("callback: on remove stream {e:?}");
                    }));
                peer.set_onremovestream(Some(callback.as_ref().unchecked_ref()));
                callback.forget();
            }

            peer
        }
        Err(err) => {
            info!("Error creating new RtcPeerConnection. Error: {:?}", err);
            panic!("");
        }
    };
    (
        peer,
        ice_candidates_receiver,
        on_negotiation_needed_receiver,
    )
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MyRtcPeerConnectionIceEvent {
    pub candidate: Option<MyRTCIceCandidateInit>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MyRTCIceCandidateInit {
    pub candidate: String,
    pub sdp_mid: String,
    pub sdp_mline_index: u16,
    pub username_fragment: String,
}

impl TryFrom<&MyRTCIceCandidateInit> for RtcIceCandidate {
    type Error = anyhow::Error;

    fn try_from(i: &MyRTCIceCandidateInit) -> Result<Self, Self::Error> {
        let mut candidate = RtcIceCandidateInit::new(&i.candidate);
        candidate.sdp_mid(Some(&i.sdp_mid));
        candidate.sdp_m_line_index(Some(i.sdp_mline_index));
        RtcIceCandidate::new(&candidate).map_err(|e| {
            anyhow::anyhow!("Error building ice candidate from ice candidate init: {e:?}")
        })
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RTCSdpType {
    Unspecified = 0,
    #[serde(rename = "offer")]
    Offer,
    #[serde(rename = "pranswer")]
    Pranswer,
    #[serde(rename = "answer")]
    Answer,
    #[serde(rename = "rollback")]
    Rollback,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MySessionDescription {
    #[serde(rename = "type")]
    sdp_type: RTCSdpType,
    sdp: String,
}

impl From<&RtcIceCandidate> for MyRTCIceCandidateInit {
    fn from(c: &RtcIceCandidate) -> Self {
        MyRTCIceCandidateInit {
            candidate: c.candidate(),
            sdp_mid: c.sdp_mid().unwrap_or_default(),
            sdp_mline_index: c.sdp_m_line_index().unwrap_or_default(),
            username_fragment: "".to_string(),
        }
    }
}
impl From<&MySessionDescription> for RtcSessionDescriptionInit {
    fn from(description: &MySessionDescription) -> Self {
        let t = match description.sdp_type {
            RTCSdpType::Unspecified => RtcSdpType::__Nonexhaustive,
            RTCSdpType::Offer => RtcSdpType::Offer,
            RTCSdpType::Pranswer => RtcSdpType::Pranswer,
            RTCSdpType::Answer => RtcSdpType::Answer,
            RTCSdpType::Rollback => RtcSdpType::Rollback,
        };
        let mut d = RtcSessionDescriptionInit::new(t);
        d.sdp(&description.sdp);
        d
    }
}


pub(crate) async fn create_offer(
    connection: &RtcPeerConnection,
) -> anyhow::Result<MySessionDescription> {
    let options = {
        let mut o = RtcOfferOptions::new();
        o.offer_to_receive_audio(true)
            .offer_to_receive_video(true)
            .ice_restart(true);
        o
    };

    let p = connection.create_offer_with_rtc_offer_options(&options);
    let offer = match JsFuture::from(p).await {
        Ok(v) => {
            let description: MySessionDescription = v.into_serde()?;
            description
        }
        Err(e) => {
            anyhow::bail!("create offer error: {e:?}");
        }
    };

    log::info!("success offer create {offer:?}");

    let p = connection.set_local_description(&(&offer).into());
    match JsFuture::from(p).await {
        Ok(_) => Ok(offer),
        Err(e) => {
            anyhow::bail!("create offer: set local description error: {e:?}");
        }
    }
}


pub(crate) async fn receive_answer(
    connection: &RtcPeerConnection,
    description: &MySessionDescription,
) -> anyhow::Result<()> {
    let p = connection.set_remote_description(&description.into());
    JsFuture::from(p)
        .await
        .map_err(|e| anyhow::anyhow!("receive answer: set remote description error: {e:?}"))?;
    Ok(())
}


pub(crate) async fn add_ice_candidate(
    connection: &RtcPeerConnection,
    ice_candidate: Option<MyRTCIceCandidateInit>,
) -> anyhow::Result<()> {
    let ice_candidate: Option<RtcIceCandidate> = match ice_candidate {
        Some(i) => Some((&i).try_into()?),
        None => None,
    };
    let p = connection.add_ice_candidate_with_opt_rtc_ice_candidate(ice_candidate.as_ref());
    JsFuture::from(p)
        .await
        .map_err(|e| anyhow::anyhow!("add ice candidate: {e:?}"))?;
    Ok(())
}

pub(crate) async fn get_display_media() -> anyhow::Result<MediaStream> {
    let w = window().ok_or(anyhow::anyhow!("Missing window"))?;
    let media_devices = w
        .navigator()
        .media_devices()
        .map_err(|e| anyhow::anyhow!("Error getting media devices: {e:?}"))?;
    let display_media = media_devices
        .get_display_media()
        .map_err(|e| anyhow::anyhow!("Error getting display media: {e:?}"))?;
    let display_media: MediaStream = JsFuture::from(display_media)
        .await
        .map_err(|e| anyhow::anyhow!("Error waiting display media: {e:?}"))?
        .into();
    Ok(display_media)
}

pub(crate) async fn share_screen(
    display_media: &MediaStream,
    connection: &RtcPeerConnection,
) -> anyhow::Result<()> {
    let tracks = display_media.get_tracks();
    for i in 0..tracks.length() {
        let track: MediaStreamTrack = tracks.get(i).into();
        let result = connection.add_track(&track, display_media, &js_sys::Array::new());
        log::info!("add track: {result:?}");
    }

    Ok(())
}
