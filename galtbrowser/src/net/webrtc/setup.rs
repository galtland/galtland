// SPDX-License-Identifier: AGPL-3.0-only

use std::rc::Rc;

use anyhow::Context;
use galtcore::protocols::delegated_streaming::{WebRtcStream, WebRtcTrack};
use galtcore::bincode;
use galtcore::tokio::sync::mpsc;
use js_sys::{Array, Object, Reflect};
use log::info;
use serde::{Deserialize, Serialize};
// use tinyjson::JsonValue;
use wasm_bindgen::{prelude::*, JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    window, Event, HtmlMediaElement, MediaStream, MediaStreamConstraints, MediaStreamTrack,
    RtcConfiguration, RtcIceCandidate, RtcIceCandidateInit, RtcOfferOptions, RtcPeerConnection,
    RtcPeerConnectionIceEvent, RtcSdpType, RtcSessionDescription, RtcSessionDescriptionInit,
    RtcTrackEvent,
};

use crate::net::ConnectionStatusUpdate;

#[allow(unused_must_use)]
pub(super) fn webrtc_initialize(
    connection_status_update_sender: mpsc::UnboundedSender<ConnectionStatusUpdate>,
) -> (
    Rc<RtcPeerConnection>,
    mpsc::UnboundedReceiver<Option<MyRTCIceCandidateInit>>,
    mpsc::UnboundedReceiver<()>,
    mpsc::UnboundedReceiver<RtcTrackEvent>,
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
    let (on_track_sender, on_track_receiver) = mpsc::unbounded_channel();

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
                    let connection_status_update_sender = connection_status_update_sender.clone();
                    move |_: &Event| {
                        let state = format!("{:?}", peer.ice_connection_state());
                        log::info!("callback: on ice connection state change: {state}");
                        if connection_status_update_sender
                            .send(ConnectionStatusUpdate::IceConnectionState(state))
                            .is_err()
                        {
                            log::warn!("connection_status_update has been closed")
                        }
                    }
                }));
                peer.set_oniceconnectionstatechange(Some(callback.as_ref().unchecked_ref()));
                callback.forget();
            }

            {
                let callback: Closure<dyn FnMut(&Event)> = Closure::wrap(Box::new({
                    let peer = peer.clone();
                    let connection_status_update_sender = connection_status_update_sender.clone();
                    move |_: &Event| {
                        let state = format!("{:?}", peer.ice_gathering_state());
                        log::info!("callback: on ice gathering state change: {state}",);
                        if connection_status_update_sender
                            .send(ConnectionStatusUpdate::IceGatheringState(state))
                            .is_err()
                        {
                            log::warn!("connection_status_update has been closed")
                        }
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
                        let state = format!("{:?}", peer.signaling_state());
                        log::info!("callback: on signaling state change: {state}");
                        if connection_status_update_sender
                            .send(ConnectionStatusUpdate::SignalingState(state))
                            .is_err()
                        {
                            log::warn!("connection_status_update has been closed")
                        }
                    }
                }));
                peer.set_onsignalingstatechange(Some(callback.as_ref().unchecked_ref()));
                callback.forget();
            }

            {
                let callback: Closure<dyn FnMut(RtcTrackEvent)> =
                    Closure::wrap(Box::new(move |e: RtcTrackEvent| {
                        log::info!("callback: on track {e:?}");
                        on_track_sender.send(e);
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
        on_track_receiver,
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

impl From<&RtcSessionDescription> for MySessionDescription {
    fn from(description: &RtcSessionDescription) -> Self {
        let sdp_type = match description.type_() {
            RtcSdpType::__Nonexhaustive => RTCSdpType::Unspecified,
            RtcSdpType::Offer => RTCSdpType::Offer,
            RtcSdpType::Pranswer => RTCSdpType::Pranswer,
            RtcSdpType::Answer => RTCSdpType::Answer,
            RtcSdpType::Rollback => RTCSdpType::Rollback,
        };
        MySessionDescription {
            sdp: description.sdp(),
            sdp_type,
        }
    }
}


pub(super) async fn create_offer(
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
    let offer = match JsFuture::from(p).await {
        Ok(_) => Ok(offer),
        Err(e) => {
            anyhow::bail!("create offer: set local description error: {e:?}");
        }
    };
    offer
}


pub(super) async fn receive_answer(
    connection: &RtcPeerConnection,
    description: &MySessionDescription,
) -> anyhow::Result<()> {
    let p = connection.set_remote_description(&description.into());
    JsFuture::from(p)
        .await
        .map_err(|e| anyhow::anyhow!("receive answer: set remote description error: {e:?}"))?;
    Ok(())
}


pub(super) async fn add_ice_candidate(
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

pub(super) async fn get_display_media() -> anyhow::Result<MediaStream> {
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


pub(super) async fn share_screen(
    display_media: &MediaStream,
    connection: &RtcPeerConnection,
) -> anyhow::Result<Vec<WebRtcTrack>> {
    let mut webrtc_tracks = Vec::new();
    let tracks = display_media.get_tracks();
    for i in 0..tracks.length() {
        let track: MediaStreamTrack = tracks.get(i).into();
        webrtc_tracks.push(WebRtcTrack {
            track_id: track.id(),
            stream_id: WebRtcStream(display_media.id()),
        });
        connection.add_track(&track, display_media, &js_sys::Array::new());
    }
    Ok(webrtc_tracks)
}

pub(crate) async fn get_answer(
    connection: &RtcPeerConnection,
    remote_description: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let remote_description: MySessionDescription =
        bincode::deserialize(remote_description).context("deserializing remote offer")?;
    receive_answer(connection, &remote_description).await?;

    let p = connection.create_answer();
    let answer = &match JsFuture::from(p).await {
        Ok(r) => {
            let answer: RtcSessionDescription = r.into();
            answer
        }
        Err(e) => anyhow::bail!(
            "get answer: set remote description ({remote_description:?}) error: {e:?}"
        ),
    };
    let answer: &MySessionDescription = &answer.into();

    let p = connection.set_local_description(&answer.into());
    JsFuture::from(p).await.map_err(|e| {
        anyhow::anyhow!("get answer: set local description ({answer:?}) error: {e:?}")
    })?;

    let serialized = bincode::serialize(answer).context("serializing answer to offer")?;
    Ok(serialized)
}

pub(crate) fn play_video(stream: &MediaStream) -> anyhow::Result<()> {
    let w = window().ok_or(anyhow::anyhow!("Missing window"))?;
    let d = w.document().ok_or(anyhow::anyhow!("Missing document"))?;
    if let Some(video) = d.get_element_by_id("main_video_stream") {
        let video: JsValue = video.into();
        let video: HtmlMediaElement = video.into();
        video.set_src_object(None); // avoids some old bugs in some old browsers
        video.set_src_object(Some(stream));
    } else {
        anyhow::bail!("Bug?!: missing video element");
    }
    Ok(())
}

pub(super) async fn get_user_media() -> anyhow::Result<MediaStream> {
    let w = window().ok_or(anyhow::anyhow!("Missing window"))?;
    let media_devices = w
        .navigator()
        .media_devices()
        .map_err(|e| anyhow::anyhow!("Error getting media devices: {e:?}"))?;
    let constraints = {
        let mut c = MediaStreamConstraints::new();
        c.audio(&true.into());
        c.video(&true.into());
        c
    };
    let user_media = media_devices
        .get_user_media_with_constraints(&constraints)
        .map_err(|e| anyhow::anyhow!("Error getting user media: {e:?}"))?;
    let user_media: MediaStream = JsFuture::from(user_media)
        .await
        .map_err(|e| anyhow::anyhow!("Error waiting user media: {e:?}"))?
        .into();
    Ok(user_media)
}

pub(super) async fn share_audio_video(
    user_media: &MediaStream,
    connection: &RtcPeerConnection,
) -> anyhow::Result<Vec<WebRtcTrack>> {
    let mut webrtc_tracks = Vec::new();
    let tracks = user_media.get_tracks();
    for i in 0..tracks.length() {
        let track: MediaStreamTrack = tracks.get(i).into();
        webrtc_tracks.push(WebRtcTrack {
            track_id: track.id(),
            stream_id: WebRtcStream(user_media.id()),
        });
        connection.add_track(&track, user_media, &js_sys::Array::new());
    }
    Ok(webrtc_tracks)
}
