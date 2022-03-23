// SPDX-License-Identifier: AGPL-3.0-only

mod components;
mod net;
mod transport;
mod utils;

use std::cell::RefCell;
use std::rc::Rc;

use components::text_input::TextInput;
use galtcore::protocols::media_streaming::StreamingKey;
use net::{webrtc, ConnectionStatusUpdate};
use yew::prelude::*;


#[derive(Debug)]
enum Msg {
    Connect,
    Connected,
    FailedToConnect,
    ShareScreen,
    PublishedStream(StreamingKey),
    ScreenShareFailed,
    PlayStream(String),
    PlayingStream,
    StreamPlayFailed,
    UpdateConnectionStatus(ConnectionStatusUpdate),
}

#[derive(Default)]
struct Model {
    webrtc_state: Rc<RefCell<Option<webrtc::state::WebRtcState>>>,
    playing_stream: String,
    publishing_stream_key: String,

    signaling_state: String,
    ice_connection_state: String,
    ice_gathering_state: String,
}


impl Component for Model {
    type Message = Msg;
    type Properties = ();

    fn create(_ctx: &Context<Self>) -> Self {
        Default::default()
    }

    fn update(&mut self, ctx: &Context<Self>, msg: Self::Message) -> bool {
        let webrtc_state = self.webrtc_state.clone();
        log::info!("Calling update with {msg:?}");
        match msg {
            Msg::Connect => {
                let link = ctx.link().clone();
                let connection_status_callback = move |status: ConnectionStatusUpdate| {
                    link.send_message(Msg::UpdateConnectionStatus(status))
                };
                ctx.link().send_future(async move {
                    match net::start_websockets(connection_status_callback).await {
                        Ok(w) => {
                            webrtc_state.replace(Some(w));
                            Msg::Connected
                        }
                        Err(e) => {
                            log::warn!("Failed to connect: {e:?}");
                            Msg::FailedToConnect
                        }
                    }
                });
                false
            }
            Msg::Connected => false,
            Msg::ShareScreen => {
                ctx.link().send_future(async move {
                    if let Some(state) = webrtc_state.borrow_mut().as_mut() {
                        // FIXME: borrow_mut aborts if already borrowed
                        match state.share_screen().await {
                            Ok(streaming_key) => {
                                log::info!("Screen shared");
                                Msg::PublishedStream(streaming_key)
                            }
                            Err(e) => {
                                log::warn!("Screen share error: {e:?}");
                                Msg::ScreenShareFailed
                            }
                        }
                    } else {
                        todo!()
                    }
                });
                false
            }
            Msg::PublishedStream(streaming_key) => {
                self.publishing_stream_key = streaming_key.to_string();
                true
            }
            Msg::ScreenShareFailed => false,
            Msg::FailedToConnect => false,
            Msg::PlayStream(next_stream) => {
                self.playing_stream = next_stream.clone();
                ctx.link().send_future(async move {
                    if let Some(state) = webrtc_state.borrow_mut().as_mut() {
                        match state.play_stream(&next_stream).await {
                            Ok(_) => {
                                log::info!("Playing stream");
                                Msg::PlayingStream
                            }
                            Err(e) => {
                                log::warn!("Stream play error: {e:?}");
                                Msg::StreamPlayFailed
                            }
                        }
                    } else {
                        todo!()
                    }
                });
                true
            }
            Msg::PlayingStream => true,
            Msg::StreamPlayFailed => true,
            Msg::UpdateConnectionStatus(status) => {
                match status {
                    ConnectionStatusUpdate::SignalingState(status) => {
                        self.signaling_state = status;
                    }
                    ConnectionStatusUpdate::IceConnectionState(status) => {
                        self.ice_connection_state = status;
                    }
                    ConnectionStatusUpdate::IceGatheringState(status) => {
                        self.ice_gathering_state = status;
                    }
                };
                true
            }
        }
    }

    fn view(&self, ctx: &Context<Self>) -> Html {
        // This gives us a component's "`Scope`" which allows us to send messages, etc to the component.
        let link = ctx.link();
        html! {
            <main>
                <div class="readout">
                    <div>{"Signaling State: "} { &self.signaling_state }</div>
                    <div>{"ICE Connection State: "} { &self.ice_connection_state }</div>
                    <div>{"ICE Gathering State: "} { &self.ice_gathering_state }</div>
                    <button onclick={link.callback(|_| Msg::Connect)}>{ "Connect" }</button>
                </div>
                <div class="readout">
                    <div><h2>{"Publish"}</h2></div>
                    <div>
                        <span>{"publishing: "}</span>
                        <span>
                        {(if self.publishing_stream_key.is_empty() { "nothing" } else {&self.publishing_stream_key}).to_string()}
                        </span>
                    </div>
                    <button onclick={link.callback(|_| Msg::ShareScreen)}>{ "Share Screen & Publish" }</button>
                </div>
                <div class="entry">
                    <div><h2>{"Play"}</h2></div>
                    <div>
                        {"Enter a stream key to play:"}
                    </div>
                    <div>
                        <TextInput on_change={link.callback(Msg::PlayStream)} value={self.playing_stream.clone()} />
                    </div>
                </div>
                <div class="entry">
                    <div>
                        <video id="main_video_stream" autoplay=true></video>
                    </div>
                </div>
            </main>
        }
    }
}

fn main() {
    utils::set_panic_hook();
    console_log::init_with_level(log::Level::Info).expect("initialize log");
    yew::start_app::<Model>();
}
