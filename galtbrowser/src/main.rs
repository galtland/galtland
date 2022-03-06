// SPDX-License-Identifier: AGPL-3.0-only

mod net;
mod transport;
mod utils;


enum Msg {
    Connect,
    Connected,
    FailedToConnect,
    ShareScreen,
    ScreenShared,
    ScreenShareFailed,
}

#[derive(Default)]
struct Model {
    webrtc_state: Rc<RefCell<webrtc::state::WebRtcState>>,
}

use std::cell::RefCell;
use std::rc::Rc;

use net::webrtc;
use yew::prelude::*;

impl Component for Model {
    type Message = Msg;
    type Properties = ();

    fn create(_ctx: &Context<Self>) -> Self {
        Default::default()
    }

    fn update(&mut self, ctx: &Context<Self>, msg: Self::Message) -> bool {
        let webrtc_state = self.webrtc_state.clone();
        match msg {
            Msg::Connect => {
                ctx.link().send_future(async move {
                    match net::start_websockets().await {
                        Ok(w) => {
                            webrtc_state.replace(w);
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
                    match webrtc_state.borrow_mut().share_screen().await {
                        Ok(_) => {
                            log::info!("Screen shared");
                            Msg::ScreenShared
                        }
                        Err(e) => {
                            log::warn!("Screen share error: {e:?}");
                            Msg::ScreenShareFailed
                        }
                    }
                });
                false
            }
            Msg::ScreenShared => false,
            Msg::ScreenShareFailed => false,
            Msg::FailedToConnect => false,
        }
    }

    fn view(&self, ctx: &Context<Self>) -> Html {
        // This gives us a component's "`Scope`" which allows us to send messages, etc to the component.
        let link = ctx.link();
        html! {
            <div>
                <button onclick={link.callback(|_| Msg::Connect)}>{ "Connect" }</button>
                <button onclick={link.callback(|_| Msg::ShareScreen)}>{ "Share Screen" }</button>
            </div>
        }
    }
}

fn main() {
    utils::set_panic_hook();
    console_log::init_with_level(log::Level::Info).expect("initialize log");

    yew::start_app::<Model>();
}
