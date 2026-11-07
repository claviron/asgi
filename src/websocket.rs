//! A message session, whichever door it came through.
//!
//! The protocol is the transport's (`claviron_transport::websocket`): on a
//! connection this process holds it answers the handshake, joins fragments and
//! answers pings; over Orbit the edge has done that already. Either way whole
//! [`Message`]s come out. What is Python's is the decision — the ASGI application
//! accepts or refuses — and where the messages go.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

use bytes::Bytes;
use claviron_transport::owner::AppRequest;
pub use claviron_transport::websocket::{DEFAULT_MESSAGE_LIMIT, Message};
use claviron_transport::websocket::{Deliver, SocketKind, accept, attempt};
use http::{Request, StatusCode};

use crate::asgi::{
    MessageTransport, PythonApplicationHandle, WebSocketDecision,
    WebSocketInbound, WebSocketSession,
};
use crate::routing::{AppResponse, full_body};

struct Inbound(WebSocketInbound);

impl Deliver for Inbound {
    fn deliver(
        &self,
        message: Message,
    ) -> impl Future<Output = bool> + Send {
        self.0.deliver(message)
    }

    fn end(&self) {
        self.0.end();
    }
}

/// Ends the application's `receive` if the attempt goes away before it decided,
/// so a dropped request does not leave it waiting.
struct Undecided(Option<WebSocketInbound>);

impl Drop for Undecided {
    fn drop(&mut self) {
        if let Some(inbound) = self.0.take() {
            inbound.end();
        }
    }
}

/// The owner's answer to a request attempting to open a session: a WebSocket, or
/// the reliable stream of a WebTransport session an edge carries, goes to the
/// application. Anything else is not one this serves.
#[derive(Clone)]
pub struct Upgrades {
    python: Arc<RwLock<Option<PythonApplicationHandle>>>,
    message_limit: usize,
}

impl Upgrades {
    pub fn new(python: Arc<RwLock<Option<PythonApplicationHandle>>>) -> Self {
        Self { python, message_limit: DEFAULT_MESSAGE_LIMIT }
    }

    /// Bound a text or binary message either way.
    pub fn message_limit(
        mut self,
        limit: usize,
    ) -> Self {
        self.message_limit = limit;
        self
    }
}

impl tower_service::Service<AppRequest> for Upgrades {
    type Response = AppResponse;
    type Error = Infallible;
    type Future =
        Pin<Box<dyn Future<Output = Result<AppResponse, Infallible>> + Send>>;

    fn poll_ready(
        &mut self,
        _: &mut Context<'_>,
    ) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(
        &mut self,
        request: AppRequest,
    ) -> Self::Future {
        let python = self.python.read().expect("python handle lock").clone();
        let limit = self.message_limit;
        Box::pin(async move {
            let transport = match attempt(&request).map(|offer| offer.kind) {
                Some(SocketKind::WebSocket) => MessageTransport::WebSocket,
                Some(SocketKind::WebTransport) => MessageTransport::WebTransport,
                _ => return Ok(status(StatusCode::NOT_IMPLEMENTED)),
            };
            let Some(python) = python else {
                return Ok(status(StatusCode::SERVICE_UNAVAILABLE));
            };
            let (parts, body) = request.into_parts();
            let mut scope = crate::routing::request_scope(&parts);
            if transport == MessageTransport::WebSocket {
                // The handshake is HTTP/1.1 whatever the connection negotiated.
                scope.http_version = "1.1".to_owned();
            }
            let Ok(WebSocketSession { decision, inbound, outbound }) =
                python.handle_message_session(scope, transport)
            else {
                return Ok(status(StatusCode::SERVICE_UNAVAILABLE));
            };
            let mut undecided = Undecided(Some(inbound));
            let headers = match decision.await {
                Ok(WebSocketDecision::Accept { headers }) => headers,
                // Refused, or the application ended first: ASGI's answer is 403.
                _ => return Ok(status(StatusCode::FORBIDDEN)),
            };
            let inbound = undecided.0.take().expect("taken once");
            Ok(accept(
                Request::from_parts(parts, body),
                headers,
                Inbound(inbound),
                outbound,
                limit,
            ))
        })
    }
}

fn status(status: StatusCode) -> AppResponse {
    let mut response = http::Response::new(full_body(Bytes::new()));
    *response.status_mut() = status;
    response
}
