//! Native WebTransport sessions on an Edge's HTTP/3 listener.
//!
//! A `python:` route names the paths a session may open on. Each reliable
//! bidirectional stream of a session is one ASGI WebSocket connection of the
//! application, with `scope["transport"] == "webtransport"`. The stream carries
//! the records a stream relayed over Orbit carries, so an application and its
//! clients see one protocol on either door:
//!
//! ```text
//! u8 kind | u32 length (little endian) | payload
//! ```

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use claviron::edge::{
    AnyAuthenticated, AuthContext, H3WebTransportBiStream,
    H3WebTransportSession, Host, HostConfig, Routable, WebTransportAccept,
    WebTransportHandler, WebTransportRegistry, WebTransportReject,
    WebTransportResult,
};
use claviron::http::ext::PathPrefix;
use claviron::orbit::link::application::{MessageReader, write_message};
use claviron::runtime::AppState;
use claviron::runtime::registry::{self, Component, Error};
use http::StatusCode;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use crate::asgi::{
    MessageTransport, PythonApplicationHandle, WebSocketDecision,
    WebSocketInbound, WebSocketSession,
};
use crate::backend::declared;
use crate::service::Handle;
use crate::websocket::{DEFAULT_MESSAGE_LIMIT, Message};

/// Bind the WebTransport paths each host's `python:` scopes declare.
pub(crate) fn register(
    state: &AppState,
    handle: Handle,
) -> registry::Result<()> {
    claviron::edge::register_routable(state, Arc::new(Sessions { handle }))
}

struct Sessions {
    handle: Handle,
}

impl Component<AppState> for Sessions {
    type Deps = ();

    fn name(&self) -> &str {
        "PythonEdgeSessions"
    }
}

impl Routable for Sessions {
    fn match_host(
        &self,
        _: &Host,
        host: &HostConfig,
    ) -> bool {
        // A declaration that does not decode selects nothing here; the
        // backend's own preparation of the same scope reports it.
        endpoints(host).is_ok_and(|endpoints| !endpoints.is_empty())
    }

    fn prepare_sessions(
        &self,
        state: &AppState,
        key: &Host,
        host: &HostConfig,
    ) -> registry::Result<()> {
        let endpoints = endpoints(host).map_err(|error| {
            Error::msg(format!("python directive of host '{key}': {error}"))
        })?;
        if endpoints.is_empty() {
            return Ok(());
        }
        let registry = state
            .registry_ref()
            .resolve::<WebTransportRegistry>()
            .ok_or_else(|| {
            Error::msg("Python WebTransport requires the WebTransport registry")
        })?;
        for endpoint in endpoints {
            registry.bind_handler(
                key.clone(),
                endpoint.path.clone(),
                Arc::new(Handler {
                    handle: Arc::clone(&self.handle),
                    endpoint,
                }),
            )?;
        }
        Ok(())
    }
}

/// One WebTransport path as it was prepared. Admission compares against it
/// and decodes nothing.
struct Endpoint {
    path: Arc<str>,
    /// The directive the path was decoded from, as its scope declared it.
    source: Arc<serde_yaml::Value>,
    /// The mount of the scope that declared it.
    mount: Option<PathPrefix>,
}

impl Endpoint {
    /// Whether `host` still declares exactly what this endpoint was prepared
    /// from. A registry outlives a publication, so a binding can survive its
    /// route's removal or masking; such a binding must not admit.
    fn still_declared(
        &self,
        host: &claviron_pipeline::Site,
    ) -> bool {
        let selected =
            host.routes.find(&self.path).map(|route| &**route).unwrap_or(host);
        selected.directive("python") == Some(&*self.source)
            && selected.files.mapping.as_ref().map(|mapping| mapping.prefix())
                == self.mount.as_ref()
    }
}

/// Every WebTransport path the host's scopes declare, decoded once.
fn endpoints(host: &HostConfig) -> Result<Vec<Endpoint>, serde_yaml::Error> {
    let mut endpoints = Vec::new();
    for (_, scope) in host.scopes() {
        let Some(route) = declared(scope)? else { continue };
        if route.webtransport.is_empty() {
            continue;
        }
        let source =
            Arc::new(scope.directive("python").cloned().unwrap_or_default());
        let mount = scope
            .files
            .mapping
            .as_ref()
            .map(|mapping| mapping.prefix().clone());
        for path in &route.webtransport {
            endpoints.push(Endpoint {
                path: mounted(mount.as_ref(), path),
                source: Arc::clone(&source),
                mount: mount.clone(),
            });
        }
    }
    Ok(endpoints)
}

fn mounted(
    mount: Option<&PathPrefix>,
    path: &PathPrefix,
) -> Arc<str> {
    match mount.map(PathPrefix::as_str) {
        None | Some("/") => Arc::from(path.as_str()),
        Some(mount) if path.as_str() == "/" => Arc::from(mount),
        Some(mount) => Arc::from(format!("{mount}{}", path.as_str())),
    }
}

struct Handler {
    handle: Handle,
    endpoint: Endpoint,
}

/// A stream's ASGI connection, accepted by the application.
struct Opened {
    inbound: WebSocketInbound,
    outbound: mpsc::Receiver<Message>,
}

/// The first stream's connection, decided before the session's 200 so that a
/// refusal stays an HTTP status. Taken once.
struct First(Mutex<Option<Opened>>);

impl Handler {
    fn application(&self) -> Option<PythonApplicationHandle> {
        self.handle.read().expect("python handle lock").clone()
    }
}

/// Ask the application to accept one stream as a WebSocket connection.
async fn open(
    python: &PythonApplicationHandle,
    ctx: &AuthContext,
) -> Result<Opened, StatusCode> {
    let mut request = http::Request::new(());
    *request.method_mut() = http::Method::CONNECT;
    *request.uri_mut() = ctx.uri.clone();
    *request.version_mut() = http::Version::HTTP_3;
    *request.headers_mut() = ctx.headers.clone();
    request.extensions_mut().insert(Arc::clone(&ctx.conn));
    let (parts, ()) = request.into_parts();
    let scope = crate::routing::request_scope(&parts);
    let WebSocketSession { decision, inbound, outbound } = python
        .handle_message_session(scope, MessageTransport::WebTransport)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    match decision.await {
        Ok(WebSocketDecision::Accept { .. }) => {
            Ok(Opened { inbound, outbound })
        }
        // Refused, or the application ended first: ASGI's answer is 403.
        _ => {
            inbound.end();
            Err(StatusCode::FORBIDDEN)
        }
    }
}

#[async_trait]
impl WebTransportHandler for Handler {
    fn name(&self) -> &'static str {
        "python_webtransport"
    }

    async fn accept(
        &self,
        _: &AppState,
        ctx: &AuthContext,
    ) -> WebTransportResult {
        if ctx.admission.is_none() {
            return Err(WebTransportReject::new(
                StatusCode::SERVICE_UNAVAILABLE,
            ));
        }
        if *ctx.path != *self.endpoint.path
            || !self.endpoint.still_declared(&ctx.host_config)
        {
            return Err(WebTransportReject::new(StatusCode::NOT_FOUND));
        }
        let python = self.application().ok_or_else(|| {
            WebTransportReject::new(StatusCode::SERVICE_UNAVAILABLE)
        })?;
        let first =
            open(&python, ctx).await.map_err(WebTransportReject::new)?;
        Ok(WebTransportAccept::authenticated(First(Mutex::new(Some(first)))))
    }

    async fn handle(
        &self,
        state: AppState,
        ctx: AuthContext,
        accepted: Option<AnyAuthenticated>,
        session: H3WebTransportSession,
    ) -> registry::Result<()> {
        let mut first = accepted
            .ok_or_else(|| {
                Error::msg("Python WebTransport acceptance missing")
            })?
            .take::<First>()?
            .0
            .into_inner()
            .expect("first stream lock");
        let (stop, stopped) = watch::channel(false);
        let mut streams = JoinSet::new();
        let shutdown = async {
            tokio::select! {
                () = ctx.wait_graceful_shutdown() => {}
                () = state.on_shutdown() => {}
                _ = session.closed() => {}
            }
        };
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                biased;
                () = &mut shutdown => break,
                completed = streams.join_next(), if !streams.is_empty() => {
                    if let Some(Err(error)) = completed {
                        tracing::debug!(%error, "Python WebTransport stream ended");
                    }
                }
                stream = session.accept_bi(),
                    if streams.len() < session.incoming_capacity().get() =>
                {
                    let Some(stream) = stream else { break };
                    let opened = match first.take() {
                        Some(opened) => Ok(opened),
                        None => match self.application() {
                            Some(python) => open(&python, &ctx).await,
                            None => Err(StatusCode::SERVICE_UNAVAILABLE),
                        },
                    };
                    match opened {
                        Ok(opened) => {
                            streams.spawn(serve(stream, opened, stopped.clone()));
                        }
                        Err(status) => {
                            tracing::debug!(%status, "Python WebTransport stream refused");
                        }
                    }
                }
            }
        }
        // A connection accepted for a stream the peer never opened ends here.
        if let Some(opened) = first {
            opened.inbound.end();
        }
        let _ = stop.send(true);
        while let Some(result) = streams.join_next().await {
            if let Err(error) = result {
                tracing::debug!(%error, "Python WebTransport stream drain failed");
            }
        }
        Ok(())
    }
}

/// Carry one stream's records between the peer and the application until
/// either is done, or the session stops.
async fn serve(
    stream: H3WebTransportBiStream,
    opened: Opened,
    stopped: watch::Receiver<bool>,
) {
    let Opened { inbound, outbound } = opened;
    let (mut incoming, mut outgoing) = tokio::io::split(stream);
    relay(&mut incoming, &mut outgoing, &inbound, outbound, stopped).await;
}

async fn relay<R, W>(
    incoming: &mut R,
    outgoing: &mut W,
    inbound: &WebSocketInbound,
    mut outbound: mpsc::Receiver<Message>,
    mut stopped: watch::Receiver<bool>,
) where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let receive = async {
        let mut reader = MessageReader::for_application(DEFAULT_MESSAGE_LIMIT);
        loop {
            let message = tokio::select! {
                biased;
                _ = stopped.wait_for(|stop| *stop) => break,
                message = reader.read(incoming) => message,
            };
            let Ok(Some(message)) = message else { break };
            let close = matches!(message, Message::Close { .. });
            if !inbound.deliver(message).await || close {
                break;
            }
        }
        // The application hears the end of the client's side.
        inbound.end();
    };
    let send = async {
        while let Some(message) = outbound.recv().await {
            let close = matches!(message, Message::Close { .. });
            if write_message(outgoing, &message).await.is_err()
                || outgoing.flush().await.is_err()
                || close
            {
                break;
            }
        }
        let _ = outgoing.shutdown().await;
    };
    tokio::pin!(receive, send);
    tokio::select! {
        // The client finished sending: what the application still says goes out.
        () = &mut receive => send.await,
        // The application is done: nothing it would hear is left.
        () = &mut send => inbound.end(),
    }
}
