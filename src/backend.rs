//! `python:` on a site's route: this process's application behind that scope.
//!
//! ```yaml
//! routes:
//!   /api:
//!     python: {}
//! ```
//!
//! The application is the one `python.yaml` names, so a route selects it. The
//! path reaches it as the client sent it. With `webtransport`, a route also
//! names the paths a native WebTransport session may open on, under its mount:
//!
//! ```yaml
//! routes:
//!   /:
//!     python:
//!       webtransport: [/ws/echo]
//! ```

use std::sync::Arc;

use claviron::http::{RouteBody, ServiceResult, err_status};
use claviron_pipeline::factory::{BackendContext, BackendPlacement, factory};
use claviron_pipeline::{
    BackendContract, BackendHandler, BackendIdentity, BackendReply,
    DispatchCall, DispatchStep, HostContext, SiteRegistry,
};
use claviron_transport::owner::RequestBody;
use http::{Request, StatusCode};
use serde::Deserialize;
use tower_service::Service;

use crate::routing::PythonService;
use crate::service::PythonService as Application;
use crate::websocket::Upgrades;

/// Let this site registry prepare `python:` routes. An Edge and any other host
/// that compiles a site supply one; [`register`](crate::register) brings up
/// the application they reach.
pub fn register(
    registry: &SiteRegistry
) -> claviron::runtime::registry::Result<()> {
    registry.register_backend(factory(
        "python",
        BackendPlacement::AfterFiles,
        prepare,
    ))
}

/// A process has one application, so a route names no application.
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Route {
    /// Paths a native WebTransport session may open on, under the scope's
    /// mount. Each needs the `webtransport` feature to be bound.
    #[serde(default)]
    pub(crate) webtransport: Vec<claviron::http::ext::PathPrefix>,
}

/// A scope's `python:` declaration, decoded. `None` when it declares none.
#[cfg(feature = "webtransport")]
pub(crate) fn declared(
    scope: &claviron_pipeline::Site
) -> Result<Option<Route>, serde_yaml::Error> {
    match scope.directive("python") {
        None => Ok(None),
        Some(serde_yaml::Value::Null) => Ok(Some(Route::default())),
        Some(value) => serde_yaml::from_value(value.clone()).map(Some),
    }
}

fn prepare(
    context: &BackendContext<'_>,
    route: Route,
) -> claviron::runtime::registry::Result<Vec<Arc<dyn BackendHandler>>> {
    // Declared paths without the sessions that serve them would be ignored.
    if !cfg!(feature = "webtransport") && !route.webtransport.is_empty() {
        return Err(claviron::runtime::registry::Error::msg(format!(
            "backend 'python' at '{}': webtransport requires the webtransport feature",
            context.prefix
        )));
    }
    let application = context
        .state
        .registry_ref()
        .resolve::<Application>()
        .ok_or_else(|| {
            claviron::runtime::registry::Error::msg(format!(
                "backend 'python' at '{}': the application is not registered",
                context.prefix
            ))
        })?;
    let handle = application.handle();
    Ok(vec![Arc::new(PythonBackend {
        http: PythonService::new(Arc::clone(&handle)),
        upgrades: Upgrades::new(handle),
        h1_upgrade: context.transport.h1.upgrade,
    })])
}

struct PythonBackend {
    http: PythonService,
    upgrades: Upgrades,
    /// The transport's `h1.upgrade`: what the connection's terms allow.
    h1_upgrade: bool,
}

impl BackendHandler for PythonBackend {
    fn contract(&self) -> BackendContract {
        BackendContract {
            execution: claviron::ExecutorSpec::Dedicated(
                claviron::ExecutorKind::CurrentThread,
            ),
            // The ASGI scope reports the accepted connection.
            identity: BackendIdentity::Request,
            reply: BackendReply::Owner,
        }
    }
}

impl DispatchStep for PythonBackend {
    fn call(
        &self,
        request: Request<RouteBody>,
        _: Option<HostContext<'_>>,
    ) -> DispatchCall {
        if let Some(offer) =
            request.extensions().get::<claviron::http::socket::Offer>()
        {
            // A native WebTransport session asks the plan only whether it may
            // reach this scope; the session handler bound to its path answers
            // its streams, so the application is not asked here.
            if offer.info().kind
                == claviron::http::socket::SocketKind::WebTransport
                && offer.info().carrier == claviron::http::socket::Carrier::H3
            {
                return DispatchCall::ready(ServiceResult::Handled(
                    match offer.claim() {
                        Some(claim) => claim.reach(),
                        None => err_status(StatusCode::FORBIDDEN),
                    },
                ));
            }
            if offer.info().carrier == claviron::http::socket::Carrier::H1
                && !self.h1_upgrade
            {
                tracing::debug!(
                    path = request.uri().path(),
                    "a WebSocket upgrade was refused: this transport has no h1.upgrade"
                );
                return DispatchCall::ready(ServiceResult::Handled(
                    err_status(StatusCode::FORBIDDEN),
                ));
            }
            let mut upgrades = self.upgrades.clone();
            let request = request.map(RequestBody::new);
            return DispatchCall::response(Box::pin(async move {
                Ok(match upgrades.call(request).await {
                    Ok(response) => response,
                    Err(never) => match never {},
                })
            }));
        }
        let http = self.http.clone();
        DispatchCall::response(Box::pin(async move {
            Ok(http.handle(request).await)
        }))
    }
}
