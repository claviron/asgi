//! `python:` on a site's route: this process's application behind that scope.
//!
//! ```yaml
//! routes:
//!   /api:
//!     python: {}
//! ```
//!
//! The application is the one `python.yaml` names, so a route selects it and
//! says nothing else. The path reaches it as the client sent it.

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

/// Nothing to say yet: a process has one application.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Route {}

fn prepare(
    context: &BackendContext<'_>,
    _: Route,
) -> claviron::runtime::registry::Result<Vec<Arc<dyn BackendHandler>>> {
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
