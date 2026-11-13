//! The one Python application of this process, in the host's lifecycle.
//!
//! The interpreter is process-wide and has one environment, so `python.yaml`
//! names one application rather than a list. A second application runs in a
//! process of its own and comes over Orbit.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use claviron::runtime::congif::{self, ConfigFile, Configurable};
use claviron::runtime::{
    AppState, Bootable, Component, ConfigCheck, Finalizable, registry,
};
use serde::Deserialize;

use crate::asgi::{
    PythonApplication, PythonApplicationHandle, RunningPythonApplication,
};

/// Where a host's application is written down, in its configuration directory.
pub const CONFIG_FILE: &str = "python.yaml";

#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The application, as `module:attribute`.
    target: String,
    /// The directory the module is imported from.
    module_path: PathBuf,
    /// The Python environment. Unwritten, the installation selected at build
    /// time. Read at boot: an interpreter does not change its environment, so
    /// changing this takes a restart.
    venv: Option<PathBuf>,
}

pub(crate) type Handle = Arc<RwLock<Option<PythonApplicationHandle>>>;

pub(crate) struct PythonService {
    source: PathBuf,
    handle: Handle,
    running: Mutex<Option<RunningPythonApplication>>,
}

impl PythonService {
    pub(crate) fn handle(&self) -> Handle {
        Arc::clone(&self.handle)
    }
}

/// Bring up the application `python.yaml` names, in every process that serves.
/// With `edge`, also install the `python:` route factory on Edge.
pub fn register(state: &AppState) -> registry::Result<()> {
    state.registry_ref().register(Arc::new(PythonService {
        source: state.config_dir().join(CONFIG_FILE),
        handle: Arc::new(RwLock::new(None)),
        running: Mutex::new(None),
    }))?;
    #[cfg(feature = "edge")]
    if let Some(hosts) = state.registry_ref().resolve::<claviron::edge::HostAppRegistry>() {
        crate::backend::register(hosts.site())?;
    }
    Ok(())
}

#[async_trait]
impl Configurable for PythonService {
    type Config = Config;

    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.source)
    }

    async fn configure(
        &self,
        config: &Config,
    ) -> congif::Result<()> {
        PythonApplication::new(config.target.clone())
            .map_err(congif::Error::msg)?;
        if !config.module_path.is_dir() {
            return Err(congif::Error::msg(
                "Python module_path must be an existing directory",
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl Component<AppState> for PythonService {
    type Deps = ();

    fn name(&self) -> &'static str {
        "PythonService"
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }

    fn as_finalizable(&self) -> Option<&dyn Finalizable<AppState>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState, ()> for PythonService {
    async fn boot(
        &self,
        _: (),
        state: &AppState,
    ) -> registry::Result<()> {
        // A master serves nothing and a check runs nothing: neither needs an
        // interpreter or a copy of the application's memory.
        if state.is_master()
            || state.registry_ref().resolve::<ConfigCheck>().is_some()
        {
            return Ok(());
        }
        let config = state.config_reader().snapshot::<Self>()?;
        let mut application = PythonApplication::new(config.target.clone())
            .map_err(|error| registry::Error::msg(error.to_string()))?
            .module_path(&config.module_path);
        if let Some(venv) = &config.venv {
            application = application.virtual_environment(venv);
        }
        // Booting waits for ASGI lifespan startup; not on an async reactor.
        let running = tokio::task::spawn_blocking(move || application.boot())
            .await
            .map_err(|error| registry::Error::msg(error.to_string()))?
            .map_err(|error| registry::Error::msg(error.to_string()))?;
        *self.handle.write().expect("python handle lock") =
            Some(running.handle());
        *self.running.lock().expect("python owner lock") = Some(running);
        Ok(())
    }
}

#[async_trait]
impl Finalizable<AppState> for PythonService {
    async fn finalize(
        &self,
        _: &AppState,
    ) -> registry::Result<()> {
        *self.handle.write().expect("python handle lock") = None;
        let running = self.running.lock().expect("python owner lock").take();
        if let Some(running) = running {
            tokio::task::spawn_blocking(move || running.shutdown())
                .await
                .map_err(|error| registry::Error::msg(error.to_string()))?
                .map_err(|error| registry::Error::msg(error.to_string()))?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../tests/service.rs"]
mod tests;
