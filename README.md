# claviron-asgi

Embeddable CPython and ASGI execution for Rust hosts. This package owns PyO3,
the interpreter boundary, ASGI lifespan and streaming HTTP/message execution.
It does not start a runner, HTTP listener or process supervisor.

The `gufi` runner uses this engine over Orbit. Hosts that own routing and
lifecycle can depend on the engine directly:

```toml
[dependencies]
claviron-asgi = "0.2.3"
```

## Ownership

`asgi::PythonApplication` declares the `module:attribute` target, module paths
and optional virtual environment. `boot()` starts the application-loop thread
and returns its `RunningPythonApplication` owner only after lifespan startup
completes. Its cloneable handle accepts streaming HTTP and message sessions.

The host retains the running owner while requests are admitted. It stops its
ingress and drains accepted work before calling `shutdown()`, which waits for
accepted exchanges, ASGI lifespan shutdown and the loop thread. Drop uses the
same shutdown path. Boot and shutdown may block; async hosts must run these
ownership transitions on their blocking executor. No implicit grace deadline
or forced Python-task cancellation is introduced.

`routing::PythonService` is a Tower-compatible HTTP adapter. Its shared handle
cell follows the host-managed application generation; an empty cell returns
503. `routing::request_scope` preserves Claviron's accepted connection and
trusted-proxy context when adapting message handshakes.

Initialization owns one process-wide CPython 3.15 interpreter and its selected
environment. This package provides no independent virtual-environment or
subinterpreter isolation inside a process. Application stop/restart runs ASGI
lifespan without finalizing CPython or evicting imported modules.

## In a host

With `edge` enabled, the registration call installs the Python application
and the `python:` route factory on Edge.

```rust
claviron_asgi::register(state)?;
```

Hosts that supply their own site registry can select `backend` and call
`claviron_asgi::backend::register(site)` alongside runtime registration.

```yaml
# python.yaml
target: app:app
module_path: apps/python
venv: .venv
```

```yaml
# a host in vhost.yaml
routes:
  /api:
    python: {}
```

`python.yaml` names one application, because a process has one interpreter and
one environment. A second application runs in its own process and joins over
Orbit, which is what the `gufi` runner does. An interpreter does not change its
environment, so a changed `python.yaml` takes a restart. Each worker boots its
own interpreter; a master boots none.

The path reaches the application as the client sent it. An HTTP/1.1 WebSocket
goes to the application when the host's transport allows `h1.upgrade`, and is
refused with 403 otherwise. WebSocket over HTTP/2 or HTTP/3 is not served from
a route yet.

With the `webtransport` feature, a route names the paths a WebTransport session
may open on, under its mount, and the host's transport allows `h3.webtransport`:

```yaml
routes:
  /:
    python:
      webtransport: [/ws/echo]
```

Each reliable bidirectional stream of a session is one ASGI WebSocket
connection, with `scope["transport"]` set to `"webtransport"`. The stream
carries one record per message, as a stream relayed over Orbit does:

```text
u8 kind (1 text, 2 binary, 8 close) | u32 length (little endian) | payload
```

The application decides the first stream before the session is answered, so a
refusal stays an HTTP status; a later stream it refuses is reset. Without the
feature, a route that names paths is refused when it is prepared.

## Native Python capabilities

The native Python namespace is `claviron`, including invocation and counters,
plus optional broadcast, logging and Django Channels capabilities. The
`broadcast`, `log` and `django` features select their native adapters; the host
supplies the matching services. They do not install a Python compatibility
package or change the ASGI protocol.

The module exists only inside the host process. Add this repository's `stubs/`
directory to your editor's stub search path; it holds type declarations only.

## Building

Select CPython 3.15 with `PYO3_PYTHON` or PyO3 configuration. This package owns
ABI validation and publishes the shared-library directory to a direct host's
build script as `DEP_CLAVIRON_ASGI_PYTHON_LIB_DIR`. The `gufi` runner forwards the
same directory under its existing `DEP_GUFI_PYTHON_LIB_DIR` key.

Existing engine tests live under `tests/`. The Python driver checks can also
run directly, without booting a Rust host:

```sh
python3.15 -B tests/python/test_asgi_driver.py
python3.15 -B tests/python/test_native_module.py
```
