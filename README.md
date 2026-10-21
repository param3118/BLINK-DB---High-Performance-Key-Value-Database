# BlinkDB

BlinkDB is a single-node, dataset-scoped key/value database project. The
canonical service implementation is the Python package in `src/blinkdbd/`,
using SQLite WAL storage, a RESP TCP endpoint, and a small JSON/HTTP API. The
C++ SQLite storage library remains available for native embedding and shares
the same SQLite schema and versioned value compression envelope.

BlinkDB's goal is simple: set something, get it back in a blink. It can provide
a local cache through the familiar RESP `SET`, `GET`, `DEL`, and `PING` command
subset, while persisting values in SQLite. It is Redis-like for these basic
cache operations, but is not a drop-in Redis replacement or a full Redis
implementation.

> This is an active development project, not a production-certified database.
> Do not expose it to untrusted networks or use it as the only copy of
> important data until the release checks in
> [PRODUCTION_READINESS.md](./PRODUCTION_READINESS.md) have been completed.

## Project map

```text
src/blinkdbd/             Canonical Python service, storage, protocols, CLI
clients/python/           Python RESP and HTTP client helpers
tests/python/             Python storage and network integration tests
part-a/src/, part-a/tests/ Optional C++ SQLite storage library and native tests
docs/                     Service API, configuration, and operations guide
.archify/                 Architecture diagram source and generated viewer
PRODUCTION_READINESS.md   Risks, validation status, and release roadmap
CMakeLists.txt, vcpkg.json Native C++ build and dependencies
pyproject.toml            Python package and `blinkdbd` command
server                    Unidentified preserved binary; not used by BlinkDB
```

The new Python service is the supported network-server entry point. The
optional C++ storage library is kept as a native embedding/build target; it is
not a network server. Old coursework and presentation deliverables have been
removed from this cleaned service workspace. The diagram under `.archify/` is
a historical snapshot and predates the current Python service. The root
`server` binary was preserved as requested, but its purpose is unknown and the
service does not use it.

## Run the service on Windows

Requires Python 3.11 or newer. In PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e .

if (-not [Environment]::GetEnvironmentVariable("BLINKDB_AUTH_TOKEN", "User")) {
    $token = python -c "import secrets; print(secrets.token_urlsafe(32))"
    [Environment]::SetEnvironmentVariable("BLINKDB_AUTH_TOKEN", $token, "User")
}
$env:BLINKDB_AUTH_TOKEN = [Environment]::GetEnvironmentVariable("BLINKDB_AUTH_TOKEN", "User")
blinkdbd --data-dir "$env:LOCALAPPDATA\BlinkDB\data"
```

Create the token once; do not generate a new token each time the server starts.
New terminals should be opened after first setting the per-user environment
variable.

By default BlinkDB binds both endpoints to loopback only: RESP on `6379` and
HTTP on `8080`. Use the generated token with both clients. For remote binds,
BlinkDB requires TLS certificate/key environment variables as well as the
authentication token. See [docs/SERVICE_GUIDE.md](./docs/SERVICE_GUIDE.md)
before changing bind settings.

### Connect another project

Keep BlinkDB running on the computer that owns the database and give each
project its own dataset name. Install only the standalone client into another
Python project's virtual environment:

```powershell
python -m pip install -e D:\projects\24CS60R65_deslab_project\clients\python
```

Set `BLINKDB_HOST`, `BLINKDB_AUTH_TOKEN`, and optionally `BLINKDB_TLS=true`
in that project's environment, then:

```python
from blinkdb_client import RespClient

with RespClient.from_env(dataset="shop-api") as db:
    db.set(b"order:123", b'{"status":"saved"}')
    saved = db.get(b"order:123")
```

`RespClient.from_env()` also accepts `BLINKDB_RESP_PORT` (default `6379`).
Installing the standalone client does not install or run the BlinkDB server.

To inspect your stored data on the BlinkDB host, use
`blinkdbd --data-dir <data-folder> --list-datasets`, then
`--list-keys <dataset>` and `--get <dataset> <text-key>`. For Docker, run those
commands through `docker compose exec blinkdb blinkdbd --data-dir /data`.
See the service guide for pagination and binary-key examples.

For a project on another computer, set `BLINKDB_HOST` to the database host's
private-network name, enable TLS, and limit access to a trusted private
network. Do not expose the ports to the public internet. The host must stay
powered on and BlinkDB must remain running; see the
[multi-project and remote-use guide](./docs/SERVICE_GUIDE.md#use-blinkdb-from-multiple-projects).

### Docker image and GitHub publishing

The repository includes a Docker image and Compose configuration. For a local
test, configure an authentication token and provide a TLS certificate/key as
described in the
[Docker deployment guide](./docs/SERVICE_GUIDE.md#docker-image-and-github-container-registry).
Then run `docker compose up --build -d`. Compose keeps data in a named volume
and restarts the container if the process exits. Do not use `docker compose
down -v` unless you intend to delete that stored data.

On every push and pull request, GitHub Actions runs the Python tests and checks
that the image builds. On the default branch and version tags such as `v0.4.0`,
it also publishes the image to GitHub Container Registry (GHCR). The workflow
is under `.github/workflows/container.yml`. Make the GHCR package public in
GitHub package settings if other projects should pull it without GitHub
authentication.

## Test

After installing the package dependencies:

```powershell
python -m unittest discover -s tests\python -v
```

Run the complete local service checks and concurrent cache load test:

```powershell
python tests\python\run_blinkdb_checks.py
```

The runner executes unit/integration tests, starts an isolated loopback-only
BlinkDB instance, checks RESP/HTTP/auth/storage/backup/restore behavior, then
performs 10,000 verified `SET`+`GET` pairs with 20 parallel clients by default.
Tune the local workload with `--requests`, `--concurrency`, and `--value-size`;
for example:

```powershell
python tests\python\run_blinkdb_checks.py --requests 100000 --concurrency 50 --value-size 1024
```

The script reports successful pairs/sec, RESP operations/sec, and p50/p95/p99
pair latency. Benchmark results depend on the PC, filesystem, and chosen
settings; do not treat one run as a production capacity guarantee.

Build and run native C++ storage tests with CMake and vcpkg (SQLite and
Zstandard dependencies):

```powershell
cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE="$env:VCPKG_ROOT\scripts\buildsystems\vcpkg.cmake"
cmake --build build --config Release
ctest --test-dir build -C Release --output-on-failure
```

## Docs and diagrams

- [Service guide](./docs/SERVICE_GUIDE.md): configuration, protocol/API, clients,
  backups, restore, and operational limitations.
- [Production readiness roadmap](./PRODUCTION_READINESS.md): implementation
  status and remaining validation/release work.
- Architecture sources and interactive diagrams are under `.archify/`.
