# BlinkDB service guide

## Storage and durability

BlinkDB stores binary key/value entries in a SQLite database named
`blinkdb.sqlite3` inside the configured data directory. A key is unique within
its dataset; datasets have independent key spaces. The default dataset is
`default`. Empty values are valid and distinct from missing keys. Keys and
values are binary-safe.

The service enables SQLite WAL mode and `synchronous=FULL`, and stores values
using a shared length-checked `BLK2R` raw / `BLK2Z` Zstandard envelope. Values
are compressed only when doing so reduces stored size. Decompression is
bounded by the configured value limit. Opening a schema-version-1 database
atomically wraps its old raw values in the current envelope. This improves
storage use but does not provide encryption at rest.

An acknowledged write is committed through SQLite before the service sends
success. Actual machine/power-loss guarantees still depend on the operating
system, filesystem, storage device, and hardware honoring flush requests.
SQLite's page quota does not fully cap WAL growth, temporary storage, or
backup files.

## Install and start

Install Python 3.11+ and the project dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e .

$token = [Environment]::GetEnvironmentVariable("BLINKDB_AUTH_TOKEN", "User")
if (-not $token) {
    $token = python -c "import secrets; print(secrets.token_urlsafe(32))"
    [Environment]::SetEnvironmentVariable("BLINKDB_AUTH_TOKEN", $token, "User")
}
$env:BLINKDB_AUTH_TOKEN = $token
blinkdbd --data-dir .\blinkdb-data
```

The default bind address is `127.0.0.1`; default ports are RESP `6379` and
HTTP `8080`. The service rejects startup if the token is missing or shorter
than 32 UTF-8 bytes. Non-loopback binds additionally require both
`BLINKDB_TLS_CERT` and `BLINKDB_TLS_KEY`. TLS, when configured, is enabled on
both listeners. Protect the token and private key as secrets.

### Configuration

| Setting | Default | Meaning |
| --- | --- | --- |
| `BLINKDB_DATA_DIR` / `--data-dir` | `blinkdb-data` | SQLite data directory |
| `BLINKDB_BIND` / `--bind` | `127.0.0.1` | Listener bind address |
| `BLINKDB_RESP_PORT` / `--resp-port` | `6379` | RESP listener port |
| `BLINKDB_HTTP_PORT` / `--http-port` | `8080` | HTTP listener port |
| `BLINKDB_AUTH_TOKEN` | unset | Required bearer/RESP secret, at least 32 bytes |
| `BLINKDB_TLS_CERT`, `BLINKDB_TLS_KEY` | unset | TLS certificate and private-key paths |
| `BLINKDB_BUSY_TIMEOUT_MS` | `5000` | SQLite busy timeout |
| `BLINKDB_MEMORY_LIMIT_BYTES` | `67108864` | SQLite page-cache target |
| `BLINKDB_MAX_DATABASE_BYTES` | `0` | Main database page limit; zero disables it |
| `BLINKDB_MAX_DATASET_BYTES` | `255` | Maximum UTF-8 dataset-name size |
| `BLINKDB_MAX_KEY_BYTES` | `1048576` | Maximum key size |
| `BLINKDB_MAX_VALUE_BYTES` | `67108864` | Maximum decoded value size |
| `BLINKDB_MAX_REQUEST_BYTES` | `73400320` | Maximum protocol request size |
| `BLINKDB_COMPRESSION` | `true` | Set to `false` to disable Zstandard compression |

Ports must be distinct. Configure limits for the machine and workload; these
are guardrails, not a substitute for monitoring free disk space.

## RESP API

All commands use RESP arrays (bulk strings) for binary safety. Authenticate
once per connection:

```text
AUTH <token>
PING
SET <key> <value>
GET <key>
DEL <key>
SETX <dataset> <key> <value>
GETX <dataset> <key>
DELX <dataset> <key>
TXN SET <dataset> <key> <value> DEL <dataset> <key> ...
INFO
```

`GET`/`GETX` return `$-1` for a missing key, and `$0` plus an empty bulk value
for a stored empty value. `TXN` accepts up to 1000 mutations and commits them
atomically. It is not a general Redis-compatible server; commands outside
this list are unsupported.

These basic commands can be used as a small persistent cache, and are
compatible with simple RESP clients (including `redis-cli` for the listed
commands). Features such as Redis expiration/TTL, eviction policies, pub/sub,
Lua, and the rest of the Redis command set are not implemented.

## HTTP API

All endpoints except `GET /healthz` require
`Authorization: Bearer <token>`. Requests are bounded and connections close
after one request. Values and keys in JSON use base64; keys in the URL use
unpadded base64url. Dataset names in the URL must be URL-encoded.

| Method and path | Purpose |
| --- | --- |
| `GET /healthz` | Unauthenticated process liveness probe |
| `GET /readyz` | Authenticated SQLite integrity/readiness check |
| `GET /v1/health` | Alias for readiness |
| `GET /metrics` | Authenticated JSON counters, latency percentiles, and storage stats |
| `GET /v1/datasets/{dataset}/keys/{base64url-key}` | Read a value |
| `PUT /v1/datasets/{dataset}/keys/{base64url-key}` | Store JSON `{"value":"<base64>"}` |
| `DELETE /v1/datasets/{dataset}/keys/{base64url-key}` | Delete a key |
| `POST /v1/transactions` | Atomic JSON mutation batch |

Example value write:

```http
PUT /v1/datasets/project-a/keys/a2V5 HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <token>
Content-Type: application/json

{"value":"dmFsdWU="}
```

Transaction body:

```json
{
  "mutations": [
    {"op": "set", "dataset": "project-a", "key": "a2V5", "value": "dmFsdWU="},
    {"op": "del", "dataset": "project-a", "key": "b2xk"}
  ]
}
```

Each `key` and `value` is standard base64 in transaction JSON. A transaction
contains 1 to 1000 mutations.

## Python clients

The Python client is independently installable and has no BlinkDB server
dependencies. From the repository checkout:

```powershell
python -m pip install -e .\clients\python
```

Then use the short import:

```python
from blinkdb_client import HttpClient, RespClient

with RespClient("127.0.0.1", 6379, token) as db:
    db.set(b"key", b"value")
    value = db.get(b"key")

http = HttpClient("http://127.0.0.1:8080", token)
http.set("project-a", b"key", b"\x00binary")
value = http.get("project-a", b"key")
```

Pass `tls=True` to `RespClient` when the service has TLS enabled. `HttpClient`
accepts an optional `ssl.SSLContext` for TLS; its default TLS context validates
the server certificate.

For an application-specific default dataset, and to avoid repeating
connection details in code, set `BLINKDB_HOST`, `BLINKDB_AUTH_TOKEN`, and
optionally `BLINKDB_RESP_PORT` (default `6379`) and `BLINKDB_TLS` (`true` for
TLS). For a private/self-signed certificate, optionally set `BLINKDB_TLS_CA`
to the trusted CA or certificate file path. Then:

```python
from blinkdb_client import RespClient

with RespClient.from_env(dataset="inventory-app") as db:
    db.set(b"user:42:settings", b'{"theme":"dark"}')
    settings = db.get(b"user:42:settings")
```

Set those environment variables in your deployment's secret/configuration
settings; never commit the token to source control. For another computer, build
or install the standalone client package there through your normal trusted
package distribution method.

## Use BlinkDB from multiple projects

Run one BlinkDB service on the PC that owns the database. Every other project
connects to that service; they should not each open or create their own
database file. The `--data-dir` path is where BlinkDB keeps its SQLite file,
so keep it stable and outside a project checkout. The service must be running
and the host PC must be powered on for clients to use it.

### Same PC

Install and start BlinkDB once in PowerShell:

```powershell
Set-Location D:\projects\24CS60R65_deslab_project
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e .

$token = [Environment]::GetEnvironmentVariable("BLINKDB_AUTH_TOKEN", "User")
if (-not $token) {
    $token = python -c "import secrets; print(secrets.token_urlsafe(32))"
    [Environment]::SetEnvironmentVariable("BLINKDB_AUTH_TOKEN", $token, "User")
}
$env:BLINKDB_AUTH_TOKEN = $token
.\.venv\Scripts\python.exe -m blinkdbd.cli `
  --data-dir "$env:LOCALAPPDATA\BlinkDB\data" `
  --bind 127.0.0.1 --resp-port 6379 --http-port 8080
```

The commands above create a per-user token once and use it immediately to
start the server. Leave this terminal running. The database persists under
`%LOCALAPPDATA%\BlinkDB\data` across service restarts. Keep the same secret
available to the server and each client process; for a real deployment,
provision it from a protected secret store rather than committing it or
hardcoding it into source code.

Restart other terminals and applications after creating the token so they
inherit the user environment variable. Do not generate a different token for
each client; clients and server must use the same secret. This stores the
token in your Windows user environment, which is convenient for personal
development but is not a dedicated secret vault. On shared or production
machines, use a proper secret manager and a restricted service account.

In each separate Python project, install the standalone client into that
project's virtual environment (replace the path with the checkout location):

```powershell
python -m pip install -e D:\projects\24CS60R65_deslab_project\clients\python
```

This editable install requires the BlinkDB checkout to exist on the same
computer. The client can also be built as a wheel with
`python -m pip wheel --no-deps --wheel-dir dist .\clients\python` and
distributed to other computers through your own trusted package or deployment
process; this project does not currently publish a package to a package index.

Then use a dataset name for each project so their key spaces are separated:

```python
import json
import os

from blinkdb_client import RespClient

token = os.environ["BLINKDB_AUTH_TOKEN"]

with RespClient("127.0.0.1", 6379, token) as db:
    db.set(
        b"user:42:settings",
        json.dumps({"theme": "dark"}).encode("utf-8"),
        dataset="inventory-app",
    )

    raw = db.get(b"user:42:settings", dataset="inventory-app")
    settings = json.loads(raw) if raw is not None else None
```

Each `set` is stored in SQLite before the service confirms success, so the
value remains after the client closes or BlinkDB restarts. Use the same
dataset/key in a later run to retrieve it. `GET` returns `None` when absent;
it returns `b""` for a stored empty value. Store structured values by encoding
them (for example, JSON or MessagePack) in your application. This API stores
bytes, not Python objects or files.

Keep one `RespClient` connection per application worker/thread; do not share
one client instance concurrently between threads. For a short-lived script,
open it with `with` as shown above.

### Other PCs or locations

`127.0.0.1` always means “this computer.” A project running on another PC
cannot use that address to reach the database host.

For remote projects, keep BlinkDB on a reliable, always-on PC/server and use
its private-network DNS name or address from the client. The current service
defaults to loopback and rejects non-loopback binds unless TLS certificate and
private-key files are configured. Use a private VPN/network, restrict access
with a firewall to only trusted client devices, configure a valid TLS
certificate, and provision a strong token to each client securely. Do **not**
port-forward the RESP or HTTP ports to the public internet. The shared token
grants access to every dataset; dataset names separate keys but are not
per-project access controls.

Once the private DNS name and certificate are configured, client code uses
that name (which must match the TLS certificate) instead of `127.0.0.1`:

```python
with RespClient("blinkdb.your-private-network", 6379, token, tls=True) as db:
    db.set(b"job:latest", b"complete", dataset="reporting-app")
```

`HttpClient` can also connect over HTTPS when supplied a validating
`ssl.SSLContext`. Ensure the host firewall allows the configured ports only
from the private network. If the host sleeps, shuts down, loses network, or
BlinkDB exits, remote clients cannot connect until it is available again.
This project does not currently install itself as a Windows service or set up
automatic restart; configure process supervision/Windows Task Scheduler if
you need it to start and recover automatically.

### Persistent data and backups

Do not delete the data directory when uninstalling the Python package; it
contains the database. Back up the database regularly to a different disk or
protected storage location, and test restoring a backup. For example:

```powershell
.\.venv\Scripts\python.exe -m blinkdbd.cli `
  --data-dir "$env:LOCALAPPDATA\BlinkDB\data" `
  --backup "E:\BlinkDB-backups\blinkdb-2026-10-21.sqlite3"
```

The CLI refuses to overwrite an existing backup file. Choose a new backup
filename each time or move old backup files according to a retention plan.
Secure backups like the live database; they are not encrypted by BlinkDB.

## Full checks and load test

Run the complete local verification script from the repository root after
installing the package:

```powershell
python tests\python\run_blinkdb_checks.py
```

It runs the Python unit/integration suite, starts a temporary loopback-only
BlinkDB process with a generated test token, checks auth, protocols, cache
semantics, transactions, health, metrics, then runs a concurrent RESP
`SET`+`GET` workload and verifies every returned value. Finally it stops the
service and exercises integrity check, backup, restore, and data verification.
The default load is 10,000 SET/GET pairs with 20 connections and 128-byte
values. Example larger local load:

```powershell
python tests\python\run_blinkdb_checks.py --requests 100000 --concurrency 50 --value-size 1024
```

The runner uses an isolated temporary data directory and fixed loopback
address; it does not target another machine. Run larger loads only when the
device has enough free disk space. The reported throughput and latency
describe that machine and configuration, not a general performance promise.
The script cannot simulate physical power loss, fully validate TLS certificate
lifecycle, or replace native C++ build/CTest, security, and long-duration
stress testing.

## Docker image and GitHub Container Registry

The repository includes a Docker image, Compose configuration, and a GitHub
Actions workflow. The workflow runs the Python tests and checks the Docker
image build on pushes and pull requests. If checks pass, it publishes the
image to GHCR from the default branch and version tags such as `v0.4.0`; the
default branch also receives the `latest` tag. Pull requests do not publish an
image.

### Run locally with Docker Compose

Install Docker Desktop with Compose support. From the repository root, create
an `.env` file with a random token and prepare a TLS certificate. OpenSSL is
required for the example certificate command:

```powershell
Copy-Item .env.example .env
$token = python -c "import secrets; print(secrets.token_urlsafe(32))"
Set-Content -Path .env -Value "BLINKDB_AUTH_TOKEN=$token" -Encoding ascii
New-Item -ItemType Directory -Force certs | Out-Null
openssl req -x509 -nodes -newkey rsa:2048 `
  -keyout certs/server.key -out certs/server.crt -days 365 `
  -subj "/CN=localhost" `
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
docker compose up --build -d
```

This development certificate is self-signed. Configure a local client to trust
it and connect over TLS:

```powershell
$env:BLINKDB_HOST = "localhost"
$env:BLINKDB_AUTH_TOKEN = $token
$env:BLINKDB_TLS = "true"
$env:BLINKDB_TLS_CA = (Resolve-Path .\certs\server.crt).Path
```

Then, in a Python project with the standalone client installed:

```python
from blinkdb_client import RespClient

with RespClient.from_env(dataset="my-project") as db:
    db.set(b"hello", b"world")
    print(db.get(b"hello"))
```

Compose publishes ports only on the local PC (`127.0.0.1`) and stores data in
the persistent `blinkdb-data` volume. `restart: unless-stopped` restarts the
container after a crash or Docker restart. Do not run `docker compose down -v`
unless you intend to delete the database volume. For use from other machines,
use a certificate valid for the server's DNS name, expose ports only on a
trusted private network, and restrict them with the host firewall. Never
publish these database ports openly to the public internet.

### Use the image after pushing to GitHub

After the first successful workflow run, open the repository's **Packages**
page on GitHub and change the container package visibility to **Public** if
other projects should pull it without GitHub authentication. Replace `OWNER`
and `REPOSITORY` below with the lowercase GitHub owner and repository names:

```powershell
docker pull ghcr.io/OWNER/REPOSITORY:latest
$env:BLINKDB_IMAGE = "ghcr.io/OWNER/REPOSITORY:latest"
docker compose pull
docker compose up -d
```

For a stable deployment, use a versioned image tag such as `:v0.4.0` instead
of `:latest`. Keep the Compose file, authentication token, TLS certificate
and key, and persistent `/data` volume configured on the server. The included
Compose file binds host ports to loopback by default; change that deliberately
only when configuring remote access and firewall restrictions.

### Install the Python client from a public GitHub repository

The client package can be installed directly from the public repository. In
the consuming project's virtual environment, replace the placeholders:

```powershell
python -m pip install "blinkdb-client @ git+https://github.com/OWNER/REPOSITORY.git#subdirectory=clients/python"
```

This installs only the client package, not the server. For a private repository,
GitHub authentication is also required; do not put access tokens in the
install URL or commit them to project files.

## Backup, restore, and integrity check

Stop the service before restore. Backups are created using SQLite's online
backup API and will not overwrite an existing path:

```powershell
blinkdbd --data-dir .\blinkdb-data --backup .\backups\blinkdb.sqlite3
blinkdbd --data-dir .\blinkdb-data --check
blinkdbd --data-dir .\blinkdb-data --restore .\backups\blinkdb.sqlite3
```

Restore validates the source's SQLite integrity, BlinkDB application ID, and
schema version before replacing the active database contents. Keep backups
outside the live data directory, restrict their access, and periodically test
restores to a separate data directory.

## Current limitations

- Single node, single service process; multi-process write coordination is not
  a supported deployment mode.
- This is key/value storage, not a SQL server and not PostgreSQL-compatible.
- Authentication is a single shared bearer/RESP token; per-user identities,
  roles, rotation workflow, and audit logging are not implemented.
- Metrics are in-memory and reset on restart. They are JSON snapshots, not a
  Prometheus exposition endpoint.
- No automated backup scheduling, replication, automatic failover, or
  production service manager configuration is included.
- The optional C++ storage library is a storage-only embedding target; it
  does not implement the network service.
