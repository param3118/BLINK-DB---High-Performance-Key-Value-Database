# BlinkDB production-readiness status

## Summary

BlinkDB is a Windows-friendly, single-node, dataset-scoped key/value database
under development. The canonical service is in `src/blinkdbd/`; it uses SQLite
WAL storage, exposes RESP and JSON/HTTP listeners, and includes a CLI,
Python clients, backups, integrity checks, and basic metrics.

This work advances the project, but **does not make it production-certified**.
The Python unit/integration tests and a launched-service smoke test pass in
this environment. The C++ target has not been compiled or tested here because
CMake and a C++ compiler are unavailable. Do not expose the service to
untrusted networks or make it the only copy of important data until the
release checks below are complete.

## Implemented in this milestone

- **SQLite persistence:** WAL mode, `synchronous=FULL`, binary key/value
  storage, dataset isolation, explicit missing-versus-empty semantics,
  transaction batches, size/cache controls, database identity checks, and
  schema migration from v1 raw values to the v2 value envelope.
- **Compression:** shared C++/Python `BLK2R` raw and `BLK2Z` Zstandard
  length-checked envelopes. Compresses only when smaller and bounds
  decompression by the configured value limit. The older unframed v1 schema is
  migrated transactionally.
- **Network service:** asyncio TCP listeners for RESP and HTTP; bounded request
  parsing, per-connection timeouts, a 256-connection active limit,
  authentication, TLS configuration, graceful listener shutdown, and
  loopback defaults.
- **RESP:** authenticated `PING`, `SET`, `GET`, `DEL`, dataset variants,
  atomic `TXN`, and `INFO`; unsupported commands are rejected.
- **HTTP:** authenticated health/readiness/metrics and dataset key
  get/set/delete routes plus atomic transaction batches. JSON keys/values use
  base64; key paths use base64url.
- **Operations:** configurable data directory and SQLite cache/page limits;
  no-overwrite online backup, validated restore, and integrity-check CLI
  operations.
- **Clients:** Python RESP and HTTP helpers, including TLS support and
  binary-safe values. The native C++ storage library remains available.
- **Organization:** the README identifies the canonical service, client,
  tests, docs, and optional native library. Legacy coursework and presentation
  deliverables have been removed from this workspace.

## Validation evidence

- `python -m unittest discover -s tests\python -v`: **11 tests passed**,
  including binary/empty values, schema migration, compression, transactions,
  backup/restore, auth, malformed requests, clients, and RESP/HTTP behavior.
- Launched `blinkdbd` on temporary ports and verified `/healthz`, authenticated
  RESP and HTTP clients, and binary/empty values: **passed**.
- `python tests\python\run_blinkdb_checks.py`: **passed**, including the unit
  suite, live protocol/auth/cache checks, backup/restore verification, and
  10,000 concurrent-client SET/GET pairs. This run measured 714.4 pairs/sec
  (1,428.9 RESP commands/sec), with 27.31/35.78/44.82 ms p50/p95/p99 pair
  latency on the test machine. These are local sample results, not an SLO or
  capacity guarantee.
- `blinkdbd --help`: **passed**.
- Native C++ CMake/CTest: **not run**; CMake and C++ compilers are not present
  in this environment.
- No crash/power-loss simulation, long-duration soak test, multi-process
  contention test, sanitizer run, or Windows service-manager test has been
  performed.

## Remaining release blockers

1. **Native validation:** build the C++ storage library and tests on supported
   Windows toolchains with the configured SQLite and Zstandard packages. Run
   CTest and verify the v1-to-v2 migration and compressed values. Also build
   on any additional platforms BlinkDB intends to support.
2. **Fault and concurrency testing:** exercise process termination during
   writes, machine/power-loss behavior where practical, disk-full and WAL
   growth, corrupted databases/values, concurrent clients, and SQLite
   busy/locked conditions. Confirm acknowledged-write behavior on the actual
   filesystem and device.
3. **Security hardening:** add a threat model and test suite for TLS
   certificate rotation/expiry, secret provisioning, request abuse, bind
   configuration, and dependency updates. The service currently uses one
   shared token, no identities/roles, and has no encryption-at-rest feature.
4. **Operations:** define backup retention and off-device copies, restore
   drills, disk-space alerting, log rotation, upgrades/rollback, process
   supervision, and recovery objectives. Metrics are in-memory JSON snapshots,
   not persistent or Prometheus-formatted.
5. **Performance and quota evidence:** benchmark representative read/write
   workloads and compression tradeoffs. SQLite page quotas constrain the main
   database, not all WAL, temporary, backup, and filesystem usage; configure
   external disk monitoring and free-space alerts.
6. **Protocol scope:** document and maintain the explicit supported RESP/API
   contract. BlinkDB is not a PostgreSQL server and does not claim complete
   Redis compatibility.

## Operational boundaries

- Deploy as a single process against one data directory. Multiple service
  processes sharing a database are unsupported.
- Keep the default loopback binding unless remote access is explicitly
  required. For non-loopback addresses, configure TLS and a strong token, and
  restrict network access with a firewall.
- Stop the service before running restore. Keep backups outside the live data
  directory; test restores against a separate copy before relying on them.
- `synchronous=FULL` requests durable SQLite commits; the platform, filesystem,
  device firmware, and hardware ultimately determine whether flushes survive
  power failure.
- The old POSIX/epoll network server and presentation/coursework assets were
  removed from this cleaned service workspace; the Python asyncio service is
  the supported network-server entry point.

See [docs/SERVICE_GUIDE.md](./docs/SERVICE_GUIDE.md) for API, configuration,
backup, restore, and client details.
