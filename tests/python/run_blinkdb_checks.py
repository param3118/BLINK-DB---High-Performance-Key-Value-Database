from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import secrets
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from clients.python.blinkdb_client import (  # noqa: E402
    HttpClient,
    RespClient,
)


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def run_unit_tests() -> bool:
    suite = unittest.defaultTestLoader.discover(
        str(ROOT / "tests" / "python"),
        pattern="test_*.py",
    )
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return result.wasSuccessful()


class LocalBlinkDB:
    def __init__(self, data_directory: Path, token: str, timeout: float) -> None:
        self.data_directory = data_directory
        self.token = token
        self.timeout = timeout
        self.resp_port = free_port()
        self.http_port = free_port()
        while self.http_port == self.resp_port:
            self.http_port = free_port()
        self.process: subprocess.Popen[str] | None = None
        self.stderr_path = data_directory.parent / "blinkdb-service.stderr.log"

    @property
    def http_url(self) -> str:
        return f"http://127.0.0.1:{self.http_port}"

    def start(self) -> None:
        self.data_directory.mkdir(parents=True, exist_ok=True)
        environment = os.environ.copy()
        environment["BLINKDB_AUTH_TOKEN"] = self.token
        environment["PYTHONPATH"] = os.pathsep.join(
            (str(ROOT / "src"), str(ROOT), environment.get("PYTHONPATH", ""))
        )
        error_log = self.stderr_path.open("w", encoding="utf-8")
        self.process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "blinkdbd.cli",
                "--data-dir",
                str(self.data_directory),
                "--bind",
                "127.0.0.1",
                "--resp-port",
                str(self.resp_port),
                "--http-port",
                str(self.http_port),
            ],
            cwd=ROOT,
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=error_log,
            text=True,
        )
        self._error_log = error_log
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(
                    "BlinkDB exited during startup:\n" + self.stderr_path.read_text("utf-8")
                )
            try:
                request = urllib.request.Request(self.http_url + "/healthz")
                with urllib.request.urlopen(request, timeout=0.5) as response:
                    if response.status == 200:
                        return
            except (OSError, urllib.error.URLError):
                time.sleep(0.05)
        self.stop()
        raise TimeoutError("BlinkDB did not become healthy before the startup timeout")

    def stop(self) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=self.timeout)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=self.timeout)
        if hasattr(self, "_error_log"):
            self._error_log.close()

    def __enter__(self) -> "LocalBlinkDB":
        self.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.stop()


def assert_http_rejected(url: str, token: str, expected_status: int) -> None:
    request = urllib.request.Request(
        url + "/readyz",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        urllib.request.urlopen(request, timeout=3)
    except urllib.error.HTTPError as error:
        if error.code != expected_status:
            raise AssertionError(
                f"Expected HTTP {expected_status}, got HTTP {error.code}"
            ) from error
        return
    raise AssertionError(f"Expected HTTP {expected_status}, but the request succeeded")


def exercise_real_service(server: LocalBlinkDB, timeout: float) -> None:
    print("\n[Live service] health, security, cache operations, APIs, and metrics")
    with urllib.request.urlopen(server.http_url + "/healthz", timeout=timeout) as response:
        if response.status != 200:
            raise AssertionError("Health endpoint did not return HTTP 200")

    assert_http_rejected(server.http_url, "incorrect-token", 401)

    try:
        RespClient("127.0.0.1", server.resp_port, "incorrect-token", timeout=timeout)
    except PermissionError:
        pass
    else:
        raise AssertionError("RESP accepted an incorrect authentication token")

    with RespClient(
        "127.0.0.1", server.resp_port, server.token, timeout=timeout
    ) as redis_like:
        if not redis_like.ping():
            raise AssertionError("RESP PING did not return PONG")
        redis_like.set(b"cache:binary", b"\x00\xffblink")
        if redis_like.get(b"cache:binary") != b"\x00\xffblink":
            raise AssertionError("RESP binary SET/GET did not preserve bytes")
        redis_like.set(b"cache:empty", b"")
        if redis_like.get(b"cache:empty") != b"":
            raise AssertionError("RESP empty values are not preserved")
        if redis_like.get(b"cache:missing") is not None:
            raise AssertionError("RESP missing key should return null")
        redis_like.set(b"cache:binary", b"overwritten")
        if redis_like.get(b"cache:binary") != b"overwritten":
            raise AssertionError("RESP SET did not overwrite an existing key")
        if not redis_like.delete(b"cache:binary"):
            raise AssertionError("RESP DEL did not report deleting an existing key")
        if redis_like.delete(b"cache:binary"):
            raise AssertionError("RESP DEL should return false for a missing key")
        redis_like.set(b"shared-key", b"dataset-one", dataset="project-one")
        redis_like.set(b"shared-key", b"dataset-two", dataset="project-two")
        if redis_like.get(b"shared-key", dataset="project-one") != b"dataset-one":
            raise AssertionError("Dataset-one value was not isolated")
        if redis_like.get(b"shared-key", dataset="project-two") != b"dataset-two":
            raise AssertionError("Dataset-two value was not isolated")
        large_value = (b"BlinkDB-cache-load-" * 8192)[:128 * 1024]
        redis_like.set(b"cache:large", large_value)
        if redis_like.get(b"cache:large") != large_value:
            raise AssertionError("Large/compressed cache value did not round-trip")
        redis_like.set(b"persist:for-restore", b"restore-me")

    http = HttpClient(server.http_url, server.token, timeout=timeout)
    http.set("http-project", b"\x00http-key", b"\x00http-value")
    if http.get("http-project", b"\x00http-key") != b"\x00http-value":
        raise AssertionError("HTTP binary key/value round-trip failed")
    http.set("http-project", b"empty", b"")
    if http.get("http-project", b"empty") != b"":
        raise AssertionError("HTTP empty value was not preserved")
    if not http.delete("http-project", b"\x00http-key"):
        raise AssertionError("HTTP DELETE did not report a deleted key")
    if http.get("http-project", b"\x00http-key") is not None:
        raise AssertionError("HTTP GET should report a deleted key as missing")

    http.transact(
        [
            {"op": "set", "dataset": "transaction-check", "key": b"one", "value": b"1"},
            {"op": "set", "dataset": "transaction-check", "key": b"two", "value": b"2"},
            {"op": "del", "dataset": "transaction-check", "key": b"one"},
        ]
    )
    with RespClient(
        "127.0.0.1", server.resp_port, server.token, timeout=timeout
    ) as redis_like:
        if redis_like.get(b"one", dataset="transaction-check") is not None:
            raise AssertionError("Transaction did not delete its requested key")
        if redis_like.get(b"two", dataset="transaction-check") != b"2":
            raise AssertionError("Transaction did not store its requested key")

    request = urllib.request.Request(
        server.http_url + "/readyz",
        headers={"Authorization": f"Bearer {server.token}"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        ready = json.loads(response.read())
    if ready.get("status") != "ready" or ready.get("integrity") != "ok":
        raise AssertionError("Authenticated readiness/integrity check failed")

    request = urllib.request.Request(
        server.http_url + "/metrics",
        headers={"Authorization": f"Bearer {server.token}"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        metrics = json.loads(response.read())
    if (
        metrics.get("requests_total", 0) <= 0
        or metrics.get("storage", {}).get("entries", 0) <= 0
    ):
        raise AssertionError("Metrics did not report real request/storage activity")
    print(
        "PASS: PING/AUTH/SET/GET/DEL, binary and empty values, overwrite, "
        "missing keys, datasets, large values, HTTP, transaction, readiness, metrics"
    )


def percentile(samples: list[float], fraction: float) -> float:
    ordered = sorted(samples)
    position = min(int((len(ordered) - 1) * fraction), len(ordered) - 1)
    return ordered[position]


def run_cache_load(
    server: LocalBlinkDB,
    *,
    requests: int,
    concurrency: int,
    value_size: int,
    timeout: float,
) -> None:
    print(
        f"\n[Cache load] {requests:,} SET+GET pairs, {concurrency} clients, "
        f"{value_size:,}-byte values"
    )
    next_worker = 0
    assignment_lock = threading.Lock()
    samples: list[float] = []
    sample_lock = threading.Lock()
    completed = 0
    failures: list[str] = []
    value_template = bytes((index * 31 + 17) % 256 for index in range(value_size))

    def worker() -> None:
        nonlocal next_worker, completed
        with assignment_lock:
            worker_id = next_worker
            next_worker += 1
        try:
            with RespClient(
                "127.0.0.1",
                server.resp_port,
                server.token,
                timeout=timeout,
            ) as client:
                if not client.ping():
                    raise AssertionError("Load-test connection failed PING")
                local_samples: list[float] = []
                for sequence in range(worker_id, requests, concurrency):
                    key = f"load:{worker_id}:{sequence}".encode("ascii")
                    value = value_template + f":{sequence}".encode("ascii")
                    started = time.perf_counter()
                    client.set(key, value)
                    actual = client.get(key)
                    elapsed = time.perf_counter() - started
                    if actual != value:
                        raise AssertionError(f"Cache value mismatch for {key!r}")
                    local_samples.append(elapsed)
                with sample_lock:
                    samples.extend(local_samples)
                    completed += len(local_samples)
        except Exception as error:
            with sample_lock:
                failures.append(f"{type(error).__name__}: {error}")

    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker) for _ in range(concurrency)]
        for future in concurrent.futures.as_completed(futures):
            future.result()
    elapsed = time.perf_counter() - started

    if failures:
        raise RuntimeError("Cache load worker failed: " + failures[0])
    if completed != requests:
        raise AssertionError(f"Completed {completed} of {requests} requested cache pairs")
    pairs_per_second = completed / elapsed
    operations_per_second = (completed * 2) / elapsed
    print(
        f"PASS: {completed:,}/{requests:,} cache pairs verified\n"
        f"  elapsed: {elapsed:.2f}s\n"
        f"  SET+GET pairs/sec: {pairs_per_second:,.1f}\n"
        f"  RESP operations/sec: {operations_per_second:,.1f}\n"
        f"  pair latency p50/p95/p99: "
        f"{percentile(samples, 0.50) * 1000:.2f}/"
        f"{percentile(samples, 0.95) * 1000:.2f}/"
        f"{percentile(samples, 0.99) * 1000:.2f} ms"
    )


def run_cli_checks(data_directory: Path, timeout: float) -> None:
    print("\n[Operations] integrity, online backup, restore, restored-value check")
    backup = data_directory.parent / "blinkdb-test-backup.sqlite3"
    restored_directory = data_directory.parent / "blinkdb-restored"
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        (str(ROOT / "src"), str(ROOT), environment.get("PYTHONPATH", ""))
    )

    def invoke(*arguments: str) -> str:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "blinkdbd.cli",
                "--data-dir",
                str(data_directory),
                *arguments,
            ],
            cwd=ROOT,
            env=environment,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"BlinkDB CLI {arguments!r} failed:\n{result.stdout}\n{result.stderr}"
            )
        return result.stdout

    invoke("--check")
    invoke("--backup", str(backup))
    restored_directory.mkdir(parents=True, exist_ok=True)
    restore = subprocess.run(
        [
            sys.executable,
            "-m",
            "blinkdbd.cli",
            "--data-dir",
            str(restored_directory),
            "--restore",
            str(backup),
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if restore.returncode != 0:
        raise RuntimeError(f"Restore failed:\n{restore.stdout}\n{restore.stderr}")
    subprocess.run(
        [
            sys.executable,
            "-m",
            "blinkdbd.cli",
            "--data-dir",
            str(restored_directory),
            "--check",
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=True,
    )

    sys.path.insert(0, str(ROOT / "src"))
    from blinkdbd.config import Settings
    from blinkdbd.metrics import Metrics
    from blinkdbd.storage import Storage

    settings = Settings(
        data_directory=restored_directory,
        bind="127.0.0.1",
        resp_port=1,
        http_port=2,
        auth_token="x" * 32,
        tls_certificate=None,
        tls_private_key=None,
        busy_timeout_ms=5000,
        memory_limit_bytes=64 * 1024 * 1024,
        max_database_size_bytes=0,
        max_dataset_bytes=255,
        max_key_bytes=1024 * 1024,
        max_value_bytes=64 * 1024 * 1024,
        max_request_bytes=70 * 1024 * 1024,
        compression=True,
    )
    storage = Storage(settings, Metrics())
    try:
        if storage.get("default", b"persist:for-restore") != b"restore-me":
            raise AssertionError("Backup/restore did not preserve the live service value")
    finally:
        storage.close()
    print("PASS: integrity checks, backup, restore, and persisted data verification")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run BlinkDB unit/integration checks, launch an isolated local server, "
            "and load-test Redis-like SET/GET cache operations."
        )
    )
    parser.add_argument(
        "--requests",
        type=int,
        default=10_000,
        help="Number of verified SET+GET pairs (default: 10000)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=20,
        help="Number of parallel RESP clients (default: 20)",
    )
    parser.add_argument(
        "--value-size",
        type=int,
        default=128,
        help="Base value size in bytes (default: 128)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="Startup, socket, and CLI timeout in seconds (default: 15)",
    )
    parser.add_argument(
        "--skip-unit",
        action="store_true",
        help="Skip the unittest suite; still run live-service and load checks",
    )
    parser.add_argument(
        "--skip-load",
        action="store_true",
        help="Run correctness/operations checks without the concurrent load phase",
    )
    args = parser.parse_args()
    if args.requests <= 0 or args.concurrency <= 0 or args.value_size < 0:
        parser.error("--requests and --concurrency must be positive; --value-size cannot be negative")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.concurrency > 256:
        parser.error("--concurrency cannot exceed the service's 256-connection limit")
    if args.concurrency > args.requests and not args.skip_load:
        parser.error("--concurrency must not exceed --requests")
    if args.value_size > 1024 * 1024:
        parser.error("--value-size is capped at 1 MiB for this test runner")
    return args


def main() -> int:
    args = parse_args()
    print("BlinkDB complete checks")
    print(f"Repository: {ROOT}")
    print("Target: isolated local loopback service; no external host is contacted")

    if not args.skip_unit:
        print("\n[Unit/integration suite] storage, RESP, HTTP, auth, migration, backup")
        if not run_unit_tests():
            return 1

    token = secrets.token_urlsafe(32)
    with tempfile.TemporaryDirectory(prefix="blinkdb-check-") as temporary:
        root = Path(temporary)
        data_directory = root / "data"
        server = LocalBlinkDB(data_directory, token, args.timeout)
        try:
            server.start()
            exercise_real_service(server, args.timeout)
            if not args.skip_load:
                run_cache_load(
                    server,
                    requests=args.requests,
                    concurrency=args.concurrency,
                    value_size=args.value_size,
                    timeout=args.timeout,
                )
        finally:
            server.stop()
        run_cli_checks(data_directory, args.timeout)

    print("\nALL BLINKDB CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
