from __future__ import annotations

import argparse
import ipaddress
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    data_directory: Path
    bind: str
    resp_port: int
    http_port: int
    auth_token: str
    tls_certificate: Path | None
    tls_private_key: Path | None
    busy_timeout_ms: int
    memory_limit_bytes: int
    max_database_size_bytes: int
    max_dataset_bytes: int
    max_key_bytes: int
    max_value_bytes: int
    max_request_bytes: int
    compression: bool

    @property
    def is_loopback(self) -> bool:
        try:
            return ipaddress.ip_address(self.bind).is_loopback
        except ValueError:
            return self.bind.casefold() == "localhost"


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    return default if value is None else int(value)


def settings_from_args() -> tuple[Settings, argparse.Namespace]:
    parser = argparse.ArgumentParser(prog="blinkdbd")
    parser.add_argument("--data-dir", default=os.environ.get("BLINKDB_DATA_DIR", "blinkdb-data"))
    parser.add_argument("--bind", default=os.environ.get("BLINKDB_BIND", "127.0.0.1"))
    parser.add_argument("--resp-port", type=int, default=_env_int("BLINKDB_RESP_PORT", 6379))
    parser.add_argument("--http-port", type=int, default=_env_int("BLINKDB_HTTP_PORT", 8080))
    operations = parser.add_mutually_exclusive_group()
    operations.add_argument("--backup")
    operations.add_argument("--restore")
    operations.add_argument("--check", action="store_true")
    operations.add_argument("--list-datasets", action="store_true")
    operations.add_argument("--list-keys", metavar="DATASET")
    operations.add_argument("--get", nargs=2, metavar=("DATASET", "KEY"))
    operations.add_argument("--get-base64", nargs=2, metavar=("DATASET", "KEY_BASE64"))
    parser.add_argument("--limit", type=int, default=100, help="Maximum keys to show")
    parser.add_argument("--offset", type=int, default=0, help="Key-list offset")
    args = parser.parse_args()

    settings = Settings(
        data_directory=Path(args.data_dir).expanduser().resolve(),
        bind=args.bind,
        resp_port=args.resp_port,
        http_port=args.http_port,
        auth_token=os.environ.get("BLINKDB_AUTH_TOKEN", ""),
        tls_certificate=(
            Path(os.environ["BLINKDB_TLS_CERT"]).expanduser().resolve()
            if os.environ.get("BLINKDB_TLS_CERT")
            else None
        ),
        tls_private_key=(
            Path(os.environ["BLINKDB_TLS_KEY"]).expanduser().resolve()
            if os.environ.get("BLINKDB_TLS_KEY")
            else None
        ),
        busy_timeout_ms=_env_int("BLINKDB_BUSY_TIMEOUT_MS", 5000),
        memory_limit_bytes=_env_int("BLINKDB_MEMORY_LIMIT_BYTES", 64 * 1024 * 1024),
        max_database_size_bytes=_env_int("BLINKDB_MAX_DATABASE_BYTES", 0),
        max_dataset_bytes=_env_int("BLINKDB_MAX_DATASET_BYTES", 255),
        max_key_bytes=_env_int("BLINKDB_MAX_KEY_BYTES", 1024 * 1024),
        max_value_bytes=_env_int("BLINKDB_MAX_VALUE_BYTES", 64 * 1024 * 1024),
        max_request_bytes=_env_int("BLINKDB_MAX_REQUEST_BYTES", 70 * 1024 * 1024),
        compression=os.environ.get("BLINKDB_COMPRESSION", "true").casefold()
        not in {"0", "false", "no", "off"},
    )
    return settings, args


def validate_settings(settings: Settings) -> None:
    if not 1 <= settings.resp_port <= 65535 or not 1 <= settings.http_port <= 65535:
        raise ValueError("RESP and HTTP ports must be between 1 and 65535")
    if settings.resp_port == settings.http_port:
        raise ValueError("RESP and HTTP must use different ports")
    if not settings.auth_token or len(settings.auth_token.encode("utf-8")) < 32:
        raise ValueError("Set BLINKDB_AUTH_TOKEN to a random secret of at least 32 bytes")
    if (settings.tls_certificate is None) != (settings.tls_private_key is None):
        raise ValueError("Set both BLINKDB_TLS_CERT and BLINKDB_TLS_KEY, or neither")
    if not settings.is_loopback and (
        settings.tls_certificate is None or settings.tls_private_key is None
    ):
        raise ValueError("Non-loopback binds require TLS certificate and private key")
    positive_limits = (
        settings.busy_timeout_ms,
        settings.memory_limit_bytes,
        settings.max_dataset_bytes,
        settings.max_key_bytes,
        settings.max_value_bytes,
        settings.max_request_bytes,
    )
    if any(value <= 0 for value in positive_limits):
        raise ValueError("Timeouts and dataset/key/value/request limits must be positive")
    if settings.max_database_size_bytes < 0:
        raise ValueError("Database size limit must not be negative")
