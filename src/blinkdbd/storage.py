from __future__ import annotations

import sqlite3
import threading
import time
from contextlib import closing
from pathlib import Path

from .compression import decode, encode
from .config import Settings
from .metrics import Metrics

APPLICATION_ID = 0x424C4442
SCHEMA_VERSION = 2


class StorageError(RuntimeError):
    pass


class Storage:
    def __init__(self, settings: Settings, metrics: Metrics) -> None:
        self.settings = settings
        self.metrics = metrics
        settings.data_directory.mkdir(parents=True, exist_ok=True)
        self.path = settings.data_directory / "blinkdb.sqlite3"
        self._lock = threading.RLock()
        self._db = sqlite3.connect(
            self.path,
            timeout=settings.busy_timeout_ms / 1000,
            isolation_level=None,
            check_same_thread=False,
        )
        try:
            self._db.execute(f"PRAGMA busy_timeout={settings.busy_timeout_ms}")
            self._initialize()
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=FULL")
            self._db.execute("PRAGMA foreign_keys=ON")
            self._db.execute("PRAGMA mmap_size=0")
            self._db.execute(
                f"PRAGMA cache_size=-{max(1, settings.memory_limit_bytes // 1024)}"
            )
            if settings.max_database_size_bytes:
                page_size = self._db.execute("PRAGMA page_size").fetchone()[0]
                pages = settings.max_database_size_bytes // page_size
                if pages < 1:
                    raise ValueError("Database quota must allow at least one SQLite page")
                self._db.execute(f"PRAGMA max_page_count={pages}")
                actual_pages = self._db.execute("PRAGMA max_page_count").fetchone()[0]
                if actual_pages > pages:
                    raise ValueError("Existing database exceeds configured database quota")
            self.check_integrity()
            self.metrics.record_recovery()
        except Exception:
            self._db.close()
            raise

    def _initialize(self) -> None:
        version = self._db.execute("PRAGMA user_version").fetchone()[0]
        application_id = self._db.execute("PRAGMA application_id").fetchone()[0]
        if version not in (0, 1, SCHEMA_VERSION):
            raise StorageError(f"Unsupported BlinkDB schema version: {version}")
        if application_id not in (0, APPLICATION_ID):
            raise StorageError("SQLite file belongs to another application")
        if version and application_id != APPLICATION_ID:
            raise StorageError("BlinkDB database is missing its application identifier")

        if version == 0:
            user_tables = self._db.execute(
                "SELECT COUNT(*) FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchone()[0]
            if user_tables:
                raise StorageError("Refusing to initialize a non-empty SQLite database")
            try:
                self._db.execute("BEGIN IMMEDIATE")
                self._db.execute(f"PRAGMA application_id={APPLICATION_ID}")
                self._db.execute(
                    "CREATE TABLE kv_entries("
                    "dataset TEXT NOT NULL,"
                    "key BLOB NOT NULL,"
                    "value BLOB NOT NULL,"
                    "PRIMARY KEY(dataset, key)"
                    ") WITHOUT ROWID"
                )
                self._db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                self._db.execute("COMMIT")
            except Exception:
                if self._db.in_transaction:
                    self._db.execute("ROLLBACK")
                raise
        else:
            self._db.execute("SELECT dataset, key, value FROM kv_entries LIMIT 0").fetchall()
            if version == 1:
                self._db.execute("BEGIN IMMEDIATE")
                try:
                    self._db.execute(
                        "UPDATE kv_entries SET value=CAST("
                        "CAST('BLK2R:' || length(value) || ':' AS BLOB) || value AS BLOB)"
                    )
                    self._db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                    self._db.execute("COMMIT")
                except Exception:
                    if self._db.in_transaction:
                        self._db.execute("ROLLBACK")
                    raise

    def _validate(self, dataset: str, key: bytes, value: bytes | None = None) -> None:
        if not isinstance(dataset, str) or not isinstance(key, bytes):
            raise ValueError("Dataset must be text and key must be bytes")
        if value is not None and not isinstance(value, bytes):
            raise ValueError("Value must be bytes")
        dataset_bytes = dataset.encode("utf-8")
        if not dataset or len(dataset_bytes) > self.settings.max_dataset_bytes:
            raise ValueError("Dataset must be non-empty and within configured byte limit")
        if len(key) > self.settings.max_key_bytes:
            raise ValueError("Key exceeds configured byte limit")
        if value is not None and len(value) > self.settings.max_value_bytes:
            raise ValueError("Value exceeds configured byte limit")

    def _timed(self, operation: str, callback):
        start = time.perf_counter()
        failed = False
        try:
            return callback()
        except Exception:
            failed = True
            raise
        finally:
            self.metrics.record(
                operation,
                time.perf_counter() - start,
                failed=failed,
                request=False,
            )

    def set(self, dataset: str, key: bytes, value: bytes) -> None:
        self._validate(dataset, key, value)

        def write() -> None:
            stored = encode(value, enabled=self.settings.compression)
            with self._lock:
                self._db.execute(
                    "INSERT INTO kv_entries(dataset,key,value) VALUES(?,?,?) "
                    "ON CONFLICT(dataset,key) DO UPDATE SET value=excluded.value",
                    (dataset, sqlite3.Binary(key), sqlite3.Binary(stored)),
                )

        self._timed("set", write)

    def get(self, dataset: str, key: bytes) -> bytes | None:
        self._validate(dataset, key)

        def read() -> bytes | None:
            with self._lock:
                row = self._db.execute(
                    "SELECT value FROM kv_entries WHERE dataset=? AND key=?",
                    (dataset, sqlite3.Binary(key)),
                ).fetchone()
            if row is None:
                return None
            return decode(bytes(row[0]), max_output_size=self.settings.max_value_bytes)

        return self._timed("get", read)

    def delete(self, dataset: str, key: bytes) -> bool:
        self._validate(dataset, key)

        def remove() -> bool:
            with self._lock:
                cursor = self._db.execute(
                    "DELETE FROM kv_entries WHERE dataset=? AND key=?",
                    (dataset, sqlite3.Binary(key)),
                )
                return cursor.rowcount > 0

        return self._timed("delete", remove)

    def list_datasets(self) -> list[str]:
        def read() -> list[str]:
            with self._lock:
                rows = self._db.execute(
                    "SELECT DISTINCT dataset FROM kv_entries ORDER BY dataset"
                ).fetchall()
            return [row[0] for row in rows]

        return self._timed("list_datasets", read)

    def list_keys(self, dataset: str, *, limit: int = 100, offset: int = 0) -> list[bytes]:
        self._validate(dataset, b"")
        if not 1 <= limit <= 1000:
            raise ValueError("Key-list limit must be between 1 and 1000")
        if offset < 0:
            raise ValueError("Key-list offset must not be negative")

        def read() -> list[bytes]:
            with self._lock:
                rows = self._db.execute(
                    "SELECT key FROM kv_entries WHERE dataset=? ORDER BY key LIMIT ? OFFSET ?",
                    (dataset, limit, offset),
                ).fetchall()
            return [bytes(row[0]) for row in rows]

        return self._timed("list_keys", read)

    def transact(self, mutations: list[dict[str, object]]) -> None:
        normalized: list[tuple[str, str, bytes, bytes | None]] = []
        for mutation in mutations:
            operation = mutation.get("op")
            dataset = mutation.get("dataset", "default")
            key = mutation.get("key")
            value = mutation.get("value")
            if (
                not isinstance(operation, str)
                or operation not in {"set", "del"}
                or not isinstance(dataset, str)
            ):
                raise ValueError("Each transaction mutation needs a valid op and dataset")
            if not isinstance(key, bytes) or (operation == "set" and not isinstance(value, bytes)):
                raise ValueError("Transaction keys and values must be bytes")
            self._validate(dataset, key, value if isinstance(value, bytes) else None)
            normalized.append((operation, dataset, key, value if isinstance(value, bytes) else None))

        def apply() -> None:
            with self._lock:
                self._db.execute("BEGIN IMMEDIATE")
                try:
                    for operation, dataset, key, value in normalized:
                        if operation == "set":
                            stored = encode(value or b"", enabled=self.settings.compression)
                            self._db.execute(
                                "INSERT INTO kv_entries(dataset,key,value) VALUES(?,?,?) "
                                "ON CONFLICT(dataset,key) DO UPDATE SET value=excluded.value",
                                (dataset, sqlite3.Binary(key), sqlite3.Binary(stored)),
                            )
                        else:
                            self._db.execute(
                                "DELETE FROM kv_entries WHERE dataset=? AND key=?",
                                (dataset, sqlite3.Binary(key)),
                            )
                    self._db.execute("COMMIT")
                except Exception:
                    if self._db.in_transaction:
                        self._db.execute("ROLLBACK")
                    raise

        self._timed("transaction", apply)

    def backup(self, destination: Path) -> None:
        destination = destination.expanduser().resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists() or destination == self.path.resolve():
            raise ValueError("Backup destination already exists or is the active database")
        temporary = destination.with_name(destination.name + f".tmp-{time.time_ns()}")
        try:
            with self._lock:
                target = sqlite3.connect(temporary)
                try:
                    self._db.backup(target)
                    result = target.execute("PRAGMA integrity_check").fetchone()[0]
                finally:
                    target.close()
                if result != "ok":
                    raise StorageError(f"Backup integrity check failed: {result}")
            temporary.replace(destination)
        finally:
            if temporary.exists():
                temporary.unlink()

    def restore(self, source: Path) -> None:
        source = source.expanduser().resolve()
        if not source.is_file() or source == self.path.resolve():
            raise ValueError("Restore source must be a different existing database file")
        with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)) as candidate:
            app_id = candidate.execute("PRAGMA application_id").fetchone()[0]
            version = candidate.execute("PRAGMA user_version").fetchone()[0]
            result = candidate.execute("PRAGMA integrity_check").fetchone()[0]
            if (app_id, version, result) != (APPLICATION_ID, SCHEMA_VERSION, "ok"):
                raise StorageError("Restore source failed BlinkDB identity or integrity checks")
        with self._lock:
            candidate = sqlite3.connect(source)
            try:
                candidate.backup(self._db)
            finally:
                candidate.close()
        self.check_integrity()

    def check_integrity(self) -> str:
        with self._lock:
            result = self._db.execute("PRAGMA integrity_check").fetchone()[0]
        if result != "ok":
            raise StorageError(f"SQLite integrity check failed: {result}")
        return result

    def stats(self) -> dict[str, int]:
        with self._lock:
            entries, value_bytes = self._db.execute(
                "SELECT COUNT(*), COALESCE(SUM(length(value)),0) FROM kv_entries"
            ).fetchone()
            page_count = self._db.execute("PRAGMA page_count").fetchone()[0]
            page_size = self._db.execute("PRAGMA page_size").fetchone()[0]
        disk_bytes = sum(
            path.stat().st_size
            for path in (
                self.path,
                Path(str(self.path) + "-wal"),
                Path(str(self.path) + "-shm"),
            )
            if path.exists()
        )
        return {
            "entries": entries,
            "stored_value_bytes": value_bytes,
            "database_bytes": disk_bytes,
            "allocated_database_bytes": page_count * page_size,
            "memory_limit_bytes": self.settings.memory_limit_bytes,
            "database_limit_bytes": self.settings.max_database_size_bytes,
        }

    def close(self) -> None:
        with self._lock:
            self._db.close()
