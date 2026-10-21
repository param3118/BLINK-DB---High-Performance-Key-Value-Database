from __future__ import annotations

import asyncio
import base64
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from clients.python.blinkdb_client import HttpClient, RespClient
from blinkdbd.config import Settings, validate_settings
from blinkdbd.http_api import HttpProtocol
from blinkdbd.metrics import Metrics
from blinkdbd.resp import RespProtocol
from blinkdbd.storage import APPLICATION_ID, Storage

TOKEN = "test-token-" + ("x" * 32)


def make_settings(directory: Path) -> Settings:
    return Settings(
        data_directory=directory,
        bind="127.0.0.1",
        resp_port=16379,
        http_port=18080,
        auth_token=TOKEN,
        tls_certificate=None,
        tls_private_key=None,
        busy_timeout_ms=1000,
        memory_limit_bytes=1024 * 1024,
        max_database_size_bytes=0,
        max_dataset_bytes=255,
        max_key_bytes=1024,
        max_value_bytes=1024 * 1024,
        max_request_bytes=2 * 1024 * 1024,
        compression=True,
    )


async def request_server(
    host: str,
    port: int,
    payload: bytes,
) -> bytes:
    reader, writer = await asyncio.open_connection(host, port)
    writer.write(payload)
    await writer.drain()
    response = await reader.read()
    writer.close()
    await writer.wait_closed()
    return response


class StorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.metrics = Metrics()
        self.storage = Storage(make_settings(Path(self.temp.name)), self.metrics)
        self.addCleanup(self.storage.close)

    def test_binary_empty_missing_and_dataset_isolation(self) -> None:
        self.storage.set("alpha", b"\x00\xff", b"")
        self.storage.set("beta", b"\x00\xff", b"\x00\xfe")
        self.assertEqual(self.storage.get("alpha", b"\x00\xff"), b"")
        self.assertEqual(self.storage.get("beta", b"\x00\xff"), b"\x00\xfe")
        self.assertIsNone(self.storage.get("missing", b"\x00\xff"))

    def test_transaction_and_backup_restore(self) -> None:
        self.storage.transact(
            [
                {"op": "set", "dataset": "one", "key": b"k", "value": b"value"},
                {"op": "set", "dataset": "one", "key": b"empty", "value": b""},
            ]
        )
        backup = Path(self.temp.name) / "copy.sqlite3"
        self.storage.backup(backup)
        self.storage.delete("one", b"k")
        self.storage.restore(backup)
        self.assertEqual(self.storage.get("one", b"k"), b"value")
        self.assertEqual(self.storage.get("one", b"empty"), b"")
        self.assertEqual(self.storage.check_integrity(), "ok")

    def test_compression_round_trip_and_corrupt_transaction_rollback(self) -> None:
        large_value = b"repeated-value-" * 5000
        self.storage.set("one", b"large", large_value)
        self.assertEqual(self.storage.get("one", b"large"), large_value)
        with self.storage._lock:
            stored_value = self.storage._db.execute(
                "SELECT value FROM kv_entries WHERE dataset='one' AND key=?",
                (sqlite3.Binary(b"large"),),
            ).fetchone()[0]
        self.assertTrue(bytes(stored_value).startswith(b"BLK2Z:"))
        with self.assertRaises(ValueError):
            self.storage.transact(
                [
                    {"op": "set", "dataset": "one", "key": b"valid", "value": b"yes"},
                    {"op": "invalid", "dataset": "one", "key": b"bad"},
                ]
            )
        self.assertIsNone(self.storage.get("one", b"valid"))

    def test_schema_v1_migration_preserves_binary_values_and_marker_prefixes(self) -> None:
        legacy_directory = Path(self.temp.name) / "v1"
        legacy_directory.mkdir()
        database = legacy_directory / "blinkdb.sqlite3"
        connection = sqlite3.connect(database)
        connection.execute(f"PRAGMA application_id={APPLICATION_ID}")
        connection.execute("PRAGMA user_version=1")
        connection.execute(
            "CREATE TABLE kv_entries(dataset TEXT NOT NULL,key BLOB NOT NULL,"
            "value BLOB NOT NULL,PRIMARY KEY(dataset,key)) WITHOUT ROWID"
        )
        connection.execute(
            "INSERT INTO kv_entries VALUES(?,?,?)",
            ("old", sqlite3.Binary(b"key"), sqlite3.Binary(b"\x00\xffBLK1old")),
        )
        connection.commit()
        connection.close()

        migrated = Storage(make_settings(legacy_directory), Metrics())
        self.addCleanup(migrated.close)
        self.assertEqual(migrated.get("old", b"key"), b"\x00\xffBLK1old")
        with migrated._lock:
            version = migrated._db.execute("PRAGMA user_version").fetchone()[0]
        self.assertEqual(version, 2)


class NetworkTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addAsyncCleanup(asyncio.to_thread, self.temp.cleanup)
        self.metrics = Metrics()
        self.storage = Storage(make_settings(Path(self.temp.name)), self.metrics)
        self.addAsyncCleanup(asyncio.to_thread, self.storage.close)
        self.resp = await asyncio.start_server(
            RespProtocol(
                self.storage,
                self.metrics,
                auth_token=TOKEN,
                max_request_bytes=2 * 1024 * 1024,
                max_value_bytes=1024 * 1024,
            ).serve,
            "127.0.0.1",
            0,
        )
        self.http = await asyncio.start_server(
            HttpProtocol(
                self.storage,
                self.metrics,
                auth_token=TOKEN,
                max_request_bytes=2 * 1024 * 1024,
            ).serve,
            "127.0.0.1",
            0,
        )
        self.addAsyncCleanup(self._close_servers)

    async def _close_servers(self) -> None:
        self.resp.close()
        self.http.close()
        await self.resp.wait_closed()
        await self.http.wait_closed()

    def _resp_command(self, *parts: bytes) -> bytes:
        encoded = [b"$" + str(len(part)).encode() + b"\r\n" + part + b"\r\n" for part in parts]
        return b"*" + str(len(parts)).encode() + b"\r\n" + b"".join(encoded)

    async def test_resp_fragmented_pipelined_binary_and_auth(self) -> None:
        port = self.resp.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(self._resp_command(b"PING")[:7])
        await writer.drain()
        writer.write(self._resp_command(b"PING")[7:])
        await writer.drain()
        self.assertEqual(await reader.readline(), b"-ERR authenticate first with AUTH <token>\r\n")
        writer.write(
            self._resp_command(b"AUTH", TOKEN.encode())
            + self._resp_command(b"SET", b"binary", b"\x00\xff")
            + self._resp_command(b"GET", b"binary")
            + self._resp_command(b"SET", b"empty", b"")
            + self._resp_command(b"GET", b"empty")
            + self._resp_command(b"GET", b"missing")
        )
        await writer.drain()
        self.assertEqual(await reader.readline(), b"+OK\r\n")
        self.assertEqual(await reader.readline(), b"+OK\r\n")
        self.assertEqual(await reader.readline(), b"$2\r\n")
        self.assertEqual(await reader.readexactly(4), b"\x00\xff\r\n")
        self.assertEqual(await reader.readline(), b"+OK\r\n")
        self.assertEqual(await reader.readline(), b"$0\r\n")
        self.assertEqual(await reader.readexactly(2), b"\r\n")
        self.assertEqual(await reader.readline(), b"$-1\r\n")
        writer.close()
        await writer.wait_closed()

    async def test_resp_transaction_accepts_more_than_32_arguments(self) -> None:
        port = self.resp.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(self._resp_command(b"AUTH", TOKEN.encode()))
        await writer.drain()
        self.assertEqual(await reader.readline(), b"+OK\r\n")
        command = [b"TXN"]
        for index in range(9):
            command.extend(
                [b"SET", b"bulk", f"key-{index}".encode(), f"value-{index}".encode()]
            )
        writer.write(self._resp_command(*command))
        await writer.drain()
        self.assertEqual(await reader.readline(), b"+OK\r\n")
        self.assertEqual(self.storage.get("bulk", b"key-8"), b"value-8")
        writer.close()
        await writer.wait_closed()

    async def test_http_auth_binary_empty_delete_and_transaction(self) -> None:
        port = self.http.sockets[0].getsockname()[1]
        key = base64.urlsafe_b64encode(b"\x00key").rstrip(b"=").decode()
        body = json.dumps({"value": base64.b64encode(b"\x00\xff").decode()}).encode()
        request = (
            f"PUT /v1/datasets/demo/keys/{key} HTTP/1.1\r\n"
            f"Host: localhost\r\nAuthorization: Bearer {TOKEN}\r\n"
            f"Content-Length: {len(body)}\r\n\r\n"
        ).encode() + body
        response = await request_server("127.0.0.1", port, request)
        self.assertTrue(response.startswith(b"HTTP/1.1 201"))

        get_request = (
            f"GET /v1/datasets/demo/keys/{key} HTTP/1.1\r\n"
            f"Host: localhost\r\nAuthorization: Bearer {TOKEN}\r\n\r\n"
        ).encode()
        response = await request_server("127.0.0.1", port, get_request)
        result = json.loads(response.split(b"\r\n\r\n", 1)[1])
        self.assertEqual(base64.b64decode(result["value"]), b"\x00\xff")

        mutation = {
            "op": "set",
            "dataset": "demo",
            "key": base64.b64encode(b"empty").decode(),
            "value": "",
        }
        tx_body = json.dumps({"mutations": [mutation]}).encode()
        tx_request = (
            f"POST /v1/transactions HTTP/1.1\r\nHost: localhost\r\n"
            f"Authorization: Bearer {TOKEN}\r\nContent-Length: {len(tx_body)}\r\n\r\n"
        ).encode() + tx_body
        response = await request_server("127.0.0.1", port, tx_request)
        self.assertTrue(response.startswith(b"HTTP/1.1 200"))
        self.assertEqual(self.storage.get("demo", b"empty"), b"")

    async def test_http_rejects_unauthorized_and_bad_key(self) -> None:
        port = self.http.sockets[0].getsockname()[1]
        response = await request_server(
            "127.0.0.1",
            port,
            b"GET /readyz HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        self.assertTrue(response.startswith(b"HTTP/1.1 401"))
        response = await request_server(
            "127.0.0.1",
            port,
            f"GET /v1/datasets/d/keys/%ZZ HTTP/1.1\r\nHost: localhost\r\n"
            f"Authorization: Bearer {TOKEN}\r\n\r\n".encode(),
        )
        self.assertTrue(response.startswith(b"HTTP/1.1 400"))

    async def test_http_invalid_transaction_is_client_error_not_server_error(self) -> None:
        port = self.http.sockets[0].getsockname()[1]
        body = json.dumps({"mutations": [{"op": "unknown", "key": 7}]}).encode()
        request = (
            f"POST /v1/transactions HTTP/1.1\r\nHost: localhost\r\n"
            f"Authorization: Bearer {TOKEN}\r\nContent-Length: {len(body)}\r\n\r\n"
        ).encode() + body
        response = await request_server("127.0.0.1", port, request)
        self.assertTrue(response.startswith(b"HTTP/1.1 400"))

    async def test_python_clients_cover_resp_and_http_binary_values(self) -> None:
        resp_port = self.resp.sockets[0].getsockname()[1]
        resp_client = await asyncio.to_thread(RespClient, "127.0.0.1", resp_port, TOKEN)
        try:
            await asyncio.to_thread(resp_client.set, b"client-key", b"\x00\xff")
            value = await asyncio.to_thread(resp_client.get, b"client-key")
            self.assertEqual(value, b"\x00\xff")
        finally:
            resp_client.close()

        http_port = self.http.sockets[0].getsockname()[1]
        http_client = HttpClient(f"http://127.0.0.1:{http_port}", TOKEN)
        await asyncio.to_thread(http_client.set, "team/alpha", b"client-key", b"")
        self.assertEqual(
            await asyncio.to_thread(http_client.get, "team/alpha", b"client-key"),
            b"",
        )
        self.assertTrue(
            await asyncio.to_thread(http_client.delete, "team/alpha", b"client-key")
        )

    async def test_resp_client_from_env_uses_default_dataset(self) -> None:
        resp_port = self.resp.sockets[0].getsockname()[1]
        client = await asyncio.to_thread(
            RespClient.from_env,
            dataset="env-client",
            environ={
                "BLINKDB_HOST": "127.0.0.1",
                "BLINKDB_RESP_PORT": str(resp_port),
                "BLINKDB_AUTH_TOKEN": TOKEN,
                "BLINKDB_TLS": "false",
            },
        )
        try:
            await asyncio.to_thread(client.set, b"key", b"value")
            self.assertEqual(await asyncio.to_thread(client.get, b"key"), b"value")
            self.assertEqual(self.storage.get("env-client", b"key"), b"value")
        finally:
            client.close()


class ConfigurationTests(unittest.TestCase):
    def test_resp_client_from_env_requires_host_and_token(self) -> None:
        with self.assertRaisesRegex(ValueError, "BLINKDB_HOST"):
            RespClient.from_env(environ={"BLINKDB_AUTH_TOKEN": TOKEN})
        with self.assertRaisesRegex(ValueError, "BLINKDB_AUTH_TOKEN"):
            RespClient.from_env(environ={"BLINKDB_HOST": "localhost"})

    def test_public_bind_requires_tls_and_secure_token(self) -> None:
        settings = make_settings(Path("data"))
        with self.assertRaisesRegex(ValueError, "at least 32 bytes"):
            validate_settings(settings.__class__(**{**settings.__dict__, "auth_token": "short"}))
        public = settings.__class__(**{**settings.__dict__, "bind": "0.0.0.0"})
        with self.assertRaisesRegex(ValueError, "require TLS"):
            validate_settings(public)


if __name__ == "__main__":
    unittest.main()
