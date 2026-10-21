from __future__ import annotations

import asyncio
import base64
import binascii
import hmac
import json
import logging
import re
import time
from urllib.parse import unquote, urlsplit

from .metrics import Metrics
from .storage import Storage

logger = logging.getLogger("blinkdb.http")


class HttpProtocol:
    def __init__(
        self,
        storage: Storage,
        metrics: Metrics,
        *,
        auth_token: str,
        max_request_bytes: int,
    ) -> None:
        self.storage = storage
        self.metrics = metrics
        self.auth_token = auth_token.encode("utf-8")
        self.max_request_bytes = max_request_bytes

    @staticmethod
    def _response(status: int, body: bytes, content_type: str = "application/json") -> bytes:
        reason = {
            200: "OK",
            201: "Created",
            204: "No Content",
            400: "Bad Request",
            401: "Unauthorized",
            404: "Not Found",
            405: "Method Not Allowed",
            413: "Payload Too Large",
            500: "Internal Server Error",
        }.get(status, "Error")
        headers = (
            f"HTTP/1.1 {status} {reason}\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Content-Type: {content_type}\r\n"
            "Cache-Control: no-store\r\n"
            "Connection: close\r\n\r\n"
        ).encode("ascii")
        return headers + body

    async def serve(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer = writer.get_extra_info("peername")
        started = time.perf_counter()
        failed = False
        try:
            request_line = await asyncio.wait_for(reader.readline(), timeout=10)
            if not request_line or len(request_line) > 8192 or not request_line.endswith(b"\r\n"):
                raise ValueError("Malformed HTTP request line")
            try:
                method, target, version = request_line[:-2].decode("ascii").split(" ", 2)
            except (UnicodeError, ValueError) as error:
                raise ValueError("Malformed HTTP request line") from error
            if version != "HTTP/1.1":
                raise ValueError("Only HTTP/1.1 is supported")

            headers: dict[str, str] = {}
            header_bytes = 0
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=10)
                header_bytes += len(line)
                if not line or header_bytes > 16384 or not line.endswith(b"\r\n"):
                    raise ValueError("Malformed or oversized HTTP headers")
                if line == b"\r\n":
                    break
                if b":" not in line:
                    raise ValueError("Malformed HTTP header")
                name, value = line[:-2].split(b":", 1)
                key = name.decode("ascii").strip().casefold()
                if key in headers:
                    raise ValueError("Duplicate HTTP headers are not supported")
                headers[key] = value.decode("latin-1").strip()

            if not headers.get("host"):
                raise ValueError("HTTP/1.1 Host header is required")
            if "transfer-encoding" in headers:
                raise ValueError("Transfer-Encoding is not supported")
            raw_content_length = headers.get("content-length", "0")
            if not re.fullmatch(r"[0-9]{1,12}", raw_content_length):
                raise ValueError("Invalid Content-Length header")
            content_length = int(raw_content_length)
            if content_length < 0 or content_length > self.max_request_bytes:
                failed = True
                writer.write(self._response(413, b'{"error":"request too large"}'))
                await writer.drain()
                return
            body = await asyncio.wait_for(reader.readexactly(content_length), timeout=15)

            if method == "GET" and urlsplit(target).path == "/healthz":
                writer.write(self._response(200, b'{"status":"ok"}'))
                await writer.drain()
                return
            if not self._authorized(headers.get("authorization", "")):
                failed = True
                writer.write(self._response(401, b'{"error":"unauthorized"}'))
                await writer.drain()
                return
            if method == "GET" and urlsplit(target).path in {"/readyz", "/v1/health"}:
                result = await asyncio.to_thread(self._readiness)
                writer.write(self._response(200, self._json(result)))
            elif method == "GET" and urlsplit(target).path == "/metrics":
                result = await asyncio.to_thread(self._metrics_snapshot)
                writer.write(
                    self._response(
                        200,
                        self._json(result),
                        "application/json; charset=utf-8",
                    )
                )
            else:
                status, response_body = await self._route(method, target, body)
                failed = status >= 400
                writer.write(self._response(status, response_body))
            await writer.drain()
        except asyncio.IncompleteReadError:
            failed = True
            writer.write(self._response(400, b'{"error":"incomplete request"}'))
            await writer.drain()
        except (ValueError, UnicodeError, json.JSONDecodeError) as error:
            failed = True
            writer.write(self._response(400, self._json({"error": str(error)})))
            await writer.drain()
        except (asyncio.TimeoutError, asyncio.LimitOverrunError):
            failed = True
            writer.write(self._response(400, b'{"error":"request timed out or exceeded limit"}'))
            await writer.drain()
        except Exception:
            failed = True
            logger.exception("HTTP request failed from %s", peer)
            writer.write(self._response(500, b'{"error":"internal database error"}'))
            try:
                await writer.drain()
            except ConnectionError:
                pass
        finally:
            self.metrics.record("http", time.perf_counter() - started, failed=failed)
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass

    def _authorized(self, header: str) -> bool:
        scheme, separator, token = header.partition(" ")
        return (
            separator == " "
            and scheme.casefold() == "bearer"
            and hmac.compare_digest(token.encode("utf-8"), self.auth_token)
        )

    @staticmethod
    def _json(value: object) -> bytes:
        return json.dumps(value, separators=(",", ":"), ensure_ascii=True).encode("utf-8")

    async def _route(self, method: str, target: str, body: bytes) -> tuple[int, bytes]:
        path = urlsplit(target).path
        if path == "/v1/transactions":
            if method != "POST":
                return 405, b'{"error":"method not allowed"}'
            payload = json.loads(body)
            if not isinstance(payload, dict) or not isinstance(payload.get("mutations"), list):
                raise ValueError("Expected a JSON object with a mutations array")
            mutations = []
            for item in payload["mutations"]:
                if not isinstance(item, dict):
                    raise ValueError("Each mutation must be a JSON object")
                operation = item.get("op")
                dataset = item.get("dataset", "default")
                if (
                    not isinstance(operation, str)
                    or operation not in {"set", "del"}
                    or not isinstance(dataset, str)
                ):
                    raise ValueError("Mutation op must be set/del and dataset must be text")
                key = self._decode_base64(item.get("key"), "key")
                mutation: dict[str, object] = {
                    "op": operation,
                    "dataset": dataset,
                    "key": key,
                }
                if operation == "set":
                    mutation["value"] = self._decode_base64(item.get("value"), "value")
                mutations.append(mutation)
            if not mutations or len(mutations) > 1000:
                raise ValueError("Transaction must contain between 1 and 1000 mutations")
            await asyncio.to_thread(self.storage.transact, mutations)
            return 200, b'{"status":"committed"}'

        parts = path.split("/")
        if len(parts) == 6 and parts[:3] == ["", "v1", "datasets"] and parts[4] == "keys":
            dataset = unquote(parts[3])
            if not re.fullmatch(r"[A-Za-z0-9_-]*", parts[5]):
                raise ValueError("Key path must be unpadded base64url")
            try:
                key = base64.b64decode(
                    parts[5] + "=" * (-len(parts[5]) % 4),
                    altchars=b"-_",
                    validate=True,
                )
            except (ValueError, binascii.Error) as error:
                raise ValueError("Key path must be unpadded base64url") from error
            if method == "GET":
                value = await asyncio.to_thread(self.storage.get, dataset, key)
                if value is None:
                    return 404, b'{"error":"key not found"}'
                encoded = base64.b64encode(value).decode("ascii")
                return 200, self._json({"dataset": dataset, "key": base64.b64encode(key).decode(), "value": encoded})
            if method == "PUT":
                payload = json.loads(body)
                if not isinstance(payload, dict) or not isinstance(payload.get("value"), str):
                    raise ValueError("Expected JSON with a base64-encoded value")
                value = self._decode_base64(payload["value"], "value")
                await asyncio.to_thread(self.storage.set, dataset, key, value)
                return 201, b'{"status":"stored"}'
            if method == "DELETE":
                deleted = await asyncio.to_thread(self.storage.delete, dataset, key)
                return (200, b'{"deleted":true}') if deleted else (404, b'{"deleted":false}')
            return 405, b'{"error":"method not allowed"}'
        return 404, b'{"error":"route not found"}'

    @staticmethod
    def _decode_base64(value: object, name: str) -> bytes:
        if not isinstance(value, str):
            raise ValueError(f"{name.capitalize()} must be base64 text")
        try:
            return base64.b64decode(value, validate=True)
        except (ValueError, binascii.Error) as error:
            raise ValueError(f"{name.capitalize()} is not valid base64") from error

    def _readiness(self) -> dict[str, object]:
        return {
            "status": "ready",
            "storage": self.storage.stats(),
            "integrity": self.storage.check_integrity(),
        }

    def _metrics_snapshot(self) -> dict[str, object]:
        return {"storage": self.storage.stats(), **self.metrics.snapshot()}
