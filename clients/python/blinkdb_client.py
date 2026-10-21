from __future__ import annotations

import base64
import json
import os
import socket
import ssl
import urllib.error
import urllib.request
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote


def _bulk(value: bytes) -> bytes:
    return b"$" + str(len(value)).encode("ascii") + b"\r\n" + value + b"\r\n"


def _command(*parts: bytes) -> bytes:
    return b"*" + str(len(parts)).encode("ascii") + b"\r\n" + b"".join(_bulk(p) for p in parts)


class BlinkDBHttpError(RuntimeError):
    def __init__(self, status: int, detail: str) -> None:
        super().__init__(f"BlinkDB HTTP {status}: {detail}")
        self.status = status


class RespClient:
    def __init__(
        self,
        host: str,
        port: int,
        token: str,
        *,
        tls: bool = False,
        timeout: float = 5.0,
        ssl_context: ssl.SSLContext | None = None,
        dataset: str = "default",
    ) -> None:
        self.dataset = dataset
        self._socket = socket.create_connection((host, port), timeout=timeout)
        self._socket.settimeout(timeout)
        if tls:
            self._socket = (ssl_context or ssl.create_default_context()).wrap_socket(
                self._socket, server_hostname=host
            )
        self._file = self._socket.makefile("rwb")
        self._send(b"AUTH", token.encode())
        try:
            response = self._read_response()
        except RuntimeError as error:
            self.close()
            raise PermissionError("BlinkDB RESP authentication failed") from error
        except Exception:
            self.close()
            raise
        if response != b"OK":
            self.close()
            raise PermissionError("BlinkDB RESP authentication failed")

    @classmethod
    def from_env(
        cls,
        *,
        dataset: str = "default",
        timeout: float = 5.0,
        environ: Mapping[str, str] | None = None,
    ) -> "RespClient":
        """Connect using BLINKDB_HOST, BLINKDB_AUTH_TOKEN and optional settings."""
        values = os.environ if environ is None else environ
        host = values.get("BLINKDB_HOST")
        token = values.get("BLINKDB_AUTH_TOKEN")
        if not host:
            raise ValueError("Set BLINKDB_HOST to the BlinkDB server hostname")
        if not token:
            raise ValueError("Set BLINKDB_AUTH_TOKEN to the BlinkDB access token")
        try:
            port = int(values.get("BLINKDB_RESP_PORT", "6379"))
        except ValueError as error:
            raise ValueError("BLINKDB_RESP_PORT must be an integer") from error

        tls_value = values.get("BLINKDB_TLS", "false").casefold()
        if tls_value not in {"true", "false", "1", "0", "yes", "no"}:
            raise ValueError("BLINKDB_TLS must be true or false")
        tls = tls_value in {"true", "1", "yes"}
        ca_file = values.get("BLINKDB_TLS_CA")
        if ca_file and not tls:
            raise ValueError("Set BLINKDB_TLS=true when configuring BLINKDB_TLS_CA")
        ssl_context = ssl.create_default_context(cafile=ca_file) if ca_file else None
        return cls(
            host,
            port,
            token,
            tls=tls,
            timeout=timeout,
            ssl_context=ssl_context,
            dataset=dataset,
        )

    def _send(self, *parts: bytes) -> None:
        self._file.write(_command(*parts))
        self._file.flush()

    def _read_response(self) -> bytes | int | None:
        marker = self._file.read(1)
        if not marker:
            raise ConnectionError("BlinkDB closed the RESP connection")
        line = self._file.readline()
        if not line.endswith(b"\r\n"):
            raise ConnectionError("Malformed RESP response")
        value = line[:-2]
        if marker == b"+":
            return value
        if marker == b"-":
            raise RuntimeError(value.decode("utf-8", errors="replace"))
        if marker == b":":
            return int(value)
        if marker == b"$":
            size = int(value)
            if size == -1:
                return None
            body = self._file.read(size + 2)
            if len(body) != size + 2 or not body.endswith(b"\r\n"):
                raise ConnectionError("Malformed RESP bulk-string response")
            return body[:-2]
        raise ConnectionError("Unsupported RESP response type")

    def set(self, key: bytes, value: bytes, dataset: str | None = None) -> None:
        dataset = self.dataset if dataset is None else dataset
        if dataset == "default":
            self._send(b"SET", key, value)
        else:
            self._send(b"SETX", dataset.encode(), key, value)
        if self._read_response() != b"OK":
            raise RuntimeError("Unexpected response to SET")

    def get(self, key: bytes, dataset: str | None = None) -> bytes | None:
        dataset = self.dataset if dataset is None else dataset
        if dataset == "default":
            self._send(b"GET", key)
        else:
            self._send(b"GETX", dataset.encode(), key)
        response = self._read_response()
        if response is not None and not isinstance(response, bytes):
            raise RuntimeError("Unexpected response to GET")
        return response

    def delete(self, key: bytes, dataset: str | None = None) -> bool:
        dataset = self.dataset if dataset is None else dataset
        if dataset == "default":
            self._send(b"DEL", key)
        else:
            self._send(b"DELX", dataset.encode(), key)
        response = self._read_response()
        if not isinstance(response, int):
            raise RuntimeError("Unexpected response to DEL")
        return response > 0

    def ping(self) -> bool:
        self._send(b"PING")
        return self._read_response() == b"PONG"

    def close(self) -> None:
        try:
            self._file.close()
        except (AttributeError, OSError):
            pass
        try:
            self._socket.close()
        except OSError:
            pass

    def __enter__(self) -> "RespClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class HttpClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout: float = 5.0,
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.ssl_context = ssl_context

    def _request(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            self.base_url + path,
            data=data,
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(
                request,
                timeout=self.timeout,
                context=self.ssl_context,
            ) as response:
                if response.status == 204:
                    return None
                raw = response.read()
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            raise BlinkDBHttpError(error.code, detail) from error

    @staticmethod
    def _encoded_key(key: bytes) -> str:
        return base64.urlsafe_b64encode(key).rstrip(b"=").decode("ascii")

    def set(self, dataset: str, key: bytes, value: bytes) -> None:
        path = f"/v1/datasets/{quote(dataset, safe='')}/keys/{self._encoded_key(key)}"
        self._request(
            "PUT",
            path,
            {"value": base64.b64encode(value).decode("ascii")},
        )

    def get(self, dataset: str, key: bytes) -> bytes | None:
        path = f"/v1/datasets/{quote(dataset, safe='')}/keys/{self._encoded_key(key)}"
        try:
            result = self._request("GET", path)
        except BlinkDBHttpError as error:
            if error.status == 404:
                return None
            raise
        return base64.b64decode(result["value"], validate=True)

    def delete(self, dataset: str, key: bytes) -> bool:
        path = f"/v1/datasets/{quote(dataset, safe='')}/keys/{self._encoded_key(key)}"
        try:
            self._request("DELETE", path)
            return True
        except BlinkDBHttpError as error:
            if error.status == 404:
                return False
            raise

    def transact(self, mutations: list[dict[str, object]]) -> None:
        encoded = []
        for mutation in mutations:
            item = {
                "op": mutation["op"],
                "dataset": mutation.get("dataset", "default"),
                "key": base64.b64encode(mutation["key"]).decode("ascii"),
            }
            if mutation["op"] == "set":
                item["value"] = base64.b64encode(mutation["value"]).decode("ascii")
            encoded.append(item)
        self._request("POST", "/v1/transactions", {"mutations": encoded})
