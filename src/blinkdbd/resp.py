from __future__ import annotations

import asyncio
import hmac
import logging
import time

from .metrics import Metrics
from .storage import Storage

logger = logging.getLogger("blinkdb.resp")


def encode_simple(value: str) -> bytes:
    safe = value.replace("\r", " ").replace("\n", " ")
    return f"+{safe}\r\n".encode("utf-8")


def encode_error(value: str) -> bytes:
    safe = value.replace("\r", " ").replace("\n", " ")
    return f"-ERR {safe}\r\n".encode("utf-8")


def encode_bulk(value: bytes | None) -> bytes:
    if value is None:
        return b"$-1\r\n"
    return b"$" + str(len(value)).encode("ascii") + b"\r\n" + value + b"\r\n"


def encode_integer(value: int) -> bytes:
    return f":{value}\r\n".encode("ascii")


async def read_command(reader: asyncio.StreamReader, max_request_bytes: int) -> list[bytes] | None:
    consumed = 0
    first_line = await reader.readline()
    if not first_line:
        return None
    consumed += len(first_line)
    if consumed > max_request_bytes or not first_line.endswith(b"\r\n"):
        raise ValueError("Malformed or oversized RESP request")

    if first_line.startswith(b"*"):
        try:
            argument_count = int(first_line[1:-2])
        except ValueError as error:
            raise ValueError("Malformed RESP array length") from error
        if not 1 <= argument_count <= 4001:
            raise ValueError("RESP command must contain between 1 and 4001 arguments")
        arguments: list[bytes] = []
        for _ in range(argument_count):
            header = await reader.readline()
            consumed += len(header)
            if not header.endswith(b"\r\n") or not header.startswith(b"$"):
                raise ValueError("RESP arrays must contain bulk-string arguments")
            try:
                length = int(header[1:-2])
            except ValueError as error:
                raise ValueError("Malformed RESP bulk-string length") from error
            if length < 0 or consumed + length + 2 > max_request_bytes:
                raise ValueError("RESP bulk-string length is invalid or too large")
            body = await reader.readexactly(length + 2)
            consumed += len(body)
            if not body.endswith(b"\r\n"):
                raise ValueError("RESP bulk string is missing its CRLF terminator")
            arguments.append(body[:-2])
        return arguments

    inline = first_line[:-2].split()
    if not inline or len(inline) > 32:
        raise ValueError("Malformed inline command")
    return inline


class RespProtocol:
    def __init__(
        self,
        storage: Storage,
        metrics: Metrics,
        *,
        auth_token: str,
        max_request_bytes: int,
        max_value_bytes: int,
    ) -> None:
        self.storage = storage
        self.metrics = metrics
        self.auth_token = auth_token.encode("utf-8")
        self.max_request_bytes = max_request_bytes
        self.max_value_bytes = max_value_bytes

    async def serve(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        authenticated = False
        peer = writer.get_extra_info("peername")
        try:
            while True:
                command = await asyncio.wait_for(
                    read_command(reader, self.max_request_bytes),
                    timeout=30,
                )
                if command is None:
                    break
                if not command:
                    raise ValueError("Command must not be empty")

                name = command[0].decode("ascii", errors="replace").upper()
                started = time.perf_counter()
                if name == "AUTH" and len(command) == 2:
                    authenticated = hmac.compare_digest(command[1], self.auth_token)
                    self.metrics.record(
                        "auth",
                        time.perf_counter() - started,
                        failed=not authenticated,
                    )
                    writer.write(
                        encode_simple("OK")
                        if authenticated
                        else encode_error("authentication failed")
                    )
                    await writer.drain()
                    continue
                if not authenticated:
                    self.metrics.record(
                        name.lower(),
                        time.perf_counter() - started,
                        failed=True,
                    )
                    writer.write(encode_error("authenticate first with AUTH <token>"))
                    await writer.drain()
                    continue

                try:
                    response = await self._execute(command, name)
                    self.metrics.record(name.lower(), time.perf_counter() - started)
                except (ValueError, UnicodeError) as error:
                    self.metrics.record(name.lower(), time.perf_counter() - started, failed=True)
                    response = encode_error(str(error))
                except Exception:
                    self.metrics.record(name.lower(), time.perf_counter() - started, failed=True)
                    logger.exception("RESP command failed: %s", name)
                    response = encode_error("internal database error")
                writer.write(response)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError, asyncio.LimitOverrunError):
            logger.info("RESP client disconnected: %s", peer)
        except (ValueError, asyncio.TimeoutError) as error:
            self.metrics.record("protocol_error", 0, failed=True)
            logger.info("Rejected RESP connection from %s: %s", peer, error)
            writer.write(encode_error(str(error)))
            try:
                await writer.drain()
            except ConnectionError:
                pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass

    async def _execute(self, args: list[bytes], command: str) -> bytes:
        if command == "PING" and len(args) == 1:
            return encode_simple("PONG")
        if command == "SET" and len(args) == 3:
            await asyncio.to_thread(self.storage.set, "default", args[1], args[2])
            return encode_simple("OK")
        if command == "GET" and len(args) == 2:
            value = await asyncio.to_thread(self.storage.get, "default", args[1])
            return encode_bulk(value)
        if command == "DEL" and len(args) == 2:
            deleted = await asyncio.to_thread(self.storage.delete, "default", args[1])
            return encode_integer(int(deleted))
        if command == "SETX" and len(args) == 4:
            dataset = args[1].decode("utf-8")
            await asyncio.to_thread(self.storage.set, dataset, args[2], args[3])
            return encode_simple("OK")
        if command == "GETX" and len(args) == 3:
            dataset = args[1].decode("utf-8")
            value = await asyncio.to_thread(self.storage.get, dataset, args[2])
            return encode_bulk(value)
        if command == "DELX" and len(args) == 3:
            dataset = args[1].decode("utf-8")
            deleted = await asyncio.to_thread(self.storage.delete, dataset, args[2])
            return encode_integer(int(deleted))
        if command == "TXN" and len(args) >= 2:
            mutations: list[dict[str, object]] = []
            index = 1
            while index < len(args):
                operation = args[index].decode("ascii", errors="strict").upper()
                if operation == "SET" and index + 3 < len(args):
                    mutations.append(
                        {
                            "op": "set",
                            "dataset": args[index + 1].decode("utf-8"),
                            "key": args[index + 2],
                            "value": args[index + 3],
                        }
                    )
                    index += 4
                elif operation == "DEL" and index + 2 < len(args):
                    mutations.append(
                        {
                            "op": "del",
                            "dataset": args[index + 1].decode("utf-8"),
                            "key": args[index + 2],
                        }
                    )
                    index += 3
                else:
                    raise ValueError("TXN format: TXN SET dataset key value DEL dataset key ...")
            if len(mutations) > 1000:
                raise ValueError("A TXN request may contain at most 1000 mutations")
            await asyncio.to_thread(self.storage.transact, mutations)
            return encode_simple("OK")
        if command == "INFO" and len(args) == 1:
            import json

            result = {
                "storage": self.storage.stats(),
                "metrics": self.metrics.snapshot(),
            }
            return encode_bulk(json.dumps(result, separators=(",", ":")).encode())
        if command == "AUTH":
            raise ValueError("AUTH requires exactly one token")
        if command in {"SET", "SETX"} and len(args) >= 3:
            if len(args[-1]) > self.max_value_bytes:
                raise ValueError("Value exceeds the configured limit")
        raise ValueError("unknown command or wrong number of arguments")
