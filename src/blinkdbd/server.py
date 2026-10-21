from __future__ import annotations

import asyncio
import logging
import signal
import ssl
from collections.abc import Awaitable, Callable

from .config import Settings, validate_settings
from .http_api import HttpProtocol
from .metrics import Metrics
from .resp import RespProtocol
from .storage import Storage

logger = logging.getLogger("blinkdb")


def _tls_context(settings: Settings) -> ssl.SSLContext | None:
    if settings.tls_certificate is None or settings.tls_private_key is None:
        return None
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(
        certfile=settings.tls_certificate,
        keyfile=settings.tls_private_key,
    )
    return context


async def _guarded(
    semaphore: asyncio.Semaphore,
    handler: Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]],
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    if semaphore.locked():
        writer.close()
        try:
            await writer.wait_closed()
        except ConnectionError:
            pass
        return
    async with semaphore:
        await handler(reader, writer)


async def serve(settings: Settings) -> None:
    validate_settings(settings)
    settings.data_directory.mkdir(parents=True, exist_ok=True)
    tls = _tls_context(settings)
    metrics = Metrics()
    storage = Storage(settings, metrics)
    semaphore = asyncio.Semaphore(256)
    request_limit = settings.max_request_bytes + 16384

    connection_tasks: set[asyncio.Task[None]] = set()
    connection_writers: set[asyncio.StreamWriter] = set()

    def connection_callback(
        handler: Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]],
    ) -> Callable[[asyncio.StreamReader, asyncio.StreamWriter], None]:
        def accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            connection_writers.add(writer)
            task = asyncio.create_task(_guarded(semaphore, handler, reader, writer))
            connection_tasks.add(task)

            def finished(completed: asyncio.Task[None]) -> None:
                connection_writers.discard(writer)
                connection_tasks.discard(completed)
                if completed.cancelled():
                    return
                error = completed.exception()
                if error is not None:
                    logger.error(
                        "Client connection failed",
                        exc_info=(type(error), error, error.__traceback__),
                    )

            task.add_done_callback(finished)

        return accept

    resp = RespProtocol(
        storage,
        metrics,
        auth_token=settings.auth_token,
        max_request_bytes=settings.max_request_bytes,
        max_value_bytes=settings.max_value_bytes,
    )
    http = HttpProtocol(
        storage,
        metrics,
        auth_token=settings.auth_token,
        max_request_bytes=settings.max_request_bytes,
    )

    servers: list[asyncio.AbstractServer] = []
    try:
        servers.append(
            await asyncio.start_server(
                connection_callback(resp.serve),
                settings.bind,
                settings.resp_port,
                ssl=tls,
                limit=request_limit,
                backlog=128,
            )
        )
        servers.append(
            await asyncio.start_server(
                connection_callback(http.serve),
                settings.bind,
                settings.http_port,
                ssl=tls,
                limit=16384,
                backlog=128,
            )
        )

        for server in servers:
            for sock in server.sockets or []:
                logger.info("Listening on %s", sock.getsockname())
        logger.info("BlinkDB started; data directory: %s", settings.data_directory)

        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(signum, stop.set)
            except (NotImplementedError, RuntimeError):
                pass
        await stop.wait()
    finally:
        for server in servers:
            server.close()
        for server in servers:
            await server.wait_closed()
        if connection_tasks:
            _, pending = await asyncio.wait(tuple(connection_tasks), timeout=10)
            for writer in tuple(connection_writers):
                writer.close()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        storage.close()
        logger.info("BlinkDB stopped")
