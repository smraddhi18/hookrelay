"""A tiny scriptable HTTP/1.1 receiver on 127.0.0.1 for webhook tests (raw asyncio, no deps).

Each request is recorded; the response comes from a handler that may return a canned
:class:`Reply`, sleep (to trip timeouts) or close the connection.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class Received:
    method: str
    path: str
    headers: dict[str, str]
    body: bytes


@dataclass(slots=True)
class Reply:
    status: int = 200
    body: bytes = b"ok"
    headers: dict[str, str] = field(default_factory=dict)
    delay_s: float = 0.0  # before sending anything
    body_delay_s: float = 0.0  # after the headers, before the body
    raw: bytes | None = None  # send these bytes verbatim instead of a well-formed response


Handler = Callable[[Received], Awaitable[Reply] | Reply]


class Receiver:
    def __init__(self, handler: Handler | None = None) -> None:
        self.handler: Handler = handler or (lambda _r: Reply())
        self.requests: list[Received] = []
        self.port = 0
        self._server: asyncio.Server | None = None
        self.arrived = asyncio.Event()

    @property
    def url(self) -> str:
        return f"http://receiver.test:{self.port}/hook"

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._serve, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self._server.wait_closed(), 1)

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                request = await _read_request(reader)
                if request is None:
                    return
                self.requests.append(request)
                self.arrived.set()
                outcome = self.handler(request)
                reply = await outcome if isinstance(outcome, Awaitable) else outcome
                if reply.delay_s:
                    await asyncio.sleep(reply.delay_s)
                if reply.raw is not None:
                    writer.write(reply.raw)
                    await writer.drain()
                    return
                head = [f"HTTP/1.1 {reply.status} X", f"content-length: {len(reply.body)}"]
                head += [f"{k}: {v}" for k, v in reply.headers.items()]
                writer.write(("\r\n".join(head) + "\r\n\r\n").encode())
                await writer.drain()
                if reply.body_delay_s:
                    await asyncio.sleep(reply.body_delay_s)
                writer.write(reply.body)
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            return
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()


async def _read_request(reader: asyncio.StreamReader) -> Received | None:
    line = await reader.readline()
    if not line:
        return None
    method, path, _ = line.decode().split(" ", 2)
    headers: dict[str, str] = {}
    while True:
        raw = await reader.readline()
        if raw in (b"\r\n", b"\n", b""):
            break
        name, _, value = raw.decode().partition(":")
        headers[name.strip().lower()] = value.strip()
    length = int(headers.get("content-length", "0"))
    body = await reader.readexactly(length) if length else b""
    return Received(method=method, path=path, headers=headers, body=body)


@contextlib.asynccontextmanager
async def running_receiver(handler: Handler | None = None) -> AsyncIterator[Receiver]:
    receiver = Receiver(handler)
    await receiver.start()
    try:
        yield receiver
    finally:
        await receiver.stop()


async def local_resolver(host: str, port: int) -> list[str]:
    """Resolves the test host names to loopback (the box/CI DNS is never consulted)."""
    if host in {"receiver.test", "localhost"}:
        return ["127.0.0.1"]
    if host == "metadata.test":
        return ["169.254.169.254"]
    if host == "rebind.test":
        return ["127.0.0.1", "10.0.0.5", "169.254.169.254"]
    return []
