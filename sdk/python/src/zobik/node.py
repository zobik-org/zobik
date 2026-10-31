"""The Logic Container's side of the Node Runtime Interface (implementation §1.2.13, §3).

A blueprint registers a handler per delivery class on a Node and runs it:

    node = Node()

    @node.on_task
    async def handle(task):
        ref = await task.write_context("result", task.data["task_text"])
        await task.complete({"context_ref": ref})

    node.run()
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import traceback
from collections.abc import AsyncIterable, Awaitable, Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import grpc

from zobik.nri.v1 import nri_pb2 as pb
from zobik.nri.v1 import nri_pb2_grpc

# NRI_VERSION is the version the library sends in its Hello, and the one the base
# image declares in io.zobik.nri.version (implementation §1.2.13, §3).
NRI_VERSION = "1.0"
_MINOR = int(NRI_VERSION.split(".")[1])

# The socket and the work areas are part of the transport (implementation §2.1).
SOCKET = "/run/zobik/nri.sock"
AREAS = "/run/zobik/tasks"

# A chunk of a Context Store portion (implementation §3).
_CHUNK = 1 << 20

# How a handler that ends without closing its task fails it (implementation §1.2.13).
REASON_LIMIT = 512
TRACEBACK_PORTION = "traceback"
UNCLOSED_REASON = "the handler returned without closing its task"

# How long a terminal event that arrives before its PublishAck waits for it.
_ORPHAN_TTL = 60.0
# The pause between dials while the socket does not exist or the session ended.
_REDIAL = 1.0

log = logging.getLogger("zobik")

Handler = Callable[["Task"], Awaitable[None]]


@dataclass(frozen=True)
class ContextRef:
    """A portion of the Context Store: a name within a task's section (implementation §3)."""

    trace_id: str
    task_id: str
    name: str

    def to_json(self) -> dict[str, str]:
        # With the proto's field names, as it travels in data (implementation §3).
        return {"trace_id": self.trace_id, "task_id": self.task_id, "name": self.name}

    @classmethod
    def from_json(cls, d: dict[str, str]) -> ContextRef:
        return cls(d["trace_id"], d["task_id"], d["name"])

    @classmethod
    def _from_pb(cls, r: pb.ContextRef) -> ContextRef:
        return cls(r.trace_id, r.task_id, r.name)

    def _pb(self) -> pb.ContextRef:
        return pb.ContextRef(trace_id=self.trace_id, task_id=self.task_id, name=self.name)


def _ref(ref: ContextRef | dict[str, str]) -> ContextRef:
    return ref if isinstance(ref, ContextRef) else ContextRef.from_json(ref)


def _encode(data: dict[str, Any] | None) -> bytes:
    def default(o: Any) -> Any:
        if isinstance(o, ContextRef):
            return o.to_json()
        raise TypeError(f"{type(o).__name__} is not JSON serializable")

    return json.dumps(data or {}, default=default).encode()


@dataclass(frozen=True)
class Result:
    """The terminal event of a subtask, as the session delivers it (implementation §3)."""

    event: dict[str, Any]

    @property
    def type(self) -> str:
        return self.event["type"]

    @property
    def data(self) -> dict[str, Any]:
        return self.event.get("data") or {}

    @property
    def ok(self) -> bool:
        return self.type == "task.completed"


@dataclass(frozen=True)
class Response:
    """The response of a call: its fields, with status, and the file of its body, if any."""

    fields: dict[str, Any]
    body: Path | None = None

    @property
    def status(self) -> int | None:
        return self.fields.get("status")


class Subtask:
    """A subtask a task opened with announce or ask. Awaiting it returns its Result."""

    def __init__(self, task_id: str, result: asyncio.Future[Result]):
        self.task_id = task_id
        self._result = result

    def __await__(self):
        # Cancelling whoever waits does not cancel the wait of the others.
        return asyncio.shield(self._result).__await__()

    def done(self) -> bool:
        return self._result.done()


class Task:
    """A task assigned to the node, and the requests that hang off it (implementation §1.2.13)."""

    def __init__(self, session: _Session, delivery: pb.Delivery):
        self._session = session
        self.event: dict[str, Any] = json.loads(delivery.event)
        self.announcement: dict[str, Any] = json.loads(delivery.announcement) if delivery.announcement else {}
        self.task_id: str = self.event["subject"]
        self.work_area = Path(session.areas) / self.task_id
        self._subtasks: list[Subtask] = []
        self.closed = False

    @property
    def data(self) -> dict[str, Any]:
        """The data of the task's task.announced: what there is to do (implementation §3, The session)."""
        return self.announcement.get("data") or {}

    async def read_context(self, ref: ContextRef | dict[str, str]) -> bytes:
        return b"".join([c async for c in self.stream_context(ref)])

    async def stream_context(self, ref: ContextRef | dict[str, str]):
        call = self._session.stub.ReadContext(pb.ContextRead(task_id=self.task_id, ref=_ref(ref)._pb()))
        async for chunk in call:
            yield chunk.data

    async def write_context(
        self, name: str, content: bytes | str | Iterable[bytes] | AsyncIterable[bytes]
    ) -> ContextRef:
        async def messages():
            yield pb.ContextWrite(header=pb.ContextWriteHeader(task_id=self.task_id, name=name))
            async for piece in _pieces(content):
                for i in range(0, len(piece), _CHUNK):
                    yield pb.ContextWrite(chunk=piece[i : i + _CHUNK])

        return ContextRef._from_pb(await self._session.stub.WriteContext(messages()))

    async def list_context(self, section: str | None = None) -> list[ContextRef]:
        listing = await self._session.stub.ListContext(
            pb.ContextList(task_id=self.task_id, section=section or self.task_id)
        )
        return [ContextRef._from_pb(r) for r in listing.refs]

    async def call(
        self, slot: str, operation: str, fields: dict[str, Any] | None = None, body: str | os.PathLike[str] | None = None
    ) -> Response:
        """Calls an operation of an egress slot's interface (architecture §3.16.4).
        body is a file of the task's work area, which goes as the call's body; the
        response's body, when the operation has one, is left there too
        (implementation §3, Context Store and egress). A call whose rule ends in
        an audience returns once that audience answers, which can take as long
        as the person takes. A call the network does not execute raises
        grpc.aio.AioRpcError, with the rule that decided it."""
        rel = ""
        if body is not None:
            p = Path(body)
            rel = str(p.relative_to(self.work_area) if p.is_absolute() else p)
        resp = await self._session.stub.Call(
            pb.EgressRequest(task_id=self.task_id, slot=slot, operation=operation, fields=_encode(fields), body=rel)
        )
        return Response(json.loads(resp.fields) if resp.fields else {}, self.work_area / resp.body if resp.body else None)

    async def announce(
        self, topic: str, data: dict[str, Any], previous_attempt: Subtask | str | None = None
    ) -> Subtask:
        return await self._open(
            announce=pb.AnnounceTask(topic=topic, data=_encode(data), previous_attempt=_attempt(previous_attempt))
        )

    async def ask(self, data: dict[str, Any], previous_attempt: Subtask | str | None = None) -> Subtask:
        return await self._open(ask=pb.AskRequester(data=_encode(data), previous_attempt=_attempt(previous_attempt)))

    async def _open(self, **body: Any) -> Subtask:
        ack = await self._session.stub.Publish(pb.PublishRequest(task_id=self.task_id, **body))
        s = Subtask(ack.task_id, self._session.waiting(ack.task_id))
        self._subtasks.append(s)
        return s

    async def complete(self, data: dict[str, Any] | None = None) -> None:
        """Closes the task with task.completed once every subtask it opened has its
        terminal event (architecture §3.9.1)."""
        for s in self._subtasks:
            await s
        await self._close(complete=pb.CompleteTask(data=_encode(data)))

    async def fail(self, reason: str, data: dict[str, Any] | None = None, code: str = "") -> None:
        await self._close(fail=pb.FailTask(failure_reason=reason, data=_encode(data), failure_code=code))

    async def _close(self, **body: Any) -> None:
        if self.closed:
            raise RuntimeError(f"task {self.task_id} is already closed")
        await self._session.stub.Publish(pb.PublishRequest(task_id=self.task_id, **body))
        self.closed = True


def _attempt(a: Subtask | str | None) -> str:
    if a is None:
        return ""
    return a.task_id if isinstance(a, Subtask) else a


async def _pieces(content: bytes | str | Iterable[bytes] | AsyncIterable[bytes]):
    if isinstance(content, str):
        yield content.encode()
    elif isinstance(content, (bytes, bytearray, memoryview)):
        yield bytes(content)
    elif isinstance(content, AsyncIterable):
        async for piece in content:
            yield bytes(piece)
    else:
        for piece in content:
            yield bytes(piece)


class _Session:
    """One Attach: the tasks it delivered and the subtasks they wait on."""

    def __init__(self, stub: nri_pb2_grpc.NodeRuntimeStub, areas: str):
        self.stub = stub
        self.areas = areas
        self.out: asyncio.Queue[pb.LogicMessage] = asyncio.Queue()
        self.running: set[asyncio.Task[None]] = set()
        self._waiting: dict[str, asyncio.Future[Result]] = {}
        # Terminal events that arrived before the PublishAck of their subtask.
        self._orphans: dict[str, tuple[float, Result]] = {}

    def confirm(self, delivery_id: str) -> None:
        self.out.put_nowait(pb.LogicMessage(received=pb.Received(delivery_id=delivery_id)))

    def waiting(self, task_id: str) -> asyncio.Future[Result]:
        f = asyncio.get_running_loop().create_future()
        orphan = self._orphans.pop(task_id, None)
        if orphan is not None:
            f.set_result(orphan[1])
        else:
            self._waiting[task_id] = f
        return f

    def resolve(self, r: Result) -> None:
        f = self._waiting.pop(r.event["subject"], None)
        if f is not None:
            if not f.done():
                f.set_result(r)
            return
        # One no one waits on is discarded (implementation §1.2.13), once it is
        # clear it did not arrive ahead of its PublishAck.
        now = time.monotonic()
        self._orphans = {k: v for k, v in self._orphans.items() if now - v[0] < _ORPHAN_TTL}
        self._orphans[r.event["subject"]] = (now, r)

    async def requests(self):
        while True:
            yield await self.out.get()

    def end(self) -> None:
        for t in self.running:
            t.cancel()
        for f in self._waiting.values():
            f.cancel()


class Node:
    """The Logic Container: it opens the session and runs each task in its own coroutine."""

    def __init__(self, *, socket: str = SOCKET, areas: str = AREAS):
        self._socket = socket
        self._areas = areas
        self._on_task: Handler | None = None

    def on_task(self, handler: Handler) -> Handler:
        self._on_task = handler
        return handler

    def run(self) -> None:
        if not logging.getLogger().handlers:
            logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
        asyncio.run(self.serve())

    async def serve(self) -> None:
        """Holds a session with the Integration Sidecar, opening another when one ends."""
        if self._on_task is None:
            raise RuntimeError("the blueprint registers on_task")
        while True:
            try:
                await self.session()
            except grpc.aio.AioRpcError as e:
                log.warning("session ended: %s %s", e.code().name, e.details())
            await asyncio.sleep(_REDIAL)

    async def session(self) -> None:
        """One session: it waits for the socket, sends Hello and, once Accepted,
        takes each delivery until the session ends (implementation §2.4, §3)."""
        while not os.path.exists(self._socket):
            await asyncio.sleep(_REDIAL)
        async with grpc.aio.insecure_channel("unix://" + self._socket) as channel:
            s = _Session(nri_pb2_grpc.NodeRuntimeStub(channel), self._areas)
            s.out.put_nowait(pb.LogicMessage(hello=pb.Hello(minor=_MINOR)))
            call = s.stub.Attach(s.requests())
            try:
                first = await call.read()
                if first is grpc.aio.EOF or first.WhichOneof("body") != "accepted":
                    log.warning("the session did not open with Accepted")
                    return
                log.info("session open, NRI %s", NRI_VERSION)
                while (m := await call.read()) is not grpc.aio.EOF:
                    self._deliver(s, m)
            finally:
                # The Integration Sidecar failed the tasks of a cut session
                # (architecture §3.6.1): no request of theirs would be taken.
                call.cancel()
                s.end()

    def _deliver(self, s: _Session, m: pb.SidecarMessage) -> None:
        kind = m.WhichOneof("body")
        d = getattr(m, kind)
        s.confirm(d.delivery_id)
        if kind == "assignment":
            t = asyncio.create_task(self._run(Task(s, d)))
            s.running.add(t)
            t.add_done_callback(s.running.discard)
        elif kind == "result":
            s.resolve(Result(json.loads(d.event)))
        else:
            log.warning("discarding a %s delivery: the blueprint has no handler for it", kind)

    async def _run(self, task: Task) -> None:
        try:
            await self._on_task(task)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if task.closed:
                log.exception("task %s raised after closing", task.task_id)
                return
            await _fail_with(task, exc)
            return
        if not task.closed:
            await _close_quietly(task.fail(UNCLOSED_REASON), task)


def summary(exc: BaseException) -> str:
    """The last line of the traceback: the exception's type and the first line of
    its message, up to REASON_LIMIT bytes (implementation §1.2.13)."""
    lines = traceback.format_exception_only(exc)[0].splitlines()
    first = lines[0] if lines else type(exc).__name__
    return first.encode()[:REASON_LIMIT].decode(errors="ignore")


async def _fail_with(task: Task, exc: BaseException) -> None:
    data = None
    try:
        tb = "".join(traceback.format_exception(exc))
        data = {"context_ref": await task.write_context(TRACEBACK_PORTION, tb)}
    except Exception:
        log.exception("task %s: writing its traceback", task.task_id)
    await _close_quietly(task.fail(summary(exc), data), task)


async def _close_quietly(close: Awaitable[None], task: Task) -> None:
    try:
        await close
    except Exception:
        log.exception("task %s: failing it", task.task_id)
