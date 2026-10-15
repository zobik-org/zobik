"""Tests of the library against a fake Integration Sidecar on a Unix socket.

Run from sdk/python: PYTHONPATH=src python -m unittest discover -s tests
"""

import asyncio
import json
import re
import tempfile
import unittest
from pathlib import Path

import grpc

import zobik
from zobik import node as nodemod
from zobik.nri.v1 import nri_pb2 as pb
from zobik.nri.v1 import nri_pb2_grpc

REPO = Path(__file__).resolve().parents[3]


def assignment(task_id, data):
    event = {"type": "task.assigned", "subject": task_id, "id": "assigned-" + task_id, "data": {}}
    announcement = {"type": "task.announced", "subject": task_id, "data": data}
    return pb.SidecarMessage(
        assignment=pb.Delivery(
            delivery_id="assigned-" + task_id,
            event=json.dumps(event).encode(),
            announcement=json.dumps(announcement).encode(),
        )
    )


def result(task_id, typ="task.completed", data=None):
    event = {"type": typ, "subject": task_id, "id": "result-" + task_id, "data": data or {}}
    return pb.SidecarMessage(result=pb.Delivery(delivery_id="result-" + task_id, event=json.dumps(event).encode()))


class FakeSidecar(nri_pb2_grpc.NodeRuntimeServicer):
    """Serves the NRI the way the Integration Sidecar does, recording what it receives."""

    def __init__(self):
        self.hello = asyncio.get_running_loop().create_future()
        self.to_send: asyncio.Queue = asyncio.Queue()
        self.received: list[str] = []
        self.published: asyncio.Queue = asyncio.Queue()
        self.portions: dict[tuple[str, str], bytes] = {}
        self.subtask_ids = iter(f"sub-{i}" for i in range(100))
        # before_ack, if set, runs before a PublishAck of a subtask leaves.
        self.before_ack = None
        self.fail_writes = False

    async def Attach(self, request_iterator, context):
        first = await request_iterator.__anext__()
        self.hello.set_result(first.hello.minor)
        yield pb.SidecarMessage(accepted=pb.Accepted())

        async def read():
            async for m in request_iterator:
                self.received.append(m.received.delivery_id)

        reader = asyncio.ensure_future(read())
        try:
            while True:
                yield await self.to_send.get()
        finally:
            reader.cancel()

    async def Publish(self, request, context):
        await self.published.put(request)
        if request.WhichOneof("body") in ("announce", "ask"):
            sub = next(self.subtask_ids)
            if self.before_ack:
                await self.before_ack(sub)
            return pb.PublishAck(task_id=sub)
        return pb.PublishAck()

    async def WriteContext(self, request_iterator, context):
        header, body = None, b""
        async for m in request_iterator:
            if m.WhichOneof("body") == "header":
                header = m.header
            else:
                body += m.chunk
        if self.fail_writes:
            await context.abort(grpc.StatusCode.UNAVAILABLE, "the Context Store is down")
        self.portions[(header.task_id, header.name)] = body
        return pb.ContextRef(trace_id="trace", task_id=header.task_id, name=header.name)

    async def ReadContext(self, request, context):
        yield pb.Chunk(data=self.portions[(request.ref.task_id, request.ref.name)])


class LibraryTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.socket = str(Path(self.dir.name) / "nri.sock")
        self.fake = FakeSidecar()
        self.server = grpc.aio.server()
        nri_pb2_grpc.add_NodeRuntimeServicer_to_server(self.fake, self.server)
        self.server.add_insecure_port("unix://" + self.socket)
        self.node = zobik.Node(socket=self.socket, areas=self.dir.name)

    async def asyncTearDown(self):
        self.serving.cancel()
        await self.server.stop(None)
        self.dir.cleanup()

    async def start(self, handler, server_first=True):
        self.node.on_task(handler)
        if server_first:
            await self.server.start()
        self.serving = asyncio.ensure_future(self.node.serve())

    async def closing(self, kind):
        while True:
            r = await asyncio.wait_for(self.fake.published.get(), 5)
            if r.WhichOneof("body") == kind:
                return r

    async def test_echo(self):
        async def echo(task):
            ref = await task.write_context("result", task.data["task_text"])
            await task.complete({"context_ref": ref})

        await self.start(echo)
        self.assertEqual(await asyncio.wait_for(self.fake.hello, 5), 0)
        await self.fake.to_send.put(assignment("t1", {"task_text": "hola"}))
        done = await self.closing("complete")
        self.assertEqual(done.task_id, "t1")
        self.assertEqual(
            json.loads(done.complete.data),
            {"context_ref": {"trace_id": "trace", "task_id": "t1", "name": "result"}},
        )
        self.assertEqual(self.fake.portions[("t1", "result")], b"hola")
        self.assertIn("assigned-t1", self.fake.received)

    async def test_dials_until_the_socket_exists(self):
        async def complete(task):
            await task.complete()

        await self.start(complete, server_first=False)
        await asyncio.sleep(1.5)
        await self.server.start()
        self.assertEqual(await asyncio.wait_for(self.fake.hello, 5), 0)

    async def test_an_exception_fails_with_its_last_line_and_traceback(self):
        async def broken(task):
            raise KeyError("task_text")

        await self.start(broken)
        await self.fake.to_send.put(assignment("t1", {}))
        failed = await self.closing("fail")
        self.assertEqual(failed.fail.failure_reason, "KeyError: 'task_text'")
        ref = json.loads(failed.fail.data)["context_ref"]
        self.assertEqual(ref["name"], zobik.node.TRACEBACK_PORTION)
        tb = self.fake.portions[("t1", ref["name"])].decode()
        self.assertIn("Traceback", tb)
        self.assertIn('raise KeyError("task_text")', tb)

    async def test_a_failed_traceback_write_still_fails_the_task(self):
        async def broken(task):
            raise RuntimeError("boom")

        self.fake.fail_writes = True
        await self.start(broken)
        await self.fake.to_send.put(assignment("t1", {}))
        failed = await self.closing("fail")
        self.assertEqual(failed.fail.failure_reason, "RuntimeError: boom")
        self.assertEqual(json.loads(failed.fail.data), {})

    async def test_returning_without_closing_fails(self):
        async def forgetful(task):
            pass

        await self.start(forgetful)
        await self.fake.to_send.put(assignment("t1", {}))
        failed = await self.closing("fail")
        self.assertEqual(failed.fail.failure_reason, nodemod.UNCLOSED_REASON)

    async def test_complete_waits_for_the_subtasks(self):
        opened = asyncio.get_running_loop().create_future()

        async def parent(task):
            sub = await task.announce("echo", {"task_text": "child"})
            opened.set_result(sub.task_id)
            await task.complete({"child": sub.task_id})

        await self.start(parent)
        await self.fake.to_send.put(assignment("t1", {}))
        sub = await asyncio.wait_for(opened, 5)
        await asyncio.sleep(0.2)
        kinds = []
        while not self.fake.published.empty():
            kinds.append(self.fake.published.get_nowait().WhichOneof("body"))
        self.assertEqual(kinds, ["announce"])
        await self.fake.to_send.put(result(sub))
        done = await self.closing("complete")
        self.assertEqual(json.loads(done.complete.data), {"child": sub})
        self.assertIn("result-" + sub, self.fake.received)

    async def test_a_result_ahead_of_its_ack_reaches_the_subtask(self):
        async def ahead(sub):
            await self.fake.to_send.put(result(sub, "task.failed", {"failure_reason": "no"}))
            await asyncio.sleep(0.2)

        self.fake.before_ack = ahead

        async def parent(task):
            r = await (await task.ask({"kind": "confirm"}))
            await task.complete({"ok": r.ok, "reason": r.data["failure_reason"]})

        await self.start(parent)
        await self.fake.to_send.put(assignment("t1", {}))
        done = await self.closing("complete")
        self.assertEqual(json.loads(done.complete.data), {"ok": False, "reason": "no"})

class PlainTest(unittest.TestCase):
    def test_the_summary_is_the_first_line_bounded(self):
        s = nodemod.summary(ValueError("ñ" * 600 + "\nsecond line"))
        self.assertTrue(s.startswith("ValueError: ñ"))
        self.assertLessEqual(len(s.encode()), nodemod.REASON_LIMIT)
        self.assertNotIn("second", s)
        self.assertEqual(nodemod.summary(TimeoutError()), "TimeoutError")

    def test_the_base_image_declares_the_library_version(self):
        dockerfile = (REPO / "images" / "zobik-python.Dockerfile").read_text()
        labels = re.findall(r'^LABEL io\.zobik\.nri\.version="([^"]+)"$', dockerfile, re.M)
        self.assertEqual(labels, [zobik.NRI_VERSION])


if __name__ == "__main__":
    unittest.main()
