"""historian: asks trace_query for the reconstruction of its own trace, leaves it in
the Context Store and completes with its reference."""

from zobik import Node

node = Node()


@node.on_task
async def historian(task):
    # The queried trace is the one of the announcement's traceparent (architecture §3.10).
    query = await (await task.announce("trace_query", {}))
    if not query.ok:
        raise RuntimeError(f"trace_query ended with {query.type}: {query.data.get('failure_reason', '')}")
    reconstruction = await task.read_context(query.data["context_ref"])
    # The node's log shows it to whoever runs the stage by hand.
    print(reconstruction.decode(), flush=True)
    ref = await task.write_context("result", reconstruction)
    await task.complete({"context_ref": ref})


if __name__ == "__main__":
    node.run()
