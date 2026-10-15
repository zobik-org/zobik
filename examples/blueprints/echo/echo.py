"""echo: leaves the text of its task in the Context Store and completes with its reference."""

from zobik import Node

node = Node()


@node.on_task
async def echo(task):
    ref = await task.write_context("result", task.data["task_text"])
    await task.complete({"context_ref": ref})


if __name__ == "__main__":
    node.run()
