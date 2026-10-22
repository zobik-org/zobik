"""echo: leaves the text of its task in the Context Store and completes with its reference.

A text that starts with "?" is first asked to whoever originated the trace,
with the options yes and no, and the answer goes after the text.
"""

from zobik import Node

node = Node()


@node.on_task
async def echo(task):
    text = task.data["task_text"]
    if text.startswith("?"):
        question = text[1:].strip()
        query = await task.ask({"task_text": question, "options": ["yes", "no"]})
        result = await query
        answer = result.data.get("decision") if result.ok else None
        text = f"{question}\n→ {answer or 'no answer'}"
    ref = await task.write_context("result", text)
    await task.complete({"context_ref": ref})


if __name__ == "__main__":
    node.run()
