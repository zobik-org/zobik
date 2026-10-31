import unittest

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

from zobik import Response
from zobik_openai import OPERATION, ChatResponses, ModelError


class FakeTask:
    """Records each call and answers it with the next response."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    async def call(self, slot, operation, fields=None, body=None):
        self.calls.append((slot, operation, fields))
        return Response(self.responses.pop(0))


def answer(text, status=200):
    return {
        "status": status,
        "model": "gpt-5-mini-2025-08-07",
        "output": [
            {"type": "reasoning", "summary": []},
            {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": text}]},
        ],
        "usage": {"input_tokens": 12, "input_tokens_details": {"cached_tokens": 0}, "output_tokens": 5},
    }


class ChatResponsesTest(unittest.IsolatedAsyncioTestCase):
    async def test_call(self):
        task = FakeTask(answer("Paris."))
        model = ChatResponses(task=task, model="gpt-5-mini", reasoning={"effort": "low"})
        out = await model.ainvoke(
            [SystemMessage("Answer briefly."), HumanMessage("Capital of France?"), AIMessage("Paris."), HumanMessage("Sure?")]
        )
        self.assertEqual(out.content, "Paris.")
        self.assertEqual(out.usage_metadata["input_tokens"], 12)
        self.assertEqual(out.usage_metadata["total_tokens"], 17)
        self.assertEqual(out.response_metadata["model_name"], "gpt-5-mini-2025-08-07")
        slot, operation, fields = task.calls[0]
        self.assertEqual((slot, operation), ("model", OPERATION))
        self.assertEqual(
            fields,
            {
                "model": "gpt-5-mini",
                "instructions": "Answer briefly.",
                "input": [
                    {"role": "user", "content": "Capital of France?"},
                    {"role": "assistant", "content": "Paris."},
                    {"role": "user", "content": "Sure?"},
                ],
                "store": False,
                "reasoning": {"effort": "low"},
            },
        )

    async def test_error_response(self):
        model = ChatResponses(task=FakeTask(answer("", status=429)), model="gpt-5-mini")
        with self.assertRaisesRegex(ModelError, "429"):
            await model.ainvoke([HumanMessage("hi")])

    async def test_no_text(self):
        empty = answer("x")
        empty["output"] = [{"type": "reasoning", "summary": []}]
        model = ChatResponses(task=FakeTask(empty), model="gpt-5-mini")
        with self.assertRaises(ModelError):
            await model.ainvoke([HumanMessage("hi")])

    async def test_unsupported_message(self):
        model = ChatResponses(task=FakeTask(), model="gpt-5-mini")
        with self.assertRaises(ValueError):
            await model.ainvoke([ToolMessage("42", tool_call_id="c1")])

    def test_sync(self):
        model = ChatResponses(task=FakeTask(), model="gpt-5-mini")
        with self.assertRaises(NotImplementedError):
            model.invoke([HumanMessage("hi")])


if __name__ == "__main__":
    unittest.main()
