"""The model adapter of the OpenAI variant: a LangChain chat model whose calls go
through the model slot, the Responses API's create_response (implementation §7.2).

The model is a field of the call, set by each member (implementation §7.1). Each
call carries store = false.
"""

from __future__ import annotations

from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from pydantic import ConfigDict

OPERATION = "create_response"

_ROLES = {"human": "user", "ai": "assistant"}


class ModelError(Exception):
    """A response the model slot returned without an answer: an HTTP error, or an
    output with no text."""


class ChatResponses(BaseChatModel):
    """A chat model on a task's egress slot. It only runs asynchronously, because
    the call is a request of the task's session (implementation §1.2.13)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    task: Any
    model: str
    slot: str = "model"
    reasoning: dict[str, Any] | None = None
    max_output_tokens: int | None = None

    @property
    def _llm_type(self) -> str:
        return "zobik-openai-responses"

    def _generate(self, messages: list[BaseMessage], stop: list[str] | None = None, **kwargs: Any) -> ChatResult:
        raise NotImplementedError("ChatResponses runs only asynchronously: use ainvoke")

    async def _agenerate(
        self, messages: list[BaseMessage], stop: list[str] | None = None, **kwargs: Any
    ) -> ChatResult:
        if stop:
            raise ValueError("the Responses API has no stop sequences")
        response = await self.task.call(self.slot, OPERATION, self.fields(messages))
        return ChatResult(generations=[ChatGeneration(message=message(response.fields))])

    def fields(self, messages: list[BaseMessage]) -> dict[str, Any]:
        """The fields of the call: the system messages as instructions, the rest as input."""
        instructions = [_text(m) for m in messages if m.type == "system"]
        items = []
        for m in messages:
            if m.type == "system":
                continue
            if m.type not in _ROLES:
                raise ValueError(f"a {m.type} message has no place in the call")
            items.append({"role": _ROLES[m.type], "content": _text(m)})
        fields: dict[str, Any] = {"model": self.model, "input": items, "store": False}
        if instructions:
            fields["instructions"] = "\n\n".join(instructions)
        if self.reasoning is not None:
            fields["reasoning"] = self.reasoning
        if self.max_output_tokens is not None:
            fields["max_output_tokens"] = self.max_output_tokens
        return fields


def message(fields: dict[str, Any]) -> AIMessage:
    """The answer of a response: the text of its message items, with its usage."""
    status = fields.get("status")
    if status is None or status >= 400:
        raise ModelError(f"the model slot returned HTTP {status}")
    parts = [
        c.get("text", "")
        for item in fields.get("output") or []
        if item.get("type") == "message"
        for c in item.get("content") or []
        if c.get("type") == "output_text"
    ]
    if not parts:
        raise ModelError("the response has no output text")
    usage = fields.get("usage") or {}
    tokens_in, tokens_out = usage.get("input_tokens", 0), usage.get("output_tokens", 0)
    return AIMessage(
        content="".join(parts),
        usage_metadata={"input_tokens": tokens_in, "output_tokens": tokens_out, "total_tokens": tokens_in + tokens_out},
        response_metadata={"model_name": fields.get("model")},
    )


def _text(m: BaseMessage) -> str:
    if isinstance(m.content, str):
        return m.content
    return "".join(b.get("text", "") for b in m.content if isinstance(b, dict) and b.get("type") == "text")
