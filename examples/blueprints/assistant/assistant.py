"""assistant: answers its task's text with a language model, through a LangGraph
graph over the model slot, and completes with the answer's reference.

The call to the model follows the slot's entry rules: with one that ends in an
audience, it waits for that audience's approval.
"""

from typing import Annotated, TypedDict

from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages

from zobik import Node
from zobik_openai import ChatResponses

# The dated snapshot, so the model the response names is the one the rate table prices.
MODEL = "gpt-5-mini-2025-08-07"
INSTRUCTIONS = "Answer the request of the person who wrote it, in the language they wrote it in."

node = Node()


class State(TypedDict):
    messages: Annotated[list, add_messages]


def graph(model: ChatResponses):
    async def respond(state: State) -> State:
        return {"messages": [await model.ainvoke(state["messages"])]}

    g = StateGraph(State)
    g.add_node("respond", respond)
    g.add_edge(START, "respond")
    g.add_edge("respond", END)
    return g.compile()


@node.on_task
async def answer(task):
    model = ChatResponses(task=task, model=MODEL, reasoning={"effort": "low"})
    state = await graph(model).ainvoke(
        {"messages": [SystemMessage(INSTRUCTIONS), HumanMessage(task.data["task_text"])]}
    )
    ref = await task.write_context("result", state["messages"][-1].content)
    await task.complete({"context_ref": ref})


if __name__ == "__main__":
    node.run()
