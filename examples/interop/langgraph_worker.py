from __future__ import annotations

from janus_sdk import JanusClient, JanusWorker, WorkerResult


def build_graph():
    try:
        from langgraph.graph import StateGraph, END
        from typing import TypedDict

        class WorkerState(TypedDict):
            task_id: str
            content: str
            result: str

        def process_node(state: WorkerState) -> WorkerState:
            state["result"] = f"Processed by LangGraph: {state['content']}"
            return state

        graph = StateGraph(WorkerState)
        graph.add_node("process", process_node)
        graph.set_entry_point("process")
        graph.add_edge("process", END)
        return graph.compile()
    except ImportError:
        return None


def main():
    client = JanusClient(
        base_url="http://localhost:8080",
        tenant_id="acme",
    )

    graph = build_graph()

    def handler(task_id: str, content: str, agent_id: str) -> WorkerResult:
        if graph is not None:
            result = graph.invoke({"task_id": task_id, "content": content, "result": ""})
            return WorkerResult(result_ref=f"langgraph://{result['result']}")
        return WorkerResult(result_ref=f"fallback://{content}")

    worker = JanusWorker(
        client=client,
        mailbox_id="langgraph-mb",
        agent_id="langgraph-agent",
    )

    print("LangGraph worker started, polling for tasks...")
    worker.run(handler)


if __name__ == "__main__":
    main()
