from __future__ import annotations

from janus_broker import JanusClient, JanusWorker, WorkerResult


def build_team():
    try:
        import autogen

        assistant = autogen.AssistantAgent(
            name="janus_worker",
            llm_config={"model": "gpt-4", "api_type": "openai"},
        )
        return assistant
    except ImportError:
        return None


def main():
    client = JanusClient(base_url="http://localhost:8080", tenant_id="acme")
    assistant = build_team()

    def handler(task_id: str, content: str, agent_id: str) -> WorkerResult:
        if assistant is not None:
            reply = assistant.generate_reply(messages=[{"content": content, "role": "user"}])
            return WorkerResult(result_ref=f"autogen://{reply}")
        return WorkerResult(result_ref=f"fallback://{content}")

    worker = JanusWorker(client=client, mailbox_id="autogen-mb", agent_id="autogen-agent")
    print("AutoGen worker started, polling for tasks...")
    worker.run(handler)


if __name__ == "__main__":
    main()
