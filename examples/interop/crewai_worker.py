from __future__ import annotations

from janus_sdk import JanusClient, JanusWorker, WorkerResult


def build_crew():
    try:
        from crewai import Agent, Crew, Task

        researcher = Agent(
            role="Researcher",
            goal="Process incoming tasks",
            backstory="A diligent worker agent",
        )
        return researcher
    except ImportError:
        return None


def main():
    client = JanusClient(base_url="http://localhost:8080", tenant_id="acme")
    crew_agent = build_crew()

    def handler(task_id: str, content: str, agent_id: str) -> WorkerResult:
        if crew_agent is not None:
            result = crew_agent.execute_task(content)
            return WorkerResult(result_ref=f"crewai://{result}")
        return WorkerResult(result_ref=f"fallback://{content}")

    worker = JanusWorker(client=client, mailbox_id="crewai-mb", agent_id="crewai-agent")
    print("CrewAI worker started, polling for tasks...")
    worker.run(handler)


if __name__ == "__main__":
    main()
