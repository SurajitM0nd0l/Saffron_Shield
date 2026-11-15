import mlflow
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks_langchain import (
    ChatDatabricks,
    DatabricksMCPServer,
    DatabricksMultiServerMCPClient
)
from langgraph.prebuilt import create_react_agent
from langchain_core.messages import AIMessage

workspace_client = WorkspaceClient()

SERVER_URL = (
    f"{workspace_client.config.host}"
    "/api/2.0/mcp/functions/claims/genai"
)

mcp_langchain_client = DatabricksMultiServerMCPClient([
    DatabricksMCPServer(
        name="claims-genai",
        url=SERVER_URL,
        workspace_client=workspace_client
    )
])

# Tool discovery requires await, which is not allowed at module
# level in an importable module, so the agent is built lazily in
# _get_agent() below.

llm = ChatDatabricks(
    endpoint="databricks-meta-llama-3-3-70b-instruct",
    temperature=0
)

SYSTEM_PROMPT = """
You are the Saffron Shield claims assistant.

Use the available Unity Catalog MCP tools to answer claims
and policy questions.

Use lookup_claim for claim facts.
Use plan_at to identify the plan valid on the relevant date.
Use search_policy for policy evidence.
Use compute_payable for deterministic payout calculations.

Never invent claim facts or promise a payout without checking
the relevant claim and policy information.
"""

agent = None


async def _get_agent():
    """Build the agent lazily so this module stays importable."""
    global agent

    if agent is None:
        # Discover tools from the MCP server.
        agent_tools = await mcp_langchain_client.get_tools()

        print("Agent MCP tools:")
        for tool in agent_tools:
            print(tool.name)

        agent = create_react_agent(
            model=llm,
            tools=agent_tools,
            prompt=SYSTEM_PROMPT
        )

    return agent

mlflow.set_tracking_uri("databricks")
mlflow.set_experiment(
    "/Users/surajitm0nd0l12@gmail.com/Saffron_Shield_R6"
)

@mlflow.trace(name="r6_mcp_agent")
async def run_mcp_agent(question):
    # MCP tools are async-only (StructuredTool with coroutine only),
    # so the agent must be built and invoked asynchronously.
    agent = await _get_agent()

    result = await agent.ainvoke({
        "messages": [
            {"role": "user", "content": question}
        ]
    })

    messages = result["messages"]

    tool_calls = []

    for message in messages:
        for call in getattr(message, "tool_calls", []):
            tool_calls.append({
                "name": call["name"],
                "args": call["args"]
            })

    final_answer = next(
        (
            message.content
            for message in reversed(messages)
            if isinstance(message, AIMessage)
            and isinstance(message.content, str)
            and message.content.strip()
        ),
        ""
    )

    return {
        "tool_calls": tool_calls,
        "final_answer": final_answer,
        "cited_chunk_ids": []
    }