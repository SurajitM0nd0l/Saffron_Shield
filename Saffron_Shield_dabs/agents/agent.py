# ============================================================
# SAFFRON SHIELD - R2 TOOL-CALLING AGENT
# Framework : LangGraph
# LLM       : Databricks Meta Llama 3.3 70B Instruct
# Tools     : Unity Catalog Functions
# Tracing   : MLflow
# ============================================================

import re
import json
from pathlib import Path
from typing import TypedDict

import mlflow

from databricks_langchain import (
    ChatDatabricks,
    UCFunctionToolkit
)

from unitycatalog.ai.core.databricks import DatabricksFunctionClient

from langgraph.prebuilt import create_react_agent
from langgraph.graph import StateGraph, START, END
from langchain_core.messages import HumanMessage


# ============================================================
# 1. CONFIGURATION
# ============================================================

BASE_PATH = (
    "/Workspace/Users/surajitm0nd0l12@gmail.com/"
    "Saffron_Shield/Saffron_Shield_dabs/src"
)

PROMPT_PATH = (
    f"{BASE_PATH}/agent/prompts/system_v1.md"
)

LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

EXPERIMENT_PATH = (
    "/Users/surajitm0nd0l12@gmail.com/"
    "Saffron_Shield/R2_Tool_Calling_Agent"
)

UC_FUNCTIONS = [
    "claims.genai.lookup_claim",
    "claims.genai.plan_at",
    "claims.genai.search_policy",
    "claims.genai.compute_payable"
]


# ============================================================
# 2. LOAD SYSTEM PROMPT
# ============================================================

with open(PROMPT_PATH, "r", encoding="utf-8") as file:
    SYSTEM_PROMPT = file.read()


# ============================================================
# 3. CONFIGURE MLFLOW TRACING
# ============================================================

mlflow.set_tracking_uri("databricks")

mlflow.set_experiment(EXPERIMENT_PATH)

# Enable automatic tracing for LangChain and LangGraph.
# Call this before invoking the agent.
mlflow.langchain.autolog()


# ============================================================
# 4. INITIALIZE DATABRICKS LLM
# ============================================================

llm = ChatDatabricks(
    endpoint=LLM_ENDPOINT,
    temperature=0.0
)


# ============================================================
# 5. CONNECT TO UNITY CATALOG FUNCTIONS
# ============================================================

function_client = DatabricksFunctionClient()

toolkit = UCFunctionToolkit(
    function_names=UC_FUNCTIONS,
    client=function_client
)

all_tools = toolkit.tools


# Map the generated LangChain tool names to UC function names.
# This avoids assuming how the toolkit formats tool names.

def find_uc_tool(function_suffix):

    matches = [
        tool for tool in all_tools
        if tool.name.endswith(function_suffix)
    ]

    if len(matches) != 1:
        raise RuntimeError(
            f"Expected exactly one tool ending in "
            f"'{function_suffix}', found: "
            f"{[tool.name for tool in matches]}"
        )

    return matches[0]


lookup_claim_tool = find_uc_tool("lookup_claim")
plan_at_tool = find_uc_tool("plan_at")
search_policy_tool = find_uc_tool("search_policy")
compute_payable_tool = find_uc_tool("compute_payable")


# Display registered tools.

print("REGISTERED UNITY CATALOG TOOLS")

for tool in all_tools:
    print(
        f"\nTool name: {tool.name}\n"
        f"Description: {tool.description}\n"
        f"Arguments: {tool.args}"
    )


# ============================================================
# 6. CREATE ROUTE-SPECIFIC LANGGRAPH AGENTS
# ============================================================

STATUS_PROMPT = SYSTEM_PROMPT + """

ADDITIONAL STATUS ROUTING INSTRUCTIONS:

For a claim-status question:

1. You MUST call lookup_claim first.
2. Use the returned member_id and admission_date.
3. You MUST then call plan_at using those exact returned values.
4. Answer the status using the status field returned by lookup_claim.
5. Preserve the exact status value. Do not paraphrase it.

Do not call any other tool.
"""

POLICY_PROMPT = SYSTEM_PROMPT + """

ADDITIONAL POLICY ROUTING INSTRUCTIONS:

For a policy question:

1. You MUST call search_policy.
2. Use only the returned policy chunks to formulate the answer.
3. Cite the actual document name and heading.
4. Include the actual bare chunk_id in your answer.
5. For corporate group PED questions, use evidence from
   03_Waiting_Periods_Group_Plans.md.
6. Do not use individual-plan documents as evidence.

Do not call any other tool.
"""

PLAN_PROMPT = SYSTEM_PROMPT + """

For member-plan questions, use plan_at only.
Do not call other tools.
"""

PAYOUT_PROMPT = SYSTEM_PROMPT + """

For payout calculation questions, use compute_payable only.
Do not calculate the payout yourself.
Do not call other tools.
"""


# Status agent: only lookup_claim and plan_at are available.
status_agent = create_react_agent(
    model=llm,
    tools=[
        lookup_claim_tool,
        plan_at_tool
    ],
    prompt=STATUS_PROMPT
)


# Policy agent: only search_policy is available.
policy_agent = create_react_agent(
    model=llm,
    tools=[
        search_policy_tool
    ],
    prompt=POLICY_PROMPT
)


# Plan agent: only plan_at is available.
plan_agent = create_react_agent(
    model=llm,
    tools=[
        plan_at_tool
    ],
    prompt=PLAN_PROMPT
)


# Payout agent: only compute_payable is available.
payout_agent = create_react_agent(
    model=llm,
    tools=[
        compute_payable_tool
    ],
    prompt=PAYOUT_PROMPT
)


# ============================================================
# 7. DEFINE OUTER LANGGRAPH STATE
# ============================================================

class AgentState(TypedDict):
    question: str
    route: str
    answer: str


# ============================================================
# 8. ROUTE THE USER QUESTION
# ============================================================

def route_question(question: str) -> str:

    q = question.lower()

    has_claim_id = bool(
        re.search(r"\bclm-[a-z0-9-]+\b", q)
    )

    # Claim status takes priority.
    if has_claim_id and any(
        word in q for word in ["status", "state", "progress"]
    ):
        return "status"

    # Policy document questions.
    if any(
        word in q for word in [
            "policy",
            "waiting period",
            "pre-existing",
            "pre existing",
            "ped",
            "exclusion",
            "coverage"
        ]
    ):
        return "policy"

    # Payout questions.
    if any(
        word in q for word in [
            "payable amount",
            "payout",
            "calculate payable",
            "how much will be paid"
        ]
    ):
        return "payout"

    # Member plan questions.
    if "plan" in q and (
        "member" in q or "policy" in q
    ):
        return "plan"

    # General claim-fact lookup.
    if has_claim_id:
        return "status"

    return "unknown"


def classify_request(state: AgentState):

    route = route_question(state["question"])

    print(f"\nSelected route: {route}")

    return {"route": route}


# ============================================================
# 9. AGENT EXECUTION NODES
# ============================================================

def execute_status_agent(state: AgentState):

    result = status_agent.invoke({
        "messages": [
            HumanMessage(content=state["question"])
        ]
    })

    final_message = result["messages"][-1]

    return {
        "answer": final_message.content
    }


def execute_policy_agent(state: AgentState):

    result = policy_agent.invoke({
        "messages": [
            HumanMessage(content=state["question"])
        ]
    })

    final_message = result["messages"][-1]

    return {
        "answer": final_message.content
    }


def execute_plan_agent(state: AgentState):

    result = plan_agent.invoke({
        "messages": [
            HumanMessage(content=state["question"])
        ]
    })

    final_message = result["messages"][-1]

    return {
        "answer": final_message.content
    }


def execute_payout_agent(state: AgentState):

    result = payout_agent.invoke({
        "messages": [
            HumanMessage(content=state["question"])
        ]
    })

    final_message = result["messages"][-1]

    return {
        "answer": final_message.content
    }


def unknown_request(state: AgentState):

    return {
        "answer": (
            "I could not determine which claims tool is "
            "appropriate for this question. Please clarify "
            "whether you need claim facts, plan information, "
            "policy evidence, or a payout calculation."
        )
    }


# ============================================================
# 10. BUILD THE OUTER LANGGRAPH
# ============================================================

workflow = StateGraph(AgentState)

workflow.add_node("router", classify_request)
workflow.add_node("status", execute_status_agent)
workflow.add_node("policy", execute_policy_agent)
workflow.add_node("plan", execute_plan_agent)
workflow.add_node("payout", execute_payout_agent)
workflow.add_node("unknown", unknown_request)


workflow.add_edge(START, "router")


workflow.add_conditional_edges(
    "router",
    lambda state: state["route"],
    {
        "status": "status",
        "policy": "policy",
        "plan": "plan",
        "payout": "payout",
        "unknown": "unknown"
    }
)


workflow.add_edge("status", END)
workflow.add_edge("policy", END)
workflow.add_edge("plan", END)
workflow.add_edge("payout", END)
workflow.add_edge("unknown", END)


agent = workflow.compile()


# ============================================================
# 11. RUN THE AGENT AND CAPTURE MLFLOW TRACE
# ============================================================

def run_agent(question: str):

    print("\n" + "=" * 70)
    print("USER QUESTION:", question)
    print("=" * 70)

    result = agent.invoke({
        "question": question,
        "route": "",
        "answer": ""
    })

    answer = result["answer"]

    # Retrieve the trace created by LangGraph autologging.
    trace_id = mlflow.get_last_active_trace_id(
        thread_local=True
    )

    if trace_id is None:
        raise RuntimeError(
            "No MLflow trace was generated. "
            "Check mlflow.langchain.autolog()."
        )

    # Flush and retrieve the actual trace.
    trace = mlflow.get_trace(
        trace_id,
        flush=True
    )

    print("\nFINAL ANSWER:")
    print(answer)

    print("\nMLFLOW TRACE ID:")
    print(trace_id)

    print("\nTRACE SPANS:")

    for span in trace.search_spans():

        print({
            "name": span.name,
            "span_type": str(span.span_type),
            "inputs": span.inputs,
            "outputs": span.outputs
        })

    return {
        "question": question,
        "route": result["route"],
        "final_answer": answer,
        "trace_id": trace_id,
        "trace": trace
    }


# ============================================================
# END OF AGENT IMPLEMENTATION
# ============================================================
