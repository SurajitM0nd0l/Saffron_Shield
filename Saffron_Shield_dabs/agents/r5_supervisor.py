# /// script
# [tool.databricks.environment]
# dependencies = [
#   "unitycatalog-langchain[databricks]",
#   "databricks-langchain",
#   "langgraph==1.0.10",
# ]
# ///

# Step 1: Initialize the UC tools
import json
import re
import csv
from io import StringIO
from pathlib import Path

import mlflow

from databricks_langchain import ChatDatabricks, UCFunctionToolkit
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

from langgraph.graph import StateGraph, END
from typing import TypedDict, Any


MODEL_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

uc_client = DatabricksFunctionClient()

toolkit = UCFunctionToolkit(
    function_names=[
        "claims.genai.lookup_claim",
        "claims.genai.plan_at",
        "claims.genai.search_policy",
        "claims.genai.compute_payable"
    ],
    client=uc_client
)

tools = {
    tool.name.split("__")[-1]: tool
    for tool in toolkit.tools
}

llm = ChatDatabricks(
    endpoint=MODEL_ENDPOINT,
    temperature=0
)

# Step 2: Added a tool execution player
def call_tool(name, args):
    tool = tools[name]
    return tool.invoke(args)


def parse_rows(result):
    if isinstance(result, str):
        result = json.loads(result)

    if isinstance(result, dict) and "value" in result:
        reader = csv.DictReader(StringIO(result["value"]))
        return list(reader)

    if isinstance(result, list):
        return result

    if isinstance(result, dict):
        return [result]

    return []

# Step 3: Define the supervisor state
class R5State(TypedDict):
    question: str
    claim_id: str
    claim: dict
    plan: dict
    policy_chunks: list
    payout: dict
    verified_payout: dict
    verifier_agrees: bool
    route: list
    specialists: dict
    verdict: dict


# Step 4: Implement the data specalist 
def data_agent(state: R5State):
    claim_rows = parse_rows(
        call_tool(
            "lookup_claim",
            {"claim_id": state["claim_id"]}
        )
    )

    if not claim_rows:
        raise ValueError("Claim not found")

    claim = claim_rows[0]

    plan_rows = parse_rows(
        call_tool(
            "plan_at",
            {
                "member_id": claim["member_id"],
                "as_of_date": claim["admission_date"]
            }
        )
    )

    if not plan_rows:
        raise ValueError("Plan at admission not found")

    return {
        "claim": claim,
        "plan": plan_rows[0],
        "route": state["route"] + ["data"],
        "specialists": {
            **state["specialists"],
            "data": {
                "tool_calls": ["lookup_claim", "plan_at"]
            }
        }
    }

# Step 5: Implement the policy specialist
def policy_agent(state: R5State):
    rows = parse_rows(
        call_tool(
            "search_policy",
            {
                "query": (
                    "Room rent cap applicable on admission date "
                    "and proportionate deduction when room rent "
                    "exceeds the plan cap"
                )
            }
        )
    )

    relevant = [
        row for row in rows
        if row.get("doc_name") ==
        "04_Room_Rent_Capping_and_Proportionate_Deduction.md"
    ]

    if not relevant:
        raise ValueError("Required room-rent policy evidence not found")

    chunk_ids = [row["chunk_id"] for row in relevant]

    return {
        "policy_chunks": relevant,
        "route": state["route"] + ["policy"],
        "specialists": {
            **state["specialists"],
            "policy": {
                "tool_calls": ["search_policy"],
                "cited_chunk_ids": chunk_ids
            }
        }
    }

# Step 6: Implement the adjudication specialist
def adjudication_agent(state: R5State):
    claim = state["claim"]
    plan = state["plan"]

    result = call_tool(
        "compute_payable",
        {
            "plan_code": plan["plan_code"],
            "is_pre_existing": claim["is_pre_existing"],
            "policy_tenure_months": int(claim["policy_tenure_months"]),
            "days_since_policy_start": int(claim["days_since_policy_start"]),
            "is_network_at_admission": claim["is_network_at_admission"],
            "room_rent_per_day": int(claim["room_rent_per_day"]),
            "billed_amount": float(claim["billed_amount"])
        }
    )

    if isinstance(result, str):
        result = json.loads(result)

    if (
        isinstance(result, dict)
        and isinstance(result.get("value"), str)
    ):
        # compute_payable returns a JSON string wrapping
        # {'format': 'SCALAR', 'value': '<json>'} — parse twice.
        result = json.loads(result["value"])

    return {
        "payout": result,
        "route": state["route"] + ["adjudication"],
        "specialists": {
            **state["specialists"],
            "adjudication": {
                "tool_calls": ["compute_payable"]
            }
        }
    }

# Step 7: Implement the independent Python verifier
def verify_payout(state: R5State):
    claim = state["claim"]
    plan = state["plan"]
    payout = state["payout"]

    cap = float(plan["room_rent_cap_per_day"])
    room_rent = float(claim["room_rent_per_day"])
    billed = float(claim["billed_amount"])

    over_cap = room_rent > cap

    deduction_pct = (
        round((1 - cap / room_rent) * 100, 2)
        if over_cap else 0.0
    )

    waiting_ok = (
        int(claim["days_since_policy_start"]) >= 30
        and (
            not str(claim["is_pre_existing"]).lower() == "true"
            or int(claim["policy_tenure_months"]) >= 36
        )
    )

    copay_pct = int(plan["copay_pct"])

    payable = waiting_ok

    payable_amount = (
        billed
        * (1 - deduction_pct / 100)
        * (1 - copay_pct / 100)
        if payable else 0.0
    )

    expected = {
        "plan_code": plan["plan_code"],
        "payable": payable,
        "waiting_period_ok": waiting_ok,
        "network_at_admission":
            str(claim["is_network_at_admission"]).lower() == "true",
        "room_rent_over_cap": over_cap,
        "room_rent_deduction_pct": deduction_pct,
        "copay_pct": copay_pct,
        "reason_code": (
            "INITIAL_WAITING" if not waiting_ok
            else "PAYABLE_WITH_DEDUCTION" if over_cap
            else "PAYABLE_IN_FULL"
        ),
        "payable_amount": round(payable_amount, 2)
    }

    agrees = all(
        (
            abs(float(payout[key]) - float(value)) < 0.01
            if isinstance(value, float)
            else payout[key] == value
        )
        for key, value in expected.items()
    )

    return {
        "verified_payout": expected,
        "verifier_agrees": agrees,
        "route": state["route"] + ["verifier"]
    }

# Step 8: Build the supervisor graph
graph = StateGraph(R5State)

graph.add_node("data", data_agent)
graph.add_node("policy", policy_agent)
graph.add_node("adjudication", adjudication_agent)
graph.add_node("verifier", verify_payout)

graph.set_entry_point("data")

graph.add_edge("data", "policy")
graph.add_edge("policy", "adjudication")
graph.add_edge("adjudication", "verifier")
graph.add_edge("verifier", END)

supervisor = graph.compile()

# Step 9: Run the client question
QUESTION = (
    "For CLM-20003: which plan applied, "
    "is room rent over cap, and what is payable?"
)

mlflow.set_tracking_uri("databricks")
mlflow.set_experiment(
    "/Users/surajitm0nd0l12@gmail.com/Saffron_Shield_R5"
)

@mlflow.trace(name="r5_supervisor")
def run_r5():
    return supervisor.invoke({
        "question": QUESTION,
        "claim_id": "CLM-20003",
        "claim": {},
        "plan": {},
        "policy_chunks": [],
        "payout": {},
        "verified_payout": {},
        "verifier_agrees": False,
        "route": [],
        "specialists": {}
    })


result = run_r5()

print("Route:", result["route"])
print("Payout:", result["payout"])
print("Verified:", result["verified_payout"])
print("Verifier agrees:", result["verifier_agrees"])