
import re


# Topics that are relevant to Saffron Shield claims and policies.
DOMAIN_KEYWORDS = {
    "saffron shield",
    "claim",
    "claims",
    "policy",
    "policies",
    "insurance",
    "member",
    "hospital",
    "admission",
    "discharge",
    "billed amount",
    "approved amount",
    "settlement",
    "settled",
    "rejected",
    "rejection",
    "coverage",
    "covered",
    "payable",
    "payout",
    "premium",
    "plan",
    "plans",
    "waiting period",
    "pre-existing",
    "pre existing",
    "ped",
    "copay",
    "co-pay",
    "room rent",
    "network",
    "cashless",
    "deduction",
    "eligibility",
    "benefit",
    "benefits",
    "reimbursement",
    "claim status",
    "claim id",
    "member id",
    "policy number",
    "policy id"
}


# Requests to disclose sensitive identity or contact information.
SENSITIVE_DISCLOSURE_PATTERNS = [
    r"\b(reveal|show|display|give|tell|provide|share|print|expose|retrieve)\b.{0,60}\b(aadhaar|aadhar|pan number|passport number|identity number|government id|social security number|ssn)\b",
    r"\b(reveal|show|display|give|tell|provide|share|print|expose|retrieve)\b.{0,60}\b(phone number|mobile number|email address|home address|contact details|contact information)\b",
    r"\bwhat is\b.{0,40}\b(aadhaar|aadhar|pan number|passport number|phone number|mobile number|email address|home address)\b",
    r"\b(list|export|dump|show|display)\b.{0,60}\b(all members|member contact details|customer contact details|personal details)\b"
]


def check_input(text: str) -> dict:
    """
    Check whether an incoming prompt should be blocked.

    Returns:
        {
            "blocked": bool,
            "reason": str
        }
    """

    if not isinstance(text, str) or not text.strip():
        return {
            "blocked": True,
            "reason": "empty_or_invalid_input"
        }

    normalized_text = re.sub(r"\s+", " ", text.lower()).strip()

    # Rule 1: Block requests to disclose sensitive information.
    for pattern in SENSITIVE_DISCLOSURE_PATTERNS:
        if re.search(pattern, normalized_text):
            return {
                "blocked": True,
                "reason": "sensitive_information_request"
            }

    # Rule 2: Block prompts outside the Saffron Shield domain.
    is_domain_related = any(
        keyword in normalized_text
        for keyword in DOMAIN_KEYWORDS
    )

    if not is_domain_related:
        return {
            "blocked": True,
            "reason": "outside_saffron_shield_scope"
        }

    # Otherwise, allow the prompt to reach the agent.
    return {
        "blocked": False,
        "reason": "allowed"
    }