from langchain_core.tools import tool
from memory import save_note as db_save_note
from memory import recall_notes as db_recall_notes

USER_ID = "adjuster.demo@saffronshield.example"


@tool
def save_note(note: str) -> dict:
    """Save an adjuster's note permanently in Lakebase."""
    return db_save_note(USER_ID, note)


@tool
def recall_notes() -> list:
    """Retrieve the adjuster's previously saved notes from Lakebase."""
    return db_recall_notes(USER_ID)