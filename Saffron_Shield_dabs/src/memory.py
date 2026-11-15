# /// script
# [tool.databricks.environment]
# dependencies = [
#   "psycopg[binary]>=3.1.0",
#   "databricks-sdk>=0.118.0",
# ]
# ///

from lakebase import get_lakebase_connection


def save_note(user_id: str, note: str) -> dict:
    """
    Save an adjuster's note in persistent Lakebase storage.
    """

    if not user_id or not note:
        raise ValueError("user_id and note are required")

    with get_lakebase_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.adjuster_notes
                    (user_id, note)
                VALUES (%s, %s)
                RETURNING id, created_at
                """,
                (user_id, note)
            )

            row = cur.fetchone()

    return {
        "success": True,
        "note_id": row[0],
        "created_at": row[1].isoformat()
    }


def recall_notes(user_id: str) -> list:
    """
    Retrieve an adjuster's previously saved notes.
    """

    if not user_id:
        raise ValueError("user_id is required")

    with get_lakebase_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT note, created_at
                FROM public.adjuster_notes
                WHERE user_id = %s
                ORDER BY created_at DESC
                """,
                (user_id,)
            )

            rows = cur.fetchall()

    return [
        {
            "note": row[0],
            "created_at": row[1].isoformat()
        }
        for row in rows
    ]
