# services/professional_service.py

from psycopg import sql

from core.db import get_connection

PROFESSIONAL_SEARCH_COLUMNS = [
    "professional_id", "professional_name", "professional_nickname",
]

SEARCH_LIMIT = 20


def search_professionals(term: str) -> list:
    query = sql.SQL("""
        SELECT {columns}
        FROM core.professional
        WHERE professional_name ILIKE %s OR professional_nickname ILIKE %s
        ORDER BY professional_name NULLS LAST
        LIMIT {limit}
    """).format(
        columns=sql.SQL(", ").join(sql.Identifier(c) for c in PROFESSIONAL_SEARCH_COLUMNS),
        limit=sql.Literal(SEARCH_LIMIT),
    )

    like_term = f"%{term.strip()}%"

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (like_term, like_term))
            rows = cur.fetchall()

    return [dict(zip(PROFESSIONAL_SEARCH_COLUMNS, row)) for row in rows]


def get_professional_id_by_person(person_id: int):
    query = sql.SQL("SELECT professional_id FROM core.professional WHERE person_id = %s")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (person_id,))
            row = cur.fetchone()

    return row[0] if row else None


def get_professional_display_name_by_person(person_id: int):
    query = sql.SQL("""
        SELECT COALESCE(professional_nickname, professional_name)
        FROM core.professional
        WHERE person_id = %s
    """)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (person_id,))
            row = cur.fetchone()

    return row[0] if row else None
