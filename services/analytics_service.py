from core.db import get_connection


def get_familias_service():

    query = """
    SELECT DISTINCT family
    FROM core.prices
    WHERE family IS NOT NULL
    ORDER BY family;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

    return [r[0] for r in rows]

def get_nivel2_service(family: str):

    query = """
    SELECT DISTINCT level2
    FROM core.prices
    WHERE family = %s
    AND level2 IS NOT NULL
    ORDER BY level2;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (family,))
            rows = cur.fetchall()

    return [r[0] for r in rows]
