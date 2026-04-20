from core.db import get_connection

def get_nivel4_service(family: str, level2: str, level3: str):

    query = """
    SELECT DISTINCT level4
    FROM core.prices
    WHERE family = %s
    AND level2 = %s
    AND level3 = %s
    AND level4 IS NOT NULL
    ORDER BY level4;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (family, level2, level3))
            rows = cur.fetchall()

    return [r[0] for r in rows]
    
def get_nivel3_service(family: str, level2: str):

    query = """
    SELECT DISTINCT level3
    FROM core.prices
    WHERE family = %s
    AND level2 = %s
    AND level3 IS NOT NULL
    ORDER BY level3;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (family, level2))
            rows = cur.fetchall()

    return [r[0] for r in rows]


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
