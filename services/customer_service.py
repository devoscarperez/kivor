# services/customer_service.py

from psycopg import sql

from core.db import get_connection

CUSTOMER_SEARCH_COLUMNS = [
    "customer_id", "customer_full_name", "customer_nickname", "customer_mobile",
]

SEARCH_LIMIT = 20


def _validated_schema(tenant_schema: str) -> str:
    if not tenant_schema or not tenant_schema.isidentifier():
        raise Exception("invalid_tenant")
    return tenant_schema


def search_customers(tenant_schema: str, term: str) -> list:
    tenant_schema = _validated_schema(tenant_schema)

    query = sql.SQL("""
        SELECT {columns}
        FROM {schema}.customer
        WHERE customer_fullsearch ILIKE %s
        ORDER BY customer_full_name NULLS LAST
        LIMIT {limit}
    """).format(
        columns=sql.SQL(", ").join(sql.Identifier(c) for c in CUSTOMER_SEARCH_COLUMNS),
        schema=sql.Identifier(tenant_schema),
        limit=sql.Literal(SEARCH_LIMIT),
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (f"%{term.strip().upper()}%",))
            rows = cur.fetchall()

    return [dict(zip(CUSTOMER_SEARCH_COLUMNS, row)) for row in rows]
