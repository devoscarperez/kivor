# services/ticket_service.py

from datetime import date

from psycopg import sql

from core.db import get_connection

TICKET_COLUMNS = [
    "ticket_id", "ticket_status", "ticket_created_at",
    "ticket_updated_at", "ticket_status_changed_at",
]


def _validated_schema(tenant_schema: str) -> str:
    if not tenant_schema or not tenant_schema.isidentifier():
        raise Exception("invalid_tenant")
    return tenant_schema


def create_ticket(tenant_schema: str, organization_id: int) -> dict:
    tenant_schema = _validated_schema(tenant_schema)

    query = sql.SQL("""
        INSERT INTO {schema}.ticket (organization_id)
        VALUES (%s)
        RETURNING {columns}
    """).format(
        schema=sql.Identifier(tenant_schema),
        columns=sql.SQL(", ").join(sql.Identifier(c) for c in TICKET_COLUMNS),
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (organization_id,))
            row = cur.fetchone()
        conn.commit()

    return dict(zip(TICKET_COLUMNS, row))


def list_tickets(
    tenant_schema: str,
    organization_id: int,
    date_from: date,
    date_to: date,
    statuses: list,
    order: str,
) -> list:
    tenant_schema = _validated_schema(tenant_schema)

    order_sql = sql.SQL("ASC") if order == "asc" else sql.SQL("DESC")

    conditions = [
        sql.SQL("organization_id = %s"),
        sql.SQL("ticket_created_at::date BETWEEN %s AND %s"),
    ]
    params = [organization_id, date_from, date_to]

    if statuses:
        conditions.append(sql.SQL("ticket_status = ANY(%s)"))
        params.append(statuses)

    query = sql.SQL("""
        SELECT {columns}
        FROM {schema}.ticket
        WHERE {conditions}
        ORDER BY ticket_created_at {order}
    """).format(
        columns=sql.SQL(", ").join(sql.Identifier(c) for c in TICKET_COLUMNS),
        schema=sql.Identifier(tenant_schema),
        conditions=sql.SQL(" AND ").join(conditions),
        order=order_sql,
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

    return [dict(zip(TICKET_COLUMNS, row)) for row in rows]
