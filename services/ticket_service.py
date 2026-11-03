# services/ticket_service.py

from datetime import date

from psycopg import sql

from core.db import get_connection

TICKET_COLUMNS = [
    "ticket_id", "ticket_status", "ticket_created_at",
    "ticket_updated_at", "ticket_status_changed_at", "ticket_customer_id",
]

TICKET_LIST_RESULT_COLUMNS = TICKET_COLUMNS + [
    "customer_full_name", "customer_nickname", "customer_mobile", "professional_count",
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
        sql.SQL("t.organization_id = %s"),
        sql.SQL("t.ticket_created_at::date BETWEEN %s AND %s"),
    ]
    params = [organization_id, date_from, date_to]

    if statuses:
        conditions.append(sql.SQL("t.ticket_status = ANY(%s)"))
        params.append(statuses)

    query = sql.SQL("""
        SELECT t.ticket_id, t.ticket_status, t.ticket_created_at, t.ticket_updated_at,
               t.ticket_status_changed_at, t.ticket_customer_id,
               c.customer_full_name, c.customer_nickname, c.customer_mobile,
               (SELECT COUNT(*) FROM {schema}.ticket_professional tp2 WHERE tp2.ticket_id = t.ticket_id) AS professional_count
        FROM {schema}.ticket t
        LEFT JOIN {schema}.customer c ON c.customer_id = t.ticket_customer_id
        WHERE {conditions}
        ORDER BY t.ticket_created_at {order}
    """).format(
        schema=sql.Identifier(tenant_schema),
        conditions=sql.SQL(" AND ").join(conditions),
        order=order_sql,
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

    return [dict(zip(TICKET_LIST_RESULT_COLUMNS, row)) for row in rows]


def get_ticket(tenant_schema: str, ticket_id: int) -> dict:
    tenant_schema = _validated_schema(tenant_schema)

    query = sql.SQL("""
        SELECT t.ticket_id, t.ticket_status, t.ticket_created_at, t.ticket_updated_at,
               t.ticket_status_changed_at, t.ticket_customer_id,
               c.customer_full_name, c.customer_nickname, c.customer_mobile
        FROM {schema}.ticket t
        LEFT JOIN {schema}.customer c ON c.customer_id = t.ticket_customer_id
        WHERE t.ticket_id = %s
    """).format(
        schema=sql.Identifier(tenant_schema),
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (ticket_id,))
            row = cur.fetchone()

    if not row:
        raise Exception("ticket_not_found")

    return dict(zip(TICKET_LIST_RESULT_COLUMNS[:-1], row))


def set_ticket_customer(tenant_schema: str, ticket_id: int, customer_id: int) -> dict:
    tenant_schema = _validated_schema(tenant_schema)

    query = sql.SQL("""
        UPDATE {schema}.ticket
        SET ticket_customer_id = %s,
            ticket_updated_at = NOW(),
            ticket_status_changed_at = CASE WHEN ticket_status = 'Inactive' THEN NOW() ELSE ticket_status_changed_at END,
            ticket_status = CASE WHEN ticket_status = 'Inactive' THEN 'Assigned' ELSE ticket_status END
        WHERE ticket_id = %s
        RETURNING {columns}
    """).format(
        schema=sql.Identifier(tenant_schema),
        columns=sql.SQL(", ").join(sql.Identifier(c) for c in TICKET_COLUMNS),
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (customer_id, ticket_id))
            row = cur.fetchone()
        conn.commit()

    if not row:
        raise Exception("ticket_not_found")

    return dict(zip(TICKET_COLUMNS, row))
