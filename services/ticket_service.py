# services/ticket_service.py

from datetime import date, datetime

from psycopg import sql

from core.db import get_connection

# Claves expuestas hacia el frontend (API/JSON) - se mantienen iguales
# aunque las columnas reales de la tabla "ticket" se renombraron (se
# les quitó el prefijo "ticket_"), para no tener que tocar el frontend.
TICKET_COLUMNS = [
    "ticket_id", "ticket_status", "ticket_created_at",
    "ticket_updated_at", "ticket_status_changed_at", "ticket_customer_id",
    "ticket_booking_date", "ticket_booking_deposit",
]

# Columnas reales de lindasylunaticas.ticket, en el mismo orden que TICKET_COLUMNS.
TICKET_DB_COLUMNS = [
    "id", "status", "created_at",
    "updated_at", "status_changed_at", "customer_id",
    "booking_date", "booking_deposit",
]

BOOKING_DEPOSIT_MIN = 0
BOOKING_DEPOSIT_MAX = 99999

VALID_STATUSES = ["Inactive", "Assigned", "Started", "Completed", "Approved", "Finished"]

TICKET_LIST_RESULT_COLUMNS = TICKET_COLUMNS + [
    "customer_full_name", "customer_nickname", "customer_mobile", "professional_count",
    "professional_aliases", "whatsapp_sent", "sent_date",
]


def _validated_schema(tenant_schema: str) -> str:
    if not tenant_schema or not tenant_schema.isidentifier():
        raise Exception("invalid_tenant")
    return tenant_schema


def create_ticket(tenant_schema: str, organization_id: int, booking_date: datetime) -> dict:
    tenant_schema = _validated_schema(tenant_schema)

    query = sql.SQL("""
        INSERT INTO {schema}.ticket (organization_id, booking_date)
        VALUES (%s, %s)
        RETURNING {columns}
    """).format(
        schema=sql.Identifier(tenant_schema),
        columns=sql.SQL(", ").join(sql.Identifier(c) for c in TICKET_DB_COLUMNS),
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (organization_id, booking_date))
            row = cur.fetchone()
        conn.commit()

    return dict(zip(TICKET_COLUMNS, row))


DATE_FIELD_COLUMNS = {
    "created": "created_at",
    "booking": "booking_date",
}


def list_tickets(
    tenant_schema: str,
    organization_id: int,
    date_from: date,
    date_to: date,
    statuses: list,
    order: str,
    date_field: str = "created",
) -> list:
    tenant_schema = _validated_schema(tenant_schema)

    if date_field not in DATE_FIELD_COLUMNS:
        raise Exception(f"date_field inválido: '{date_field}'. Debe ser 'created' o 'booking'.")

    order_sql = sql.SQL("ASC") if order == "asc" else sql.SQL("DESC")
    date_column = sql.Identifier(DATE_FIELD_COLUMNS[date_field])

    conditions = [
        sql.SQL("t.organization_id = %s"),
        sql.SQL("t.{column}::date BETWEEN %s AND %s").format(column=date_column),
    ]
    params = [organization_id, date_from, date_to]

    if statuses:
        conditions.append(sql.SQL("t.status = ANY(%s)"))
        params.append(statuses)

    query = sql.SQL("""
        SELECT t.id, t.status, t.created_at, t.updated_at,
               t.status_changed_at, t.customer_id, t.booking_date, t.booking_deposit,
               c.customer_full_name, c.customer_nickname, c.customer_mobile,
               (SELECT COUNT(*) FROM {schema}.ticket_professional tp2 WHERE tp2.ticket_id = t.id) AS professional_count,
               (SELECT ARRAY_AGG(alias ORDER BY primera_fecha)
                FROM (
                    SELECT COALESCE(p.professional_nickname, p.professional_name) AS alias,
                           MIN(tpc.booking_date) AS primera_fecha
                    FROM {schema}.ticket_professional tp3
                    JOIN core.professional p ON p.professional_id = tp3.professional_id
                    LEFT JOIN {schema}.ticket_professional_customer tpc ON tpc.ticket_professional_id = tp3.id
                    WHERE tp3.ticket_id = t.id
                    GROUP BY p.professional_id, COALESCE(p.professional_nickname, p.professional_name)
                ) agg
               ) AS professional_aliases,
               t.whatsapp_sent, t.sent_date
        FROM {schema}.ticket t
        LEFT JOIN {schema}.customer c ON c.customer_id = t.customer_id
        WHERE {conditions}
        ORDER BY t.created_at {order}
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
        SELECT t.id, t.status, t.created_at, t.updated_at,
               t.status_changed_at, t.customer_id, t.booking_date, t.booking_deposit,
               c.customer_full_name, c.customer_nickname, c.customer_mobile
        FROM {schema}.ticket t
        LEFT JOIN {schema}.customer c ON c.customer_id = t.customer_id
        WHERE t.id = %s
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


def set_ticket_customer(tenant_schema: str, ticket_id: int, customer_id: int, booking_deposit: int) -> dict:
    tenant_schema = _validated_schema(tenant_schema)

    if booking_deposit is None or not (BOOKING_DEPOSIT_MIN <= booking_deposit <= BOOKING_DEPOSIT_MAX):
        raise Exception(
            f"booking_deposit inválido: debe ser un entero entre {BOOKING_DEPOSIT_MIN} y {BOOKING_DEPOSIT_MAX}."
        )

    query = sql.SQL("""
        UPDATE {schema}.ticket
        SET customer_id = %s,
            booking_deposit = %s,
            updated_at = NOW(),
            status_changed_at = CASE WHEN status = 'Inactive' THEN NOW() ELSE status_changed_at END,
            status = CASE WHEN status = 'Inactive' THEN 'Assigned' ELSE status END
        WHERE id = %s
        RETURNING {columns}
    """).format(
        schema=sql.Identifier(tenant_schema),
        columns=sql.SQL(", ").join(sql.Identifier(c) for c in TICKET_DB_COLUMNS),
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (customer_id, booking_deposit, ticket_id))
            row = cur.fetchone()
        conn.commit()

    if not row:
        raise Exception("ticket_not_found")

    return dict(zip(TICKET_COLUMNS, row))


def set_ticket_status(tenant_schema: str, ticket_id: int, status: str) -> dict:
    tenant_schema = _validated_schema(tenant_schema)

    if status not in VALID_STATUSES:
        raise Exception(f"status inválido: '{status}'")

    query = sql.SQL("""
        UPDATE {schema}.ticket
        SET status = %s,
            status_changed_at = NOW(),
            updated_at = NOW()
        WHERE id = %s
        RETURNING {columns}
    """).format(
        schema=sql.Identifier(tenant_schema),
        columns=sql.SQL(", ").join(sql.Identifier(c) for c in TICKET_DB_COLUMNS),
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (status, ticket_id))
            row = cur.fetchone()
        conn.commit()

    if not row:
        raise Exception("ticket_not_found")

    return dict(zip(TICKET_COLUMNS, row))


def registrar_envio_whatsapp(tenant_schema: str, ticket_id: int, username: str) -> dict:
    tenant_schema = _validated_schema(tenant_schema)

    update_query = sql.SQL("""
        UPDATE {schema}.ticket
        SET whatsapp_sent = %s,
            sent_date = NOW()
        WHERE id = %s AND whatsapp_sent IS NULL
        RETURNING whatsapp_sent, sent_date
    """).format(schema=sql.Identifier(tenant_schema))

    select_query = sql.SQL("""
        SELECT whatsapp_sent, sent_date FROM {schema}.ticket WHERE id = %s
    """).format(schema=sql.Identifier(tenant_schema))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(update_query, (username, ticket_id))
            row = cur.fetchone()

            if not row:
                cur.execute(select_query, (ticket_id,))
                row = cur.fetchone()
        conn.commit()

    if not row:
        raise Exception("ticket_not_found")

    return {"whatsapp_sent": row[0], "sent_date": row[1]}


def set_ticket_booking_date(tenant_schema: str, ticket_id: int, booking_date: datetime) -> dict:
    tenant_schema = _validated_schema(tenant_schema)

    query = sql.SQL("""
        UPDATE {schema}.ticket
        SET booking_date = %s,
            updated_at = NOW()
        WHERE id = %s
        RETURNING {columns}
    """).format(
        schema=sql.Identifier(tenant_schema),
        columns=sql.SQL(", ").join(sql.Identifier(c) for c in TICKET_DB_COLUMNS),
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (booking_date, ticket_id))
            row = cur.fetchone()
        conn.commit()

    if not row:
        raise Exception("ticket_not_found")

    return dict(zip(TICKET_COLUMNS, row))
