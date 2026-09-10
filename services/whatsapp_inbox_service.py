from psycopg import sql

from core.db import get_connection

CONTROL_STATES_PENDIENTES = ("EN_MANO_HUMANA", "FICHA_LISTA")

INBOX_LIST_COLUMNS = [
    "phone", "whatsapp_name", "customer_name", "state", "control_state",
    "intent", "service_family", "assigned_to", "last_customer_message",
    "last_customer_message_at", "window_expires_at", "updated_at",
]

INBOX_DETAIL_COLUMNS = [
    "phone", "whatsapp_name", "customer_id", "customer_name", "state", "control_state",
    "bot_enabled", "intent", "service_family", "service_detail", "preferred_day",
    "preferred_time", "preferred_professional", "notes", "has_photos", "summary_json",
    "assigned_to", "handoff_reason", "last_customer_message", "last_customer_message_at",
    "window_expires_at", "last_bot_message_at", "last_human_message_at",
    "created_at", "updated_at",
]


def _validated_schema(tenant_schema: str) -> str:
    if not tenant_schema or not tenant_schema.isidentifier():
        raise Exception("invalid_tenant")
    return tenant_schema


def list_pending_conversations(tenant_schema: str) -> list:
    tenant_schema = _validated_schema(tenant_schema)

    query = sql.SQL("""
        SELECT {columns}
        FROM {schema}.ai_whatsapp_conversation
        WHERE control_state = ANY(%s)
        ORDER BY updated_at DESC
    """).format(
        columns=sql.SQL(", ").join(sql.Identifier(c) for c in INBOX_LIST_COLUMNS),
        schema=sql.Identifier(tenant_schema),
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (list(CONTROL_STATES_PENDIENTES),))
            rows = cur.fetchall()

    return [dict(zip(INBOX_LIST_COLUMNS, row)) for row in rows]


def get_conversation_detail(tenant_schema: str, phone: str) -> dict:
    tenant_schema = _validated_schema(tenant_schema)

    query = sql.SQL("""
        SELECT {columns}
        FROM {schema}.ai_whatsapp_conversation
        WHERE phone = %s
        LIMIT 1
    """).format(
        columns=sql.SQL(", ").join(sql.Identifier(c) for c in INBOX_DETAIL_COLUMNS),
        schema=sql.Identifier(tenant_schema),
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (phone,))
            row = cur.fetchone()

    if not row:
        raise Exception("conversation_not_found")

    return dict(zip(INBOX_DETAIL_COLUMNS, row))


def take_conversation(tenant_schema: str, phone: str, agent_username: str) -> dict:
    tenant_schema = _validated_schema(tenant_schema)

    query = sql.SQL("""
        UPDATE {schema}.ai_whatsapp_conversation
        SET assigned_to = %s, updated_at = NOW()
        WHERE phone = %s
        RETURNING phone
    """).format(schema=sql.Identifier(tenant_schema))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (agent_username, phone))
            row = cur.fetchone()

    if not row:
        raise Exception("conversation_not_found")

    return {"status": "ok", "phone": phone, "assigned_to": agent_username}


def close_conversation(tenant_schema: str, phone: str) -> dict:
    tenant_schema = _validated_schema(tenant_schema)

    query = sql.SQL("""
        UPDATE {schema}.ai_whatsapp_conversation
        SET control_state = 'CERRADO', updated_at = NOW()
        WHERE phone = %s
        RETURNING phone
    """).format(schema=sql.Identifier(tenant_schema))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (phone,))
            row = cur.fetchone()

    if not row:
        raise Exception("conversation_not_found")

    return {"status": "ok", "phone": phone, "control_state": "CERRADO"}


def return_to_bot(tenant_schema: str, phone: str) -> dict:
    tenant_schema = _validated_schema(tenant_schema)

    query = sql.SQL("""
        UPDATE {schema}.ai_whatsapp_conversation
        SET state = 'NUEVA',
            control_state = 'BOT_ACTIVO',
            bot_enabled = TRUE,
            assigned_to = NULL,
            handoff_reason = NULL,
            updated_at = NOW()
        WHERE phone = %s
        RETURNING phone
    """).format(schema=sql.Identifier(tenant_schema))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (phone,))
            row = cur.fetchone()

    if not row:
        raise Exception("conversation_not_found")

    return {"status": "ok", "phone": phone, "control_state": "BOT_ACTIVO"}
