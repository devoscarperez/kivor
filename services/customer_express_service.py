from uuid import uuid4
from psycopg import sql

from core.db import get_connection


def generate_customer_express_service(current_user: dict):

    token = uuid4().hex
    tenant_schema = current_user.get("tenant_schema")

    if not tenant_schema or not tenant_schema.isidentifier():
        raise Exception("Invalid tenant schema")

    with get_connection() as conn:
        with conn.cursor() as cur:

            query = sql.SQL("""
                INSERT INTO {}.customers_express
                (
                    customers_express_token,
                    customers_express_token_created_at,
                    customers_express_token_expires_at,
                    customers_express_link_status
                )
                VALUES
                (
                    %s,
                    NOW(),
                    NOW() + interval '24 hours',
                    'created'
                )
                RETURNING customers_express_id
            """).format(sql.Identifier(tenant_schema))

            cur.execute(query, (token,))
            result = cur.fetchone()

            cur.execute("""
                INSERT INTO core.customers_express_token_map (token, tenant_schema)
                VALUES (%s, %s)
            """, (token, tenant_schema))

    return {
        "status": "ok",
        "customers_express_id": result[0],
        "token": token
    }
