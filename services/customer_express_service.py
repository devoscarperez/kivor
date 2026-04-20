from uuid import uuid4
from psycopg import sql

from core.db import get_connection
from datetime import datetime

def search_customer_express_by_mobile_service(mobile: str, current_user: dict):

    import logging

    with get_connection() as conn:
        with conn.cursor() as cur:

            # campos configurados
            cur.execute("""
                SELECT
                    customer_capture_settings_field
                FROM lindasylunaticas.customer_capture_settings
                WHERE customer_capture_settings_is_active = TRUE
                ORDER BY customer_capture_settings_display_order
            """)

            field_rows = cur.fetchall()
            fields = [r[0] for r in field_rows]

            tenant_schema = current_user["tenant_schema"]

            query = f"""
            SELECT *
            FROM {tenant_schema}.customers_express
            WHERE customers_express_mobile = %s
            AND customers_express_completed_at IS NOT NULL
            ORDER BY customers_express_completed_at DESC
            """

            logging.basicConfig(level=logging.INFO)

            logging.info(f"TENANT: {tenant_schema}")
            logging.info(f"MOBILE: {mobile}")
            logging.info(f"QUERY: {query}")

            cur.execute(query, (mobile,))

            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

    results = [dict(zip(columns, row)) for row in rows]

    return {
        "status": "ok",
        "fields": fields,
        "results": results
    }
def save_customer_express_service(token: str, payload: dict):

    from psycopg import sql

    with get_connection() as conn:
        with conn.cursor() as cur:

            # resolver tenant
            cur.execute("""
                SELECT tenant_schema
                FROM core.customers_express_token_map
                WHERE token = %s
            """, (token,))

            row = cur.fetchone()

            if not row:
                raise Exception("invalid_link")

            tenant_schema = row[0]

            if not tenant_schema or not tenant_schema.isidentifier():
                raise Exception("invalid_tenant")

            fields = []
            values = []

            for key, value in payload.items():

                clean_key = key.replace("customers_", "")
                column = f"customers_express_{clean_key}"

                fields.append(
                    sql.SQL("{} = %s").format(sql.Identifier(column))
                )
                values.append(value)

            # campos de control
            fields.append(sql.SQL("customers_express_completed_at = NOW()"))
            fields.append(sql.SQL("customers_express_link_status = 'completed'"))

            values.append(token)

            query = sql.SQL("""
                UPDATE {}.customers_express
                SET {}
                WHERE customers_express_token = %s
            """).format(
                sql.Identifier(tenant_schema),
                sql.SQL(", ").join(fields)
            )

            cur.execute(query, values)

    return {"status": "ok"}

def get_customer_express_service(token: str):

    with get_connection() as conn:
        with conn.cursor() as cur:

            # resolver tenant
            cur.execute("""
                SELECT tenant_schema
                FROM core.customers_express_token_map
                WHERE token = %s
            """, (token,))

            row = cur.fetchone()

            if not row:
                raise Exception("invalid_link")

            tenant_schema = row[0]

            if not tenant_schema or not tenant_schema.isidentifier():
                raise Exception("invalid_tenant")

            # validar token
            query = sql.SQL("""
                SELECT
                    customers_express_id,
                    customers_express_token_expires_at,
                    customers_express_link_status
                FROM {}.customers_express
                WHERE customers_express_token = %s
                AND customers_express_token_expires_at > NOW()
            """).format(sql.Identifier(tenant_schema))

            cur.execute(query, (token,))
            record = cur.fetchone()

            if not record:
                raise Exception("invalid_link")

            customers_express_id, expires_at, status = record

            if expires_at and expires_at < datetime.utcnow():
                raise Exception("expired_link")

            if status == "completed":
                raise Exception("form_completed")

            # campos
            query_fields = sql.SQL("""
                SELECT
                    customer_capture_settings_field,
                    customer_capture_settings_label,
                    customer_capture_settings_is_required,
                    customer_capture_settings_display_order
                FROM {}.customer_capture_settings
                WHERE customer_capture_settings_is_active = TRUE
                ORDER BY customer_capture_settings_display_order
            """).format(sql.Identifier(tenant_schema))

            cur.execute(query_fields)

            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            fields = [dict(zip(columns, row)) for row in rows]

            identifier_types = []

            if any(f["customer_capture_settings_field"] == "identifier_type" for f in fields):

                query_identifiers = sql.SQL("""
                    SELECT
                        identifier_type_settings_code,
                        identifier_type_settings_label
                    FROM {}.identifier_type_settings
                    WHERE identifier_type_settings_is_active = TRUE
                    ORDER BY identifier_type_settings_display_order
                """).format(sql.Identifier(tenant_schema))

                cur.execute(query_identifiers)

                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                identifier_types = [dict(zip(columns, row)) for row in rows]

    return {
        "status": "ok",
        "token": token,
        "fields": fields,
        "identifier_types": identifier_types
    }
    

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
