from services.customer_express_service import generate_customer_express_service
from services.customer_express_service import get_customer_express_service

from fastapi import APIRouter, HTTPException, Depends, Body
from datetime import datetime
from uuid import uuid4
from psycopg import sql
import logging

from core.db import get_connection
from core.security import verify_token

router = APIRouter(prefix="/customers-express")

@router.post("/generate")
def generate_customer_express(current_user: dict = Depends(verify_token)):

    try:
        return generate_customer_express_service(current_user)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{token}")
def get_customer_express(token: str):

    try:
        return get_customer_express_service(token)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/{token}")
async def save_customer_express(token: str, payload: dict = Body(...)):

    with get_connection() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT tenant_schema
                FROM core.customers_express_token_map
                WHERE token = %s
            """, (token,))

            row = cur.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="invalid_link")

            tenant_schema = row[0]

            if not tenant_schema or not tenant_schema.isidentifier():
                raise HTTPException(status_code=400, detail="invalid_tenant")

            fields = []
            values = []

            for key, value in payload.items():
                clean_key = key.replace("customers_", "")
                column = f"customers_express_{clean_key}"

                fields.append(
                    sql.SQL("{} = %s").format(sql.Identifier(column))
                )
                values.append(value)

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


@router.get("/by-mobile/{mobile}")
def search_customers_express(mobile: str, current_user: dict = Depends(verify_token)):

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:

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

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
