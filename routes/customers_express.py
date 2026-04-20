from services.customer_express_service import generate_customer_express_service
from services.customer_express_service import get_customer_express_service
from services.customer_express_service import save_customer_express_service

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

    try:
        return save_customer_express_service(token, payload)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


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
