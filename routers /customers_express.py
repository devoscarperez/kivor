from fastapi import APIRouter, Depends
from uuid import uuid4
from datetime import datetime
from core.db import get_tenant_connection

router = APIRouter()

@router.post("/customers-express/generate")
def generate_customer_express(conn = Depends(get_tenant_connection)):

    token = uuid4().hex

    with conn.cursor() as cur:

        cur.execute("""
            INSERT INTO customers_express
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
        """, (token,))

        result = cur.fetchone()

    return {
        "status": "ok",
        "customers_express_id": result[0],
        "token": token
    }
