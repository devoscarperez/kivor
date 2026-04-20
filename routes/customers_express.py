from fastapi import APIRouter, HTTPException, Depends, Body
from services.customer_express_service import search_customer_express_by_mobile_service

from core.security import verify_token

from services.customer_express_service import (
    generate_customer_express_service,
    get_customer_express_service,
    save_customer_express_service
)

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
def search_customers_express(
    mobile: str,
    current_user: dict = Depends(verify_token)
):

    try:
        return search_customer_express_by_mobile_service(mobile, current_user)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
