from fastapi import APIRouter, HTTPException, Depends, Query

from core.security import verify_token
from services.customer_service import search_customers

router = APIRouter(prefix="/customer", tags=["Customer"])


@router.get("/search")
def get_customer_search(
    q: str = Query(..., min_length=2),
    current_user: dict = Depends(verify_token),
):
    try:
        return search_customers(current_user["tenant_schema"], q)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
