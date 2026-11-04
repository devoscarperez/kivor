from datetime import date

from fastapi import APIRouter, HTTPException, Depends, Query

from core.security import verify_token
from services.customer_service import search_customers
from services.ticket_professional_service import get_customer_agenda

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


@router.get("/{customer_id}/agenda")
def get_customer_agenda_route(
    customer_id: int,
    agenda_date: date = Query(default=None, alias="date"),
    current_user: dict = Depends(verify_token),
):
    try:
        return get_customer_agenda(
            current_user["tenant_schema"], customer_id, agenda_date or date.today()
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
