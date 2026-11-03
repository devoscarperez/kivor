from datetime import date

from fastapi import APIRouter, HTTPException, Depends, Query

from core.security import verify_token
from services.ticket_service import create_ticket, list_tickets

router = APIRouter(prefix="/ticket", tags=["Ticket"])


@router.post("")
def post_ticket(current_user: dict = Depends(verify_token)):
    try:
        return create_ticket(current_user["tenant_schema"], current_user["organization_id"])
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("")
def get_tickets(
    date_from: date = Query(default=None),
    date_to: date = Query(default=None),
    status: list[str] = Query(default=None),
    order: str = Query(default="desc"),
    current_user: dict = Depends(verify_token),
):
    today = date.today()

    try:
        return list_tickets(
            current_user["tenant_schema"],
            current_user["organization_id"],
            date_from or today,
            date_to or today,
            status,
            order,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
