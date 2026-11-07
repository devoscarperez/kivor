from datetime import date, datetime

from fastapi import APIRouter, HTTPException, Depends, Query, Response
from pydantic import BaseModel

from core.security import verify_token
from services.ticket_service import (
    create_ticket, list_tickets, set_ticket_customer, get_ticket, set_ticket_booking_date,
    set_ticket_status,
)
from services.ticket_voucher_service import generar_voucher_pdf, generar_voucher_imagen

router = APIRouter(prefix="/ticket", tags=["Ticket"])


class SetCustomerBody(BaseModel):
    customer_id: int


class CreateTicketBody(BaseModel):
    booking_date: datetime


class SetBookingDateBody(BaseModel):
    booking_date: datetime


class SetStatusBody(BaseModel):
    status: str


@router.post("")
def post_ticket(body: CreateTicketBody, current_user: dict = Depends(verify_token)):
    try:
        return create_ticket(current_user["tenant_schema"], current_user["organization_id"], body.booking_date)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("")
def get_tickets(
    date_from: date = Query(default=None),
    date_to: date = Query(default=None),
    status: list[str] = Query(default=None),
    order: str = Query(default="desc"),
    date_field: str = Query(default="created"),
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
            date_field,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{ticket_id}")
def get_single_ticket(ticket_id: int, current_user: dict = Depends(verify_token)):
    try:
        return get_ticket(current_user["tenant_schema"], ticket_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{ticket_id}/voucher")
def get_ticket_voucher(ticket_id: int, current_user: dict = Depends(verify_token)):
    try:
        pdf_bytes, nombre_archivo = generar_voucher_pdf(current_user["tenant_schema"], ticket_id)
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{nombre_archivo}"'},
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{ticket_id}/voucher-imagen")
def get_ticket_voucher_imagen(ticket_id: int, current_user: dict = Depends(verify_token)):
    try:
        png_bytes, nombre_imagen = generar_voucher_imagen(current_user["tenant_schema"], ticket_id)
        return Response(
            content=png_bytes,
            media_type="image/png",
            headers={"Content-Disposition": f'inline; filename="{nombre_imagen}"'},
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.patch("/{ticket_id}/customer")
def patch_ticket_customer(
    ticket_id: int,
    body: SetCustomerBody,
    current_user: dict = Depends(verify_token),
):
    try:
        return set_ticket_customer(current_user["tenant_schema"], ticket_id, body.customer_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.patch("/{ticket_id}/status")
def patch_ticket_status(
    ticket_id: int,
    body: SetStatusBody,
    current_user: dict = Depends(verify_token),
):
    try:
        return set_ticket_status(current_user["tenant_schema"], ticket_id, body.status)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.patch("/{ticket_id}/booking-date")
def patch_ticket_booking_date(
    ticket_id: int,
    body: SetBookingDateBody,
    current_user: dict = Depends(verify_token),
):
    try:
        return set_ticket_booking_date(current_user["tenant_schema"], ticket_id, body.booking_date)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
