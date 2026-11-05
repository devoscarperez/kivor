from datetime import datetime

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from core.security import verify_token
from services.ticket_professional_service import (
    list_ticket_professionals,
    add_ticket_professional,
    remove_ticket_professional,
    add_ticket_professional_customer,
    remove_ticket_professional_customer,
    set_ticket_professional_customer_booking_date,
    add_ticket_professional_customer_product,
)

router = APIRouter(prefix="/ticket/{ticket_id}/professionals", tags=["Ticket Professional"])


class AddProfessionalBody(BaseModel):
    professional_id: int


class AddCustomerBody(BaseModel):
    customer_id: int
    booking_date: datetime


class SetCustomerBookingDateBody(BaseModel):
    booking_date: datetime


class AddCustomerProductBody(BaseModel):
    product_id: int
    product_name: str
    unit_price: float
    professional_unit_price: float
    quantity: float = 1


@router.get("")
def get_ticket_professionals(ticket_id: int, current_user: dict = Depends(verify_token)):
    try:
        return list_ticket_professionals(current_user["tenant_schema"], ticket_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("")
def post_ticket_professional(
    ticket_id: int,
    body: AddProfessionalBody,
    current_user: dict = Depends(verify_token),
):
    try:
        return add_ticket_professional(current_user["tenant_schema"], ticket_id, body.professional_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{ticket_professional_id}")
def delete_ticket_professional(
    ticket_id: int,
    ticket_professional_id: int,
    current_user: dict = Depends(verify_token),
):
    try:
        return remove_ticket_professional(current_user["tenant_schema"], ticket_id, ticket_professional_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{ticket_professional_id}/customers")
def post_ticket_professional_customer(
    ticket_id: int,
    ticket_professional_id: int,
    body: AddCustomerBody,
    current_user: dict = Depends(verify_token),
):
    try:
        return add_ticket_professional_customer(
            current_user["tenant_schema"], ticket_id, ticket_professional_id, body.customer_id, body.booking_date
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{ticket_professional_id}/customers/{ticket_professional_customer_id}")
def delete_ticket_professional_customer(
    ticket_id: int,
    ticket_professional_id: int,
    ticket_professional_customer_id: int,
    current_user: dict = Depends(verify_token),
):
    try:
        return remove_ticket_professional_customer(
            current_user["tenant_schema"], ticket_professional_customer_id
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{ticket_professional_id}/customers/{ticket_professional_customer_id}/products")
def post_ticket_professional_customer_product(
    ticket_id: int,
    ticket_professional_id: int,
    ticket_professional_customer_id: int,
    body: AddCustomerProductBody,
    current_user: dict = Depends(verify_token),
):
    try:
        return add_ticket_professional_customer_product(
            current_user["tenant_schema"], ticket_id, ticket_professional_id, ticket_professional_customer_id,
            body.product_id, body.product_name, body.unit_price,
            body.professional_unit_price, body.quantity,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.patch("/{ticket_professional_id}/customers/{ticket_professional_customer_id}/booking-date")
def patch_ticket_professional_customer_booking_date(
    ticket_id: int,
    ticket_professional_id: int,
    ticket_professional_customer_id: int,
    body: SetCustomerBookingDateBody,
    current_user: dict = Depends(verify_token),
):
    try:
        return set_ticket_professional_customer_booking_date(
            current_user["tenant_schema"], ticket_professional_customer_id, body.booking_date
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
