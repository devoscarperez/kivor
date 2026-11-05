from datetime import date

from fastapi import APIRouter, HTTPException, Depends, Query

from core.security import verify_token
from services.professional_service import search_professionals, get_professional_id_by_person
from services.ticket_professional_service import list_my_agenda_today

router = APIRouter(prefix="/professional", tags=["Professional"])


@router.get("/search")
def get_professional_search(
    q: str = Query(..., min_length=2),
    current_user: dict = Depends(verify_token),
):
    try:
        return search_professionals(q)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/me/agenda-hoy")
def get_my_agenda_hoy(
    agenda_date: date = Query(default=None, alias="date"),
    current_user: dict = Depends(verify_token),
):
    try:
        professional_id = get_professional_id_by_person(current_user["person_id"])
        if not professional_id:
            raise Exception("El usuario actual no tiene una profesional asociada.")

        return list_my_agenda_today(current_user["tenant_schema"], professional_id, agenda_date or date.today())
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
