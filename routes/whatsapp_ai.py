from fastapi import APIRouter, HTTPException, Body
from services.whatsapp_ai_service import process_whatsapp_message


router = APIRouter(prefix="/whatsapp")


@router.post("/brain")
def whatsapp_brain(payload: dict = Body(...)):
    try:
        return process_whatsapp_message(payload)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
