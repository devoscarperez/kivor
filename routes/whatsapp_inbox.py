from fastapi import APIRouter, HTTPException, Depends

from core.security import verify_token
from services.whatsapp_inbox_service import (
    list_pending_conversations,
    get_conversation_detail,
    take_conversation,
    close_conversation,
    return_to_bot,
)

router = APIRouter(prefix="/whatsapp-inbox")


@router.get("")
def get_inbox(current_user: dict = Depends(verify_token)):
    try:
        return list_pending_conversations(current_user["tenant_schema"])
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{phone}")
def get_inbox_conversation(phone: str, current_user: dict = Depends(verify_token)):
    try:
        return get_conversation_detail(current_user["tenant_schema"], phone)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{phone}/take")
def take_inbox_conversation(phone: str, current_user: dict = Depends(verify_token)):
    try:
        return take_conversation(current_user["tenant_schema"], phone, current_user["username"])
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{phone}/close")
def close_inbox_conversation(phone: str, current_user: dict = Depends(verify_token)):
    try:
        return close_conversation(current_user["tenant_schema"], phone)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{phone}/return-to-bot")
def return_inbox_conversation_to_bot(phone: str, current_user: dict = Depends(verify_token)):
    try:
        return return_to_bot(current_user["tenant_schema"], phone)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
