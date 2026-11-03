from fastapi import APIRouter, HTTPException, Depends, Query

from core.security import verify_token
from services.professional_service import search_professionals

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
