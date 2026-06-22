from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException
from core.security import verify_token
from services.ventas_lyl_service import upload_ventas_service
from schemas.ventas_lyl_schema import UploadVentasResponse

router = APIRouter(prefix="/ventas-lyl", tags=["Ventas LYL"])


@router.post("/upload", response_model=UploadVentasResponse)
async def upload_ventas(
    anio: int = Form(...),
    mes: int = Form(...),
    file: UploadFile = File(...),
    current_user: dict = Depends(verify_token)
):
    try:
        return await upload_ventas_service(anio, mes, file, current_user)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
