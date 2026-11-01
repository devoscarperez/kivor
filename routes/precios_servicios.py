from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException
from core.security import verify_token
from services.precios_servicios_service import upload_precios_service
from schemas.precios_servicios_schema import UploadPreciosResponse

router = APIRouter(prefix="/precios-servicios", tags=["Precios Servicios"])


@router.post("/upload", response_model=UploadPreciosResponse)
async def upload_precios(
    file: UploadFile = File(...),
    mes: str = Form(...),
    anio: int = Form(...),
    modo: str = Form(...),
    current_user: dict = Depends(verify_token)
):
    try:
        return await upload_precios_service(file, mes, anio, modo, current_user)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
