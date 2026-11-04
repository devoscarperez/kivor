from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException, Query
from core.security import verify_token
from services.precios_servicios_service import upload_precios_service, get_opciones_servicio
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


@router.get("/opciones")
def get_opciones(
    familia: str = Query(default=None),
    nivel_2: str = Query(default=None),
    nivel_3: str = Query(default=None),
    current_user: dict = Depends(verify_token),
):
    try:
        return get_opciones_servicio(current_user["tenant_schema"], familia, nivel_2, nivel_3)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
