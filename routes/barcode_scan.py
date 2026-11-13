from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from core.security import verify_token
from services.barcode_scan_service import guardar_lectura_codigo_barra

router = APIRouter()


class BarcodeScanBody(BaseModel):
    codigo: str
    hora_lectura_cliente: str


@router.post("/barcode-scan")
def post_barcode_scan(body: BarcodeScanBody, current_user: dict = Depends(verify_token)):
    try:
        return guardar_lectura_codigo_barra(
            body.codigo, body.hora_lectura_cliente, current_user["username"]
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
