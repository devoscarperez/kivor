from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from core.security import verify_token
from services.barcode_scan_service import (
    guardar_lectura_codigo_barra,
    guardar_duracion_lectura,
)

router = APIRouter()


class BarcodeScanBody(BaseModel):
    codigo: str
    hora_lectura_cliente: str


class BarcodeScanDurationBody(BaseModel):
    fila: int
    duracion_ms: int


@router.post("/barcode-scan")
def post_barcode_scan(body: BarcodeScanBody, current_user: dict = Depends(verify_token)):
    try:
        return guardar_lectura_codigo_barra(
            body.codigo, body.hora_lectura_cliente, current_user["username"]
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.patch("/barcode-scan/duration")
def patch_barcode_scan_duration(body: BarcodeScanDurationBody, current_user: dict = Depends(verify_token)):
    try:
        return guardar_duracion_lectura(body.fila, body.duracion_ms)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
