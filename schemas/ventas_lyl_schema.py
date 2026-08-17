from typing import Optional
from pydantic import BaseModel


class UploadVentasResponse(BaseModel):
    success: bool
    anio: int
    mes: Optional[int] = None
    anio_mes: Optional[str] = None
    rows_deleted: int
    rows_inserted: int
    message: str


class ReporteVentasResponse(BaseModel):
    anio1: int
    anio2: int
    metrica: str
    meses: list[str]
    valores_anio1: list[Optional[float]]
    valores_anio2: list[Optional[float]]
