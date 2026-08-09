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
