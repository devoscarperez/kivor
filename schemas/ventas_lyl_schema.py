from pydantic import BaseModel


class UploadVentasResponse(BaseModel):
    success: bool
    anio: int
    mes: int
    anio_mes: str
    rows_deleted: int
    rows_inserted: int
    message: str
