from pydantic import BaseModel

class UploadVentasResponse(BaseModel):
    success: bool
    rows_deleted: int
    rows_inserted: int
