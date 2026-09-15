from pydantic import BaseModel


class UploadPreciosResponse(BaseModel):
    success: bool
    rows_deleted: int
    rows_inserted: int
    message: str
