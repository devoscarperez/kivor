router = APIRouter(prefix="/ventas-lyl")

@router.post("/upload")
async def upload_ventas(
        anio: int,
        mes: int,
        file: UploadFile = File(...)
):
