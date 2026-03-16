from fastapi import APIRouter

router = APIRouter()

@router.get("/dashboard/test")
def dashboard_test():
    return {"dashboard": "ok"}
