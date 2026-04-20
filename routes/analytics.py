from fastapi import APIRouter, HTTPException, Depends
from core.db import get_connection, set_tenant_schema
from core.security import verify_token
from services.analytics_service import get_familias_service
from services.analytics_service import get_nivel2_service
from services.analytics_service import get_nivel3_service
from services.analytics_service import get_nivel4_service
from services.analytics_service import get_precios_service
from services.analytics_service import get_ganancias_por_mes_service

router = APIRouter()


@router.get("/ganancias-por-mes")
def ganancias_por_mes(mes: str, current_user: dict = Depends(verify_token)):

    if not mes.isdigit() or len(mes) != 2 or int(mes) < 1 or int(mes) > 12:
        raise HTTPException(status_code=400, detail="Mes inválido. Usa formato 01-12.")

    try:
        return get_ganancias_por_mes_service(
            mes,
            current_user["tenant_schema"]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/familias")
def obtener_familias(current_user: dict = Depends(verify_token)):

    try:
        return get_familias_service()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/niveles2")
def obtener_nivel2(family: str, current_user: dict = Depends(verify_token)):

    try:
        return get_nivel2_service(family)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/niveles3")
def obtener_nivel3(family: str, level2: str, current_user: dict = Depends(verify_token)):

    try:
        return get_nivel3_service(family, level2)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/niveles4")
def obtener_nivel4(
    family: str,
    level2: str,
    level3: str,
    current_user: dict = Depends(verify_token)
):

    try:
        return get_nivel4_service(family, level2, level3)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/precios")
def obtener_precios(
    family: str,
    level2: str = None,
    level3: str = None,
    level4: str = None,
    current_user: dict = Depends(verify_token)
):

    try:
        return get_precios_service(family, level2, level3, level4)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
