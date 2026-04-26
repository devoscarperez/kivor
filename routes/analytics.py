from fastapi import APIRouter, HTTPException, Depends
from core.security import verify_token

from services.analytics_service import (
    get_familias_service,
    get_nivel2_service,
    get_nivel3_service,
    get_nivel4_service,
    get_precios_service,
    get_ganancias_por_mes_service
)

router = APIRouter()


@router.get("/ganancias-por-mes")
def ganancias_por_mes(mes: str, current_user: dict = Depends(verify_token)):

    if not mes.isdigit() or len(mes) != 2 or int(mes) < 1 or int(mes) > 12:
        raise HTTPException(status_code=400, detail="Mes inválido. Usa formato 01-12.")

    return get_ganancias_por_mes_service(
        mes,
        current_user["tenant_schema"]
    )


@router.get("/familias")
def obtener_familias(current_user: dict = Depends(verify_token)):

    return get_familias_service()


@router.get("/niveles2")
def obtener_nivel2(family: str, current_user: dict = Depends(verify_token)):

    return get_nivel2_service(family)


@router.get("/niveles3")
def obtener_nivel3(family: str, level2: str, current_user: dict = Depends(verify_token)):

    return get_nivel3_service(family, level2)


@router.get("/niveles4")
def obtener_nivel4(
    family: str,
    level2: str,
    level3: str,
    current_user: dict = Depends(verify_token)
):

    return get_nivel4_service(family, level2, level3)


@router.get("/precios")
def obtener_precios(
    family: str,
    level2: str = None,
    level3: str = None,
    level4: str = None,
    current_user: dict = Depends(verify_token)
):

    return get_precios_service(family, level2, level3, level4)

@router.get("/preciosGS")
def obtener_precios(
    family: str = None,
    level2: str = None,
    level3: str = None,
    level4: str = None,
):
    return get_precios_service_GS(family, level2, level3, level4)

