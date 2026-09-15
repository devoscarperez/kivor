from typing import List, Literal, Optional
from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException, Query
from core.security import verify_token
from services.ventas_lyl_service import (
    upload_ventas_service,
    get_anios_disponibles_service,
    get_familias_reporte_service,
    get_profesionales_reporte_service,
    get_reporte_ventas_service,
    get_reporte_kpis_service,
)
from schemas.ventas_lyl_schema import UploadVentasResponse, ReporteVentasResponse, ReporteKpisResponse

router = APIRouter(prefix="/ventas-lyl", tags=["Ventas LYL"])


@router.post("/upload", response_model=UploadVentasResponse)
async def upload_ventas(
    anio: int = Form(...),
    mes: Optional[int] = Form(None),
    file: UploadFile = File(...),
    current_user: dict = Depends(verify_token)
):
    try:
        return await upload_ventas_service(anio, mes, file, current_user)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/filtros/anios")
def obtener_anios_reporte(current_user: dict = Depends(verify_token)):
    return get_anios_disponibles_service()


@router.get("/filtros/familias")
def obtener_familias_reporte(current_user: dict = Depends(verify_token)):
    return get_familias_reporte_service()


@router.get("/filtros/profesionales")
def obtener_profesionales_reporte(current_user: dict = Depends(verify_token)):
    return get_profesionales_reporte_service()


@router.get("/reporte", response_model=ReporteVentasResponse)
def obtener_reporte_ventas(
    anio1: int,
    anio2: int,
    metrica: Literal["ganancia_salon", "ganancia_prof", "precio_web"],
    familias: Optional[List[str]] = Query(None),
    profesionales: Optional[List[str]] = Query(None),
    dias_semana: Optional[List[int]] = Query(None),
    quincenas: Optional[List[int]] = Query(None),
    current_user: dict = Depends(verify_token)
):
    try:
        return get_reporte_ventas_service(
            anio1, anio2, metrica, familias, profesionales, dias_semana, quincenas
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/reporte-kpis", response_model=ReporteKpisResponse)
def obtener_reporte_kpis(
    anio1: int,
    anio2: int,
    metrica: Literal["ganancia_salon", "ganancia_prof", "precio_web"],
    familias: Optional[List[str]] = Query(None),
    profesionales: Optional[List[str]] = Query(None),
    dias_semana: Optional[List[int]] = Query(None),
    quincenas: Optional[List[int]] = Query(None),
    current_user: dict = Depends(verify_token)
):
    try:
        return get_reporte_kpis_service(
            anio1, anio2, metrica, familias, profesionales, dias_semana, quincenas
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
