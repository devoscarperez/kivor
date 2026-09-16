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


class CrossSellingAnio(BaseModel):
    familias: list[str]
    matriz: list[list[int]]


class AbcFamiliaRow(BaseModel):
    familia: str
    valor: float
    porcentaje: float
    porcentaje_acumulado: float
    umbrales: list[int]


class ReporteKpisResponse(BaseModel):
    anio1: int
    anio2: int
    metrica: str
    meses: list[str]
    ticket_mensual_anio1: list[Optional[float]]
    ticket_mensual_anio2: list[Optional[float]]
    ticket_mensual_tickets_anio1: list[int]
    ticket_mensual_tickets_anio2: list[int]
    ticket_semestral_anio1: list[Optional[float]]
    ticket_semestral_anio2: list[Optional[float]]
    ticket_semestral_tickets_anio1: list[int]
    ticket_semestral_tickets_anio2: list[int]
    ticket_anual_anio1: Optional[float]
    ticket_anual_anio2: Optional[float]
    ticket_anual_tickets_anio1: int
    ticket_anual_tickets_anio2: int
    clientas_nuevas_anio1: list[int]
    clientas_nuevas_anio2: list[int]
    clientas_nuevas_anual_anio1: int
    clientas_nuevas_anual_anio2: int
    cross_selling_anio1: CrossSellingAnio
    cross_selling_anio2: CrossSellingAnio
    abc_anio1: list[AbcFamiliaRow]
    abc_anio2: list[AbcFamiliaRow]
