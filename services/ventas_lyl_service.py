# services/ventas_lyl_service.py

from typing import Optional
from datetime import datetime
import pandas as pd
from fastapi import UploadFile
from core.db import get_connection


EXCEL_SHEET_NAME = "VENTAS"


COLUMN_MAP = {
    "SP": "sp",
    "FECHA ENTREGA": "fecha_entrega",
    "PROFESIONAL": "profesional",
    "FECHA RECAU.": "fecha_recau",
    "RUT / CELULAR": "rut_celular",
    "NOMBRE": "nombre",
    "ORIGEN": "origen",
    "N° FORMULARIO": "nro_formulario",
    "FAMILIA": "familia",
    "NIVEL 2": "nivel_2",
    "NIVEL 3": "nivel_3",
    "NIVEL 4": "nivel_4",
    "PRECIO PROFESIONAL": "precio_profesional",
    "PRECIO WEB": "precio_web",
    "% PROFESIONAL": "porcentaje_profesional",
    "ABONO": "abono",
    "$ PAGADOS": "pagados",
    "TOTAL": "total",
    "N° GETNET": "nro_getnet",
    "TOTAL PW": "total_pw",
    "VALIDA FORM": "valida_form",
    "ABONO PERDIDO": "abono_perdido",
    "DESCUENTO": "descuento",
    "$ DESCUENTOS": "descuentos",
    "GANANCIA PROF": "ganancia_prof",
    "TOTAL GANANCIA PROF": "total_ganancia_prof",
    "GANANCIA SALON": "ganancia_salon",
    "DESCPROF_A_CLIENTAS": "descprof_a_clientas",
    "AÑO": "anio",
    "AÑO-MES": "anio_mes",
    "OBS": "obs",
}


REQUIRED_COLUMNS = [
    "AÑO",
    "AÑO-MES",
    "RUT / CELULAR",
    "FECHA ENTREGA",
]


DATE_FORMATS = [
    "%d-%m-%Y",
    "%d/%m/%Y",
    "%Y-%m-%d",
    "%d-%m-%Y %H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
]


DEFAULT_RUT_CELULAR = "999"


def clean_value(value):
    if pd.isna(value):
        return None

    value = str(value).strip()

    if value.lower() in ["nan", "none", "nat", ""]:
        return None

    if value.endswith(".0"):
        value = value[:-2]

    return value


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(col).strip() for col in df.columns]
    return df


def validate_columns(df: pd.DataFrame):
    missing = []

    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            missing.append(col)

    if missing:
        raise Exception(f"Faltan columnas obligatorias en el Excel: {', '.join(missing)}")


def parse_fecha_entrega(value) -> Optional[datetime]:
    if not value:
        return None

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    return None


def build_ventas_key(rut_celular: str, fecha_entrega: datetime, correlativo: int) -> str:
    mes = str(fecha_entrega.month).zfill(2)
    dia = str(fecha_entrega.day).zfill(2)

    return f"{rut_celular}-{mes}{dia}-{correlativo}"


def validate_ventas_key_inputs(df: pd.DataFrame):
    errors = []

    for index, row in df.iterrows():
        fila_excel = int(index) + 2

        fecha_entrega_raw = clean_value(row.get("FECHA ENTREGA"))
        if not fecha_entrega_raw or parse_fecha_entrega(fecha_entrega_raw) is None:
            errors.append(f"Fila {fila_excel}: FECHA ENTREGA vacía o con formato inválido.")

    if errors:
        raise Exception(
            "No se puede procesar el archivo. Errores encontrados:\n" + "\n".join(errors)
        )


def filter_period(df: pd.DataFrame, anio: int, mes: Optional[int] = None) -> pd.DataFrame:
    anio_text = str(anio)

    df["AÑO"] = df["AÑO"].apply(clean_value)
    df["AÑO-MES"] = df["AÑO-MES"].apply(clean_value)

    if mes is None:
        return df[df["AÑO"] == anio_text].copy()

    anio_mes = f"{anio}-{str(mes).zfill(2)}"

    df_filtered = df[
        (df["AÑO"] == anio_text) &
        (df["AÑO-MES"] == anio_mes)
    ].copy()

    return df_filtered


def build_insert_rows(df: pd.DataFrame, archivo_origen: str):
    rows = []
    correlativo = 0

    for index, row in df.iterrows():
        correlativo += 1
        record = {}

        for excel_col, db_col in COLUMN_MAP.items():
            if excel_col in df.columns:
                record[db_col] = clean_value(row[excel_col])
            else:
                record[db_col] = None

        rut_celular = clean_value(row.get("RUT / CELULAR")) or DEFAULT_RUT_CELULAR
        record["rut_celular"] = rut_celular

        fecha_entrega = parse_fecha_entrega(clean_value(row.get("FECHA ENTREGA")))

        record["ventas_key"] = build_ventas_key(rut_celular, fecha_entrega, correlativo)

        record["archivo_origen"] = archivo_origen
        record["hoja_origen"] = EXCEL_SHEET_NAME
        record["fila_excel"] = int(index) + 2

        rows.append(record)

    return rows


def delete_period(cur, anio: int, anio_mes: Optional[str] = None) -> int:
    if anio_mes is None:
        cur.execute(
            "DELETE FROM core.stg_ventas_lyl WHERE anio = %s",
            (str(anio),)
        )
    else:
        cur.execute(
            """
            DELETE FROM core.stg_ventas_lyl
            WHERE anio = %s
            AND anio_mes = %s
            """,
            (str(anio), anio_mes)
        )

    return cur.rowcount


def insert_dataframe_ventas(cur, rows: list) -> int:
    if not rows:
        return 0

    db_columns = list(COLUMN_MAP.values()) + [
        "ventas_key",
        "archivo_origen",
        "hoja_origen",
        "fila_excel",
    ]

    columns_sql = ", ".join(db_columns)
    placeholders = ", ".join(["%s"] * len(db_columns))

    insert_sql = f"""
        INSERT INTO core.stg_ventas_lyl (
            {columns_sql}
        )
        VALUES (
            {placeholders}
        )
    """

    values = [
        tuple(row.get(col) for col in db_columns)
        for row in rows
    ]

    cur.executemany(insert_sql, values)

    return len(values)


async def upload_ventas_service(
    anio: int,
    mes: Optional[int],
    file: UploadFile,
    current_user: dict
):
    if mes is not None and (mes < 1 or mes > 12):
        raise Exception("Mes inválido. Debe estar entre 1 y 12.")

    anio_mes = f"{anio}-{str(mes).zfill(2)}" if mes is not None else None
    periodo_label = anio_mes if anio_mes else str(anio)

    try:
        df = pd.read_excel(
            file.file,
            sheet_name=EXCEL_SHEET_NAME,
            engine="openpyxl",
            dtype=str
        )

        df = normalize_columns(df)
        validate_columns(df)

        df_filtered = filter_period(df, anio, mes)

        if df_filtered.empty:
            raise Exception(f"No existen registros para el período {periodo_label} en el Excel.")

        validate_ventas_key_inputs(df_filtered)

        rows = build_insert_rows(df_filtered, file.filename)

        with get_connection() as conn:
            with conn.cursor() as cur:
                rows_deleted = delete_period(cur, anio, anio_mes)
                rows_inserted = insert_dataframe_ventas(cur, rows)

            conn.commit()

        return {
            "success": True,
            "anio": anio,
            "mes": mes,
            "anio_mes": anio_mes,
            "rows_deleted": rows_deleted,
            "rows_inserted": rows_inserted,
            "message": "Carga realizada correctamente"
        }

    except Exception as e:
        raise Exception(f"Error cargando ventas: {str(e)}")


# ============================================================
# REPORTE COMPARATIVO DE VENTAS (Año 1 vs Año 2)
# ============================================================

METRICAS_REPORTE = {
    "ganancia_salon": "ganancia_salon",
    "ganancia_prof": "ganancia_prof",
    "precio_web": "precio_web",
}

MESES_REPORTE = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]


def get_anios_disponibles_service():
    query = """
        SELECT DISTINCT EXTRACT(YEAR FROM fecha_entrega)::int AS anio
        FROM core.stg_ventas_lyl
        WHERE fecha_entrega IS NOT NULL
        ORDER BY anio;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

    return [r[0] for r in rows]


def get_familias_reporte_service():
    query = """
        SELECT service_template_code
        FROM core.service
        WHERE service_template_code IS NOT NULL;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

    return [r[0] for r in rows]


def get_profesionales_reporte_service():
    query = """
        SELECT professional_nickname
        FROM core.professional
        WHERE professional_nickname IS NOT NULL
        ORDER BY professional_nickname ASC;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

    return [r[0] for r in rows]


def get_reporte_ventas_service(
    anio1: int,
    anio2: int,
    metrica: str,
    familias: Optional[list] = None,
    profesionales: Optional[list] = None,
):
    if metrica not in METRICAS_REPORTE:
        raise Exception(f"Métrica inválida: {metrica}")

    columna_metrica = METRICAS_REPORTE[metrica]

    condiciones = ["EXTRACT(YEAR FROM fecha_entrega) IN (%s, %s)"]
    params = [anio1, anio2]

    if familias:
        condiciones.append("familia = ANY(%s)")
        params.append(familias)

    if profesionales:
        condiciones.append("profesional = ANY(%s)")
        params.append(profesionales)

    where_sql = " AND ".join(condiciones)

    query = f"""
        SELECT
            EXTRACT(YEAR FROM fecha_entrega)::int AS anio,
            EXTRACT(MONTH FROM fecha_entrega)::int AS mes,
            SUM({columna_metrica}) AS valor
        FROM core.stg_ventas_lyl
        WHERE {where_sql}
        GROUP BY 1, 2
        ORDER BY 1, 2;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            rows = cur.fetchall()

    valores_anio1 = [None] * 12
    valores_anio2 = [None] * 12

    for anio, mes, valor in rows:
        valor = float(valor) if valor is not None else None
        if anio == anio1:
            valores_anio1[mes - 1] = valor
        elif anio == anio2:
            valores_anio2[mes - 1] = valor

    return {
        "anio1": anio1,
        "anio2": anio2,
        "metrica": metrica,
        "meses": MESES_REPORTE,
        "valores_anio1": valores_anio1,
        "valores_anio2": valores_anio2,
    }
