# services/precios_servicios_service.py

from datetime import date

import pandas as pd
from fastapi import UploadFile
from core.db import get_connection
from services.ventas_lyl_service import clean_value, normalize_columns


TENANT_SCHEMA = "lindasylunaticas"

MES_A_NUMERO = {
    "ENE": 1, "FEB": 2, "MAR": 3, "ABR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AGO": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DIC": 12,
}

MODOS_VALIDOS = {"full", "parcial"}

# El Excel real trae el encabezado como "SERVICO" (sin la segunda "I").
# Se busca por prefijo para que funcione igual si en algún momento se
# corrige a "SERVICIO".
SERVICIO_PREFIX = "SERVIC"

COLUMN_MAP_FIJO = {
    "CLAVE": "clave",
    "FAMILIA": "familia",
    "NIVEL_2": "nivel_2",
    "NIVEL_3": "nivel_3",
    "NIVEL_4": "nivel_4",
    "NOMBRE_CORTO": "nombre_corto",
    "DURACION_HORAS": "duracion_horas",
    "PORCENTAJE_PROFESIONAL": "porcentaje_profesional",
}

REQUIRED_COLUMNS_FIJAS = ["CLAVE", "FAMILIA", "PRECIO_PROFESIONAL", "PRECIO_WEB"]

DB_COLUMNS = [
    "clave", "familia", "nivel_2", "nivel_3", "nivel_4",
    "precio_profesional", "precio_lista", "nombre_corto",
    "duracion_horas", "porcentaje_profesional", "servicio",
    "archivo_origen", "hoja_origen", "fila_excel",
    "precio_fecha_desde", "vigencia", "clave_servicio_vigencia",
]


def find_column_by_prefix(columns, prefix):
    for col in columns:
        if col.upper().startswith(prefix):
            return col
    return None


def build_vigencia(mes: str, anio: int) -> tuple[str, date]:
    mes_normalizado = (mes or "").strip().upper()

    if mes_normalizado not in MES_A_NUMERO:
        raise Exception(
            f"Mes inválido: '{mes}'. Debe ser uno de {', '.join(MES_A_NUMERO)}."
        )

    if not (2000 <= anio <= 2100):
        raise Exception(f"Año inválido: '{anio}'.")

    vigencia = f"{mes_normalizado}{anio}"
    precio_fecha_desde = date(anio, MES_A_NUMERO[mes_normalizado], 1)

    return vigencia, precio_fecha_desde


def validate_columns(df: pd.DataFrame):
    missing = [col for col in REQUIRED_COLUMNS_FIJAS if col not in df.columns]

    if missing:
        raise Exception(f"Faltan columnas obligatorias en el Excel: {', '.join(missing)}")


def parse_servicio(raw_value) -> bool:
    return (raw_value or "").strip().upper() == "X"


def build_insert_rows(
    df: pd.DataFrame,
    col_servicio: str,
    archivo_origen: str,
    vigencia: str,
    precio_fecha_desde: date,
):
    rows = []

    for index, row in df.iterrows():
        clave = clean_value(row.get("CLAVE"))
        if not clave:
            continue

        record = {}
        for excel_col, db_col in COLUMN_MAP_FIJO.items():
            record[db_col] = clean_value(row.get(excel_col)) if excel_col in df.columns else None

        record["clave"] = clave
        record["precio_profesional"] = clean_value(row.get("PRECIO_PROFESIONAL"))
        record["precio_lista"] = clean_value(row.get("PRECIO_WEB"))
        record["servicio"] = parse_servicio(clean_value(row.get(col_servicio)) if col_servicio else None)
        record["archivo_origen"] = archivo_origen
        record["hoja_origen"] = vigencia
        record["fila_excel"] = int(index) + 2
        record["precio_fecha_desde"] = precio_fecha_desde
        record["vigencia"] = vigencia
        record["clave_servicio_vigencia"] = f"{clave}-{vigencia}"

        rows.append(record)

    return rows


def delete_by_vigencia(cur, vigencia: str) -> int:
    cur.execute(
        f"DELETE FROM {TENANT_SCHEMA}.precios_servicios WHERE vigencia = %s",
        (vigencia,),
    )
    return cur.rowcount


def delete_by_claves_vigencia(cur, claves_vigencia: list) -> int:
    if not claves_vigencia:
        return 0

    cur.execute(
        f"DELETE FROM {TENANT_SCHEMA}.precios_servicios WHERE clave_servicio_vigencia = ANY(%s)",
        (claves_vigencia,),
    )
    return cur.rowcount


def insert_precios(cur, rows: list) -> int:
    if not rows:
        return 0

    columns_sql = ", ".join(DB_COLUMNS)
    placeholders = ", ".join(["%s"] * len(DB_COLUMNS))

    insert_sql = f"""
        INSERT INTO {TENANT_SCHEMA}.precios_servicios (
            {columns_sql}
        )
        VALUES (
            {placeholders}
        )
    """

    values = [tuple(row.get(col) for col in DB_COLUMNS) for row in rows]
    cur.executemany(insert_sql, values)

    return len(values)


async def upload_precios_service(file: UploadFile, mes: str, anio: int, modo: str, current_user: dict):
    try:
        modo_normalizado = (modo or "").strip().lower()
        if modo_normalizado not in MODOS_VALIDOS:
            raise Exception(f"Modo inválido: '{modo}'. Debe ser 'full' o 'parcial'.")

        vigencia, precio_fecha_desde = build_vigencia(mes, anio)

        try:
            df = pd.read_excel(
                file.file,
                sheet_name=vigencia,
                engine="openpyxl",
                dtype=str
            )
        except ValueError:
            raise Exception(
                f"No se encontró la hoja '{vigencia}' en el archivo. "
                "Verifica que el mes/año ingresados coincidan con la hoja del Excel."
            )

        df = normalize_columns(df)

        col_servicio = find_column_by_prefix(df.columns, SERVICIO_PREFIX)

        validate_columns(df)

        rows = build_insert_rows(df, col_servicio, file.filename, vigencia, precio_fecha_desde)

        if not rows:
            raise Exception(f"No se encontraron filas con CLAVE en la hoja {vigencia}.")

        with get_connection() as conn:
            with conn.cursor() as cur:
                if modo_normalizado == "full":
                    rows_deleted = delete_by_vigencia(cur, vigencia)
                else:
                    claves_vigencia = [row["clave_servicio_vigencia"] for row in rows]
                    rows_deleted = delete_by_claves_vigencia(cur, claves_vigencia)

                rows_inserted = insert_precios(cur, rows)

            conn.commit()

        return {
            "success": True,
            "rows_deleted": rows_deleted,
            "rows_inserted": rows_inserted,
            "vigencia": vigencia,
            "modo": modo_normalizado,
            "message": "Carga de precios realizada correctamente"
        }

    except Exception as e:
        raise Exception(f"Error cargando precios: {str(e)}")
