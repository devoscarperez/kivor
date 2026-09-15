# services/precios_servicios_service.py

import pandas as pd
from fastapi import UploadFile
from core.db import get_connection
from services.ventas_lyl_service import clean_value, normalize_columns


EXCEL_SHEET_NAME = "PRECIOS"
TENANT_SCHEMA = "lindasylunaticas"

# PROF_SEPT_2026 / WEB_SEPT_2026 traen el periodo incrustado en el nombre de
# columna. Buscar por prefijo evita romper la carga cuando el Excel del mes
# siguiente venga con PROF_OCT_2026, etc.
PRECIO_PROFESIONAL_PREFIX = "PROF_"
PRECIO_LISTA_PREFIX = "WEB_"

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
    "PORCENTAJE": "porcentaje_profesional",
}

REQUIRED_COLUMNS_FIJAS = ["CLAVE", "FAMILIA"]

DB_COLUMNS = [
    "clave", "familia", "nivel_2", "nivel_3", "nivel_4",
    "precio_profesional", "precio_lista", "nombre_corto",
    "duracion_horas", "porcentaje_profesional", "servicio",
    "archivo_origen", "hoja_origen", "fila_excel",
]


def find_column_by_prefix(columns, prefix):
    for col in columns:
        if col.upper().startswith(prefix):
            return col
    return None


def validate_columns(df: pd.DataFrame, col_precio_prof: str, col_precio_lista: str):
    missing = [col for col in REQUIRED_COLUMNS_FIJAS if col not in df.columns]

    if not col_precio_prof:
        missing.append(f"columna con prefijo '{PRECIO_PROFESIONAL_PREFIX}' (precio profesional)")
    if not col_precio_lista:
        missing.append(f"columna con prefijo '{PRECIO_LISTA_PREFIX}' (precio lista)")

    if missing:
        raise Exception(f"Faltan columnas obligatorias en el Excel: {', '.join(missing)}")


def parse_servicio(raw_value) -> bool:
    return (raw_value or "").strip().upper() == "X"


def build_insert_rows(df: pd.DataFrame, col_precio_prof: str, col_precio_lista: str, col_servicio: str, archivo_origen: str):
    rows = []

    for index, row in df.iterrows():
        clave = clean_value(row.get("CLAVE"))
        if not clave:
            continue

        record = {}
        for excel_col, db_col in COLUMN_MAP_FIJO.items():
            record[db_col] = clean_value(row.get(excel_col)) if excel_col in df.columns else None

        record["clave"] = clave
        record["precio_profesional"] = clean_value(row.get(col_precio_prof))
        record["precio_lista"] = clean_value(row.get(col_precio_lista))
        record["servicio"] = parse_servicio(clean_value(row.get(col_servicio)) if col_servicio else None)
        record["archivo_origen"] = archivo_origen
        record["hoja_origen"] = EXCEL_SHEET_NAME
        record["fila_excel"] = int(index) + 2

        rows.append(record)

    return rows


def delete_all_precios(cur) -> int:
    cur.execute(f"DELETE FROM {TENANT_SCHEMA}.precios_servicios")
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


async def upload_precios_service(file: UploadFile, current_user: dict):
    try:
        df = pd.read_excel(
            file.file,
            sheet_name=EXCEL_SHEET_NAME,
            engine="openpyxl",
            dtype=str
        )

        df = normalize_columns(df)

        col_precio_prof = find_column_by_prefix(df.columns, PRECIO_PROFESIONAL_PREFIX)
        col_precio_lista = find_column_by_prefix(df.columns, PRECIO_LISTA_PREFIX)
        col_servicio = find_column_by_prefix(df.columns, SERVICIO_PREFIX)

        validate_columns(df, col_precio_prof, col_precio_lista)

        rows = build_insert_rows(df, col_precio_prof, col_precio_lista, col_servicio, file.filename)

        if not rows:
            raise Exception("No se encontraron filas con CLAVE en la hoja PRECIOS.")

        with get_connection() as conn:
            with conn.cursor() as cur:
                rows_deleted = delete_all_precios(cur)
                rows_inserted = insert_precios(cur, rows)

            conn.commit()

        return {
            "success": True,
            "rows_deleted": rows_deleted,
            "rows_inserted": rows_inserted,
            "message": "Carga de precios realizada correctamente"
        }

    except Exception as e:
        raise Exception(f"Error cargando precios: {str(e)}")
