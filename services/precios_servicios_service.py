# services/precios_servicios_service.py

from datetime import date

import pandas as pd
from fastapi import UploadFile
from psycopg import sql
from core.db import get_connection
from services.ventas_lyl_service import clean_value, normalize_columns


EXCEL_SHEET_NAME = "PRECIOS"
TENANT_SCHEMA = "lindasylunaticas"

MES_A_NUMERO = {
    "ENE": 1, "FEB": 2, "MAR": 3, "ABR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AGO": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DIC": 12,
}

NUMERO_A_MES = {numero: mes for mes, numero in MES_A_NUMERO.items()}


def vigencia_actual() -> str:
    hoy = date.today()
    return f"{NUMERO_A_MES[hoy.month]}{hoy.year}"


def _validated_schema(tenant_schema: str) -> str:
    if not tenant_schema or not tenant_schema.isidentifier():
        raise Exception("invalid_tenant")
    return tenant_schema

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

REQUIRED_COLUMNS_FIJAS = ["CLAVE", "FAMILIA"]

DB_COLUMNS = [
    "clave", "familia", "nivel_2", "nivel_3", "nivel_4",
    "precio_profesional", "precio_lista", "nombre_corto",
    "duracion_horas", "porcentaje_profesional", "servicio",
    "archivo_origen", "hoja_origen", "fila_excel",
    "precio_fecha_desde", "vigencia", "clave_servicio_vigencia",
    "service_master_id",
]


def find_column_by_prefix(columns, prefix):
    for col in columns:
        if col.upper().startswith(prefix):
            return col
    return None


def build_vigencia(mes: str, anio: int) -> tuple[str, date, str]:
    mes_normalizado = (mes or "").strip().upper()

    if mes_normalizado not in MES_A_NUMERO:
        raise Exception(
            f"Mes inválido: '{mes}'. Debe ser uno de {', '.join(MES_A_NUMERO)}."
        )

    if not (2000 <= anio <= 2100):
        raise Exception(f"Año inválido: '{anio}'.")

    vigencia = f"{mes_normalizado}{anio}"
    precio_fecha_desde = date(anio, MES_A_NUMERO[mes_normalizado], 1)

    return vigencia, precio_fecha_desde, mes_normalizado


def build_columnas_precio(mes_normalizado: str, anio: int) -> tuple[str, str]:
    return f"PROF_{mes_normalizado}_{anio}", f"WEB_{mes_normalizado}_{anio}"


def validate_columns(df: pd.DataFrame, col_precio_prof: str, col_precio_lista: str):
    missing = [col for col in REQUIRED_COLUMNS_FIJAS if col not in df.columns]

    if col_precio_prof not in df.columns:
        missing.append(col_precio_prof)
    if col_precio_lista not in df.columns:
        missing.append(col_precio_lista)

    if missing:
        raise Exception(
            f"Faltan columnas obligatorias en el Excel: {', '.join(missing)}. "
            "Verifica que el mes/año seleccionados coincidan con el Excel."
        )


def parse_servicio(raw_value) -> bool:
    return (raw_value or "").strip().upper() == "X"


def build_insert_rows(
    df: pd.DataFrame,
    col_precio_prof: str,
    col_precio_lista: str,
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
        record["precio_profesional"] = clean_value(row.get(col_precio_prof))
        record["precio_lista"] = clean_value(row.get(col_precio_lista))
        record["servicio"] = parse_servicio(clean_value(row.get(col_servicio)) if col_servicio else None)
        record["archivo_origen"] = archivo_origen
        record["hoja_origen"] = EXCEL_SHEET_NAME
        record["fila_excel"] = int(index) + 2
        record["precio_fecha_desde"] = precio_fecha_desde
        record["vigencia"] = vigencia
        record["clave_servicio_vigencia"] = f"{clave}-{vigencia}"

        rows.append(record)

    return rows


def get_or_create_service_master_id(cur, clave, familia, nivel_2, nivel_3, nivel_4) -> int:
    cur.execute(
        f"""
        INSERT INTO {TENANT_SCHEMA}.service_master
            (service_master_family, service_master_level2, service_master_level3, service_master_level4, service_master_clave)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (service_master_clave)
        DO UPDATE SET service_master_clave = EXCLUDED.service_master_clave
        RETURNING service_master_id
        """,
        (familia, nivel_2, nivel_3, nivel_4, clave),
    )
    return cur.fetchone()[0]


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

        vigencia, precio_fecha_desde, mes_normalizado = build_vigencia(mes, anio)
        col_precio_prof, col_precio_lista = build_columnas_precio(mes_normalizado, anio)

        try:
            df = pd.read_excel(
                file.file,
                sheet_name=EXCEL_SHEET_NAME,
                engine="openpyxl",
                dtype=str
            )
        except ValueError:
            raise Exception(f"No se encontró la hoja '{EXCEL_SHEET_NAME}' en el archivo.")

        df = normalize_columns(df)

        col_servicio = find_column_by_prefix(df.columns, SERVICIO_PREFIX)

        validate_columns(df, col_precio_prof, col_precio_lista)

        rows = build_insert_rows(
            df, col_precio_prof, col_precio_lista, col_servicio,
            file.filename, vigencia, precio_fecha_desde
        )

        if not rows:
            raise Exception(f"No se encontraron filas con CLAVE en la hoja {EXCEL_SHEET_NAME}.")

        with get_connection() as conn:
            with conn.cursor() as cur:
                for row in rows:
                    row["service_master_id"] = get_or_create_service_master_id(
                        cur, row["clave"], row["familia"],
                        row["nivel_2"], row["nivel_3"], row["nivel_4"]
                    )

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


def get_opciones_servicio(
    tenant_schema: str, familia: str = None, nivel_2: str = None, nivel_3: str = None
) -> dict:
    tenant_schema = _validated_schema(tenant_schema)
    vigencia = vigencia_actual()

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("""
                    SELECT DISTINCT familia FROM {schema}.precios_servicios
                    WHERE vigencia = %s AND servicio = true AND familia IS NOT NULL
                    ORDER BY familia
                """).format(schema=sql.Identifier(tenant_schema)),
                (vigencia,),
            )
            familias = [row[0] for row in cur.fetchall()]

            niveles_2 = []
            if familia:
                cur.execute(
                    sql.SQL("""
                        SELECT DISTINCT nivel_2 FROM {schema}.precios_servicios
                        WHERE vigencia = %s AND servicio = true AND familia = %s AND nivel_2 IS NOT NULL
                        ORDER BY nivel_2
                    """).format(schema=sql.Identifier(tenant_schema)),
                    (vigencia, familia),
                )
                niveles_2 = [row[0] for row in cur.fetchall()]

            niveles_3 = []
            if familia and nivel_2:
                cur.execute(
                    sql.SQL("""
                        SELECT DISTINCT nivel_3 FROM {schema}.precios_servicios
                        WHERE vigencia = %s AND servicio = true AND familia = %s AND nivel_2 = %s
                          AND nivel_3 IS NOT NULL
                        ORDER BY nivel_3
                    """).format(schema=sql.Identifier(tenant_schema)),
                    (vigencia, familia, nivel_2),
                )
                niveles_3 = [row[0] for row in cur.fetchall()]

            servicios = []
            if familia and nivel_2 and nivel_3:
                cur.execute(
                    sql.SQL("""
                        SELECT service_master_id, clave, nivel_4, precio_lista, precio_profesional
                        FROM {schema}.precios_servicios
                        WHERE vigencia = %s AND servicio = true AND familia = %s AND nivel_2 = %s
                          AND nivel_3 = %s
                        ORDER BY nivel_4
                    """).format(schema=sql.Identifier(tenant_schema)),
                    (vigencia, familia, nivel_2, nivel_3),
                )
                servicios = [
                    {
                        "service_master_id": row[0],
                        "clave": row[1],
                        "nivel_4": row[2],
                        "precio_lista": row[3],
                        "precio_profesional": row[4],
                    }
                    for row in cur.fetchall()
                ]

    return {
        "vigencia": vigencia,
        "familias": familias,
        "niveles_2": niveles_2,
        "niveles_3": niveles_3,
        "servicios": servicios,
    }
