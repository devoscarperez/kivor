# services/ventas_lyl_service.py

import pandas as pd
from fastapi import UploadFile
from core.db import get_connection


EXCEL_SHEET_NAME = "VENTAS"


COLUMN_MAP = {
    "VENTAS_KEY": "ventas_key",
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
    "VENTAS_KEY",
    "AÑO",
    "AÑO-MES",
]


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


def filter_period(df: pd.DataFrame, anio: int, mes: int) -> pd.DataFrame:
    anio_text = str(anio)
    anio_mes = f"{anio}-{str(mes).zfill(2)}"

    df["AÑO"] = df["AÑO"].apply(clean_value)
    df["AÑO-MES"] = df["AÑO-MES"].apply(clean_value)

    df_filtered = df[
        (df["AÑO"] == anio_text) &
        (df["AÑO-MES"] == anio_mes)
    ].copy()

    return df_filtered


def build_insert_rows(df: pd.DataFrame, archivo_origen: str):
    rows = []

    for index, row in df.iterrows():
        record = {}

        for excel_col, db_col in COLUMN_MAP.items():
            if excel_col in df.columns:
                record[db_col] = clean_value(row[excel_col])
            else:
                record[db_col] = None

        record["archivo_origen"] = archivo_origen
        record["hoja_origen"] = EXCEL_SHEET_NAME
        record["fila_excel"] = int(index) + 2

        rows.append(record)

    return rows


def delete_period(cur, anio: int, anio_mes: str) -> int:
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
    mes: int,
    file: UploadFile,
    current_user: dict
):
    if mes < 1 or mes > 12:
        raise Exception("Mes inválido. Debe estar entre 1 y 12.")

    anio_mes = f"{anio}-{str(mes).zfill(2)}"

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
            raise Exception(f"No existen registros para el período {anio_mes} en el Excel.")

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
    return {
        "success": True,
        "anio": anio,
        "mes": mes,
        "anio_mes": anio_mes,
        "rows_deleted": rows_deleted,
        "rows_inserted": len(df),
        "message": "Carga realizada correctamente"
    }
