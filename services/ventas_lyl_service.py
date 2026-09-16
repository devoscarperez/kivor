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


QUINCENA_SQL = """
    CASE
        WHEN EXTRACT(DAY FROM fecha_entrega) <= FLOOR(
            EXTRACT(DAY FROM (date_trunc('month', fecha_entrega) + interval '1 month - 1 day')) / 2
        )
        THEN 1
        ELSE 2
    END
"""

# Un "ticket" (una atencion) es la combinacion fecha_entrega + nro_formulario,
# no una clienta. Si nro_formulario viene vacio, se usa ventas_key como
# respaldo para que cada fila sin formulario cuente como su propio ticket en
# vez de agruparse por error con otras filas tambien vacias (NULL agrupa con
# NULL en SQL).
TICKET_ID_SQL = "(fecha_entrega, COALESCE(NULLIF(TRIM(nro_formulario), ''), ventas_key))"


def get_reporte_ventas_service(
    anio1: int,
    anio2: int,
    metrica: str,
    familias: Optional[list] = None,
    profesionales: Optional[list] = None,
    dias_semana: Optional[list] = None,
    quincenas: Optional[list] = None,
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

    if dias_semana:
        condiciones.append("EXTRACT(ISODOW FROM fecha_entrega)::int = ANY(%s)")
        params.append(dias_semana)

    if quincenas:
        condiciones.append(f"({QUINCENA_SQL}) = ANY(%s)")
        params.append(quincenas)

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


# ============================================================
# REPORTE DE KPIs (Ticket promedio, clientas nuevas,
# cross-selling y ABC de familias)
# ============================================================

UMBRALES_ABC = [50, 60, 70, 80]


def _condiciones_filtros(familias=None, profesionales=None, dias_semana=None, quincenas=None, incluir_familia=True):
    condiciones = []
    params = []

    if incluir_familia and familias:
        condiciones.append("familia = ANY(%s)")
        params.append(familias)

    if profesionales:
        condiciones.append("profesional = ANY(%s)")
        params.append(profesionales)

    if dias_semana:
        condiciones.append("EXTRACT(ISODOW FROM fecha_entrega)::int = ANY(%s)")
        params.append(dias_semana)

    if quincenas:
        condiciones.append(f"({QUINCENA_SQL}) = ANY(%s)")
        params.append(quincenas)

    return condiciones, params


def _serie_mensual(rows, anio1, anio2, valor_default=None):
    serie1 = [valor_default] * 12
    serie2 = [valor_default] * 12

    for anio, mes, valor in rows:
        if anio == anio1:
            serie1[mes - 1] = valor
        elif anio == anio2:
            serie2[mes - 1] = valor

    return serie1, serie2


def _calcular_ticket_promedio(cur, anio1, anio2, metrica, familias, profesionales, dias_semana, quincenas):
    columna = METRICAS_REPORTE[metrica]
    condiciones, params_extra = _condiciones_filtros(familias, profesionales, dias_semana, quincenas)
    condiciones = ["EXTRACT(YEAR FROM fecha_entrega) IN (%s, %s)"] + condiciones
    params = [anio1, anio2] + params_extra
    where_sql = " AND ".join(condiciones)

    def ticket(suma, tickets):
        suma = float(suma) if suma is not None else 0.0
        return (suma / tickets) if tickets else None

    cur.execute(f"""
        SELECT EXTRACT(YEAR FROM fecha_entrega)::int AS anio,
               EXTRACT(MONTH FROM fecha_entrega)::int AS mes,
               SUM({columna}) AS suma,
               COUNT(DISTINCT {TICKET_ID_SQL}) AS tickets
        FROM core.stg_ventas_lyl
        WHERE {where_sql}
        GROUP BY 1, 2
    """, tuple(params))
    filas_mes = [(anio, mes, ticket(suma, tickets), tickets) for anio, mes, suma, tickets in cur.fetchall()]
    mensual_anio1, mensual_anio2 = _serie_mensual(
        [(a, m, v) for a, m, v, t in filas_mes], anio1, anio2
    )
    mensual_tickets_anio1, mensual_tickets_anio2 = _serie_mensual(
        [(a, m, t) for a, m, v, t in filas_mes], anio1, anio2, valor_default=0
    )

    cur.execute(f"""
        SELECT EXTRACT(YEAR FROM fecha_entrega)::int AS anio,
               CASE WHEN EXTRACT(MONTH FROM fecha_entrega) <= 6 THEN 1 ELSE 2 END AS semestre,
               SUM({columna}) AS suma,
               COUNT(DISTINCT {TICKET_ID_SQL}) AS tickets
        FROM core.stg_ventas_lyl
        WHERE {where_sql}
        GROUP BY 1, 2
    """, tuple(params))
    filas_sem = [(anio, sem, ticket(suma, tickets), tickets) for anio, sem, suma, tickets in cur.fetchall()]
    semestral_anio1 = [None, None]
    semestral_anio2 = [None, None]
    semestral_tickets_anio1 = [0, 0]
    semestral_tickets_anio2 = [0, 0]
    for anio, sem, valor, tickets in filas_sem:
        if anio == anio1:
            semestral_anio1[sem - 1] = valor
            semestral_tickets_anio1[sem - 1] = tickets
        elif anio == anio2:
            semestral_anio2[sem - 1] = valor
            semestral_tickets_anio2[sem - 1] = tickets

    cur.execute(f"""
        SELECT EXTRACT(YEAR FROM fecha_entrega)::int AS anio,
               SUM({columna}) AS suma,
               COUNT(DISTINCT {TICKET_ID_SQL}) AS tickets
        FROM core.stg_ventas_lyl
        WHERE {where_sql}
        GROUP BY 1
    """, tuple(params))
    anual_anio1 = None
    anual_anio2 = None
    anual_tickets_anio1 = 0
    anual_tickets_anio2 = 0
    for anio, suma, tickets in cur.fetchall():
        if anio == anio1:
            anual_anio1 = ticket(suma, tickets)
            anual_tickets_anio1 = tickets
        elif anio == anio2:
            anual_anio2 = ticket(suma, tickets)
            anual_tickets_anio2 = tickets

    return {
        "mensual_anio1": mensual_anio1, "mensual_anio2": mensual_anio2,
        "mensual_tickets_anio1": mensual_tickets_anio1, "mensual_tickets_anio2": mensual_tickets_anio2,
        "semestral_anio1": semestral_anio1, "semestral_anio2": semestral_anio2,
        "semestral_tickets_anio1": semestral_tickets_anio1, "semestral_tickets_anio2": semestral_tickets_anio2,
        "anual_anio1": anual_anio1, "anual_anio2": anual_anio2,
        "anual_tickets_anio1": anual_tickets_anio1, "anual_tickets_anio2": anual_tickets_anio2,
    }


def _calcular_clientas_nuevas(cur, anio1, anio2, familias, profesionales, dias_semana, quincenas):
    condiciones, params_extra = _condiciones_filtros(familias, profesionales, dias_semana, quincenas)
    filtro_extra_sql = (" AND " + " AND ".join(condiciones)) if condiciones else ""
    params = [anio1, anio2] + params_extra

    cur.execute(f"""
        WITH primera_compra AS (
            SELECT rut_celular, MIN(fecha_entrega) AS primera_fecha
            FROM core.stg_ventas_lyl
            WHERE rut_celular IS NOT NULL
            GROUP BY rut_celular
        )
        SELECT
            EXTRACT(YEAR FROM v.fecha_entrega)::int AS anio,
            EXTRACT(MONTH FROM v.fecha_entrega)::int AS mes,
            COUNT(DISTINCT v.rut_celular) AS clientas_nuevas
        FROM core.stg_ventas_lyl v
        JOIN primera_compra p ON p.rut_celular = v.rut_celular
        WHERE EXTRACT(YEAR FROM v.fecha_entrega) IN (%s, %s)
          AND EXTRACT(YEAR FROM p.primera_fecha) = EXTRACT(YEAR FROM v.fecha_entrega)
          AND EXTRACT(MONTH FROM p.primera_fecha) = EXTRACT(MONTH FROM v.fecha_entrega)
          {filtro_extra_sql}
        GROUP BY 1, 2
    """, tuple(params))

    filas = [(anio, mes, int(cantidad)) for anio, mes, cantidad in cur.fetchall()]
    return _serie_mensual(filas, anio1, anio2, valor_default=0)


def _calcular_cross_selling_anio(cur, anio, profesionales, dias_semana, quincenas):
    condiciones, params_extra = _condiciones_filtros(
        profesionales=profesionales, dias_semana=dias_semana, quincenas=quincenas, incluir_familia=False
    )
    condiciones = [
        "EXTRACT(YEAR FROM fecha_entrega) = %s",
        "familia IS NOT NULL",
        "rut_celular IS NOT NULL",
        "TRIM(rut_celular) <> ''",
    ] + condiciones
    params = [anio] + params_extra
    where_sql = " AND ".join(condiciones)

    cur.execute(f"""
        SELECT DISTINCT familia, TRIM(rut_celular) AS rut_celular
        FROM core.stg_ventas_lyl
        WHERE {where_sql}
    """, tuple(params))

    clientas_por_familia = {}
    for familia, rut in cur.fetchall():
        clientas_por_familia.setdefault(familia, set()).add(rut)

    familias_ordenadas = sorted(clientas_por_familia.keys())
    matriz = [
        [len(clientas_por_familia[fa] & clientas_por_familia[fb]) for fb in familias_ordenadas]
        for fa in familias_ordenadas
    ]

    return {"familias": familias_ordenadas, "matriz": matriz}


def _calcular_abc_familias_anio(cur, anio, profesionales, dias_semana, quincenas):
    condiciones, params_extra = _condiciones_filtros(
        profesionales=profesionales, dias_semana=dias_semana, quincenas=quincenas, incluir_familia=False
    )
    condiciones = ["EXTRACT(YEAR FROM fecha_entrega) = %s", "familia IS NOT NULL"] + condiciones
    params = [anio] + params_extra
    where_sql = " AND ".join(condiciones)

    cur.execute(f"""
        SELECT familia, SUM(ganancia_salon) AS valor
        FROM core.stg_ventas_lyl
        WHERE {where_sql}
        GROUP BY familia
        ORDER BY valor DESC NULLS LAST
    """, tuple(params))

    filas = [(familia, float(valor) if valor is not None else 0.0) for familia, valor in cur.fetchall()]
    total = sum(valor for _, valor in filas)

    resultado = []
    acumulado = 0.0
    umbrales_restantes = list(UMBRALES_ABC)

    for familia, valor in filas:
        acumulado += valor
        porcentaje = (valor / total * 100) if total else 0.0
        porcentaje_acumulado = (acumulado / total * 100) if total else 0.0

        umbrales_alcanzados = []
        while umbrales_restantes and porcentaje_acumulado >= umbrales_restantes[0]:
            umbrales_alcanzados.append(umbrales_restantes.pop(0))

        resultado.append({
            "familia": familia,
            "valor": round(valor, 2),
            "porcentaje": round(porcentaje, 2),
            "porcentaje_acumulado": round(porcentaje_acumulado, 2),
            "umbrales": umbrales_alcanzados,
        })

    return resultado


def get_reporte_kpis_service(
    anio1: int,
    anio2: int,
    metrica: str,
    familias: Optional[list] = None,
    profesionales: Optional[list] = None,
    dias_semana: Optional[list] = None,
    quincenas: Optional[list] = None,
):
    if metrica not in METRICAS_REPORTE:
        raise Exception(f"Métrica inválida: {metrica}")

    with get_connection() as conn:
        with conn.cursor() as cur:
            ticket = _calcular_ticket_promedio(
                cur, anio1, anio2, metrica, familias, profesionales, dias_semana, quincenas
            )
            clientas_nuevas_anio1, clientas_nuevas_anio2 = _calcular_clientas_nuevas(
                cur, anio1, anio2, familias, profesionales, dias_semana, quincenas
            )
            # Una clienta nueva tiene una unica fecha de primera compra, que cae en un
            # solo mes: sumar los 12 meses del anio no duplica a nadie.
            clientas_nuevas_anual_anio1 = sum(clientas_nuevas_anio1)
            clientas_nuevas_anual_anio2 = sum(clientas_nuevas_anio2)
            cross_selling_anio1 = _calcular_cross_selling_anio(cur, anio1, profesionales, dias_semana, quincenas)
            cross_selling_anio2 = _calcular_cross_selling_anio(cur, anio2, profesionales, dias_semana, quincenas)
            abc_anio1 = _calcular_abc_familias_anio(cur, anio1, profesionales, dias_semana, quincenas)
            abc_anio2 = _calcular_abc_familias_anio(cur, anio2, profesionales, dias_semana, quincenas)

    return {
        "anio1": anio1,
        "anio2": anio2,
        "metrica": metrica,
        "meses": MESES_REPORTE,
        "ticket_mensual_anio1": ticket["mensual_anio1"],
        "ticket_mensual_anio2": ticket["mensual_anio2"],
        "ticket_mensual_tickets_anio1": ticket["mensual_tickets_anio1"],
        "ticket_mensual_tickets_anio2": ticket["mensual_tickets_anio2"],
        "ticket_semestral_anio1": ticket["semestral_anio1"],
        "ticket_semestral_anio2": ticket["semestral_anio2"],
        "ticket_semestral_tickets_anio1": ticket["semestral_tickets_anio1"],
        "ticket_semestral_tickets_anio2": ticket["semestral_tickets_anio2"],
        "ticket_anual_anio1": ticket["anual_anio1"],
        "ticket_anual_anio2": ticket["anual_anio2"],
        "ticket_anual_tickets_anio1": ticket["anual_tickets_anio1"],
        "ticket_anual_tickets_anio2": ticket["anual_tickets_anio2"],
        "clientas_nuevas_anio1": clientas_nuevas_anio1,
        "clientas_nuevas_anio2": clientas_nuevas_anio2,
        "clientas_nuevas_anual_anio1": clientas_nuevas_anual_anio1,
        "clientas_nuevas_anual_anio2": clientas_nuevas_anual_anio2,
        "cross_selling_anio1": cross_selling_anio1,
        "cross_selling_anio2": cross_selling_anio2,
        "abc_anio1": abc_anio1,
        "abc_anio2": abc_anio2,
    }
