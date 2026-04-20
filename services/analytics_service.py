from core.db import get_connection

def get_ganancias_por_mes_service(mes: str, tenant_schema: str):

    from core.db import get_connection, set_tenant_schema

    query = """
    SELECT 
        TO_CHAR(v.date,'YYYYMM') AS fecha,
        FLOOR(SUM(CASE WHEN v.family='CABELLO' THEN (v.listprice-v.amounttopayprofessional-v.salondiscount) ELSE 0 END)/(1+(19.0/100))) AS cabello,
        FLOOR(SUM(CASE WHEN v.family='MANOS_Y_PIES' THEN (v.listprice-v.amounttopayprofessional-v.salondiscount) ELSE 0 END)/(1+(19.0/100))) AS manos_y_pies,
        FLOOR(SUM(CASE WHEN v.family='DEPILACION' THEN (v.listprice-v.amounttopayprofessional-v.salondiscount) ELSE 0 END)/(1+(19.0/100))) AS depilacion,
        FLOOR(SUM(CASE WHEN v.family='CEJAS_Y_PESTAÑAS' THEN (v.listprice-v.amounttopayprofessional-v.salondiscount) ELSE 0 END)/(1+(19.0/100))) AS cejas_y_pestanas,
        FLOOR(SUM(CASE WHEN v.family='FACIALES' THEN (v.listprice-v.amounttopayprofessional-v.salondiscount) ELSE 0 END)/(1+(19.0/100))) AS faciales,
        FLOOR(SUM(CASE WHEN v.family='CORPORAL' THEN (v.listprice-v.amounttopayprofessional-v.salondiscount) ELSE 0 END)/(1+(19.0/100))) AS corporal
    FROM sales v
    WHERE TO_CHAR(v.date,'MM') = %s
    AND v.family IN ('CABELLO','MANOS_Y_PIES','DEPILACION','CEJAS_Y_PESTAÑAS','FACIALES','CORPORAL')
    GROUP BY TO_CHAR(v.date,'YYYYMM')
    ORDER BY fecha;
    """

    with get_connection() as conn:
        set_tenant_schema(conn, tenant_schema)
        with conn.cursor() as cur:
            cur.execute(query, (mes,))
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

    result = [dict(zip(columns, row)) for row in rows]

    return {
        "mes": mes,
        "data": result
    }

def get_precios_service(
    family: str,
    level2: str = None,
    level3: str = None,
    level4: str = None
):

    query = """
    SELECT family,
           level2,
           level3,
           level4,
           servicekey,
           listprice,
           professionalprice,
           salonpercentage,
           professionalpercentage
    FROM core.prices
    WHERE family = %s
    """

    params = [family]

    if level2:
        query += " AND level2 = %s"
        params.append(level2)

    if level3:
        query += " AND level3 = %s"
        params.append(level3)

    if level4:
        query += " AND level4 = %s"
        params.append(level4)

    query += " ORDER BY servicekey"

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

    return [dict(zip(columns, row)) for row in rows]


def get_nivel4_service(family: str, level2: str, level3: str):

    query = """
    SELECT DISTINCT level4
    FROM core.prices
    WHERE family = %s
    AND level2 = %s
    AND level3 = %s
    AND level4 IS NOT NULL
    ORDER BY level4;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (family, level2, level3))
            rows = cur.fetchall()

    return [r[0] for r in rows]
    
def get_nivel3_service(family: str, level2: str):

    query = """
    SELECT DISTINCT level3
    FROM core.prices
    WHERE family = %s
    AND level2 = %s
    AND level3 IS NOT NULL
    ORDER BY level3;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (family, level2))
            rows = cur.fetchall()

    return [r[0] for r in rows]


def get_familias_service():

    query = """
    SELECT DISTINCT family
    FROM core.prices
    WHERE family IS NOT NULL
    ORDER BY family;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

    return [r[0] for r in rows]

def get_nivel2_service(family: str):

    query = """
    SELECT DISTINCT level2
    FROM core.prices
    WHERE family = %s
    AND level2 IS NOT NULL
    ORDER BY level2;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (family,))
            rows = cur.fetchall()

    return [r[0] for r in rows]
