from fastapi import APIRouter, HTTPException, Depends
from core.db import get_connection, set_tenant_schema
from core.security import verify_token
from services.analytics_service import get_familias_service
from services.analytics_service import get_nivel2_service

router = APIRouter()


@router.get("/ganancias-por-mes")
def ganancias_por_mes(mes: str, current_user: dict = Depends(verify_token)):

    if not mes.isdigit() or len(mes) != 2 or int(mes) < 1 or int(mes) > 12:
        raise HTTPException(status_code=400, detail="Mes inválido. Usa formato 01-12.")

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
        set_tenant_schema(conn, current_user["tenant_schema"])
        with conn.cursor() as cur:
            cur.execute(query, (mes,))
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

    result = [dict(zip(columns, row)) for row in rows]

    return {
        "mes": mes,
        "data": result
    }


@router.get("/familias")
def obtener_familias(current_user: dict = Depends(verify_token)):

    try:
        return get_familias_service()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/niveles2")
def obtener_nivel2(family: str, current_user: dict = Depends(verify_token)):

    try:
        return get_nivel2_service(family)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/niveles3")
def obtener_nivel3(family: str, level2: str, current_user: dict = Depends(verify_token)):

    query = """
    SELECT DISTINCT level3
    FROM core.prices
    WHERE family = %s
    AND level2 = %s
    AND level3 IS NOT NULL
    ORDER BY level3;
    """

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (family, level2))
                rows = cur.fetchall()
                return [r[0] for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/niveles4")
def obtener_nivel4(
    family: str,
    level2: str,
    level3: str,
    current_user: dict = Depends(verify_token)
):

    query = """
    SELECT DISTINCT level4
    FROM core.prices
    WHERE family = %s
    AND level2 = %s
    AND level3 = %s
    AND level4 IS NOT NULL
    ORDER BY level4;
    """

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (family, level2, level3))
                rows = cur.fetchall()
                return [r[0] for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/precios")
def obtener_precios(
    family: str,
    level2: str = None,
    level3: str = None,
    level4: str = None,
    current_user: dict = Depends(verify_token)
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
