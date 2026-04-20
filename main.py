from core.db import get_connection, set_tenant_schema
from core.security import create_access_token, verify_token
from core.security import get_token_expiration_minutes


from jose import JWTError, jwt
from datetime import datetime, timedelta
from uuid import uuid4
from fastapi import FastAPI, HTTPException, Depends, Request, Body
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional


from psycopg import sql
import hashlib
import os
import logging

from fastapi.responses import Response

# =========================
# MODELOS
# =========================

class PrecioUpdate(BaseModel):
    listprice: Optional[int] = None
    professionalprice: Optional[int] = None
    salonpercentage: Optional[int] = None
    professionalpercentage: Optional[int] = None
    reason_id: int

app = FastAPI(title="KIVOR Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://kivor-frontend-dev.onrender.com",
        "https://kivor-frontend.onrender.com"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.options("/{full_path:path}")
def options_handler(full_path: str):
    return Response(status_code=200)


# =========================
# Valida RUT
# =========================
def validar_rut(rut: str) -> bool:
    try:
        rut = rut.replace(".", "").replace("-", "").upper().strip()

        cuerpo = rut[:-1]
        dv = rut[-1]

        if not cuerpo.isdigit():
            return False

        suma = 0
        multiplo = 2

        for c in reversed(cuerpo):
            suma += int(c) * multiplo
            multiplo += 1
            if multiplo == 8:
                multiplo = 2

        resto = suma % 11
        dv_calculado = 11 - resto

        if dv_calculado == 11:
            dv_calculado = "0"
        elif dv_calculado == 10:
            dv_calculado = "K"
        else:
            dv_calculado = str(dv_calculado)

        return dv == dv_calculado

    except:
        return False


# =========================
# CONFIG
# =========================

@app.get("/config")
def get_config():
    return {
        "api_base": os.getenv("API_BASE")
    }


# =========================
# CONTROL PERMISOS
# =========================

def require_data_write_permission(current_user: dict):
    pass
    
# =========================
# ROOT
# =========================

@app.get("/")
def root():
    return {"status": "ok", "service": "kivor-backend"}

@app.get("/health")
def health():
    return {"healthy": True}

@app.get("/test-db")
def test_db():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                result = cur.fetchone()
        return {"db_connection": "ok", "result": result[0]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
# =========================
# LOGIN
# =========================

@app.post("/login")
def login(request: Request, data: dict = Body(...)):

    username = data.get("username")
    password = data.get("password")

    if not username or not password:
        raise HTTPException(status_code=400, detail="Usuario y clave requeridos")

    username = username.strip().lower()

    with get_connection() as conn:
        with conn.cursor() as cur:

            # =========================
            # USER
            # =========================

            cur.execute("""
                SELECT user_id, user_password_hash, user_group_id
                FROM core."user"
                WHERE user_name = %s
                AND user_active = TRUE
            """, (username,))

            user = cur.fetchone()

            if not user:
                raise HTTPException(status_code=401, detail="Credenciales inválidas")

            user_id = user[0]
            stored_password = user[1]
            group_id = user[2]

            # =========================
            # PASSWORD
            # =========================

            hashed_input = hashlib.sha256(password.encode()).hexdigest()

            if hashed_input != stored_password:
                raise HTTPException(status_code=401, detail="Credenciales inválidas")

            # =========================
            # PERSON
            # =========================

            cur.execute("""
                SELECT person_id
                FROM core.person
                WHERE person_user_id = %s
            """, (user_id,))

            person = cur.fetchone()

            if not person:
                raise HTTPException(status_code=403, detail="Usuario no tiene persona asociada")

            person_id = person[0]

            # =========================
            # ORGANIZATION
            # =========================

            cur.execute("""
                SELECT person_organization_organization_id
                FROM core.person_organization
                WHERE person_organization_person_id = %s
                AND person_organization_is_default = true
                AND person_organization_active = true
            """, (person_id,))

            org = cur.fetchone()

            if not org:
                raise HTTPException(status_code=403, detail="Usuario no tiene organización asignada")

            organization_id = org[0]

            # =========================
            # TENANT
            # =========================

            cur.execute("""
            WITH RECURSIVE org_tree AS (

                SELECT
                    organization_id,
                    organization_parent_id,
                    organization_tenant_id
                FROM core.organization
                WHERE organization_id = %s

                UNION ALL

                SELECT
                    o.organization_id,
                    o.organization_parent_id,
                    o.organization_tenant_id
                FROM core.organization o
                JOIN org_tree t
                    ON o.organization_id = t.organization_parent_id
            )

            SELECT
                t.tenant_id,
                t.tenant_db_schema
            FROM org_tree ot
            JOIN core.tenant t
                ON t.tenant_id = ot.organization_tenant_id
            WHERE ot.organization_tenant_id IS NOT NULL
            LIMIT 1
            """, (organization_id,))

            tenant = cur.fetchone()

            if not tenant:
                raise HTTPException(status_code=403, detail="No se pudo resolver el tenant del usuario")

            tenant_id = tenant[0]
            tenant_schema = tenant[1]

    # =========================
    # SESSION
    # =========================

    client_ip = request.client.host
    user_agent = request.headers.get("user-agent")

    session_id = str(uuid4())
    expires_at = datetime.utcnow() + timedelta(minutes=get_token_expiration_minutes())

    with get_connection() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                INSERT INTO core.user_session
                (session_id, user_name, user_group_id, expires_at, ip_address, user_agent)
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (session_id, username, group_id, expires_at, client_ip, user_agent))

    access_token = create_access_token({
        "sub": username,
        "group_id": group_id,
        "session_id": session_id,
        "tenant_id": tenant_id,
        "tenant_schema": tenant_schema,
        "organization_id": organization_id,
        "person_id": person_id
    })

    return {
        "access_token": access_token,
        "token_type": "bearer"
    }

# =========================
# LOGIN USERNAME
# =========================

@app.api_route("/login-username", methods=["POST", "OPTIONS"])
def login_username(data: dict = Body(None)):

    # 👉 Manejo preflight (OPTIONS)
    if data is None:
        return {"ok": True}

    username = data.get("username")

    if not username:
        raise HTTPException(status_code=400, detail="Usuario requerido")

    username = username.strip().lower()

    with get_connection() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT user_id
                FROM core."user"
                WHERE user_name = %s
                AND user_active = TRUE
            """, (username,))

            user = cur.fetchone()

    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    return {"status": "ok"}


# =========================
# GANANCIAS
# =========================

@app.get("/ganancias-por-mes")
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

@app.get("/familias")
def obtener_familias(current_user: str = Depends(verify_token)):
    query = """
    SELECT DISTINCT family
    FROM core.prices
    WHERE family IS NOT NULL
    ORDER BY family;
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                return [r[0] for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/niveles2")
def obtener_nivel2(
    family: str,
    current_user: str = Depends(verify_token)
):
    query = """
    SELECT DISTINCT level2
    FROM core.prices
    WHERE family = %s
    AND level2 IS NOT NULL
    ORDER BY level2;
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (family,))
                rows = cur.fetchall()
                return [r[0] for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/niveles3")
def obtener_nivel3(
    family: str,
    level2: str,
    current_user: str = Depends(verify_token)
):
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


@app.get("/niveles4")
def obtener_nivel4(
    family: str,
    level2: str,
    level3: str,
    current_user: str = Depends(verify_token)
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



@app.get("/precios")
def obtener_precios(
    family: str,
    level2: str = None,
    level3: str = None,
    level4: str = None,
    current_user: str = Depends(verify_token)
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


@app.get("/menu")
def get_menu(current_user: dict = Depends(verify_token)):

    group_id = current_user.get("group_id")

    if not group_id:
        raise HTTPException(status_code=400, detail="Usuario sin grupo")

    query = """
    WITH RECURSIVE recursive_menu AS (

        -- Menús permitidos directamente
        SELECT m.*
        FROM core.menu m
        JOIN core.role_menu rm ON m.menu_id = rm.menu_id
        JOIN core.group_role gr ON rm.role_id = gr.role_id
        WHERE gr.group_id = %s
          AND m.menu_active = TRUE

        UNION

        -- Traer padres
        SELECT parent.*
        FROM core.menu parent
        JOIN recursive_menu child
          ON child.menu_parent_id = parent.menu_id
    )
    SELECT DISTINCT *
    FROM recursive_menu
    ORDER BY menu_order;
    """

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (group_id,))
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()

        return [dict(zip(columns, row)) for row in rows]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/sessions")
def get_sessions(current_user: dict = Depends(verify_token)):

    username = current_user.get("username")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT session_id,
                       created_at,
                       expires_at,
                       revoked
                FROM core.user_session
                WHERE user_name = %s
                AND revoked = FALSE
                AND expires_at > NOW()
                ORDER BY created_at DESC
            """, (username,))

            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

    return [dict(zip(columns, row)) for row in rows]


# =========================
# EXPRESS CUSTOMER
# =========================



@app.post("/customers-express/generate")
def generate_customer_express(current_user: dict = Depends(verify_token)):

    # 🔑 Se genera un token único para el link de Customer Express
    token = uuid4().hex

    # 🏢 Se obtiene el schema del tenant desde el usuario autenticado (multi-tenant)
    tenant_schema = current_user.get("tenant_schema")

    # 🛡 Validación de seguridad del schema
    if not tenant_schema or not tenant_schema.isidentifier():
        raise HTTPException(status_code=400, detail="Invalid tenant schema")

    # 🔌 Se abre conexión a la base de datos
    with get_connection() as conn:

        # 🧾 Se crea cursor para ejecutar queries
        with conn.cursor() as cur:

            # 🧱 Construcción de query segura multi-tenant
            query = sql.SQL("""
                INSERT INTO {}.customers_express
                (
                    customers_express_token,
                    customers_express_token_created_at,
                    customers_express_token_expires_at,
                    customers_express_link_status
                )
                VALUES
                (
                    %s,
                    NOW(),
                    NOW() + interval '24 hours',
                    'created'
                )
                RETURNING customers_express_id
            """).format(
                sql.Identifier(tenant_schema)
            )

            # 💾 Inserta en tabla del tenant
            cur.execute(query, (token,))
            result = cur.fetchone()

            # 🔥 NUEVO: guardar relación token → tenant (tabla global)
            cur.execute("""
                INSERT INTO core.customers_express_token_map (token, tenant_schema)
                VALUES (%s, %s)
            """, (token, tenant_schema))

    # 📤 Respuesta al frontend
    return {
        "status": "ok",
        "customers_express_id": result[0],
        "token": token
    }


# ===========================
# GET FORM CUSTOMERS EXPRESS
# ===========================

@app.get("/customers-express/{token}")
def get_customer_express(token: str):

    with get_connection() as conn:
        with conn.cursor() as cur:

            # 🔥 PASO 1: resolver tenant desde tabla global
            cur.execute("""
                SELECT tenant_schema
                FROM core.customers_express_token_map
                WHERE token = %s
            """, (token,))

            row = cur.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="invalid_link")

            tenant_schema = row[0]

            # 🛡 Validación básica
            if not tenant_schema or not tenant_schema.isidentifier():
                raise HTTPException(status_code=400, detail="invalid_tenant")

            # 🔥 PASO 2: query dinámica segura al tenant
            query = sql.SQL("""
                SELECT
                    customers_express_id,
                    customers_express_token_expires_at,
                    customers_express_link_status
                FROM {}.customers_express
                WHERE customers_express_token = %s
                AND customers_express_token_expires_at > NOW()
            """).format(sql.Identifier(tenant_schema))

            cur.execute(query, (token,))
            record = cur.fetchone()

            if not record:
                raise HTTPException(status_code=404, detail="invalid_link")

            customers_express_id, expires_at, status = record

            if expires_at and expires_at < datetime.utcnow():
                raise HTTPException(status_code=400, detail="expired_link")

            if status == "completed":
                raise HTTPException(status_code=400, detail="form_completed")

            # 🔥 PASO 3: obtener configuración de campos (tenant)
            query_fields = sql.SQL("""
                SELECT
                    customer_capture_settings_field,
                    customer_capture_settings_label,
                    customer_capture_settings_is_required,
                    customer_capture_settings_display_order
                FROM {}.customer_capture_settings
                WHERE customer_capture_settings_is_active = TRUE
                ORDER BY customer_capture_settings_display_order
            """).format(sql.Identifier(tenant_schema))

            cur.execute(query_fields)

            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            fields = [dict(zip(columns, row)) for row in rows]

            identifier_types = []

            if any(f["customer_capture_settings_field"] == "identifier_type" for f in fields):

                query_identifiers = sql.SQL("""
                    SELECT
                        identifier_type_settings_code,
                        identifier_type_settings_label
                    FROM {}.identifier_type_settings
                    WHERE identifier_type_settings_is_active = TRUE
                    ORDER BY identifier_type_settings_display_order
                """).format(sql.Identifier(tenant_schema))

                cur.execute(query_identifiers)

                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                identifier_types = [dict(zip(columns, row)) for row in rows]

    return {
        "status": "ok",
        "token": token,
        "fields": fields,
        "identifier_types": identifier_types
    }

@app.post("/customers-express/{token}")
async def save_customer_express(token: str, payload: dict = Body(...)):
    print("🔥 PAYLOAD:", payload)
    with get_connection() as conn:
        with conn.cursor() as cur:

            # 🔥 1. Resolver tenant desde token
            cur.execute("""
                SELECT tenant_schema
                FROM core.customers_express_token_map
                WHERE token = %s
            """, (token,))

            row = cur.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="invalid_link")

            tenant_schema = row[0]

            if not tenant_schema or not tenant_schema.isidentifier():
                raise HTTPException(status_code=400, detail="invalid_tenant")

            # 🔥 2. Construir UPDATE dinámico
            fields = []
            values = []

            for key, value in payload.items():
            
                # 🔥 quitar prefijo "customers_" si existe
                clean_key = key.replace("customers_", "")
            
                column = f"customers_express_{clean_key}"
            
                fields.append(
                    sql.SQL("{} = %s").format(sql.Identifier(column))
                )
                values.append(value)

            # for key, value in payload.items():
            #     column = f"customers_express_{key}"
            #     fields.append(sql.SQL("{} = %s").format(sql.Identifier(column)))
            #     values.append(value)

            # agregar campos de control
            fields.append(sql.SQL("customers_express_completed_at = NOW()"))
            fields.append(sql.SQL("customers_express_link_status = 'completed'"))

            values.append(token)

            query = sql.SQL("""
                UPDATE {}.customers_express
                SET {}
                WHERE customers_express_token = %s
            """).format(
                sql.Identifier(tenant_schema),
                sql.SQL(", ").join(fields)
            )
            print("🔥 VALUES:", values)
            print("🔥 FIELDS:", fields)

            cur.execute(query, values)

    return {
        "status": "ok"
    }
@app.get("/customers-express/by-mobile/{mobile}")
def search_customers_express(mobile: str, current_user: dict = Depends(verify_token)):

    try:

        with get_connection() as conn:
            with conn.cursor() as cur:

                # Obtener campos configurados en el formulario
                cur.execute("""
                    SELECT
                        customer_capture_settings_field
                    FROM lindasylunaticas.customer_capture_settings
                    WHERE customer_capture_settings_is_active = TRUE
                    ORDER BY customer_capture_settings_display_order
                """)

                field_rows = cur.fetchall()
                fields = [r[0] for r in field_rows]

                
                # Buscar registros del cliente
                # cur.execute("""
                #     SELECT *
                #     FROM lindasylunaticas.customers_express
                #     WHERE customers_express_mobile = %s
                #     AND customers_express_completed_at IS NOT NULL
                #     ORDER BY customers_express_completed_at DESC
                # """, (mobile,))
                # 

                tenant_schema = current_user["tenant_schema"]
                query = f"""
                SELECT *
                FROM {tenant_schema}.customers_express
                WHERE customers_express_mobile = %s
                AND customers_express_completed_at IS NOT NULL
                ORDER BY customers_express_completed_at DESC
                """
                 
                logging.basicConfig(level=logging.INFO)
                
                logging.info(f"TENANT: {tenant_schema}")
                logging.info(f"MOBILE: {mobile}")
                logging.info(f"QUERY: {query}")
                cur.execute(query, (mobile,))

                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()

        results = [dict(zip(columns, row)) for row in rows]

        return {
            "status": "ok",
            "fields": fields,
            "results": results
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/logout")
def logout(current_user: dict = Depends(verify_token)):

    session_id = current_user.get("session_id")

    if not session_id:
        raise HTTPException(status_code=400, detail="Sesión no encontrada")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE core.user_session
                SET revoked = TRUE
                WHERE session_id = %s
            """, (session_id,))

    return {"status": "ok", "message": "Sesión cerrada"}


@app.post("/logout-session")
def logout_session(
    data: dict = Body(...),
    current_user: dict = Depends(verify_token)
):

    session_id = data.get("session_id")

    if not session_id:
        raise HTTPException(status_code=400, detail="session_id requerido")

    with get_connection() as conn:
        with conn.cursor() as cur:

            # 🔒 Validar que la sesión pertenece al usuario
            cur.execute("""
                SELECT user_name
                FROM core.user_session
                WHERE session_id = %s
            """, (session_id,))

            row = cur.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="Sesión no encontrada")

            session_user = row[0]

            if session_user != current_user["username"]:
                raise HTTPException(status_code=403, detail="No autorizado")

            # 🔥 Revocar sesión
            cur.execute("""
                UPDATE core.user_session
                SET revoked = TRUE
                WHERE session_id = %s
            """, (session_id,))

    return {"status": "ok", "message": "Sesión cerrada"}


@app.post("/users")
def create_user(data: dict):

    print("==== CREATE USER START ====")
    print("DATA:", data)

    try:
        conn = get_connection()
        print("DB CONNECTED")

        cur = conn.cursor()

        print("EXECUTING INSERT...")

        cur.execute("""
            INSERT INTO core."user" (
                user_nickname,
                user_name,
                user_password_hash,
                user_firstname,
                user_lastname,
                user_group_id
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING user_id
        """, (
            data["user_nickname"],
            data["user_name"],
            data["user_password"],
            data["user_firstname"],
            data["user_lastname"],
            int(data["user_group_id"])
        ))

        print("INSERT OK")

        user_id = cur.fetchone()
        print("USER ID:", user_id)

        conn.commit()
        print("COMMIT OK")

        cur.close()
        conn.close()

        print("==== CREATE USER END ====")

        return {"user_id": user_id}

    except Exception as e:
        print("ERROR CREATE USER:", str(e))
        return {"detail": str(e)}
