from jose import JWTError, jwt
from datetime import datetime, timedelta
from uuid import uuid4
from fastapi import FastAPI, HTTPException, Depends, Request, Body
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import psycopg
import hashlib
import os

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
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

security = HTTPBearer()

# =========================
# DB CONNECTION
# =========================

def get_connection():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise Exception("DATABASE_URL no está configurada")
    return psycopg.connect(database_url)
    
def set_tenant_schema(conn, tenant_schema):

    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {tenant_schema}")
        
# =========================
# JWT
# =========================

def create_access_token(data: dict, expires_delta: timedelta = None):

    to_encode = data.copy()
    now = datetime.utcnow()
    expire = now + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))

    to_encode.update({
        "iat": now,
        "exp": expire,
        "jti": str(uuid4())
    })

    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):

    if not SECRET_KEY:
        raise HTTPException(status_code=500, detail="SECRET_KEY no configurado")

    token = credentials.credentials

    try:

        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])

        username = payload.get("sub")
        tenant_schema = payload.get("tenant_schema")
        group_id = payload.get("group_id")
        session_id = payload.get("session_id")
        person_id = payload.get("person_id")
        organization_id = payload.get("organization_id")

        if not tenant_schema:
            raise HTTPException(
            status_code=401,
            detail="Token inválido: tenant no definido"
        )
        if username is None or session_id is None:
            raise HTTPException(status_code=401, detail="Token inválido")

        with get_connection() as conn:
            with conn.cursor() as cur:

                cur.execute("""
                    SELECT revoked, expires_at
                    FROM core.user_session
                    WHERE session_id = %s
                """, (session_id,))

                session = cur.fetchone()

        if not session:
            raise HTTPException(status_code=401, detail="Sesión no válida")

        revoked, expires_at = session

        if revoked:
            raise HTTPException(status_code=401, detail="Sesión revocada")

        if expires_at < datetime.utcnow():
            raise HTTPException(status_code=401, detail="Sesión expirada")

        return {
            "username": username,
            "group_id": group_id,
            "session_id": session_id,
            "tenant_schema": tenant_schema,
            "organization_id": organization_id,
            "person_id": person_id
        }

    except JWTError:
        raise HTTPException(status_code=401, detail="Token inválido")

def get_current_token_data(token: str):

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])

        username = payload.get("sub")
        tenant_schema = payload.get("tenant_schema")
        group_id = payload.get("group_id")
        session_id = payload.get("session_id")
        person_id = payload.get("person_id")
        organization_id = payload.get("organization_id")

        if not username:
            raise HTTPException(
                status_code=401,
                detail="Token inválido: usuario no definido"
            )

        if not tenant_schema:
            raise HTTPException(
                status_code=401,
                detail="Token inválido: tenant no definido"
            )

        return {
            "username": username,
            "tenant_schema": tenant_schema,
            "group_id": group_id,
            "session_id": session_id,
            "person_id": person_id,
            "organization_id": organization_id
        }

    except JWTError:
        raise HTTPException(
            status_code=401,
            detail="Token inválido"
        )

# =========================
# VALIDACIONES
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
# ROOT
# =========================

@app.get("/")
def root():
    return {"status": "ok", "service": "kivor-backend"}

@app.get("/health")
def health():
    return {"healthy": True}

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
            cur.execute("""
                SELECT user_id, user_password_hash, user_group_id
                FROM core."user"
                WHERE user_name = %s
                AND user_active = TRUE
            """, (username,))

            user = cur.fetchone()
        
    if not user:
        raise HTTPException(status_code=401, detail="Credenciales inválidas")

    cur.execute("""
    SELECT person_id
    FROM core.person
    WHERE person_user_id = %s
    """, (user_id,))

    person = cur.fetchone()

    if not person:
        raise HTTPException(
            status_code=403,
            detail="Usuario no tiene persona asociada"
        )

    person_id = person[0]
    cur.execute("""
    SELECT person_organization_organization_id
    FROM core.person_organization
    WHERE person_organization_person_id = %s
    AND person_organization_is_default = true
    AND person_organization_active = true
    """, (person_id,))

    org = cur.fetchone()

    if not org:
        raise HTTPException(
            status_code=403,
            detail="Usuario no tiene organización asignada"
        )

    organization_id = org[0]
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
        raise HTTPException(
            status_code=403,
            detail="No se pudo resolver el tenant del usuario"
        )
    tenant_id = tenant[0]
    tenant_schema = tenant[1]

    user_id = user[0]
    stored_password = user[1]
    group_id = user[2]

    hashed_input = hashlib.sha256(password.encode()).hexdigest()

    if hashed_input != stored_password:
        raise HTTPException(status_code=401, detail="Credenciales inválidas")

    client_ip = request.client.host
    user_agent = request.headers.get("user-agent")

    session_id = str(uuid4())
    expires_at = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

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


@app.post("/login-username")
def login_username(data: dict = Body(...)):

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

    return {
        "status": "ok"
    }

# =========================
# EXPRESS CUSTOMER
# =========================

@app.post("/customers-express/generate")
def generate_customer_express(current_user: dict = Depends(verify_token)):

    token = uuid4().hex

    with get_connection() as conn:
        set_tenant_schema(conn, current_user["tenant_schema"])
        with conn.cursor() as cur:

            cur.execute("""
                INSERT INTO customers_express
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
            """, (token,))

            result = cur.fetchone()

    link = f"https://kivor-frontend.onrender.com/cx.html?token={token}"

    return {
        "status": "ok",
        "customers_express_id": result[0],
        "token": token,
        "link": link
    }

# =========================
# GET FORM
# =========================

@app.get("/customers-express/{token}")
def get_customer_express(token: str, current_user: dict = Depends(verify_token)):

    with get_connection() as conn:

        set_tenant_schema(conn, current_user["tenant_schema"])

        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    customers_express_id,
                    customers_express_token_expires_at,
                    customers_express_link_status
                FROM customers_express
                WHERE customers_express_token = %s
            """, (token,))

            record = cur.fetchone()

            if not record:
                raise HTTPException(status_code=404, detail="invalid_link")

            customers_express_id, expires_at, status = record

            if expires_at and expires_at < datetime.utcnow():
                raise HTTPException(status_code=400, detail="expired_link")

            if status == "completed":
                raise HTTPException(status_code=400, detail="form_completed")

            cur.execute("""
                SELECT
                    customer_capture_settings_field,
                    customer_capture_settings_is_required,
                    customer_capture_settings_display_order
                FROM customer_capture_settings
                WHERE customer_capture_settings_is_active = TRUE
                ORDER BY customer_capture_settings_display_order
            """)

            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

            fields = [dict(zip(columns, row)) for row in rows]

            identifier_types = []

            if any(f["customer_capture_settings_field"] == "identifier_type" for f in fields):

                cur.execute("""
                    SELECT
                        identifier_type_settings_code,
                        identifier_type_settings_label
                    FROM identifier_type_settings
                    WHERE identifier_type_settings_is_active = TRUE
                    ORDER BY identifier_type_settings_display_order
                """)

                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()

                identifier_types = [dict(zip(columns, row)) for row in rows]

    return {
        "status": "ok",
        "token": token,
        "fields": fields,
        "identifier_types": identifier_types
    }


# =========================
# CONFIG
# =========================

@app.get("/config")
def get_config():
    return {
        "api_base": os.getenv("API_BASE")
    }

# =========================
# GANANCIAS POR MES
# =========================

@app.get("/ganancias-por-mes")
def ganancias_por_mes(mes: str, current_user: str = Depends(verify_token)):

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

    try:
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

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
