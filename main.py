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
        group_id = payload.get("group_id")
        session_id = payload.get("session_id")

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
            "session_id": session_id
        }

    except JWTError:
        raise HTTPException(status_code=401, detail="Token inválido")

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
                SELECT user_password_hash, user_group_id
                FROM core."user"
                WHERE user_name = %s
                AND user_active = TRUE
            """, (username,))

            user = cur.fetchone()

    if not user:
        raise HTTPException(status_code=401, detail="Credenciales inválidas")

    stored_password = user[0]
    group_id = user[1]

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
        "session_id": session_id
    })

    return {
        "access_token": access_token,
        "token_type": "bearer"
    }

# =========================
# EXPRESS CUSTOMER
# =========================

@app.post("/customers-express/generate")
def generate_customer_express(current_user: dict = Depends(verify_token)):

    token = uuid4().hex

    with get_connection() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                INSERT INTO lindasylunaticas.customers_express
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
def get_customer_express(token: str):

    with get_connection() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    customers_express_id,
                    customers_express_token_expires_at,
                    customers_express_link_status
                FROM lindasylunaticas.customers_express
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
                FROM lindasylunaticas.customer_capture_settings
                WHERE customer_capture_settings_is_active = TRUE
                ORDER BY customer_capture_settings_display_order
            """)

            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

    fields = [dict(zip(columns, row)) for row in rows]

    identifier_types = []

    if any(f["customer_capture_settings_field"] == "identifier_type" for f in fields):

        with get_connection() as conn:
            with conn.cursor() as cur:

                cur.execute("""
                    SELECT
                        identifier_type_settings_code,
                        identifier_type_settings_label
                    FROM lindasylunaticas.identifier_type_settings
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
