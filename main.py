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

    # psycopg3 no soporta algunos parámetros como options
    if "options=" in database_url:
        database_url = database_url.split("&options=")[0]

    return psycopg.connect(database_url)


# =========================
# CONFIG
# =========================

@app.get("/config")
def get_config():
    return {
        "api_base": os.getenv("API_BASE")
    }


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
            raise HTTPException(status_code=401, detail="Token inválido: tenant no definido")

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

# =========================
# LOGIN USERNAME
# =========================

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

    return {"status": "ok"}
