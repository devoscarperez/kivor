from jose import JWTError, jwt
from datetime import datetime, timedelta
from uuid import uuid4
from fastapi import Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Body
import os
import psycopg
import hashlib
from pydantic import BaseModel
from typing import Optional
from fastapi import HTTPException, Depends, Request


# =========================
# MODELOS
# =========================

class PrecioUpdate(BaseModel):
    listprice: Optional[int] = None
    professionalprice: Optional[int] = None
    salonpercentage: Optional[int] = None
    professionalpercentage: Optional[int] = None
    reason_id: int  # obligatorio

app = FastAPI(title="KIVOR Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción luego lo restringimos
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

security = HTTPBearer()


def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()

    now = datetime.utcnow()
    expire = now + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))

    to_encode.update({
        "iat": now,              # cuándo se emitió
        "exp": expire,           # cuándo expira
        "jti": str(uuid4())      # id único del token
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

        # Verificar sesión en DB
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
        raise HTTPException(status_code=401, detail="Token inválido o expirado")


# =========================
# CONTROL PERMISOS
# =========================

def require_data_write_permission(current_user: dict):
    pass


# =========================
# CONEXIÓN DB
# =========================

def get_connection():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise Exception("DATABASE_URL no está configurada")
    return psycopg.connect(database_url)
    

# def require_admin(username: str):
    
def get_connection():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise Exception("DATABASE_URL no está configurada")
    return psycopg.connect(database_url)


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
    FROM lindasylunaticas.sales v
    WHERE TO_CHAR(v.date,'MM') = %s
    AND v.family IN ('CABELLO','MANOS_Y_PIES','DEPILACION','CEJAS_Y_PESTAÑAS','FACIALES','CORPORAL')
    GROUP BY TO_CHAR(v.date,'YYYYMM')
    ORDER BY fecha;
    """

    try:
        with get_connection() as conn:
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

@app.post("/login-username")
def login_username(data: dict = Body(...)):
    username = data.get("username")

    if not username:
        raise HTTPException(status_code=400, detail="Usuario requerido")

    username = username.strip().lower()

    query = """
    SELECT user_id
    FROM core."user"
    WHERE user_name = %s
      AND user_active = TRUE;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (username,))
            user = cur.fetchone()

    if not user:
        raise HTTPException(status_code=401, detail="Usuario no válido")

    return {"status": "ok"}



@app.post("/login")
def login(data: dict = Body(...)):

    username = data.get("username")
    password = data.get("password")

    if not username or not password:
        raise HTTPException(status_code=400, detail="Usuario y clave requeridos")

    username = username.strip().lower()

    query = """
    SELECT user_password_hash, user_group_id
    FROM core."user"
    WHERE user_name = %s
      AND user_active = TRUE;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (username,))
            user = cur.fetchone()

    if not user:
        raise HTTPException(status_code=401, detail="Credenciales inválidas")

    stored_password = user[0]
    group_id = user[1]

    hashed_input = hashlib.sha256(password.encode()).hexdigest()

    if hashed_input != stored_password:
        raise HTTPException(status_code=401, detail="Credenciales inválidas")

    # Crear sesión
    session_id = str(uuid4())
    expires_at = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO core.user_session
                (session_id, user_name, user_group_id, expires_at)
                VALUES (%s, %s, %s, %s)
            """, (session_id, username, group_id, expires_at))

    # Crear JWT con session_id
    access_token = create_access_token({
        "sub": username,
        "group_id": group_id,
        "session_id": session_id
    })

    return {
        "access_token": access_token,
        "token_type": "bearer"
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


@app.post("/logout")
def logout(current_user: dict = Depends(verify_token)):

    session_id = current_user.get("session_id")

    if not session_id:
        raise HTTPException(status_code=400, detail="Sesión inválida")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE core.user_session
                SET revoked = TRUE
                WHERE session_id = %s
            """, (session_id,))

    return {"status": "ok"}
