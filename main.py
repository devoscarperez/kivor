from core.db import get_connection, set_tenant_schema
from core.security import create_access_token, verify_token
from core.security import get_token_expiration_minutes
from routes import auth
from routes import customers_express
from routes import users
from routes import analytics


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

app.include_router(auth.router)
app.include_router(customers_express.router)
app.include_router(users.router)
app.include_router(analytics.router)

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
# GANANCIAS
# =========================



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
