from services.auth_service import login_user
from schemas.auth_schema import (
    LoginRequest,
    LoginUsernameRequest,
    LogoutSessionRequest,
)

from fastapi import APIRouter, HTTPException, Request, Body
from datetime import datetime, timedelta
from uuid import uuid4
import hashlib

from core.db import get_connection
from core.security import create_access_token, get_token_expiration_minutes

router = APIRouter()

@router.post("/login")
def login(request: Request, data: LoginRequest):

    username = data.username
    password = data.password

    if not username or not password:
        raise HTTPException(status_code=400, detail="Usuario y clave requeridos")

    try:
        return login_user(
            username,
            password,
            request.client.host,
            request.headers.get("user-agent")
        )
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))


@router.post("/login-username")
def login_username(data: LoginUsernameRequest):

    username = data.username.strip().lower()

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


@router.post("/logout")
def logout(current_user: dict):

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


@router.post("/logout-session")
def logout_session(
    data: LogoutSessionRequest,
    current_user: dict = Depends(verify_token)
):

    session_id = data.session_id

    with get_connection() as conn:
        with conn.cursor() as cur:

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

            cur.execute("""
                UPDATE core.user_session
                SET revoked = TRUE
                WHERE session_id = %s
            """, (session_id,))

    return {"status": "ok", "message": "Sesión cerrada"}
