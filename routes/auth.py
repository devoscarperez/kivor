from fastapi import APIRouter, HTTPException, Request, Body
from datetime import datetime, timedelta
from uuid import uuid4
import hashlib

from core.db import get_connection
from core.security import create_access_token, get_token_expiration_minutes

router = APIRouter()


@router.post("/login")
def login(request: Request, data: dict = Body(...)):

    username = data.get("username")
    password = data.get("password")

    if not username or not password:
        raise HTTPException(status_code=400, detail="Usuario y clave requeridos")

    username = username.strip().lower()

    with get_connection() as conn:
        with conn.cursor() as cur:

            # USER
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

            # PASSWORD
            hashed_input = hashlib.sha256(password.encode()).hexdigest()

            if hashed_input != stored_password:
                raise HTTPException(status_code=401, detail="Credenciales inválidas")

            # PERSON
            cur.execute("""
                SELECT person_id
                FROM core.person
                WHERE person_user_id = %s
            """, (user_id,))

            person = cur.fetchone()

            if not person:
                raise HTTPException(status_code=403, detail="Usuario no tiene persona asociada")

            person_id = person[0]

            # ORGANIZATION
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

            # TENANT
            cur.execute("""
            WITH RECURSIVE org_tree AS (
                SELECT organization_id, organization_parent_id, organization_tenant_id
                FROM core.organization
                WHERE organization_id = %s

                UNION ALL

                SELECT o.organization_id, o.organization_parent_id, o.organization_tenant_id
                FROM core.organization o
                JOIN org_tree t ON o.organization_id = t.organization_parent_id
            )
            SELECT t.tenant_id, t.tenant_db_schema
            FROM org_tree ot
            JOIN core.tenant t ON t.tenant_id = ot.organization_tenant_id
            WHERE ot.organization_tenant_id IS NOT NULL
            LIMIT 1
            """, (organization_id,))

            tenant = cur.fetchone()

            if not tenant:
                raise HTTPException(status_code=403, detail="No se pudo resolver el tenant del usuario")

            tenant_id = tenant[0]
            tenant_schema = tenant[1]

    # SESSION
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


@router.api_route("/login-username", methods=["POST", "OPTIONS"])
def login_username(data: dict = Body(None)):

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
    data: dict = Body(...),
    current_user: dict = None
):

    session_id = data.get("session_id")

    if not session_id:
        raise HTTPException(status_code=400, detail="session_id requerido")

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
