from jose import JWTError, jwt
from datetime import datetime, timedelta
from uuid import uuid4
from fastapi import HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import os

from core.db import get_connection

SECRET_KEY = os.getenv("SECRET_KEY")

if not SECRET_KEY:
    raise Exception("SECRET_KEY no está configurada")


ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

security = HTTPBearer()


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
