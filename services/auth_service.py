from datetime import datetime, timedelta
from uuid import uuid4
import hashlib

from core.db import get_connection
from core.security import create_access_token, get_token_expiration_minutes


def login_user(username: str, password: str, client_ip: str, user_agent: str):

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
                raise Exception("Credenciales inválidas")

            user_id, stored_password, group_id = user

            # PASSWORD
            hashed_input = hashlib.sha256(password.encode()).hexdigest()

            if hashed_input != stored_password:
                raise Exception("Credenciales inválidas")

            # PERSON
            cur.execute("""
                SELECT person_id
                FROM core.person
                WHERE person_user_id = %s
            """, (user_id,))

            person = cur.fetchone()

            if not person:
                raise Exception("Usuario sin persona")

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
                raise Exception("Usuario sin organización")

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
                raise Exception("No tenant")

            tenant_id, tenant_schema = tenant

    # SESSION
    session_id = str(uuid4())
    expires_at = datetime.utcnow() + timedelta(minutes=get_token_expiration_minutes())

    with get_connection() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                INSERT INTO core.user_session
                (session_id, user_name, user_group_id, expires_at, ip_address, user_agent)
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (session_id, username, group_id, expires_at, client_ip, user_agent))

    token = create_access_token({
        "sub": username,
        "group_id": group_id,
        "session_id": session_id,
        "tenant_id": tenant_id,
        "tenant_schema": tenant_schema,
        "organization_id": organization_id,
        "person_id": person_id
    })

    return {
        "access_token": token,
        "token_type": "bearer"
    }
