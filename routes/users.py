from fastapi import APIRouter, Body
from core.db import get_connection
from schemas.user_schema import CreateUserRequest

router = APIRouter()


@router.post("/users")
def create_user(data: CreateUserRequest):

    try:
        conn = get_connection()
        cur = conn.cursor()

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
            data.user_nickname,
            data.user_name,
            data.user_password,
            data.user_firstname,
            data.user_lastname,
            int(data.user_group_id)
        ))

        user_id = cur.fetchone()

        conn.commit()
        cur.close()
        conn.close()

        return {"user_id": user_id}

    except Exception as e:
        return {"detail": str(e)}
