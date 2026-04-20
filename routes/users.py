from fastapi import APIRouter, Body
from core.db import get_connection

router = APIRouter()


@router.post("/users")
def create_user(data: dict = Body(...)):

    print("==== CREATE USER START ====")
    print("DATA:", data)

    try:
        conn = get_connection()
        print("DB CONNECTED")

        cur = conn.cursor()

        print("EXECUTING INSERT...")

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
            data["user_nickname"],
            data["user_name"],
            data["user_password"],
            data["user_firstname"],
            data["user_lastname"],
            int(data["user_group_id"])
        ))

        print("INSERT OK")

        user_id = cur.fetchone()
        print("USER ID:", user_id)

        conn.commit()
        print("COMMIT OK")

        cur.close()
        conn.close()

        print("==== CREATE USER END ====")

        return {"user_id": user_id}

    except Exception as e:
        print("ERROR CREATE USER:", str(e))
        return {"detail": str(e)}
