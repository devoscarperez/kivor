from core.db import get_connection
from core.exceptions import InternalServerError


def create_user_service(data):

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:

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

                user_id = cur.fetchone()[0]

        return {"user_id": user_id}

    except Exception:
        raise InternalServerError("Error creando usuario")
