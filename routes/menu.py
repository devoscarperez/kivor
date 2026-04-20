from fastapi import APIRouter, HTTPException, Depends
from core.db import get_connection
from core.security import verify_token

router = APIRouter()


@router.get("/menu")
def get_menu(current_user: dict = Depends(verify_token)):

    group_id = current_user.get("group_id")

    if not group_id:
        raise HTTPException(status_code=400, detail="Usuario sin grupo")

    query = """
    WITH RECURSIVE recursive_menu AS (

        SELECT m.*
        FROM core.menu m
        JOIN core.role_menu rm ON m.menu_id = rm.menu_id
        JOIN core.group_role gr ON rm.role_id = gr.role_id
        WHERE gr.group_id = %s
          AND m.menu_active = TRUE

        UNION

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


@router.get("/sessions")
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
