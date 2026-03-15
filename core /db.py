import psycopg
import os
from fastapi import Depends
from .security import verify_token

def get_connection():

    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise Exception("DATABASE_URL no configurada")

    return psycopg.connect(database_url)


def set_tenant_schema(conn, tenant_schema):

    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {tenant_schema}, public")


def get_tenant_connection(current_user = Depends(verify_token)):

    conn = get_connection()

    set_tenant_schema(conn, current_user["tenant_schema"])

    try:
        yield conn
    finally:
        conn.close()
