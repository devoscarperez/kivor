import os
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from psycopg_pool import ConnectionPool


def _clean_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise Exception("DATABASE_URL no está configurada")

    parsed = urlparse(database_url)
    query = parse_qs(parsed.query)

    # eliminar parámetro problemático
    query.pop("options", None)

    new_query = urlencode(query, doseq=True)

    return urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        new_query,
        parsed.fragment
    ))


def _configurar_conexion(conn):
    conn.execute("SET TIME ZONE 'America/Santiago'")


# min_size=0: no abre conexiones hasta que se necesiten (evita que el
# arranque del backend falle si la BD tarda en responder). max_size bajo
# a propósito: la BD en Render (free tier) tiene un límite de conexiones
# simultáneas compartido con otras herramientas (ej. DBeaver); antes de
# este pool, cada request abría una conexión nueva y podía agotarlo.
pool = ConnectionPool(
    conninfo=_clean_database_url(),
    min_size=0,
    max_size=5,
    configure=_configurar_conexion,
    open=True,
)


def get_connection():
    return pool.connection()


def set_tenant_schema(conn, schema):
    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {schema}")
