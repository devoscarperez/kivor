import os
import psycopg
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse


def get_connection():
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise Exception("DATABASE_URL no está configurada")

    parsed = urlparse(database_url)
    query = parse_qs(parsed.query)

    # eliminar parámetro problemático
    query.pop("options", None)

    new_query = urlencode(query, doseq=True)

    clean_url = urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        new_query,
        parsed.fragment
    ))

    return psycopg.connect(clean_url)


def set_tenant_schema(conn, schema):
    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {schema}")
