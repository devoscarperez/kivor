from fastapi import FastAPI, HTTPException
import os
import psycopg

app = FastAPI(title="KIVOR Backend")


def get_connection():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise Exception("DATABASE_URL no está configurada")
    return psycopg.connect(database_url)


@app.get("/")
def root():
    return {"status": "ok", "service": "kivor-backend"}


@app.get("/health")
def health():
    return {"healthy": True}


@app.get("/test-db")
def test_db():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                result = cur.fetchone()
        return {"db_connection": "ok", "result": result[0]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

