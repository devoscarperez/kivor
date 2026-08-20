from core.db import get_connection

from routes import auth
from routes import customers_express
from routes import users
from routes import analytics
from routes import menu
from routes import ventas_lyl

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response

from fastapi.responses import JSONResponse
from core.exceptions import AppException

from pydantic import BaseModel
from typing import Optional

import os



# =========================
# MODELOS
# =========================

class WhatsAppBrainRequest(BaseModel):
    phone: str
    whatsapp_name: Optional[str] = None
    message: str
    current_state: Optional[str] = ""
    customer_exists: bool = False
    customer_first_name: Optional[str] = ""

app = FastAPI(title="KIVOR Backend")

@app.exception_handler(AppException)
async def app_exception_handler(request, exc: AppException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://kivor-frontend-dev.onrender.com",
        "https://kivor-frontend.onrender.com"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ventas_lyl.router)
app.include_router(auth.router)
app.include_router(customers_express.router)
app.include_router(users.router)
app.include_router(analytics.router)
app.include_router(menu.router)

@app.options("/{full_path:path}")
def options_handler(full_path: str):
    return Response(status_code=200)



# =========================
# Valida RUT
# =========================
def validar_rut(rut: str) -> bool:
    try:
        rut = rut.replace(".", "").replace("-", "").upper().strip()

        cuerpo = rut[:-1]
        dv = rut[-1]

        if not cuerpo.isdigit():
            return False

        suma = 0
        multiplo = 2

        for c in reversed(cuerpo):
            suma += int(c) * multiplo
            multiplo += 1
            if multiplo == 8:
                multiplo = 2

        resto = suma % 11
        dv_calculado = 11 - resto

        if dv_calculado == 11:
            dv_calculado = "0"
        elif dv_calculado == 10:
            dv_calculado = "K"
        else:
            dv_calculado = str(dv_calculado)

        return dv == dv_calculado

    except:
        return False


# =========================
# CONFIG
# =========================

@app.get("/config")
def get_config():
    return {
        "api_base": os.getenv("API_BASE")
    }



    
# =========================
# ROOT
# =========================

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
        

@app.post("/whatsapp/brain")
def whatsapp_brain(data: WhatsAppBrainRequest):
    message = data.message.strip()
    message_lower = message.lower()

    # Caso 1: estamos esperando nombre
    if data.current_state == "ESPERANDO_NOMBRE":
        palabras_bloqueadas = [
            "servicio", "servicios", "precio", "precios",
            "horario", "hora", "agendar", "agenda",
            "corte", "color", "manicure", "balayage",
            "alisado", "tratamiento", "tintura", "mechas",
            "quiero", "necesito", "consulta", "consultar",
            "?", "cuánto", "cuanto", "qué", "que"
        ]

        parece_consulta = any(palabra in message_lower for palabra in palabras_bloqueadas)
        cantidad_palabras = len(message.split())

        if cantidad_palabras >= 2 and not parece_consulta:
            primer_nombre = message.split()[0]

            return {
                "intent": "entrega_nombre",
                "is_name": True,
                "customer_name": message,
                "should_create_customer": True,
                "next_state": "ESPERANDO_SERVICIO",
                "reply": f"Gracias {primer_nombre} 😊 ¿Qué servicio te gustaría realizarte?"
            }

        return {
            "intent": "no_entrega_nombre",
            "is_name": False,
            "customer_name": "",
            "should_create_customer": False,
            "next_state": "ESPERANDO_NOMBRE",
            "reply": "Claro 😊 Te puedo ayudar con eso. Antes de continuar, ¿me puedes indicar tu nombre y apellido?"
        }

    # Caso 2: clienta ya existe
    if data.customer_exists:
        nombre = data.customer_first_name or data.whatsapp_name or ""
        saludo = f"Hola {nombre} 😊" if nombre else "Hola 😊"

        return {
            "intent": "inicio_cliente_existente",
            "is_name": False,
            "customer_name": "",
            "should_create_customer": False,
            "next_state": "ESPERANDO_SERVICIO",
            "reply": f"{saludo} Qué gusto volver a conversar contigo.\n\n¿Qué servicio te gustaría realizarte?"
        }

    # Caso 3: clienta nueva sin estado previo
    return {
        "intent": "inicio_cliente_nueva",
        "is_name": False,
        "customer_name": "",
        "should_create_customer": False,
        "next_state": "ESPERANDO_NOMBRE",
        "reply": "Hola 😊 Gracias por escribir a Lindas y Lunáticas.\n\nAntes de continuar, ¿me puedes indicar tu nombre y apellido?"
    }
