from typing import Optional
from core.db import get_connection


TENANT_SCHEMA = "lindasylunaticas"


def get_whatsapp_state(phone: str) -> str:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT state
                FROM {TENANT_SCHEMA}.ai_whatsapp_state
                WHERE phone = %s
                LIMIT 1
                """,
                (phone,)
            )
            row = cur.fetchone()

    return row[0] if row else ""


def save_whatsapp_state(phone: str, state: str, last_message: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {TENANT_SCHEMA}.ai_whatsapp_state (
                    phone,
                    state,
                    last_message,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, NOW(), NOW())
                ON CONFLICT (phone)
                DO UPDATE SET
                    state = EXCLUDED.state,
                    last_message = EXCLUDED.last_message,
                    updated_at = NOW()
                """,
                (phone, state, last_message)
            )


def get_customer_by_mobile(phone: str) -> dict:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    customers_express_first_name
                FROM {TENANT_SCHEMA}.customers_express
                WHERE customers_express_mobile = %s
                LIMIT 1
                """,
                (phone,)
            )
            row = cur.fetchone()

    if not row:
        return {
            "customer_exists": False,
            "customer_first_name": ""
        }

    return {
        "customer_exists": True,
        "customer_first_name": row[0] or ""
    }


def create_customer_from_whatsapp(phone: str, full_name: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {TENANT_SCHEMA}.customers_express (
                    customers_express_mobile,
                    customers_express_first_name,
                    customers_express_full_search,
                    customers_express_status,
                    customers_express_link_status
                )
                VALUES (%s, %s, %s, 'ACTIVE', 'WHATSAPP_AI')
                ON CONFLICT DO NOTHING
                """,
                (phone, full_name, full_name)
            )


def message_looks_like_name(message: str) -> bool:
    message_clean = message.strip()
    message_lower = message_clean.lower()

    blocked_words = [
        "servicio", "servicios", "precio", "precios",
        "horario", "hora", "agendar", "agenda",
        "corte", "color", "manicure", "balayage",
        "alisado", "tratamiento", "tintura", "mechas",
        "quiero", "necesito", "consulta", "consultar",
        "cuánto", "cuanto", "qué", "que", "?"
    ]

    if any(word in message_lower for word in blocked_words):
        return False

    return len(message_clean.split()) >= 2


def process_whatsapp_message(payload: dict) -> dict:
    phone = payload.get("phone", "").strip()
    whatsapp_name = payload.get("whatsapp_name", "").strip()
    message = payload.get("message", "").strip()

    if not phone:
        raise Exception("phone_required")

    if not message:
        raise Exception("message_required")

    current_state = get_whatsapp_state(phone)
    customer_data = get_customer_by_mobile(phone)

    customer_exists = customer_data["customer_exists"]
    customer_first_name = customer_data["customer_first_name"]

    # Caso 1: estamos esperando nombre
    if current_state == "ESPERANDO_NOMBRE":
        if message_looks_like_name(message):
            first_name = message.split()[0]

            create_customer_from_whatsapp(phone, message)

            next_state = "ESPERANDO_SERVICIO"
            save_whatsapp_state(phone, next_state, message)

            return {
                "intent": "entrega_nombre",
                "is_name": True,
                "customer_name": message,
                "should_create_customer": True,
                "next_state": next_state,
                "reply": f"Gracias {first_name} 😊 ¿Qué servicio te gustaría realizarte?"
            }

        next_state = "ESPERANDO_NOMBRE"
        save_whatsapp_state(phone, next_state, message)

        return {
            "intent": "no_entrega_nombre",
            "is_name": False,
            "customer_name": "",
            "should_create_customer": False,
            "next_state": next_state,
            "reply": "Claro 😊 Te puedo ayudar con eso. Antes de continuar, ¿me puedes indicar tu nombre y apellido?"
        }

    # Caso 2: cliente existente
    if customer_exists:
        nombre = customer_first_name or whatsapp_name
        saludo = f"Hola {nombre} 😊" if nombre else "Hola 😊"

        next_state = "ESPERANDO_SERVICIO"
        save_whatsapp_state(phone, next_state, message)

        return {
            "intent": "inicio_cliente_existente",
            "is_name": False,
            "customer_name": "",
            "should_create_customer": False,
            "next_state": next_state,
            "reply": f"{saludo} Qué gusto volver a conversar contigo. \n \n ¿Qué servicio te gustaría realizarte?"
        }

    # Caso 3: cliente nuevo sin estado previo
    next_state = "ESPERANDO_NOMBRE"
    save_whatsapp_state(phone, next_state, message)

    return {
        "intent": "inicio_cliente_nueva",
        "is_name": False,
        "customer_name": "",
        "should_create_customer": False,
        "next_state": next_state,
        "reply": "Hola 😊 Gracias por escribir a Lindas y Lunáticas. \n \n Antes de continuar, ¿me puedes indicar tu nombre y apellido?"
    }
