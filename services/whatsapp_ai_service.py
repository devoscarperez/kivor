import json
import re
from typing import Optional

from psycopg.types.json import Jsonb

from core.db import get_connection

TENANT_SCHEMA = "lindasylunaticas"

CONTROL_BOT_ACTIVO = "BOT_ACTIVO"
CONTROL_EN_MANO_HUMANA = "EN_MANO_HUMANA"
CONTROL_BOT_PAUSADO = "BOT_PAUSADO"
CONTROL_ESPERANDO_ABONO = "ESPERANDO_ABONO"
CONTROL_CERRADO = "CERRADO"

BLOCKED_CONTROL_STATES = (
    CONTROL_EN_MANO_HUMANA,
    CONTROL_BOT_PAUSADO,
    CONTROL_ESPERANDO_ABONO,
    CONTROL_CERRADO,
)

FAMILIA_SERVICIO_LABELS = {
    "CABELLO": "Cabello",
    "DEPILACION": "Depilación",
    "MANOS_Y_PIES": "Manos y pies",
    "FACIALES": "Faciales",
    "CORPORAL": "Corporal",
    "MAQUILLAJE": "Maquillaje",
    "CEJAS_O_PESTANAS": "Cejas o pestañas",
    "ASESORIA_IMAGEN": "Asesoría de imagen",
    "LO_MISMO": "Lo mismo que me realizo siempre",
}

FAMILIA_SERVICIO_PROMPTS = {
    "CABELLO": "Genial 😊 ¿Qué servicio de cabello necesitas en específico?",
    "DEPILACION": "¿Buscas con cera o hilo? ¿Qué zonas necesitas depilar? Si es rebaje, indícame el tipo.",
    "MANOS_Y_PIES": "Oki 😊 Para manos y/o pies, ¿qué servicio necesitas en específico?",
    "FACIALES": "Súper 😊 ¿Sabes qué servicio facial necesitas o prefieres que Tania te asesore?",
    "CORPORAL": "Genial 😊 Indícame qué servicio corporal te gustaría. Si es masaje, cuéntame qué necesitas.",
    "MAQUILLAJE": "Perfecto 😊 ¿Qué tipo de maquillaje necesitas y para qué ocasión?",
    "CEJAS_O_PESTANAS": "Ok 😊 Cuéntame qué servicio en específico buscas.",
    "ASESORIA_IMAGEN": "¿Cuál o cuáles de las asesorías te gustaría agendar?",
}

INFO_OPTION_LABELS = {
    "DIRECCION": "Dirección",
    "MEDIOS_PAGO": "Medios de pago",
    "HORARIOS_DIAS": "Días y horarios",
    "OTRO": "Otro",
}

# Pendiente: reemplazar por el enlace real de Google Maps antes de producción.
INFO_TEXTS = {
    "DIRECCION": "Estamos ubicadas en Av. Holanda 2964, Ñuñoa. Te dejo el link de Google Maps: (pendiente agregar enlace).",
    "MEDIOS_PAGO": "Nuestros abonos y el pago de servicio lo puedes realizar mediante transferencia o con tarjeta de débito o crédito. No aceptamos efectivo ni cheques.",
    "HORARIOS_DIAS": "Nuestro horario de atención en el salón es de Martes a Viernes de 11:00 a 19:00 hrs y Sábados de 10:00 a 16:00 hrs. Lunes y domingos permanecemos cerradas. Festivos se evalúan caso a caso.",
}

# Pendiente: reemplazar por los links reales de precios por familia de servicio.
PRICE_MESSAGE = (
    "Genial 😊 Todos los precios están publicados en nuestro sitio. "
    "Te dejo los links para revisar valores: (pendiente agregar links de precios)."
)

CONFIRMACION_MAS_SERVICIOS_LABELS = {
    "AGREGAR_SERVICIO": "Agregar otro servicio",
    "CONTINUAR": "Seguir con este",
}

POST_PRECIO_LABELS = {
    "AGENDAR": "Quiero agendar",
    "TENGO_DUDAS": "Tengo dudas o necesito asesoría",
    "NADA_MAS": "Por ahora nada más",
}

KEYWORDS_MENU_PRINCIPAL = {
    "AGENDAR": ["agendar", "quiero hora", "tiene hora", "una hora", "cita"],
    "PRECIOS": ["cotizar", "precio", "precios", "valor", "valores"],
    "ASESORIA": ["asesoria", "asesoría"],
    "INFORMACION": ["informacion", "información", "info"],
    "MODIFICAR_CITA": ["modificar"],
}

KEYWORDS_FAMILIA = {
    "CABELLO": ["cabello", "pelo", "corte", "color", "mechas", "balayage", "alisado", "tintura"],
    "DEPILACION": ["depilacion", "depilación", "cera", "hilo", "rebaje"],
    "MANOS_Y_PIES": ["manos y pies", "manicure", "pedicure", "uñas", "unas"],
    "FACIALES": ["facial", "faciales"],
    "CORPORAL": ["corporal", "masaje"],
    "MAQUILLAJE": ["maquillaje"],
    "CEJAS_O_PESTANAS": ["cejas", "pestañas", "pestanas"],
    "ASESORIA_IMAGEN": ["asesoria de imagen", "asesoría de imagen"],
    "LO_MISMO": ["lo mismo", "de siempre", "habitual"],
}

KEYWORDS_CONFIRMACION = {
    "AGREGAR_SERVICIO": ["agregar", "otro servicio", "sumar", "uno mas", "uno más"],
    "CONTINUAR": ["no", "seguir", "continuar", "listo", "eso es todo"],
}

KEYWORDS_POST_PRECIO = {
    "AGENDAR": ["agendar"],
    "TENGO_DUDAS": ["duda", "dudas", "asesoria", "asesoría", "consulta"],
    "NADA_MAS": ["no", "nada", "gracias"],
}

KEYWORDS_INFO = {
    "DIRECCION": ["direccion", "dirección", "ubicacion", "ubicación", "donde"],
    "MEDIOS_PAGO": ["pago", "pagos", "transferencia"],
    "HORARIOS_DIAS": ["horario", "horarios", "dias", "días"],
    "OTRO": ["otro"],
}

CONVERSATION_COLUMNS = [
    "phone", "whatsapp_name", "customer_id", "customer_name", "state", "control_state",
    "bot_enabled", "intent", "service_family", "service_detail", "preferred_day",
    "preferred_time", "preferred_professional", "notes", "has_photos", "summary_json",
    "assigned_to", "handoff_reason",
]


def normalize_phone(raw: str) -> str:
    return re.sub(r"\D", "", raw or "")


def build_whatsapp_interactive_list_payload(phone: str, reply: str, list_button: str, sections: list) -> str:
    payload = {
        "messaging_product": "whatsapp",
        "to": phone,
        "type": "interactive",
        "interactive": {
            "type": "list",
            "body": {
                "text": reply
            },
            "action": {
                "button": list_button,
                "sections": sections
            }
        }
    }

    return json.dumps(payload, ensure_ascii=False)


def get_conversation(phone: str) -> Optional[dict]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {", ".join(CONVERSATION_COLUMNS)}
                FROM {TENANT_SCHEMA}.ai_whatsapp_conversation
                WHERE phone = %s
                LIMIT 1
                """,
                (phone,)
            )
            row = cur.fetchone()

    if not row:
        return None

    return dict(zip(CONVERSATION_COLUMNS, row))


def save_conversation(phone: str, fields: dict):
    if not fields:
        return

    columns = list(fields.keys()) + ["last_customer_message_at", "window_expires_at"]
    placeholders = ["%s"] * len(fields) + ["NOW()", "NOW() + INTERVAL '24 hours'"]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns) + ", updated_at = NOW()"

    values = [Jsonb(value) if key == "summary_json" else value for key, value in fields.items()]

    query = f"""
        INSERT INTO {TENANT_SCHEMA}.ai_whatsapp_conversation (phone, {", ".join(columns)})
        VALUES (%s, {", ".join(placeholders)})
        ON CONFLICT (phone) DO UPDATE SET {set_clause}
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, [phone] + values)


def get_customer_by_mobile(phone: str) -> dict:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    customers_express_id,
                    customers_express_first_name
                FROM {TENANT_SCHEMA}.customers_express
                WHERE customers_express_mobile = %s
                LIMIT 1
                """,
                (phone,)
            )
            row = cur.fetchone()

    if not row:
        return {"customer_exists": False, "customer_id": None, "customer_name": ""}

    return {"customer_exists": True, "customer_id": row[0], "customer_name": row[1] or ""}


def create_customer_from_whatsapp(phone: str, whatsapp_name: str) -> dict:
    display_name = whatsapp_name.strip() if whatsapp_name else "Clienta WhatsApp"

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
                RETURNING customers_express_id, customers_express_first_name
                """,
                (phone, display_name, display_name)
            )
            row = cur.fetchone()

    if row:
        return {"customer_id": row[0], "customer_name": row[1] or display_name}

    return get_customer_by_mobile(phone)


def ensure_customer_context(phone: str, whatsapp_name: str) -> dict:
    existing = get_customer_by_mobile(phone)

    if existing["customer_exists"]:
        return {
            "customer_id": existing["customer_id"],
            "customer_name": existing["customer_name"] or whatsapp_name or "",
        }

    created = create_customer_from_whatsapp(phone, whatsapp_name)

    return {
        "customer_id": created.get("customer_id"),
        "customer_name": created.get("customer_name") or whatsapp_name or "Clienta WhatsApp",
    }


def match_keyword(text: str, keywords_map: dict) -> Optional[str]:
    text_lower = (text or "").lower()

    for option_id, keywords in keywords_map.items():
        if any(keyword in text_lower for keyword in keywords):
            return option_id

    return None


def resolve_selected_option(ctx: dict, keywords_map: dict) -> Optional[str]:
    interactive_id = ctx.get("interactive_id")

    if interactive_id:
        candidate = interactive_id.strip().upper()
        if candidate in keywords_map:
            return candidate

    return match_keyword(ctx.get("message"), keywords_map)


def build_menu_principal_sections() -> list:
    return [{
        "title": "Menú",
        "rows": [
            {"id": "AGENDAR", "title": "Agendar una cita"},
            {"id": "PRECIOS", "title": "Cotizar un servicio"},
            {"id": "ASESORIA", "title": "Necesito asesoría"},
            {"id": "INFORMACION", "title": "Información general"},
            {"id": "MODIFICAR_CITA", "title": "Modificar una cita"},
        ]
    }]


def build_familia_servicio_sections() -> list:
    return [{
        "title": "Servicios",
        "rows": [{"id": key, "title": value} for key, value in FAMILIA_SERVICIO_LABELS.items()]
    }]


def build_info_sections() -> list:
    return [{
        "title": "Información",
        "rows": [{"id": key, "title": value} for key, value in INFO_OPTION_LABELS.items()]
    }]


def build_confirmacion_mas_servicios_sections() -> list:
    return [{
        "title": "Opciones",
        "rows": [{"id": key, "title": value} for key, value in CONFIRMACION_MAS_SERVICIOS_LABELS.items()]
    }]


def build_post_precio_sections() -> list:
    return [{
        "title": "Opciones",
        "rows": [{"id": key, "title": value} for key, value in POST_PRECIO_LABELS.items()]
    }]


def append_service_entry(conv: dict, family: Optional[str], detail: Optional[str]) -> dict:
    summary = dict(conv.get("summary_json") or {})
    services = list(summary.get("services") or [])
    services.append({"family": family, "detail": detail})
    summary["services"] = services
    return summary


def build_summary_for_tania(data: dict) -> str:
    lines = []

    if data.get("customer_name"):
        lines.append(f"Clienta: {data['customer_name']}")
    if data.get("intent"):
        lines.append(f"Intención: {data['intent']}")

    services = (data.get("summary_json") or {}).get("services") or []
    if services:
        for item in services:
            family_label = FAMILIA_SERVICIO_LABELS.get(item.get("family"), item.get("family") or "")
            detail = item.get("detail") or ""
            lines.append(f"Servicio: {family_label}" + (f" - {detail}" if detail else ""))
    elif data.get("service_family"):
        label = FAMILIA_SERVICIO_LABELS.get(data["service_family"], data["service_family"])
        lines.append(f"Servicio: {label}")

    if data.get("preferred_day"):
        lines.append(f"Día preferido: {data['preferred_day']}")
    if data.get("preferred_time"):
        lines.append(f"Horario preferido: {data['preferred_time']}")
    if data.get("notes"):
        lines.append(f"Comentarios: {data['notes']}")
    if data.get("has_photos"):
        lines.append("Fotos recibidas: sí")

    if not lines:
        lines.append("Sin información adicional entregada por la clienta.")

    return "\n".join(lines)


def compute_information_level(data: dict) -> str:
    fields = [
        data.get("intent"),
        data.get("service_family"),
        data.get("service_detail"),
        data.get("preferred_day"),
        data.get("preferred_time"),
        data.get("notes"),
    ]
    filled = sum(1 for field in fields if field)

    if filled >= 4:
        return "completa"
    if filled >= 1:
        return "parcial"
    return "minima"


def reply_text(next_state: str, control_state: str, reply: str, save_fields: dict = None,
                action: str = "SEND_MESSAGE", debug_reason: str = "") -> dict:
    return {
        "message_type": "text",
        "reply": reply,
        "next_state": next_state,
        "control_state": control_state,
        "action": action,
        "save_fields": save_fields or {},
        "debug_reason": debug_reason,
    }


def reply_list(next_state: str, control_state: str, reply: str, list_button: str, sections: list,
                save_fields: dict = None, action: str = "SEND_MESSAGE", debug_reason: str = "") -> dict:
    return {
        "message_type": "interactive_list",
        "reply": reply,
        "list_button": list_button,
        "sections": sections,
        "next_state": next_state,
        "control_state": control_state,
        "action": action,
        "save_fields": save_fields or {},
        "debug_reason": debug_reason,
    }


def build_ficha_outcome(ctx: dict, reply: str, extra_save_fields: dict = None, debug_reason: str = "") -> dict:
    merged = dict(ctx["conv"])
    merged.update(extra_save_fields or {})
    merged["customer_name"] = ctx["customer_name"]

    summary_text = build_summary_for_tania(merged)
    information_level = compute_information_level(merged)

    save_fields = dict(extra_save_fields or {})
    save_fields.update({
        "assigned_to": "TANIA",
        "handoff_reason": debug_reason,
    })

    outcome = reply_text(
        "FICHA_LISTA", CONTROL_EN_MANO_HUMANA, reply,
        save_fields=save_fields, action="HANDOFF", debug_reason=debug_reason
    )
    outcome["summary_json_out"] = {
        "summary_for_tania": summary_text,
        "information_level": information_level,
        "conversation_state": "FICHA_LISTA",
    }
    return outcome


def handle_nueva(ctx: dict) -> dict:
    nombre = ctx["customer_name"] or ctx["whatsapp_name"]
    saludo = f"Hola {nombre} 😊" if nombre else "Hola 😊"
    reply = f"{saludo} Soy la asistente virtual de Tania. Cuéntame ¿en qué te puedo ayudar?"

    return reply_list(
        "ESPERANDO_OPCION_MENU", CONTROL_BOT_ACTIVO, reply, "Ver menú",
        build_menu_principal_sections(), debug_reason="R001_R002"
    )


def handle_opcion_menu(ctx: dict) -> dict:
    selected = resolve_selected_option(ctx, KEYWORDS_MENU_PRINCIPAL)

    if selected == "AGENDAR":
        reply = ("Súper 😊 Te haré algunas preguntas para que luego Tania pueda revisar tu solicitud. "
                  "Primero elige el tipo de servicio.")
        return reply_list(
            "ESPERANDO_FAMILIA_SERVICIO", CONTROL_BOT_ACTIVO, reply, "Ver servicios",
            build_familia_servicio_sections(), save_fields={"intent": "AGENDAR"}, debug_reason="R003"
        )

    if selected == "PRECIOS":
        return reply_text(
            "ESPERANDO_POST_PRECIO", CONTROL_BOT_ACTIVO, PRICE_MESSAGE,
            save_fields={"intent": "PRECIOS"}, debug_reason="R018"
        )

    if selected == "ASESORIA":
        reply = ("Cuéntame todo lo necesario para poder ayudarte: qué quieres lograr, qué buscas o qué duda "
                  "tienes. Puedes enviar fotos si corresponde.")
        return reply_text(
            "ESPERANDO_DETALLE_ASESORIA", CONTROL_BOT_ACTIVO, reply,
            save_fields={"intent": "ASESORIA"}, debug_reason="R023"
        )

    if selected == "INFORMACION":
        reply = "¿Con qué información te puedo ayudar?"
        return reply_list(
            "ESPERANDO_OPCION_INFO", CONTROL_BOT_ACTIVO, reply, "Ver opciones",
            build_info_sections(), save_fields={"intent": "INFORMACION"}, debug_reason="R025"
        )

    if selected == "MODIFICAR_CITA":
        reply = ("Ok 😊 Para poder revisar una modificación, considera que deben faltar al menos 24 horas "
                  "para tu cita. Cuéntanos qué cita necesitas modificar y para qué día u horario.")
        return reply_text(
            "ESPERANDO_DETALLE_MODIFICAR_CITA", CONTROL_BOT_ACTIVO, reply,
            save_fields={"intent": "MODIFICAR_CITA"}, debug_reason="R031"
        )

    return handle_fuera_de_contexto(ctx)


def handle_familia_servicio(ctx: dict) -> dict:
    selected = resolve_selected_option(ctx, KEYWORDS_FAMILIA)

    if not selected:
        return handle_fuera_de_contexto(ctx)

    if selected == "LO_MISMO":
        reply = "Súper 😊 Lo revisamos. ¿Tienes algún día de preferencia?"
        summary_json = append_service_entry(ctx["conv"], "LO_MISMO", None)
        return reply_text(
            "ESPERANDO_DIA_PREFERIDO", CONTROL_BOT_ACTIVO, reply,
            save_fields={"service_family": "LO_MISMO", "summary_json": summary_json}, debug_reason="R012"
        )

    reply = FAMILIA_SERVICIO_PROMPTS[selected]
    return reply_text(
        "ESPERANDO_DETALLE_SERVICIO", CONTROL_BOT_ACTIVO, reply,
        save_fields={"service_family": selected}, debug_reason="R004_R011"
    )


def handle_detalle_servicio(ctx: dict) -> dict:
    if not ctx["message"] and not ctx["has_photos"]:
        return handle_fuera_de_contexto(ctx)

    family = ctx["conv"].get("service_family")
    summary_json = append_service_entry(ctx["conv"], family, ctx["message"])

    reply = "Perfecto 😊 ¿Deseas agregar otro servicio o seguimos con este?"
    save_fields = {
        "service_detail": ctx["message"],
        "has_photos": ctx["has_photos"] or bool(ctx["conv"].get("has_photos")),
        "summary_json": summary_json,
    }
    return reply_list(
        "ESPERANDO_CONFIRMACION_MAS_SERVICIOS", CONTROL_BOT_ACTIVO, reply, "Continuar",
        build_confirmacion_mas_servicios_sections(), save_fields=save_fields, debug_reason="R013"
    )


def handle_confirmacion_mas_servicios(ctx: dict) -> dict:
    selected = resolve_selected_option(ctx, KEYWORDS_CONFIRMACION)

    if selected == "AGREGAR_SERVICIO":
        reply = "Perfecto 😊 Elige el otro tipo de servicio que deseas agregar."
        return reply_list(
            "ESPERANDO_FAMILIA_SERVICIO", CONTROL_BOT_ACTIVO, reply, "Ver servicios",
            build_familia_servicio_sections(), debug_reason="R014"
        )

    if selected == "CONTINUAR":
        reply = "¿Tienes algún día de preferencia?"
        return reply_text("ESPERANDO_DIA_PREFERIDO", CONTROL_BOT_ACTIVO, reply, debug_reason="R015")

    return handle_fuera_de_contexto(ctx)


def handle_dia_preferido(ctx: dict) -> dict:
    if not ctx["message"]:
        return handle_fuera_de_contexto(ctx)

    reply = ("¿Algún horario en especial? Cuéntame si puedes desde alguna hora específica o si tienes "
              "más flexibilidad.")
    return reply_text(
        "ESPERANDO_HORARIO_PREFERIDO", CONTROL_BOT_ACTIVO, reply,
        save_fields={"preferred_day": ctx["message"]}, debug_reason="R016"
    )


def handle_horario_preferido(ctx: dict) -> dict:
    reply = ("Perfecto 😊 Ya tengo la información necesaria para preparar tu atención. "
              "Tania continuará contigo por esta misma conversación.")
    return build_ficha_outcome(
        ctx, reply, extra_save_fields={"preferred_time": ctx["message"]}, debug_reason="R017"
    )


def handle_post_precio(ctx: dict) -> dict:
    selected = resolve_selected_option(ctx, KEYWORDS_POST_PRECIO)

    if selected == "AGENDAR":
        reply = "Súper 😊 Para avanzar, elige el tipo de servicio que te gustaría agendar."
        return reply_list(
            "ESPERANDO_FAMILIA_SERVICIO", CONTROL_BOT_ACTIVO, reply, "Ver servicios",
            build_familia_servicio_sections(), save_fields={"intent": "AGENDAR_POST_PRECIO"}, debug_reason="R019"
        )

    if selected == "TENGO_DUDAS":
        reply = "Déjame todos los detalles de tu consulta. Puedes enviarme fotos si es necesario, y Tania lo revisará."
        return reply_text("ESPERANDO_DETALLE_CONSULTA", CONTROL_BOT_ACTIVO, reply, debug_reason="R020")

    if selected == "NADA_MAS":
        reply = "Perfecto 😊 Quedamos atentas si necesitas algo más."
        return reply_text("CERRADO", CONTROL_CERRADO, reply, debug_reason="R021")

    return handle_fuera_de_contexto(ctx)


def handle_detalle_consulta(ctx: dict) -> dict:
    if not ctx["message"] and not ctx["has_photos"]:
        return handle_fuera_de_contexto(ctx)

    reply = ("Perfecto 😊 Dejo tu consulta preparada para que Tania la revise y continúe contigo por "
              "esta misma conversación.")
    save_fields = {
        "notes": ctx["message"],
        "has_photos": ctx["has_photos"] or bool(ctx["conv"].get("has_photos")),
    }
    return build_ficha_outcome(ctx, reply, extra_save_fields=save_fields, debug_reason="R022")


def handle_detalle_asesoria(ctx: dict) -> dict:
    if not ctx["message"] and not ctx["has_photos"]:
        return handle_fuera_de_contexto(ctx)

    reply = ("Perfecto 😊 Dejo tu solicitud preparada para que Tania la revise y continúe contigo por "
              "esta misma conversación.")
    save_fields = {
        "notes": ctx["message"],
        "has_photos": ctx["has_photos"] or bool(ctx["conv"].get("has_photos")),
    }
    return build_ficha_outcome(ctx, reply, extra_save_fields=save_fields, debug_reason="R024")


def handle_opcion_info(ctx: dict) -> dict:
    selected = resolve_selected_option(ctx, KEYWORDS_INFO)

    if selected in ("DIRECCION", "MEDIOS_PAGO", "HORARIOS_DIAS"):
        reply = f"{INFO_TEXTS[selected]} ¿Podemos ayudarte en algo más?"
        rule_map = {"DIRECCION": "R026", "MEDIOS_PAGO": "R027", "HORARIOS_DIAS": "R028"}
        return reply_list(
            "ESPERANDO_OPCION_MENU", CONTROL_BOT_ACTIVO, reply, "Ver menú",
            build_menu_principal_sections(), save_fields={"intent": selected}, debug_reason=rule_map[selected]
        )

    if selected == "OTRO":
        reply = "Claro 😊 Déjame detallado lo que te gustaría saber y Tania te responderá dentro del horario de atención."
        return reply_text("ESPERANDO_DETALLE_OTRA_INFO", CONTROL_BOT_ACTIVO, reply, debug_reason="R029")

    return handle_fuera_de_contexto(ctx)


def handle_detalle_otra_info(ctx: dict) -> dict:
    if not ctx["message"]:
        return handle_fuera_de_contexto(ctx)

    reply = ("Perfecto 😊 Dejo tu consulta preparada para que Tania la revise y continúe contigo por "
              "esta misma conversación.")
    return build_ficha_outcome(ctx, reply, extra_save_fields={"notes": ctx["message"]}, debug_reason="R030")


def handle_detalle_modificar_cita(ctx: dict) -> dict:
    if not ctx["message"]:
        return handle_fuera_de_contexto(ctx)

    reply = ("Perfecto 😊 Dejo tu solicitud preparada para que Tania revise opciones y continúe contigo "
              "por esta misma conversación.")
    return build_ficha_outcome(ctx, reply, extra_save_fields={"notes": ctx["message"]}, debug_reason="R032")


def handle_fuera_de_contexto(ctx: dict) -> dict:
    reply = ("Para ayudarte bien, dejaré tu mensaje para que Tania lo revise y continúe contigo por "
              "esta misma conversación.")
    notes = ctx["message"] or ctx["conv"].get("notes")
    return build_ficha_outcome(ctx, reply, extra_save_fields={"notes": notes}, debug_reason="R033")


STATE_HANDLERS = {
    "NUEVA": handle_nueva,
    "ESPERANDO_OPCION_MENU": handle_opcion_menu,
    "ESPERANDO_FAMILIA_SERVICIO": handle_familia_servicio,
    "ESPERANDO_DETALLE_SERVICIO": handle_detalle_servicio,
    "ESPERANDO_CONFIRMACION_MAS_SERVICIOS": handle_confirmacion_mas_servicios,
    "ESPERANDO_DIA_PREFERIDO": handle_dia_preferido,
    "ESPERANDO_HORARIO_PREFERIDO": handle_horario_preferido,
    "ESPERANDO_POST_PRECIO": handle_post_precio,
    "ESPERANDO_DETALLE_CONSULTA": handle_detalle_consulta,
    "ESPERANDO_DETALLE_ASESORIA": handle_detalle_asesoria,
    "ESPERANDO_OPCION_INFO": handle_opcion_info,
    "ESPERANDO_DETALLE_OTRA_INFO": handle_detalle_otra_info,
    "ESPERANDO_DETALLE_MODIFICAR_CITA": handle_detalle_modificar_cita,
}


def default_conversation(phone: str, whatsapp_name: str) -> dict:
    return {
        "phone": phone,
        "whatsapp_name": whatsapp_name,
        "customer_id": None,
        "customer_name": "",
        "state": "NUEVA",
        "control_state": CONTROL_BOT_ACTIVO,
        "bot_enabled": True,
        "intent": None,
        "service_family": None,
        "service_detail": None,
        "preferred_day": None,
        "preferred_time": None,
        "preferred_professional": None,
        "notes": None,
        "has_photos": False,
        "summary_json": {},
        "assigned_to": None,
        "handoff_reason": None,
    }


def process_whatsapp_message(payload: dict) -> dict:
    phone = normalize_phone(payload.get("phone", ""))
    whatsapp_name = (payload.get("whatsapp_name") or "").strip()
    message_type_in = (payload.get("message_type") or "text").strip().lower()
    interactive_id = (payload.get("interactive_id") or "").strip()
    raw_message = (payload.get("message") or "").strip()

    if not phone:
        raise Exception("phone_required")

    has_photos = message_type_in in ("image", "video", "document")
    message = raw_message
    if has_photos and not message:
        message = "(adjunto multimedia)"

    if not message and not interactive_id:
        raise Exception("message_or_interactive_required")

    conv = get_conversation(phone) or default_conversation(phone, whatsapp_name)

    if conv["control_state"] in BLOCKED_CONTROL_STATES:
        save_conversation(phone, {
            "whatsapp_name": whatsapp_name or conv.get("whatsapp_name") or "",
            "customer_id": conv.get("customer_id"),
            "customer_name": conv.get("customer_name") or "",
            "state": conv["state"],
            "control_state": conv["control_state"],
            "bot_enabled": False,
            "last_customer_message": message,
        })
        return {
            "action": "NO_REPLY",
            "message_type": "none",
            "reply": "",
            "next_state": conv["state"],
            "control": conv["control_state"],
            "whatsapp_payload_json": None,
            "summary_json": None,
            "debug_reason": "R035_conversation_not_owned_by_bot",
        }

    customer_ctx = ensure_customer_context(phone, whatsapp_name or conv.get("whatsapp_name") or "")

    ctx = {
        "phone": phone,
        "whatsapp_name": whatsapp_name or conv.get("whatsapp_name") or "",
        "message": message,
        "message_type_in": message_type_in,
        "interactive_id": interactive_id,
        "has_photos": has_photos,
        "customer_name": customer_ctx["customer_name"],
        "conv": conv,
    }

    handler = STATE_HANDLERS.get(conv["state"], handle_fuera_de_contexto)
    outcome = handler(ctx)

    save_fields = {
        "whatsapp_name": ctx["whatsapp_name"],
        "customer_id": customer_ctx["customer_id"],
        "customer_name": customer_ctx["customer_name"],
        "state": outcome["next_state"],
        "control_state": outcome["control_state"],
        "bot_enabled": outcome["control_state"] == CONTROL_BOT_ACTIVO,
        "last_customer_message": message,
    }
    save_fields.update(outcome.get("save_fields", {}))
    save_conversation(phone, save_fields)

    whatsapp_payload_json = None
    if outcome["message_type"] == "interactive_list":
        whatsapp_payload_json = build_whatsapp_interactive_list_payload(
            phone, outcome["reply"], outcome.get("list_button", "Ver opciones"), outcome["sections"]
        )

    return {
        "action": outcome.get("action", "SEND_MESSAGE"),
        "message_type": outcome["message_type"],
        "reply": outcome["reply"],
        "next_state": outcome["next_state"],
        "control": outcome["control_state"],
        "whatsapp_payload_json": whatsapp_payload_json,
        "summary_json": outcome.get("summary_json_out"),
        "debug_reason": outcome.get("debug_reason", ""),
    }
