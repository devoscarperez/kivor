-- Fuente: docs/especificacion_chatbot_whatsapp_tania_kivor_make.md, seccion 15.
-- Ejecutar manualmente contra la base (schema lindasylunaticas) antes de desplegar
-- la nueva version de services/whatsapp_ai_service.py.
-- No reemplaza ni borra lindasylunaticas.ai_whatsapp_state (queda en desuso, sin borrar datos del piloto).

CREATE TABLE IF NOT EXISTS lindasylunaticas.ai_whatsapp_conversation (
    phone varchar(20) PRIMARY KEY,
    whatsapp_name varchar(120),
    customer_id uuid NULL,
    customer_name varchar(200) NULL,
    state varchar(60) NOT NULL DEFAULT 'NUEVA',
    control_state varchar(60) NOT NULL DEFAULT 'BOT_ACTIVO',
    bot_enabled boolean NOT NULL DEFAULT true,
    intent varchar(80) NULL,
    service_family varchar(100) NULL,
    service_detail text NULL,
    preferred_day text NULL,
    preferred_time text NULL,
    preferred_professional text NULL,
    notes text NULL,
    has_photos boolean NOT NULL DEFAULT false,
    summary_json jsonb NULL,
    last_customer_message text NULL,
    last_customer_message_at timestamp NULL,
    window_expires_at timestamp NULL,
    last_bot_message_at timestamp NULL,
    last_human_message_at timestamp NULL,
    assigned_to varchar(100) NULL,
    handoff_reason text NULL,
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now()
);
