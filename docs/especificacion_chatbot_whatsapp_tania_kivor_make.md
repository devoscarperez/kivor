# Especificación de Implementación Chatbot WhatsApp - Lindas y Lunáticas / Kivor

Versión: 08-09-2026

Documento dirigido a Claude Code para implementar en Kivor/Python y a Oscar para configurar el flujo en Make/Meta. Este documento consolida las definiciones funcionales y técnicas acordadas durante la conversación y usa como base el archivo de estados técnicos entregado.

## 1. Objetivo

Implementar un chatbot guiado para WhatsApp que atienda mensajes iniciales de clientas de Lindas y Lunáticas, ordene la solicitud mediante menú, recoja toda la información posible y genere una ficha para que Tania continúe la atención humana.

El chatbot no debe cerrar ventas ni sustituir a Tania. Debe actuar como asistente de recepción digital, con alcance controlado.

## 2. Alcance funcional validado

El bot debe hacer:

```
- Saludar e iniciar un menú guiado.
- Identificar intención principal: agendar, cotizar, asesoría, información general o modificar cita.
- Recoger datos disponibles: servicio/familia, detalle, día preferido, horario preferido, profesional si aparece, comentarios y fotos si se reciben.
- Generar una ficha para Tania con información completa, parcial o mínima.
- Pasar a estado humano cuando se genera la ficha o cuando el caso salga del alcance permitido.
```

El bot NO debe hacer:

```
- No agenda horas.
- No confirma disponibilidad.
- No cotiza en detalle ni calcula precios.
- No cobra ni confirma pagos.
- No cancela ni reagenda automáticamente.
- No responde temas fuera del menú.
- No conversa libremente como IA autónoma.
```

## 3. Decisión técnica: chatbot guiado + IA controlada

La implementación recomendada no es IA libre desde el inicio. La solución debe ser un chatbot guiado por estados, listas y reglas, con IA/API sólo como apoyo posterior para clasificar mensajes libres cuando la clienta no use botones.

```
Primera versión:
70% chatbot guiado
30% IA controlada, opcional para clasificar intención

No implementar IA libre conversacional.
```

## 4. Arquitectura general

```
WhatsApp Clienta
    ↓
Meta WhatsApp Cloud API
    ↓ webhook
Make
    ↓ HTTP
Kivor Backend / FastAPI
    ↓
whatsapp_ai_service.py
    ↓
PostgreSQL
    ↓
Respuesta a Make
    ↓
Meta WhatsApp Cloud API
    ↓
Clienta
```

Make debe actuar como puente/orquestador. Kivor/Python debe ser el cerebro del flujo: consulta estado, aplica reglas, guarda datos y devuelve el tipo de respuesta que corresponde.

## 5. Contexto actual de integración

```
Meta App: Kivor IA Receptionist Claude
App ID observado: 28264074623209578
WABA: Lindas Y Lunáticas
WABA ID: 28443453991927094
Phone Number ID: 1234459556420032
Número Cloud API piloto: +56 9 5880 3529
Webhook Make: whatsapp_lindas_test
Verify token: kivor_whatsapp_verify_2026
Plantilla aprobada: prueba_lindas
Endpoint Kivor dev: https://kivor-dev.onrender.com/whatsapp/brain
Endpoint Kivor prod: https://kivor.onrender.com/whatsapp/brain
```

Para el piloto se mantiene el número prepago personal declarado como número API. El número real de Tania, que hoy usa WhatsApp Business App, no debe tocarse todavía. Más adelante se evaluará coexistencia Business App + Cloud API.

## 6. Regla de oro de control bot/humano

```
Cada conversación tiene su propio estado técnico por teléfono de clienta.
No existe un estado general único para todo el bot.

Ejemplo:
Clienta A: BOT_ACTIVO
Clienta B: EN_MANO_HUMANA
Clienta C: FICHA_LISTA
Clienta D: CERRADO
```

Cuando la conversación queda en mano de Tania, el bot debe quedar en silencio sólo para esa clienta.

## 7. Estados técnicos base

| Estado técnico | Descripción | Responsable |
| --- | --- | --- |
| NUEVA | Conversación sin estado previo o inicio detectado. | Bot |
| ESPERANDO_OPCION_MENU | El sistema espera una opción del menú principal. | Bot |
| ESPERANDO_FAMILIA_SERVICIO | El sistema espera que la clienta elija familia de servicio. | Bot |
| ESPERANDO_DETALLE_SERVICIO | El sistema espera el detalle específico del servicio solicitado. | Bot |
| ESPERANDO_CONFIRMACION_MAS_SERVICIOS | El sistema espera si la clienta desea agregar otro servicio o continuar. | Bot |
| ESPERANDO_DIA_PREFERIDO | El sistema espera el día preferido de atención. | Bot |
| ESPERANDO_HORARIO_PREFERIDO | El sistema espera rango u horario preferido. | Bot |
| ESPERANDO_POST_PRECIO | Después de enviar precios, espera si agenda, consulta o pide asesoría. | Bot |
| ESPERANDO_DETALLE_CONSULTA | Espera detalle de una duda para preparar ficha. | Bot |
| ESPERANDO_DETALLE_ASESORIA | Espera detalle/fotos/contexto para preparar ficha de asesoría. | Bot |
| ESPERANDO_OPCION_INFO | Espera opción dentro del menú de información general. | Bot |
| ESPERANDO_DETALLE_OTRA_INFO | Espera detalle de una consulta de información no cubierta. | Bot |
| ESPERANDO_DETALLE_MODIFICAR_CITA | Espera datos de cita a modificar para traspasar a recepción. | Bot |
| FICHA_LISTA | La información mínima está lista para que Tania tome la atención. | Sistema |
| EN_MANO_HUMANA | La conversación queda pausada para el bot y asignada a recepción humana. | Tania |
| ESPERANDO_ABONO | Recepción ya envió instrucciones de abono y espera comprobante/confirmación. | Tania |
| CERRADO | Atención terminada; no responder automáticamente salvo nuevo inicio. | Sistema/Tania |
| CONDICIONAL | El siguiente estado depende del contenido o selección de la clienta. | Bot/IA controlada |

## 8. Estados de control recomendados

| Estado | Significado | Bot responde |
| --- | --- | --- |
| BOT_ACTIVO | El asistente virtual puede responder y guiar el flujo. | Sí |
| FICHA_LISTA | El bot generó ficha con la información disponible. | No, salvo acción técnica de notificación. |
| EN_MANO_HUMANA | Tania tomó o debe tomar la conversación. | No |
| BOT_PAUSADO | Silencio temporal del bot. | No |
| ESPERANDO_ABONO | Tania ya envió instrucciones o está esperando pago/comprobante. | No, salvo acciones asistidas autorizadas. |
| CERRADO | Atención terminada. | No |

## 9. Flujo funcional principal

```
NUEVA
  → ESPERANDO_OPCION_MENU
  → ESPERANDO_FAMILIA_SERVICIO
  → ESPERANDO_DETALLE_SERVICIO
  → ESPERANDO_CONFIRMACION_MAS_SERVICIOS
  → ESPERANDO_DIA_PREFERIDO
  → ESPERANDO_HORARIO_PREFERIDO
  → FICHA_LISTA
  → EN_MANO_HUMANA
```

La ficha puede generarse aunque falten datos. La regla validada es generar todo lo disponible, sin importar si la información es completa o parcial.

## 10. Menú principal

El primer mensaje debe ordenar la conversación con opciones. Debe evitar conversación abierta innecesaria.

```
Opciones del menú principal:
- Agendar una cita
- Cotizar un servicio
- Necesito asesoría
- Información general
- Modificar una cita
```

## 11. Familias de servicio

```
Familias sugeridas:
- Cabello
- Depilación
- Manos y pies
- Faciales
- Corporal
- Maquillaje
- Cejas o pestañas
- Asesoría de imagen
- Lo mismo que me realizo siempre
```

Para WhatsApp, usar interactive_list cuando existan más de 3 opciones. No usar botones para listas largas.

## 12. Ficha para Tania

La ficha debe guardar todo lo disponible y debe permitir a Tania continuar la atención sin pedir a la clienta que repita todo.

```
Campos mínimos de ficha:
- Nombre clienta, si existe.
- Teléfono WhatsApp.
- Intención principal.
- Familia de servicio.
- Servicio específico o detalle escrito.
- Comentarios relevantes.
- Preferencia de día.
- Preferencia de horario.
- Profesional preferida, si aparece.
- Fotos recibidas: sí/no y referencias si se almacenan.
- Consulta de precio: sí/no.
- Modificación/reagenda: sí/no.
- Nivel de información: mínima/parcial/completa.
- Resumen para Tania.
- Estado: FICHA_LISTA / EN_MANO_HUMANA.
```

## 13. Reglas de fuera de alcance

Si la clienta escribe algo que no corresponde al menú o solicita una acción que requiere criterio humano, el bot debe generar ficha con lo que tenga y pasar a Tania.

```
Casos de paso a humano:
- La clienta quiere modificar una cita.
- La clienta pide disponibilidad concreta.
- La clienta pregunta precios en detalle no publicados.
- La clienta pide cobrar, abonar, confirmar pago o comprobante.
- La clienta hace una consulta compleja de asesoría.
- La clienta se sale del menú o el bot no entiende con confianza.
- La clienta indica que no desea responder más preguntas.
```

## 14. Especificación para Claude - Kivor/Python

Claude debe modificar principalmente `services/whatsapp_ai_service.py` y, si corresponde, `routes/whatsapp_ai.py`. No debe modificar `main.py` salvo que falte registrar el router.

### 14.1 Endpoint

```
POST /whatsapp/brain

Body esperado desde Make:
{
  "phone": "56985951984",
  "whatsapp_name": "Oscar",
  "message": "hola",
  "message_type": "text | interactive",
  "interactive_id": "AGENDAR | CABELLO | ...",
  "raw_payload": {}
}
```

El endpoint debe tolerar que algunos campos no vengan. `phone` y alguna forma de mensaje/evento son obligatorios.

### 14.2 Respuesta estándar del backend

```
{
  "action": "SEND_MESSAGE | NO_REPLY | HANDOFF",
  "message_type": "text | interactive_list | template | none",
  "reply": "texto visible para clienta",
  "next_state": "ESPERANDO_FAMILIA_SERVICIO",
  "control": "BOT_ACTIVO | EN_MANO_HUMANA",
  "whatsapp_payload_json": "{...}",
  "summary_json": {},
  "debug_reason": "texto interno"
}
```

Cuando `message_type = interactive_list`, Kivor debe devolver `whatsapp_payload_json` completo como string JSON para que Make no tenga que construir arrays anidados.

### 14.3 Prioridad de validación

```
1. Normalizar teléfono.
2. Leer estado actual desde PostgreSQL.
3. Si estado está en EN_MANO_HUMANA, BOT_PAUSADO o CERRADO: devolver NO_REPLY.
4. Detectar si el evento es intervención humana/echo, si aplica en coexistencia futura: marcar EN_MANO_HUMANA y devolver NO_REPLY.
5. Aplicar máquina de estados.
6. Guardar datos recolectados.
7. Actualizar estado siguiente.
8. Construir respuesta WhatsApp.
9. Devolver salida a Make.
```

### 14.4 Regla NO_REPLY

```
if conversation_state in ["EN_MANO_HUMANA", "BOT_PAUSADO", "CERRADO"]:
    return {
        "action": "NO_REPLY",
        "message_type": "none",
        "reason": "conversation_not_owned_by_bot"
    }
```

### 14.5 Listas interactivas

```
def build_whatsapp_interactive_list_payload(phone: str, reply: str, list_button: str, sections: list) -> str:
    payload = {
        "messaging_product": "whatsapp",
        "to": phone,
        "type": "interactive",
        "interactive": {
            "type": "list",
            "body": {"text": reply},
            "action": {
                "button": list_button,
                "sections": sections
            }
        }
    }
    return json.dumps(payload, ensure_ascii=False)
```

## 15. Especificación para PostgreSQL

La tabla actual `lindasylunaticas.ai_whatsapp_state` puede evolucionar a una tabla más completa de conversación. Para prototipo, puede mantenerse una tabla única si se agregan columnas de ficha.

```
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
```

Debe mantenerse consulta a `lindasylunaticas.customers_express` por `customers_express_mobile` para recuperar nombre cuando exista.

## 16. Especificación para Make

Make debe recibir mensajes desde Meta, llamar a Kivor y luego decidir si envía o no envía respuesta.

```
Webhook WhatsApp
    ↓
HTTP /whatsapp/brain en Kivor
    ↓
Router por action/message_type
        - NO_REPLY: no hacer nada
        - text: enviar payload text a Meta
        - interactive_list: enviar whatsapp_payload_json directo a Meta
        - template: enviar payload template a Meta, futuro
```

### 16.1 Body hacia Kivor

```
{
  "phone": "{{messages[].from}}",
  "whatsapp_name": "{{contacts[].profile.name}}",
  "message": "{{messages[].text.body}}",
  "message_type": "{{messages[].type}}",
  "interactive_id": "{{messages[].interactive.list_reply.id}}",
  "raw_payload": "{{payload completo si es posible}}"
}
```

### 16.2 HTTP final a Meta

```
POST https://graph.facebook.com/v26.0/1234459556420032/messages
Headers:
  Authorization: Bearer <TOKEN>
  Content-Type: application/json

Para text:
usar JSON creado por Make.

Para interactive_list:
Body content type: application/json
Body input method: JSON string
Body content: whatsapp_payload_json devuelto por Kivor.
```

Importante: evitar construir listas anidadas en Make. Ya se observó error `Invalid collection in parameter rows`. Kivor debe enviar el JSON completo.

## 17. Coexistencia y atención humana futura

Para producción, el número real de Tania hoy está en WhatsApp Business App. No debe registrarse como número Cloud API normal. Se debe estudiar habilitación por coexistencia Business App + Cloud API.

La práctica recomendada para handoff humano es tener estado interno por conversación y un botón explícito de 'Tomar conversación' en Kivor/PWA. Como apoyo futuro, si coexistencia entrega eventos de mensajes enviados desde la app, se puede pausar automáticamente el bot.

```
Si Tania toma conversación:
state/control_state = EN_MANO_HUMANA
bot_enabled = false
assigned_to = TANIA

Si Tania cierra:
state/control_state = CERRADO
bot_enabled = false

Si Tania devuelve al bot:
state/control_state = BOT_ACTIVO
bot_enabled = true
```

## 18. Ventana de 24 horas y plantillas Meta

Meta indica en su página de precios que cobra por mensajes entregados según destinatario y categoría: marketing, utility, authentication y service. También define mensajes service como respuestas dentro de una ventana de atención de 24 horas que se abre y se reinicia cuando el usuario escribe. Antes de producción, Claude/Oscar deben revalidar la tarifa vigente en la página oficial de Meta.

```
Referencias oficiales para validar:
https://business.whatsapp.com/products/platform-pricing
https://developers.facebook.com/docs/whatsapp/pricing/
https://developers.facebook.com/docs/whatsapp/cloud-api/
```

Regla funcional para Kivor: guardar `last_customer_message_at` y calcular `window_expires_at = last_customer_message_at + 24 horas`. Esto no reemplaza las reglas de Meta, pero ayuda a mostrar estado operacional y decidir cuándo usar plantilla.

## 19. Matriz de implementación basada en el Excel

| Estado actual | Evento/mensaje | Intención | Dato a guardar | Estado siguiente | Control | Paso a humano |
| --- | --- | --- | --- | --- | --- | --- |
| NUEVA | Hola | INICIO | si quiere agendar, cotizar, asesoria, modificar cita o info general del salon | ESPERANDO_OPCION_MENU | BOT_ACTIVO | no todavia |
| NUEVA | hola lindas y lunaticas | INICIO | si quiere agendar, cotizar, asesoria, modificar cita o info general del salon | ESPERANDO_OPCION_MENU | BOT_ACTIVO | no todavia |
| NUEVA | quiero agendar una hora o resolver dudas | INICIO | si quiere agendar, cotizar, asesoria, modificar cita o info general del salon | ESPERANDO_OPCION_MENU | BOT_ACTIVO | no todavia |
| NUEVA | quiero una hora | INICIO | si quiere agendar, cotizar, asesoria, modificar cita o info general del salon | ESPERANDO_OPCION_MENU | BOT_ACTIVO | no todavia |
| NUEVA | tiene hora | INICIO | si quiere agendar, cotizar, asesoria, modificar cita o info general del salon | ESPERANDO_OPCION_MENU | BOT_ACTIVO | no todavia |
| NUEVA | como estas | INICIO | si quiere agendar, cotizar, asesoria, modificar cita o info general del salon | ESPERANDO_OPCION_MENU | BOT_ACTIVO | no todavia |
| ESPERANDO_OPCION_MENU | selecciona agendar una cita | AGENDAR | que servicio quiere | ESPERANDO_FAMILIA_SERVICIO | BOT_ACTIVO | no todavia |
| ESPERANDO_FAMILIA_SERVICIO | selecciona cabello | CABELLO | detalles del servicio solicitado | ESPERANDO_DETALLE_SERVICIO | BOT_ACTIVO | no todavia |
| ESPERANDO_FAMILIA_SERVICIO | selecciona depilacion | DEPILACION | detalles del servicio solicitado | ESPERANDO_DETALLE_SERVICIO | BOT_ACTIVO | no todavia |
| ESPERANDO_FAMILIA_SERVICIO | selecciona manos y pies | MANOS Y PIES | detalles del servicio solicitado | ESPERANDO_DETALLE_SERVICIO | BOT_ACTIVO | no todavia |
| ESPERANDO_FAMILIA_SERVICIO | selecciona faciales | FACIALES | detalles del servicio solicitado | ESPERANDO_DETALLE_SERVICIO | BOT_ACTIVO | no todavia |
| ESPERANDO_FAMILIA_SERVICIO | selecciona corporal  | CORPORAL | detalles del servicio solicitado | ESPERANDO_DETALLE_SERVICIO | BOT_ACTIVO | no todavia |
| ESPERANDO_FAMILIA_SERVICIO | selecciona maquillaje | MAQUILLAJE | detalles del servicio solicitado | ESPERANDO_DETALLE_SERVICIO | BOT_ACTIVO | no todavia |
| ESPERANDO_FAMILIA_SERVICIO | selecciona cejas o pestañas | CEJAS O PESTAÑAS | detalles del servicio solicitado | ESPERANDO_DETALLE_SERVICIO | BOT_ACTIVO | no todavia |
| ESPERANDO_FAMILIA_SERVICIO | selecciona asesoria de imagen | ASESORIA DE IMAGEN | detalles del servicio solicitado | ESPERANDO_DETALLE_SERVICIO | BOT_ACTIVO | no todavia |
| ESPERANDO_FAMILIA_SERVICIO | selecciona lo mismo que me realizo siempre | LO MISMO | detalles del servicio solicitado | ESPERANDO_DIA_PREFERIDO | BOT_ACTIVO | no todavia |
| ESPERANDO_CONFIRMACION_MAS_SERVICIOS | Selecciona NO o Lo mismo que me realizo siempre | DIA_PREFERIDO | que dia se quiere atender | ESPERANDO_DIA_PREFERIDO | BOT_ACTIVO | no todavia |
| ESPERANDO_DIA_PREFERIDO | Nos menciona dia de preferencia o NO | HORARIO_PREFERIDO | horario en el que se quiere atender | ESPERANDO_HORARIO_PREFERIDO | BOT_ACTIVO | no todavia |
| ESPERANDO_HORARIO_PREFERIDO | nos meciona horario o NO                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | CIERRE_DATOS | Ninguno adicional | FICHA_LISTA | BOT_ACTIVO → EN_MANO_HUMANA | Sí, con ficha lista |
| ESPERANDO_OPCION_MENU | selecciona cotizar un servicio | PRECIOS | entregar precios | ESPERANDO_POST_PRECIO | BOT_ACTIVO | no todavia |
| ESPERANDO_POST_PRECIO | despues de enviar listado de precios  | PRECIOS-2 | con que paso seguir | CONDICIONAL | BOT_ACTIVO | no todavia |
| ESPERANDO_DETALLE_CONSULTA | despues de decir que tiene dudas o necesita asesoria | DUDAS | detalles de las consultas que tiene | FICHA_LISTA | BOT_ACTIVO → EN_MANO_HUMANA | Sí, con ficha lista |
| ESPERANDO_OPCION_MENU | selecciona necesita asesoria | ASESORIA SERVICIO | en que necesita asesoria | ESPERANDO_DETALLE_ASESORIA | BOT_ACTIVO | Sí, con ficha lista |
| ESPERANDO_OPCION_MENU | seleccióna informacion general | INFORMACION |  | ESPERANDO_OPCION_INFO | BOT_ACTIVO | no todavia |
| ESPERANDO_OPCION_INFO | selecciona dirección | DIRECCION | Ninguno adicional | ESPERANDO_OPCION_MENU | BOT_ACTIVO | no todavia |
| ESPERANDO_OPCION_INFO | selecciona medios de pago  | MEDIOS_PAGO | Ninguno adicional | ESPERANDO_OPCION_MENU | BOT_ACTIVO | no todavia |
| ESPERANDO_OPCION_INFO | selecciona días y horarios de atencion | HORARIOS_DIAS | Ninguno adicional | ESPERANDO_OPCION_MENU | BOT_ACTIVO | no todavia |
| ESPERANDO_OPCION_INFO | selecciona otro | OTRA_INFO | detalles de su consulta | ESPERANDO_DETALLE_OTRA_INFO | BOT_ACTIVO | Sí, con ficha lista |
| ESPERANDO_OPCION_MENU | selecciona modificar una cita | MODIFICAR_CITA | detalles de la cita a modificar | ESPERANDO_DETALLE_MODIFICAR_CITA | BOT_ACTIVO | Sí, con ficha lista |

## 20. Criterios de aceptación

```
- Al recibir "Hola", el bot responde menú principal y deja estado ESPERANDO_OPCION_MENU.
- Al elegir "Agendar una cita", responde lista de familias y deja estado ESPERANDO_FAMILIA_SERVICIO.
- Al elegir una familia, guarda service_family y avanza a detalle del servicio.
- Al recibir día y horario, guarda preferencias y genera ficha.
- Si se genera ficha, el bot cambia a FICHA_LISTA/EN_MANO_HUMANA y no vuelve a responder automáticamente.
- Si la conversación está en EN_MANO_HUMANA, cualquier mensaje entrante devuelve NO_REPLY.
- Make no envía nada si Kivor devuelve NO_REPLY.
- Make envía `whatsapp_payload_json` directo cuando Kivor devuelve interactive_list.
- No se confirma disponibilidad, precio final, agenda, cobro ni modificación automática.
```

## 21. Prompt recomendado para Claude Code

```
Lee este documento completo antes de modificar código.

Necesito implementar la máquina de estados del chatbot WhatsApp para Lindas y Lunáticas en Kivor.

Trabaja principalmente en:
- services/whatsapp_ai_service.py
- routes/whatsapp_ai.py si es necesario

Objetivo:
- El bot debe guiar por menú.
- Debe guardar estado y datos por teléfono.
- Debe generar ficha para Tania.
- Debe devolver text, interactive_list o NO_REPLY.
- Debe mantener a Make simple.
- No debe implementar IA libre.
- No debe agendar, cotizar en detalle, cobrar ni confirmar disponibilidad.

Antes de modificar archivos:
1. Revisa la estructura actual del proyecto.
2. Explica el plan de cambios.
3. Identifica tablas/columnas necesarias.
4. Luego implementa en cambios pequeños y probables.
```

## 22. Implementación por etapas

| Etapa | Objetivo | Resultado |
| --- | --- | --- |
| 1 | Crear/ajustar tabla de conversación y ficha. | Persistencia de estados por teléfono. |
| 2 | Implementar máquina de estados en Kivor. | /whatsapp/brain responde según estado. |
| 3 | Actualizar Make. | Rutas text, interactive_list y NO_REPLY funcionando. |
| 4 | Pruebas con número prepago. | Flujo piloto completo sin tocar número real de Tania. |
| 5 | Generar ficha para Tania. | Resumen operativo con datos disponibles. |
| 6 | Bandeja/PWA futura. | Tomar, cerrar, enviar abono, volver al bot. |
| 7 | Coexistencia número real de Tania. | Evaluar y habilitar sólo cuando piloto esté validado. |

## 23. Riesgos y controles

| Riesgo | Control |
| --- | --- |
| Bot responde mientras Tania conversa. | Estado EN_MANO_HUMANA + regla NO_REPLY. |
| Make rompe JSON de listas. | Kivor devuelve whatsapp_payload_json completo. |
| IA inventa información. | No usar IA libre; sólo clasificador opcional. |
| Se toca el número real de Tania antes de tiempo. | Mantener piloto con número prepago. |
| Se confirma una hora/precio incorrecto. | Regla explícita: bot no confirma disponibilidad ni cotiza en detalle. |
| Costos Meta cambian. | Revalidar pricing oficial antes de producción. |

## 24. Definición de listo para programar

El proyecto está listo para pasar a código cuando Claude tenga este documento dentro del repositorio Kivor, idealmente como `docs/especificacion_chatbot_whatsapp_tania.md`, y se ejecuten cambios en rama/control de versión. El primer commit debe concentrarse en persistencia de estados y respuesta por estado.
