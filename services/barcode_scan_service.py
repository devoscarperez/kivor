from datetime import datetime, timezone

from core.google_sheets import get_sheet_worksheet


def guardar_lectura_codigo_barra(codigo: str, hora_lectura_cliente: str, username: str) -> dict:
    hora_guardado = datetime.now(timezone.utc).isoformat()

    worksheet = get_sheet_worksheet()
    respuesta = worksheet.append_row([codigo, hora_lectura_cliente, hora_guardado, username])
    fila = _extraer_numero_fila(respuesta)

    return {
        "codigo": codigo,
        "hora_lectura_cliente": hora_lectura_cliente,
        "hora_guardado": hora_guardado,
        "fila": fila,
    }


def _extraer_numero_fila(respuesta_append: dict) -> int | None:
    try:
        rango = respuesta_append["updates"]["updatedRange"]
        celda_inicial = rango.split("!")[1].split(":")[0]
        return int("".join(filtro for filtro in celda_inicial if filtro.isdigit()))
    except (KeyError, IndexError, ValueError):
        return None


def guardar_duracion_lectura(fila: int, duracion_ms: int) -> dict:
    worksheet = get_sheet_worksheet()
    worksheet.update_cell(fila, 5, duracion_ms)

    return {"fila": fila, "duracion_ms": duracion_ms}
