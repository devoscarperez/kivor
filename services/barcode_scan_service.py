from datetime import datetime, timezone

from core.google_sheets import get_sheet_worksheet


def guardar_lectura_codigo_barra(codigo: str, hora_lectura_cliente: str, username: str) -> dict:
    hora_guardado = datetime.now(timezone.utc).isoformat()

    worksheet = get_sheet_worksheet()
    worksheet.append_row([codigo, hora_lectura_cliente, hora_guardado, username])

    return {
        "codigo": codigo,
        "hora_lectura_cliente": hora_lectura_cliente,
        "hora_guardado": hora_guardado,
    }
