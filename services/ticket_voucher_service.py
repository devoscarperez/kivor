# services/ticket_voucher_service.py

import unicodedata
from pathlib import Path

from fpdf import FPDF
from fpdf.enums import XPos, YPos
from psycopg import sql

from core.db import get_connection
from services.ticket_service import get_ticket

LOGO_PATH = Path(__file__).resolve().parent.parent / "assets" / "logo-lindasylunaticas.png"
SITIO_WEB = "www.lindasylunaticas.cl"


def _validated_schema(tenant_schema: str) -> str:
    if not tenant_schema or not tenant_schema.isidentifier():
        raise Exception("invalid_tenant")
    return tenant_schema


def sanitizar_nombre_archivo(texto: str) -> str:
    texto_normalizado = unicodedata.normalize("NFKD", texto or "").encode("ascii", "ignore").decode("ascii")
    texto_limpio = "".join(c if c.isalnum() else "_" for c in texto_normalizado)
    while "__" in texto_limpio:
        texto_limpio = texto_limpio.replace("__", "_")
    return texto_limpio.strip("_") or "clienta"


def formatear_moneda(valor) -> str:
    numero = int(round(float(valor or 0)))
    return f"${numero:,}".replace(",", ".")


def formatear_fecha_hora(valor) -> str:
    if not valor:
        return "-"
    return valor.strftime("%d-%m-%Y %H:%M")


def _obtener_bloques(tenant_schema: str, ticket_id: int) -> list:
    professionals_query = sql.SQL("""
        SELECT tp.id, p.professional_name, p.professional_nickname
        FROM {schema}.ticket_professional tp
        JOIN core.professional p ON p.professional_id = tp.professional_id
        WHERE tp.ticket_id = %s
        ORDER BY tp.created_at ASC
    """).format(schema=sql.Identifier(tenant_schema))

    bloques = []

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(professionals_query, (ticket_id,))
            professional_rows = cur.fetchall()

            professionals = {
                row[0]: (row[1] or row[2] or "(sin nombre)")
                for row in professional_rows
            }

            if not professionals:
                return bloques

            customers_query = sql.SQL("""
                SELECT tpc.id, tpc.ticket_professional_id, tpc.booking_date,
                       c.customer_full_name, c.customer_nickname
                FROM {schema}.ticket_professional_customer tpc
                JOIN {schema}.customer c ON c.customer_id = tpc.customer_id
                WHERE tpc.ticket_professional_id = ANY(%s)
                ORDER BY tpc.booking_date ASC
            """).format(schema=sql.Identifier(tenant_schema))

            cur.execute(customers_query, (list(professionals.keys()),))
            customer_rows = cur.fetchall()

            tpc_ids = [row[0] for row in customer_rows]
            products_por_tpc = {}

            if tpc_ids:
                products_query = sql.SQL("""
                    SELECT tpcp.ticket_professional_customer_id, tpcp.product_name,
                           tpcp.unit_price, pm.family, pm.level2, pm.level3, pm.level4
                    FROM {schema}.ticket_professional_customer_product tpcp
                    LEFT JOIN {schema}.product_master pm ON pm.id = tpcp.product_id
                    WHERE tpcp.ticket_professional_customer_id = ANY(%s)
                    ORDER BY tpcp.created_at ASC
                """).format(schema=sql.Identifier(tenant_schema))

                cur.execute(products_query, (tpc_ids,))
                for prow in cur.fetchall():
                    niveles = [nivel for nivel in (prow[4], prow[5], prow[6]) if nivel]
                    products_por_tpc.setdefault(prow[0], []).append({
                        "product_label": " · ".join(niveles) if niveles else prow[1],
                        "unit_price": prow[2],
                        "family": prow[3] or "OTROS",
                    })

            for row in customer_rows:
                tpc_id, ticket_professional_id, booking_date, customer_full_name, customer_nickname = row
                productos = products_por_tpc.get(tpc_id, [])

                familias = {}
                for p in productos:
                    familias.setdefault(p["family"], []).append(p)

                subtotal = sum(p["unit_price"] for p in productos)

                bloques.append({
                    "customer_name": customer_full_name or customer_nickname or "(sin nombre)",
                    "professional_name": professionals[ticket_professional_id],
                    "booking_date": booking_date,
                    "familias": familias,
                    "subtotal": subtotal,
                })

    return bloques


def generar_voucher_pdf(tenant_schema: str, ticket_id: int) -> tuple:
    tenant_schema = _validated_schema(tenant_schema)

    ticket = get_ticket(tenant_schema, ticket_id)
    bloques = _obtener_bloques(tenant_schema, ticket_id)

    total_general = sum(b["subtotal"] for b in bloques)
    abono = 0
    total_a_pagar = total_general - abono

    nombre_titular = ticket.get("customer_full_name") or ticket.get("customer_nickname") or "(sin nombre)"

    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    if LOGO_PATH.exists():
        pdf.image(str(LOGO_PATH), x=85, y=10, w=40)
        pdf.set_y(55)
    else:
        pdf.set_y(15)

    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 6, SITIO_WEB, align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(4)

    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 8, "COMPROBANTE DE ATENCION", align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(4)

    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 7, f"N Formulario (Ticket): {ticket['ticket_id']}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(0, 7, f"Fecha: {formatear_fecha_hora(ticket.get('ticket_booking_date'))}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(0, 7, f"Clienta: {nombre_titular}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(3)

    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(2)
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 7, "DETALLE DE SERVICIOS", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(4)

    for bloque in bloques:
        pdf.set_font("Helvetica", "", 11)
        pdf.cell(0, 6, f"Clienta: {bloque['customer_name']}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.cell(0, 6, f"Te atendio: {bloque['professional_name']}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.cell(0, 6, f"Atencion: {formatear_fecha_hora(bloque['booking_date'])}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.ln(2)

        for familia, servicios in bloque["familias"].items():
            pdf.set_font("Helvetica", "B", 10)
            pdf.cell(0, 6, f"  {familia}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            pdf.set_font("Helvetica", "", 10)
            for servicio in servicios:
                pdf.cell(140, 6, f"    {servicio['product_label']}")
                pdf.cell(50, 6, formatear_moneda(servicio["unit_price"]), align="R", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        pdf.ln(1)
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(140, 6, f"  Sub Total {bloque['customer_name']}")
        pdf.cell(50, 6, formatear_moneda(bloque["subtotal"]), align="R", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.ln(2)
        pdf.line(10, pdf.get_y(), 200, pdf.get_y())
        pdf.ln(4)

    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(140, 7, f"TOTAL GENERAL (Ticket #{ticket['ticket_id']})")
    pdf.cell(50, 7, formatear_moneda(total_general), align="R", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(140, 7, "Abono")
    pdf.cell(50, 7, formatear_moneda(abono), align="R", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(140, 7, "TOTAL A PAGAR")
    pdf.cell(50, 7, formatear_moneda(total_a_pagar), align="R", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    pdf_bytes = bytes(pdf.output())

    nombre_clienta_slug = sanitizar_nombre_archivo(nombre_titular)
    booking = ticket.get("ticket_booking_date")
    booking_slug = booking.strftime("%Y%m%d-%H%M") if booking else "sinfecha"
    nombre_archivo = f"{nombre_clienta_slug}_{ticket['ticket_id']}_{booking_slug}.pdf"

    return pdf_bytes, nombre_archivo
