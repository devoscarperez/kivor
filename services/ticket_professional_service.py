# services/ticket_professional_service.py

from datetime import date, datetime

from psycopg import sql

from core.db import get_connection
from services.precios_servicios_service import VIGENCIA_ACTUAL_TEMPORAL

DISCOUNT_PREFIX = "DESC_"
DISCOUNT_INDICATOR = "D"


def _validated_schema(tenant_schema: str) -> str:
    if not tenant_schema or not tenant_schema.isidentifier():
        raise Exception("invalid_tenant")
    return tenant_schema


def list_ticket_professionals(tenant_schema: str, ticket_id: int) -> list:
    tenant_schema = _validated_schema(tenant_schema)

    professionals_query = sql.SQL("""
        SELECT tp.id, tp.professional_id,
               p.professional_name, p.professional_nickname
        FROM {schema}.ticket_professional tp
        JOIN core.professional p ON p.professional_id = tp.professional_id
        WHERE tp.ticket_id = %s
        ORDER BY tp.created_at ASC
    """).format(schema=sql.Identifier(tenant_schema))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(professionals_query, (ticket_id,))
            professional_rows = cur.fetchall()

            professionals = [
                {
                    "ticket_professional_id": row[0],
                    "professional_id": row[1],
                    "professional_name": row[2],
                    "professional_nickname": row[3],
                    "customers": [],
                }
                for row in professional_rows
            ]

            ticket_professional_ids = [p["ticket_professional_id"] for p in professionals]

            if ticket_professional_ids:
                customers_query = sql.SQL("""
                    SELECT tpc.ticket_professional_id, tpc.id,
                           tpc.customer_id, c.customer_full_name, c.customer_nickname,
                           tpc.status,
                           tpc.booking_date
                    FROM {schema}.ticket_professional_customer tpc
                    JOIN {schema}.customer c ON c.customer_id = tpc.customer_id
                    WHERE tpc.ticket_professional_id = ANY(%s)
                    ORDER BY tpc.created_at ASC
                """).format(schema=sql.Identifier(tenant_schema))

                cur.execute(customers_query, (ticket_professional_ids,))
                customer_rows = cur.fetchall()

                by_ticket_professional = {}
                for row in customer_rows:
                    by_ticket_professional.setdefault(row[0], []).append({
                        "ticket_professional_customer_id": row[1],
                        "customer_id": row[2],
                        "customer_full_name": row[3],
                        "customer_nickname": row[4],
                        "ticket_professional_customer_status": row[5],
                        "ticket_professional_customer_booking_date": row[6],
                    })

                for professional in professionals:
                    professional["customers"] = by_ticket_professional.get(
                        professional["ticket_professional_id"], []
                    )

    return professionals


def add_ticket_professional(tenant_schema: str, ticket_id: int, professional_id: int) -> dict:
    tenant_schema = _validated_schema(tenant_schema)

    query = sql.SQL("""
        INSERT INTO {schema}.ticket_professional (ticket_id, professional_id)
        VALUES (%s, %s)
        ON CONFLICT (ticket_id, professional_id)
        DO UPDATE SET updated_at = NOW()
        RETURNING id, professional_id
    """).format(schema=sql.Identifier(tenant_schema))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (ticket_id, professional_id))
            row = cur.fetchone()
        conn.commit()

    return {"ticket_professional_id": row[0], "professional_id": row[1]}


def remove_ticket_professional(tenant_schema: str, ticket_id: int, ticket_professional_id: int) -> dict:
    tenant_schema = _validated_schema(tenant_schema)

    has_products_query = sql.SQL("""
        SELECT EXISTS(
            SELECT 1
            FROM {schema}.ticket_professional_customer_product tpcp
            JOIN {schema}.ticket_professional_customer tpc
                ON tpc.id = tpcp.ticket_professional_customer_id
            WHERE tpc.ticket_professional_id = %s
        )
    """).format(schema=sql.Identifier(tenant_schema))

    delete_customers_query = sql.SQL("""
        DELETE FROM {schema}.ticket_professional_customer
        WHERE ticket_professional_id = %s
    """).format(schema=sql.Identifier(tenant_schema))

    delete_professional_query = sql.SQL("""
        DELETE FROM {schema}.ticket_professional
        WHERE id = %s AND ticket_id = %s
        RETURNING id
    """).format(schema=sql.Identifier(tenant_schema))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(has_products_query, (ticket_professional_id,))
            if cur.fetchone()[0]:
                raise Exception(
                    "No se puede quitar la profesional: tiene clientas con servicios asociados. "
                    "Elimina esos servicios primero."
                )

            cur.execute(delete_customers_query, (ticket_professional_id,))
            cur.execute(delete_professional_query, (ticket_professional_id, ticket_id))
            row = cur.fetchone()
        conn.commit()

    if not row:
        raise Exception("ticket_professional_not_found")

    return {"ticket_professional_id": ticket_professional_id}


def add_ticket_professional_customer(
    tenant_schema: str, ticket_id: int, ticket_professional_id: int, customer_id: int, booking_date: datetime
) -> dict:
    tenant_schema = _validated_schema(tenant_schema)

    query = sql.SQL("""
        INSERT INTO {schema}.ticket_professional_customer
            (ticket_id, ticket_professional_id, customer_id, status, booking_date)
        VALUES (%s, %s, %s, 'Assigned', %s)
        ON CONFLICT (ticket_professional_id, customer_id)
        DO UPDATE SET updated_at = NOW()
        RETURNING id, customer_id, status, booking_date
    """).format(schema=sql.Identifier(tenant_schema))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (ticket_id, ticket_professional_id, customer_id, booking_date))
            row = cur.fetchone()
        conn.commit()

    return {
        "ticket_professional_customer_id": row[0],
        "customer_id": row[1],
        "ticket_professional_customer_status": row[2],
        "ticket_professional_customer_booking_date": row[3],
    }


def set_ticket_professional_customer_booking_date(
    tenant_schema: str, ticket_professional_customer_id: int, booking_date: datetime
) -> dict:
    tenant_schema = _validated_schema(tenant_schema)

    query = sql.SQL("""
        UPDATE {schema}.ticket_professional_customer
        SET booking_date = %s,
            updated_at = NOW()
        WHERE id = %s
        RETURNING id, customer_id, status, booking_date
    """).format(schema=sql.Identifier(tenant_schema))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (booking_date, ticket_professional_customer_id))
            row = cur.fetchone()
        conn.commit()

    if not row:
        raise Exception("ticket_professional_customer_not_found")

    return {
        "ticket_professional_customer_id": row[0],
        "customer_id": row[1],
        "ticket_professional_customer_status": row[2],
        "ticket_professional_customer_booking_date": row[3],
    }


def remove_ticket_professional_customer(tenant_schema: str, ticket_professional_customer_id: int) -> dict:
    tenant_schema = _validated_schema(tenant_schema)

    has_products_query = sql.SQL("""
        SELECT EXISTS(
            SELECT 1
            FROM {schema}.ticket_professional_customer_product
            WHERE ticket_professional_customer_id = %s
        )
    """).format(schema=sql.Identifier(tenant_schema))

    query = sql.SQL("""
        DELETE FROM {schema}.ticket_professional_customer
        WHERE id = %s
        RETURNING id
    """).format(schema=sql.Identifier(tenant_schema))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(has_products_query, (ticket_professional_customer_id,))
            if cur.fetchone()[0]:
                raise Exception(
                    "No se puede quitar la clienta: tiene servicios asociados. "
                    "Elimina esos servicios primero."
                )

            cur.execute(query, (ticket_professional_customer_id,))
            row = cur.fetchone()
        conn.commit()

    if not row:
        raise Exception("ticket_professional_customer_not_found")

    return {"ticket_professional_customer_id": ticket_professional_customer_id}


def complete_ticket_professional_customer(tenant_schema: str, ticket_professional_customer_id: int) -> dict:
    tenant_schema = _validated_schema(tenant_schema)

    query = sql.SQL("""
        UPDATE {schema}.ticket_professional_customer
        SET status = 'Completed',
            changed_at = NOW(),
            updated_at = NOW()
        WHERE id = %s AND status = 'Assigned'
        RETURNING id, customer_id, status, booking_date, ticket_id
    """).format(schema=sql.Identifier(tenant_schema))

    # Si esta era la ultima atencion pendiente del ticket (todas sus
    # profesionales asignadas tienen ya todas sus atenciones en Completed),
    # el ticket pasa automaticamente a Completed en la misma transaccion.
    complete_ticket_query = sql.SQL("""
        UPDATE {schema}.ticket t
        SET status = 'Completed',
            status_changed_at = NOW(),
            updated_at = NOW()
        WHERE t.id = %s
          AND t.status = 'Started'
          AND EXISTS (
              SELECT 1 FROM {schema}.ticket_professional tp WHERE tp.ticket_id = t.id
          )
          AND NOT EXISTS (
              SELECT 1 FROM {schema}.ticket_professional tp
              WHERE tp.ticket_id = t.id
                AND NOT EXISTS (
                    SELECT 1 FROM {schema}.ticket_professional_customer tpc
                    WHERE tpc.ticket_professional_id = tp.id AND tpc.status = 'Completed'
                )
          )
    """).format(schema=sql.Identifier(tenant_schema))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (ticket_professional_customer_id,))
            row = cur.fetchone()

            if row:
                cur.execute(complete_ticket_query, (row[4],))
        conn.commit()

    if not row:
        raise Exception(
            "No se pudo finalizar la atención: la clienta no existe o ya no está en estado Assigned."
        )

    return {
        "ticket_professional_customer_id": row[0],
        "customer_id": row[1],
        "ticket_professional_customer_status": row[2],
        "ticket_professional_customer_booking_date": row[3],
    }


def list_my_agenda_today(tenant_schema: str, professional_id: int, today: date) -> list:
    tenant_schema = _validated_schema(tenant_schema)

    rows_query = sql.SQL("""
        SELECT tpc.id, tpc.ticket_professional_id, tpc.customer_id,
               c.customer_full_name, c.customer_nickname,
               tpc.booking_date, tpc.ticket_id, tpc.status, t.status
        FROM {schema}.ticket_professional_customer tpc
        JOIN {schema}.ticket_professional tp ON tp.id = tpc.ticket_professional_id
        JOIN {schema}.ticket t ON t.id = tpc.ticket_id
        JOIN {schema}.customer c ON c.customer_id = tpc.customer_id
        WHERE tp.professional_id = %s
          AND t.booking_date::date = %s
          AND t.status = ANY(%s)
        ORDER BY CASE WHEN t.status = 'Started' THEN 0 ELSE 1 END,
                 CASE WHEN tpc.status = 'Assigned' THEN 0 ELSE 1 END,
                 tpc.booking_date ASC
    """).format(schema=sql.Identifier(tenant_schema))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(rows_query, (professional_id, today, ["Started", "Completed"]))
            rows = cur.fetchall()

            items = [
                {
                    "ticket_professional_customer_id": row[0],
                    "ticket_professional_id": row[1],
                    "customer_id": row[2],
                    "customer_full_name": row[3],
                    "customer_nickname": row[4],
                    "booking_date": row[5],
                    "ticket_id": row[6],
                    "ticket_professional_customer_status": row[7],
                    "ticket_status": row[8],
                    "products": [],
                }
                for row in rows
            ]

            tpc_ids = [item["ticket_professional_customer_id"] for item in items]

            if tpc_ids:
                products_query = sql.SQL("""
                    SELECT tpcp.ticket_professional_customer_id, tpcp.id,
                           tpcp.product_id, tpcp.product_name, tpcp.quantity,
                           tpcp.unit_price, tpcp.professional_unit_price,
                           pm.level2, pm.level3, pm.level4
                    FROM {schema}.ticket_professional_customer_product tpcp
                    LEFT JOIN {schema}.product_master pm ON pm.id = tpcp.product_id
                    WHERE tpcp.ticket_professional_customer_id = ANY(%s)
                    ORDER BY tpcp.created_at ASC
                """).format(schema=sql.Identifier(tenant_schema))

                cur.execute(products_query, (tpc_ids,))
                product_rows = cur.fetchall()

                by_tpc = {}
                for prow in product_rows:
                    niveles = [nivel for nivel in (prow[7], prow[8], prow[9]) if nivel]
                    by_tpc.setdefault(prow[0], []).append({
                        "ticket_professional_customer_product_id": prow[1],
                        "product_id": prow[2],
                        "product_name": prow[3],
                        "product_label": " · ".join(niveles) if niveles else prow[3],
                        "quantity": prow[4],
                        "unit_price": prow[5],
                        "professional_unit_price": prow[6],
                    })

                for item in items:
                    item["products"] = by_tpc.get(item["ticket_professional_customer_id"], [])

            for item in items:
                product_ids_existentes = {p["product_id"] for p in item["products"]}
                item["descuento_disponible"] = False

                for product_id in product_ids_existentes:
                    candidato = _resolver_servicio_descuento(cur, tenant_schema, product_id)
                    if candidato and candidato["service_master_id"] not in product_ids_existentes:
                        item["descuento_disponible"] = True
                        break

    return items


def add_ticket_professional_customer_product(
    tenant_schema: str,
    ticket_id: int,
    ticket_professional_id: int,
    ticket_professional_customer_id: int,
    product_id: int,
    product_name: str,
    unit_price: float,
    professional_unit_price: float,
    quantity: float = 1,
) -> dict:
    tenant_schema = _validated_schema(tenant_schema)

    status_query = sql.SQL("""
        SELECT status FROM {schema}.ticket_professional_customer WHERE id = %s
    """).format(schema=sql.Identifier(tenant_schema))

    query = sql.SQL("""
        INSERT INTO {schema}.ticket_professional_customer_product
            (ticket_id, ticket_professional_id, ticket_professional_customer_id,
             product_id, product_name, quantity, unit_price, professional_unit_price)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id, product_id, product_name,
                  quantity, unit_price, professional_unit_price
    """).format(schema=sql.Identifier(tenant_schema))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(status_query, (ticket_professional_customer_id,))
            status_row = cur.fetchone()

            if not status_row:
                raise Exception("ticket_professional_customer_not_found")
            if status_row[0] != "Assigned":
                raise Exception(
                    "No se pueden agregar servicios: la atención de esta clienta ya fue finalizada."
                )

            cur.execute(query, (
                ticket_id, ticket_professional_id, ticket_professional_customer_id,
                product_id, product_name, quantity, unit_price, professional_unit_price,
            ))
            row = cur.fetchone()
        conn.commit()

    return {
        "ticket_professional_customer_product_id": row[0],
        "product_id": row[1],
        "product_name": row[2],
        "quantity": row[3],
        "unit_price": row[4],
        "professional_unit_price": row[5],
    }


def _resolver_servicio_descuento(cur, tenant_schema: str, product_id: int) -> dict | None:
    """Si product_id tiene id_discount = 'D', busca el servicio con el mismo
    nivel (familia/nivel_2/nivel_3/nivel_4) prefijado con DESC_. Devuelve sus
    datos para poder agregarlo como producto, o None si no aplica."""
    indicador_query = sql.SQL("""
        SELECT id_discount FROM {schema}.precios_servicios
        WHERE service_master_id = %s AND vigencia = %s
    """).format(schema=sql.Identifier(tenant_schema))

    cur.execute(indicador_query, (product_id, VIGENCIA_ACTUAL_TEMPORAL))
    indicador_row = cur.fetchone()

    if not indicador_row or (indicador_row[0] or "").strip().upper() != DISCOUNT_INDICATOR:
        return None

    niveles_query = sql.SQL("""
        SELECT family, level2, level3, level4 FROM {schema}.product_master WHERE id = %s
    """).format(schema=sql.Identifier(tenant_schema))

    cur.execute(niveles_query, (product_id,))
    niveles_row = cur.fetchone()

    if not niveles_row:
        return None

    def con_prefijo(valor):
        return f"{DISCOUNT_PREFIX}{valor}" if valor else valor

    familia_desc, nivel_2_desc, nivel_3_desc, nivel_4_desc = (con_prefijo(v) for v in niveles_row)

    descuento_query = sql.SQL("""
        SELECT service_master_id, clave, precio_lista, precio_profesional
        FROM {schema}.precios_servicios
        WHERE vigencia = %s AND servicio = true
          AND familia IS NOT DISTINCT FROM %s
          AND nivel_2 IS NOT DISTINCT FROM %s
          AND nivel_3 IS NOT DISTINCT FROM %s
          AND nivel_4 IS NOT DISTINCT FROM %s
        LIMIT 1
    """).format(schema=sql.Identifier(tenant_schema))

    cur.execute(descuento_query, (
        VIGENCIA_ACTUAL_TEMPORAL, familia_desc, nivel_2_desc, nivel_3_desc, nivel_4_desc,
    ))
    descuento_row = cur.fetchone()

    if not descuento_row:
        return None

    service_master_id, clave, precio_lista, precio_profesional = descuento_row

    return {
        "service_master_id": service_master_id,
        "clave": clave,
        "precio_lista": precio_lista or 0,
        "precio_profesional": precio_profesional or 0,
    }


def aplicar_descuento_ticket_professional_customer(
    tenant_schema: str, ticket_professional_customer_id: int
) -> dict:
    tenant_schema = _validated_schema(tenant_schema)

    info_query = sql.SQL("""
        SELECT ticket_id, ticket_professional_id, status
        FROM {schema}.ticket_professional_customer
        WHERE id = %s
    """).format(schema=sql.Identifier(tenant_schema))

    productos_query = sql.SQL("""
        SELECT product_id
        FROM {schema}.ticket_professional_customer_product
        WHERE ticket_professional_customer_id = %s
        ORDER BY created_at ASC
    """).format(schema=sql.Identifier(tenant_schema))

    insert_query = sql.SQL("""
        INSERT INTO {schema}.ticket_professional_customer_product
            (ticket_id, ticket_professional_id, ticket_professional_customer_id,
             product_id, product_name, quantity, unit_price, professional_unit_price)
        VALUES (%s, %s, %s, %s, %s, 1, %s, %s)
        RETURNING id, product_id, product_name, quantity, unit_price, professional_unit_price
    """).format(schema=sql.Identifier(tenant_schema))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(info_query, (ticket_professional_customer_id,))
            info_row = cur.fetchone()

            if not info_row:
                raise Exception("ticket_professional_customer_not_found")

            ticket_id, ticket_professional_id, status = info_row
            if status != "Assigned":
                raise Exception(
                    "No se puede aplicar el descuento: la atención de esta clienta ya fue finalizada."
                )

            cur.execute(productos_query, (ticket_professional_customer_id,))
            product_ids_existentes = {r[0] for r in cur.fetchall()}

            descuento = None
            for product_id in product_ids_existentes:
                candidato = _resolver_servicio_descuento(cur, tenant_schema, product_id)
                if candidato and candidato["service_master_id"] not in product_ids_existentes:
                    descuento = candidato
                    break

            if not descuento:
                raise Exception("No hay servicios pendientes de aplicar descuento en esta atención.")

            cur.execute(insert_query, (
                ticket_id, ticket_professional_id, ticket_professional_customer_id,
                descuento["service_master_id"], descuento["clave"],
                descuento["precio_lista"], descuento["precio_profesional"],
            ))
            row = cur.fetchone()
        conn.commit()

    return {
        "ticket_professional_customer_product_id": row[0],
        "product_id": row[1],
        "product_name": row[2],
        "quantity": row[3],
        "unit_price": row[4],
        "professional_unit_price": row[5],
    }


def remove_ticket_professional_customer_product(
    tenant_schema: str, ticket_professional_customer_product_id: int
) -> dict:
    tenant_schema = _validated_schema(tenant_schema)

    status_query = sql.SQL("""
        SELECT tpc.status
        FROM {schema}.ticket_professional_customer_product tpcp
        JOIN {schema}.ticket_professional_customer tpc ON tpc.id = tpcp.ticket_professional_customer_id
        WHERE tpcp.id = %s
    """).format(schema=sql.Identifier(tenant_schema))

    delete_query = sql.SQL("""
        DELETE FROM {schema}.ticket_professional_customer_product
        WHERE id = %s
        RETURNING id
    """).format(schema=sql.Identifier(tenant_schema))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(status_query, (ticket_professional_customer_product_id,))
            status_row = cur.fetchone()

            if not status_row:
                raise Exception("ticket_professional_customer_product_not_found")
            if status_row[0] != "Assigned":
                raise Exception(
                    "No se puede eliminar el servicio: la atención de esta clienta ya fue finalizada."
                )

            cur.execute(delete_query, (ticket_professional_customer_product_id,))
            row = cur.fetchone()
        conn.commit()

    return {"ticket_professional_customer_product_id": row[0]}


def get_customer_agenda(tenant_schema: str, customer_id: int, agenda_date: date) -> list:
    tenant_schema = _validated_schema(tenant_schema)

    query = sql.SQL("""
        SELECT tpc.booking_date,
               p.professional_name, p.professional_nickname,
               tpc.ticket_id
        FROM {schema}.ticket_professional_customer tpc
        JOIN {schema}.ticket_professional tp ON tp.id = tpc.ticket_professional_id
        JOIN core.professional p ON p.professional_id = tp.professional_id
        WHERE tpc.customer_id = %s
          AND tpc.booking_date::date = %s
        ORDER BY tpc.booking_date ASC
    """).format(schema=sql.Identifier(tenant_schema))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (customer_id, agenda_date))
            rows = cur.fetchall()

    return [
        {
            "booking_date": row[0],
            "professional_name": row[1],
            "professional_nickname": row[2],
            "ticket_id": row[3],
        }
        for row in rows
    ]
