# services/ticket_professional_service.py

from datetime import date, datetime

from psycopg import sql

from core.db import get_connection


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
        RETURNING id, customer_id, status, booking_date
    """).format(schema=sql.Identifier(tenant_schema))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (ticket_professional_customer_id,))
            row = cur.fetchone()
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
               tpc.booking_date, tpc.ticket_id, tpc.status
        FROM {schema}.ticket_professional_customer tpc
        JOIN {schema}.ticket_professional tp ON tp.id = tpc.ticket_professional_id
        JOIN {schema}.ticket t ON t.id = tpc.ticket_id
        JOIN {schema}.customer c ON c.customer_id = tpc.customer_id
        WHERE tp.professional_id = %s
          AND t.booking_date::date = %s
        ORDER BY tpc.booking_date ASC
    """).format(schema=sql.Identifier(tenant_schema))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(rows_query, (professional_id, today))
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
                    "products": [],
                }
                for row in rows
            ]

            tpc_ids = [item["ticket_professional_customer_id"] for item in items]

            if tpc_ids:
                products_query = sql.SQL("""
                    SELECT ticket_professional_customer_id, id,
                           product_id, product_name, quantity, unit_price, professional_unit_price
                    FROM {schema}.ticket_professional_customer_product
                    WHERE ticket_professional_customer_id = ANY(%s)
                    ORDER BY created_at ASC
                """).format(schema=sql.Identifier(tenant_schema))

                cur.execute(products_query, (tpc_ids,))
                product_rows = cur.fetchall()

                by_tpc = {}
                for prow in product_rows:
                    by_tpc.setdefault(prow[0], []).append({
                        "ticket_professional_customer_product_id": prow[1],
                        "product_id": prow[2],
                        "product_name": prow[3],
                        "quantity": prow[4],
                        "unit_price": prow[5],
                        "professional_unit_price": prow[6],
                    })

                for item in items:
                    item["products"] = by_tpc.get(item["ticket_professional_customer_id"], [])

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
