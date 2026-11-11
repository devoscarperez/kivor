# Diccionario de datos de Kivor para agentes de IA

Fecha de documentación: 17 de octubre de 2026.

## Fuente y alcance

Actualización: eliminación de `lindasylunaticas.professional` y nuevo DDL de `core.professional`, comunicados por el usuario el 17 de octubre de 2026 a las 17:25 (America/Santiago).

- Fuente base más actualización del usuario: `base de datos(1).sql` (69,051 bytes).
- SHA-256 del DDL consolidado usado para generar esta versión: `6987968589dda7da267545b09055926b3af7eab4eceed662a2ef7977ed50016f`.
- Inventario: **64 tablas, 4 esquemas, 635 columnas y 46 claves foráneas declaradas**.
- Este documento combina la exportación adjunta con la actualización expresa de professional. No confirma por conexión directa el estado de la base en ejecución. El tamaño indicado corresponde al texto consolidado.
- No ejecuta ni modifica la base. Los bloques SQL son evidencia del DDL exportado, no un script de migración ordenado.

## Instrucciones de interpretación para el agente

1. Usar siempre nombres calificados `esquema.tabla`; respetar las comillas del DDL en identificadores como `core."user"`, `core."group"` y `core."local"`.
2. No confundir tablas de nombres similares ni unir por coincidencia de nombre de columna. Las relaciones declaradas se enumeran expresamente.
3. No asumir que las tablas contienen datos, ni inferir valores de organización/negocio a partir de DEFAULT.
4. No sustituir tipos UUID por INTEGER por conocimiento de conversaciones o diseños posteriores. Este archivo es una instantánea.
5. Un DEFAULT temporal se aplica al insertar cuando se omite el campo; no demuestra actualización automática en UPDATE.
6. Un trigger mencionado no implica que su función esté incluida. Verificar los objetos ausentes antes de reconstruir la base.
7. Los campos calculados GENERATED ALWAYS AS (...) STORED son expresiones, no valores que deba enviar la aplicación.
8. Los permisos y comentarios conservados en el apéndice de cada tabla son parte de la fuente, no permisos adicionales concedidos por este documento.

## Límites y diferencias relevantes de la exportación

- Se incluyen las cuatro tablas del circuito de tickets en el esquema lindasylunaticas; consultar sus definiciones exactas más adelante.
- Los tipos de identificadores deben tomarse de cada definición individual; no asumir uniformidad entre maestros ni esquemas.
- El usuario confirmó la eliminación de lindasylunaticas.professional; el maestro vigente documentado es core.professional.
- También existen core.business y core.businesses, además de customer/customers/customers_express y varios catálogos de precios/servicios. No se presume equivalencia ni sustitución.
- No aparecen definiciones CREATE FUNCTION en el archivo; la función referenciada por el trigger de customer debe obtenerse aparte.
- No se incluye configuración de zona horaria ni prueba de instalación de pg_trgm; el índice gin_trgm_ops requiere esa capacidad.

## Observaciones de la exportación base y actualización

Las diferencias UUID/INTEGER en business, local_professional y professional_service reflejan que sus definiciones provienen de la exportación anterior a las migraciones reportadas en la conversación; no son un diagnóstico de la base actual. Obtener su DDL actualizado antes de usarlas. Las longitudes VARCHAR distintas y INTEGER/BIGINT no son necesariamente incompatibles. No usar este documento como restauración validada.

- `core.business`: `CONSTRAINT business_pkey null`. Declaración incompleta/no ejecutable como restricción SQL.
- `audit.prijs_audit.reason_type` (varchar(10)) → `"catalog".parameters."type"` (varchar(20)), restricción `fk_reason`. Diferencia de tipos que requiere verificación; no se corrige en esta documentación.
- `audit.prijs_audit.reason_code` (varchar(10)) → `"catalog".parameters.code` (varchar(20)), restricción `fk_reason`. Diferencia de tipos que requiere verificación; no se corrige en esta documentación.
- `core.organization.organization_type` (varchar(10)) → `"catalog".parameters."type"` (varchar(20)), restricción `fk_organization_type`. Diferencia de tipos que requiere verificación; no se corrige en esta documentación.
- `core.organization.organization_type_code` (varchar(10)) → `"catalog".parameters.code` (varchar(20)), restricción `fk_organization_type`. Diferencia de tipos que requiere verificación; no se corrige en esta documentación.
- `core.professional.business_id` (integer) → `core.business.business_id` (uuid), restricción `fk_professional_business_ud`. Diferencia de tipos que requiere verificación; no se corrige en esta documentación.
- `core.professional_skill.professional_skill_skill_id` (integer) → `core.skill.skill_id` (bigint), restricción `fk_professional_skill_skill`. Diferencia de tipos que requiere verificación; no se corrige en esta documentación.
- `core.local_professional.professional_id` (uuid) → `core.professional.professional_id` (integer), restricción `fk_local_professional_professional_id`. Diferencia de tipos que requiere verificación; no se corrige en esta documentación.
- `core.professional_service.professional_id` (uuid) → `core.professional.professional_id` (integer), restricción `fk_professional_service_professional`. Diferencia de tipos que requiere verificación; no se corrige en esta documentación.

## Convenciones PostgreSQL

| Forma en el DDL | Interpretación |
|---|---|
| int4 | INTEGER |
| int8 | BIGINT |
| int2 | SMALLINT |
| bool | BOOLEAN |
| serial4 / bigserial / smallserial | Entero con secuencia automática; no equivale a declaración IDENTITY. |
| timestamptz | TIMESTAMP WITH TIME ZONE; no conserva el nombre de zona original. |
| timestamp | TIMESTAMP WITHOUT TIME ZONE. |
| bpchar(n) | CHAR(n). |
| NULL | Campo admite nulos, salvo otra restricción aplicable. |

## Inventario por esquema

| Esquema | Tablas |
|---|---:|
| `audit` | 2 |
| `catalog` | 4 |
| `core` | 39 |
| `lindasylunaticas` | 19 |

### Índice de tablas

| Tabla calificada | Columnas | PK declarada |
|---|---:|---|
| `audit.user_audit` | 10 | CONSTRAINT user_audit_pkey PRIMARY KEY (user_audit_id) |
| `"catalog".pais` | 4 | CONSTRAINT pais_pkey PRIMARY KEY (id) |
| `"catalog".parameters` | 4 | CONSTRAINT parameters_pk PRIMARY KEY (type, code) |
| `"catalog".tmp_cut_chile` | 7 | No declarada en la fuente |
| `core.ai_clients` | 6 | CONSTRAINT ai_clients_pkey PRIMARY KEY (phone) |
| `core.ai_conversations` | 8 | CONSTRAINT ai_conversations_pkey PRIMARY KEY (conversation_id) |
| `core.business` | 4 | No declarada en la fuente |
| `core.businesses` | 4 | CONSTRAINT businesses_pkey PRIMARY KEY (businesses_id) |
| `core.customers_express_token_map` | 3 | CONSTRAINT customers_express_token_map_pkey PRIMARY KEY (token) |
| `core."group"` | 4 | CONSTRAINT group_pkey PRIMARY KEY (group_id) |
| `core."hour"` | 11 | CONSTRAINT hour_pkey PRIMARY KEY (hour_id) |
| `core.identifier_types` | 13 | CONSTRAINT identifier_types_pkey PRIMARY KEY (identifier_types_id) |
| `core."permission"` | 3 | CONSTRAINT permission_pkey PRIMARY KEY (permission_id) |
| `core.person` | 23 | CONSTRAINT person_pkey PRIMARY KEY (person_id) |
| `core.prices` | 12 | CONSTRAINT prijs_pkey PRIMARY KEY (servicekey) |
| `core."role"` | 4 | CONSTRAINT role_pkey PRIMARY KEY (role_id) |
| `core.service` | 7 | CONSTRAINT service_pkey PRIMARY KEY (service_template_id) |
| `core.service_template` | 7 | CONSTRAINT service_template_pkey PRIMARY KEY (service_template_id) |
| `core.skill` | 6 | CONSTRAINT skill_pkey PRIMARY KEY (skill_id) |
| `core.stg_ventas_lyl` | 36 | CONSTRAINT pk_stg_ventas_lyl PRIMARY KEY (ventas_key) |
| `core.template_hour` | 11 | CONSTRAINT template_hour_pkey PRIMARY KEY (template_hour_id) |
| `core.tenant` | 5 | CONSTRAINT tenant_pkey PRIMARY KEY (tenant_id) |
| `core.token_blacklist` | 2 | CONSTRAINT token_blacklist_pkey PRIMARY KEY (jti) |
| `core.user_session` | 8 | CONSTRAINT user_session_pkey PRIMARY KEY (session_id) |
| `lindasylunaticas.ai_whatsapp_conversation` | 25 | CONSTRAINT ai_whatsapp_conversation_pkey PRIMARY KEY (phone) |
| `lindasylunaticas.ai_whatsapp_state` | 5 | CONSTRAINT ai_whatsapp_state_pkey PRIMARY KEY (phone) |
| `lindasylunaticas.customer` | 27 | CONSTRAINT customer_pkey PRIMARY KEY (customer_id) |
| `lindasylunaticas.customer_capture_settings` | 13 | CONSTRAINT customer_capture_settings_pkey PRIMARY KEY (customer_capture_settings_id) |
| `lindasylunaticas.customer_identifiers` | 6 | CONSTRAINT customer_identifiers_pkey PRIMARY KEY (customer_identifiers_id) |
| `lindasylunaticas.customers` | 27 | CONSTRAINT customers_pkey PRIMARY KEY (customers_id) |
| `lindasylunaticas.customers_express` | 11 | No declarada en la fuente |
| `lindasylunaticas.field_type_settings` | 6 | CONSTRAINT identifier_type_settings_pkey PRIMARY KEY (identifier_type_settings_code) |
| `lindasylunaticas.precios_servicios` | 15 | CONSTRAINT pk_precios_servicios PRIMARY KEY (clave) |
| `lindasylunaticas.price` | 11 | No declarada en la fuente |
| `lindasylunaticas.prices` | 11 | No declarada en la fuente |
| `lindasylunaticas.sales` | 36 | No declarada en la fuente |
| `audit.prijs_audit` | 22 | CONSTRAINT prijs_audit_pkey PRIMARY KEY (id) |
| `"catalog".division_territorial` | 8 | CONSTRAINT division_territorial_pkey PRIMARY KEY (id) |
| `core.business_theme` | 14 | CONSTRAINT business_theme_pkey PRIMARY KEY (business_theme_id) |
| `core.group_role` | 2 | CONSTRAINT group_role_pkey PRIMARY KEY (group_id, role_id) |
| `core."local"` | 4 | CONSTRAINT local_pkey PRIMARY KEY (local_id) |
| `core.menu` | 9 | CONSTRAINT menu_pkey PRIMARY KEY (menu_id) |
| `core.organization` | 8 | CONSTRAINT organization_pkey PRIMARY KEY (organization_id) |
| `core.person_organization` | 7 | CONSTRAINT person_organization_pkey PRIMARY KEY (person_organization_id) |
| `core.professional` | 6 | CONSTRAINT professional_pkey PRIMARY KEY (professional_id) |
| `core.professional_profile` | 24 | CONSTRAINT professional_profile_pkey PRIMARY KEY (professional_profile_id) |
| `core.professional_skill` | 8 | CONSTRAINT professional_skill_pkey PRIMARY KEY (professional_skill_id) |
| `core.role_menu` | 2 | CONSTRAINT role_menu_pkey PRIMARY KEY (role_id, menu_id) |
| `core.role_permission` | 2 | CONSTRAINT role_permission_pkey PRIMARY KEY (role_id, permission_id) |
| `core.service_catalog` | 14 | CONSTRAINT service_catalog_pkey PRIMARY KEY (template_catalog_node_id) |
| `core.service_template_catalog` | 14 | CONSTRAINT service_template_catalog_pkey PRIMARY KEY (template_catalog_node_id) |
| `core.theme` | 7 | CONSTRAINT theme_pkey PRIMARY KEY (theme_id) |
| `core."user"` | 9 | CONSTRAINT user_pkey PRIMARY KEY (user_id) |
| `lindasylunaticas.product_family` | 6 | CONSTRAINT product_family_pkey PRIMARY KEY (product_family_id) |
| `lindasylunaticas.product_subfamily` | 6 | CONSTRAINT product_subfamily_pkey PRIMARY KEY (product_subfamily_id) |
| `lindasylunaticas.ticket` | 6 | CONSTRAINT ticket_pkey PRIMARY KEY (ticket_id) |
| `lindasylunaticas.ticket_professional` | 6 | CONSTRAINT ticket_professional_pkey PRIMARY KEY (ticket_professional_id) |
| `lindasylunaticas.ticket_professional_customer` | 7 | CONSTRAINT ticket_professional_customer_pkey PRIMARY KEY (ticket_professional_customer_id) |
| `core.local_professional` | 4 | CONSTRAINT local_professional_id_pkey PRIMARY KEY (local_professional_id) |
| `core.professional_certification` | 8 | CONSTRAINT professional_certification_pkey PRIMARY KEY (professional_certification_id) |
| `core.professional_location` | 9 | CONSTRAINT professional_location_pkey PRIMARY KEY (professional_location_id) |
| `core.professional_service` | 8 | CONSTRAINT professional_service_pkey PRIMARY KEY (professional_service_id) |
| `lindasylunaticas.product` | 11 | CONSTRAINT product_pkey PRIMARY KEY (product_id) |
| `lindasylunaticas.ticket_professional_customer_product` | 9 | CONSTRAINT ticket_professional_customer_product_pkey PRIMARY KEY (ticket_professional_customer_product_id) |

## Relaciones declaradas

| Origen | Columnas | Destino | Columnas destino | Restricción y opciones |
|---|---|---|---|---|
| `audit.prijs_audit` | `reason_type,reason_code` | `"catalog".parameters` | `"type",code` | `fk_reason`  |
| `"catalog".division_territorial` | `pais_id` | `"catalog".pais` | `id` | `division_territorial_pais_id_fkey`  |
| `"catalog".division_territorial` | `parent_id` | `"catalog".division_territorial` | `id` | `fk_division_parent`  |
| `core.business_theme` | `business_theme_businesses_id` | `core.businesses` | `businesses_id` | `fk_business_theme_business`  |
| `core.group_role` | `group_id` | `core."group"` | `group_id` | `group_role_group_id_fkey` ON DELETE CASCADE |
| `core.group_role` | `role_id` | `core."role"` | `role_id` | `group_role_role_id_fkey` ON DELETE CASCADE |
| `core."local"` | `business_id` | `core.business` | `business_id` | `fk_local_business_id`  |
| `core.menu` | `menu_parent_id` | `core.menu` | `menu_id` | `fk_menu_parent` ON DELETE CASCADE |
| `core.organization` | `organization_tenant_id` | `core.tenant` | `tenant_id` | `fk_organization_tenant`  |
| `core.organization` | `organization_type,organization_type_code` | `"catalog".parameters` | `"type",code` | `fk_organization_type`  |
| `core.organization` | `organization_parent_id` | `core.organization` | `organization_id` | `organization_organization_parent_id_fkey`  |
| `core.person_organization` | `person_organization_organization_id` | `core.organization` | `organization_id` | `fk_person_organization_organization`  |
| `core.person_organization` | `person_organization_person_id` | `core.person` | `person_id` | `fk_person_organization_person`  |
| `core.professional` | `business_id` | `core.business` | `business_id` | `fk_professional_business_ud`  |
| `core.professional` | `organization_id` | `core.organization` | `organization_id` | `fk_professional_organization`  |
| `core.professional_profile` | `professional_profile_person_id` | `core.person` | `person_id` | `fk_professional_profile_person`  |
| `core.professional_skill` | `professional_skill_professional_profile_id` | `core.professional_profile` | `professional_profile_id` | `fk_professional_skill_profile`  |
| `core.professional_skill` | `professional_skill_skill_id` | `core.skill` | `skill_id` | `fk_professional_skill_skill`  |
| `core.role_menu` | `menu_id` | `core.menu` | `menu_id` | `role_menu_menu_id_fkey` ON DELETE CASCADE |
| `core.role_menu` | `role_id` | `core."role"` | `role_id` | `role_menu_role_id_fkey` ON DELETE CASCADE |
| `core.role_permission` | `permission_id` | `core."permission"` | `permission_id` | `role_permission_permission_id_fkey` ON DELETE CASCADE |
| `core.service_catalog` | `service_template_id` | `core.service` | `service_template_id` | `fk_service_catalog`  |
| `core.service_catalog` | `parent_node_id` | `core.service_catalog` | `template_catalog_node_id` | `fk_service_catalog_parent`  |
| `core.service_template_catalog` | `parent_node_id` | `core.service_template_catalog` | `template_catalog_node_id` | `fk_template_catalog_parent`  |
| `core.service_template_catalog` | `service_template_id` | `core.service_template` | `service_template_id` | `fk_template_catalog_template`  |
| `core.theme` | `theme_tenant_id` | `core.tenant` | `tenant_id` | `fk_theme` ON DELETE CASCADE |
| `core."user"` | `user_group_id` | `core."group"` | `group_id` | `user_user_group_id_fkey` ON DELETE RESTRICT |
| `lindasylunaticas.product_family` | `organization_id` | `core.organization` | `organization_id` | `fk_product_family_organization`  |
| `lindasylunaticas.product_subfamily` | `product_family_id` | `lindasylunaticas.product_family` | `product_family_id` | `fk_product_subfamily_family`  |
| `lindasylunaticas.ticket` | `organization_id` | `core.organization` | `organization_id` | `fk_ticket_organization`  |
| `lindasylunaticas.ticket_professional` | `professional_id` | `core.professional` | `professional_id` | `fk_tp_professional`  |
| `lindasylunaticas.ticket_professional` | `ticket_id` | `lindasylunaticas.ticket` | `ticket_id` | `fk_tp_ticket`  |
| `lindasylunaticas.ticket_professional_customer` | `customer_id` | `lindasylunaticas.customer` | `customer_id` | `fk_tpc_customer`  |
| `lindasylunaticas.ticket_professional_customer` | `ticket_professional_id` | `lindasylunaticas.ticket_professional` | `ticket_professional_id` | `fk_tpc_ticket_professional`  |
| `core.local_professional` | `business_id` | `core.business` | `business_id` | `fk_local_professional_business_id`  |
| `core.local_professional` | `local_id` | `core."local"` | `local_id` | `fk_local_professional_local_id`  |
| `core.local_professional` | `professional_id` | `core.professional` | `professional_id` | `fk_local_professional_professional_id`  |
| `core.professional_certification` | `professional_certification_professional_profile_id` | `core.professional_profile` | `professional_profile_id` | `fk_professional_certification_profile`  |
| `core.professional_location` | `professional_location_professional_profile_id` | `core.professional_profile` | `professional_profile_id` | `fk_professional_location_profile`  |
| `core.professional_service` | `business_id` | `core.business` | `business_id` | `fk_professional_service_business`  |
| `core.professional_service` | `service_catalog_node_id` | `core.service_catalog` | `template_catalog_node_id` | `fk_professional_service_catalog`  |
| `core.professional_service` | `local_id` | `core."local"` | `local_id` | `fk_professional_service_local`  |
| `core.professional_service` | `professional_id` | `core.professional` | `professional_id` | `fk_professional_service_professional`  |
| `lindasylunaticas.product` | `product_subfamily_id` | `lindasylunaticas.product_subfamily` | `product_subfamily_id` | `fk_product_subfamily`  |
| `lindasylunaticas.ticket_professional_customer_product` | `ticket_professional_customer_id` | `lindasylunaticas.ticket_professional_customer` | `ticket_professional_customer_id` | `fk_tpcp_attention`  |
| `lindasylunaticas.ticket_professional_customer_product` | `product_id` | `lindasylunaticas.product` | `product_id` | `fk_tpcp_product`  |

## Esquema `audit`

### Tabla `audit.user_audit`

- Esquema: `audit`.
- Nombre sin esquema: `user_audit`.
- Número de columnas: 10.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `user_audit_id` | `serial4` | No | NOT NULL |
| `user_audit_user_id` | `int4` | No | NOT NULL |
| `user_audit_login_time` | `timestamp` | No | DEFAULT CURRENT_TIMESTAMP NOT NULL |
| `user_audit_logout_time` | `timestamp` | Sí | NULL |
| `user_audit_source_ip` | `inet` | Sí | NULL |
| `user_audit_source_host` | `text` | Sí | NULL |
| `user_audit_access_method` | `varchar(50)` | Sí | NULL |
| `user_audit_client_info` | `text` | Sí | NULL |
| `user_audit_login_success` | `bool` | No | DEFAULT true NOT NULL |
| `user_audit_failure_reason` | `text` | Sí | NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT user_audit_pkey PRIMARY KEY (user_audit_id)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE audit.user_audit (
	user_audit_id serial4 NOT NULL,
	user_audit_user_id int4 NOT NULL,
	user_audit_login_time timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	user_audit_logout_time timestamp NULL,
	user_audit_source_ip inet NULL,
	user_audit_source_host text NULL,
	user_audit_access_method varchar(50) NULL,
	user_audit_client_info text NULL,
	user_audit_login_success bool DEFAULT true NOT NULL,
	user_audit_failure_reason text NULL,
	CONSTRAINT user_audit_pkey PRIMARY KEY (user_audit_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
CREATE INDEX idx_user_audit_user_time ON audit.user_audit USING btree (user_audit_user_id, user_audit_login_time DESC);

-- Permissions

ALTER TABLE audit.user_audit OWNER TO adminboek;
GRANT ALL ON TABLE audit.user_audit TO adminboek;
```

### Tabla `audit.prijs_audit`

- Esquema: `audit`.
- Nombre sin esquema: `prijs_audit`.
- Número de columnas: 22.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `id` | `bigserial` | No | NOT NULL |
| `servicekey` | `varchar(200)` | No | NOT NULL |
| `"family"` | `varchar(50)` | Sí | NULL |
| `level2` | `varchar(50)` | Sí | NULL |
| `level3` | `varchar(50)` | Sí | NULL |
| `level4` | `varchar(50)` | Sí | NULL |
| `old_listprice` | `numeric(10, 2)` | Sí | NULL |
| `new_listprice` | `numeric(10, 2)` | Sí | NULL |
| `old_professionalprice` | `numeric(10, 2)` | Sí | NULL |
| `new_professionalprice` | `numeric(10, 2)` | Sí | NULL |
| `old_professionalpercentage` | `numeric(6, 2)` | Sí | NULL |
| `new_professionalpercentage` | `numeric(6, 2)` | Sí | NULL |
| `old_salonpercentage` | `numeric(6, 2)` | Sí | NULL |
| `new_salonpercentage` | `numeric(6, 2)` | Sí | NULL |
| `reason_type` | `varchar(10)` | No | DEFAULT 'RP'::character varying NOT NULL |
| `reason_code` | `varchar(10)` | No | NOT NULL |
| `changed_by` | `varchar(100)` | Sí | NULL |
| `changed_role` | `varchar(20)` | Sí | NULL |
| `ip_address` | `varchar(50)` | Sí | NULL |
| `device_info` | `text` | Sí | NULL |
| `session_id` | `varchar(100)` | Sí | NULL |
| `changed_at` | `timestamp` | Sí | DEFAULT CURRENT_TIMESTAMP NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT prijs_audit_pkey PRIMARY KEY (id)`
- `CONSTRAINT fk_reason FOREIGN KEY (reason_type,reason_code) REFERENCES "catalog".parameters("type",code)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE audit.prijs_audit (
	id bigserial NOT NULL,
	servicekey varchar(200) NOT NULL,
	"family" varchar(50) NULL,
	level2 varchar(50) NULL,
	level3 varchar(50) NULL,
	level4 varchar(50) NULL,
	old_listprice numeric(10, 2) NULL,
	new_listprice numeric(10, 2) NULL,
	old_professionalprice numeric(10, 2) NULL,
	new_professionalprice numeric(10, 2) NULL,
	old_professionalpercentage numeric(6, 2) NULL,
	new_professionalpercentage numeric(6, 2) NULL,
	old_salonpercentage numeric(6, 2) NULL,
	new_salonpercentage numeric(6, 2) NULL,
	reason_type varchar(10) DEFAULT 'RP'::character varying NOT NULL,
	reason_code varchar(10) NOT NULL,
	changed_by varchar(100) NULL,
	changed_role varchar(20) NULL,
	ip_address varchar(50) NULL,
	device_info text NULL,
	session_id varchar(100) NULL,
	changed_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT prijs_audit_pkey PRIMARY KEY (id),
	CONSTRAINT fk_reason FOREIGN KEY (reason_type,reason_code) REFERENCES "catalog".parameters("type",code)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
CREATE INDEX idx_prijs_audit_changed_at ON audit.prijs_audit USING btree (changed_at DESC);

-- Permissions

ALTER TABLE audit.prijs_audit OWNER TO adminboek;
GRANT ALL ON TABLE audit.prijs_audit TO adminboek;
```

## Esquema `catalog`

### Tabla `"catalog".pais`

- Esquema: `catalog`.
- Nombre sin esquema: `pais`.
- Número de columnas: 4.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `id` | `smallserial` | No | NOT NULL |
| `codigo_iso2` | `bpchar(2)` | No | NOT NULL |
| `nombre` | `varchar(100)` | No | NOT NULL |
| `activo` | `bool` | No | DEFAULT true NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT pais_codigo_iso2_key UNIQUE (codigo_iso2)`
- `CONSTRAINT pais_pkey PRIMARY KEY (id)`

#### Referencias entrantes

- `"catalog".division_territorial` (pais_id) → `"catalog".pais` (id), restricción `division_territorial_pais_id_fkey`.

#### DDL documentado

```sql
CREATE TABLE "catalog".pais (
	id smallserial NOT NULL,
	codigo_iso2 bpchar(2) NOT NULL,
	nombre varchar(100) NOT NULL,
	activo bool DEFAULT true NOT NULL,
	CONSTRAINT pais_codigo_iso2_key UNIQUE (codigo_iso2),
	CONSTRAINT pais_pkey PRIMARY KEY (id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE "catalog".pais OWNER TO adminboek;
GRANT ALL ON TABLE "catalog".pais TO adminboek;
```

### Tabla `"catalog".parameters`

- Esquema: `catalog`.
- Nombre sin esquema: `parameters`.
- Número de columnas: 4.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `"type"` | `varchar(20)` | No | NOT NULL |
| `code` | `varchar(20)` | No | NOT NULL |
| `description` | `varchar(100)` | Sí | NULL |
| `language_code` | `varchar(10)` | Sí | NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT parameters_pk PRIMARY KEY (type, code)`

#### Referencias entrantes

- `audit.prijs_audit` (reason_type,reason_code) → `"catalog".parameters` ("type",code), restricción `fk_reason`.
- `core.organization` (organization_type,organization_type_code) → `"catalog".parameters` ("type",code), restricción `fk_organization_type`.

#### DDL documentado

```sql
CREATE TABLE "catalog".parameters (
	"type" varchar(20) NOT NULL,
	code varchar(20) NOT NULL,
	description varchar(100) NULL,
	language_code varchar(10) NULL,
	CONSTRAINT parameters_pk PRIMARY KEY (type, code)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE "catalog".parameters OWNER TO adminboek;
```

### Tabla `"catalog".tmp_cut_chile`

- Esquema: `catalog`.
- Nombre sin esquema: `tmp_cut_chile`.
- Número de columnas: 7.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `codigo_region` | `varchar(2)` | Sí | NULL |
| `nombre_region` | `varchar(100)` | Sí | NULL |
| `codigo_provincia` | `varchar(3)` | Sí | NULL |
| `nombre_provincia` | `varchar(100)` | Sí | NULL |
| `codigo_comuna` | `varchar(5)` | Sí | NULL |
| `nombre_comuna` | `varchar(100)` | Sí | NULL |
| `abreviatura_region` | `varchar(50)` | Sí | NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

No se declararon restricciones de tabla en este CREATE TABLE.

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE "catalog".tmp_cut_chile (
	codigo_region varchar(2) NULL,
	nombre_region varchar(100) NULL,
	codigo_provincia varchar(3) NULL,
	nombre_provincia varchar(100) NULL,
	codigo_comuna varchar(5) NULL,
	nombre_comuna varchar(100) NULL,
	abreviatura_region varchar(50) NULL
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE "catalog".tmp_cut_chile OWNER TO adminboek;
GRANT ALL ON TABLE "catalog".tmp_cut_chile TO adminboek;
```

### Tabla `"catalog".division_territorial`

- Esquema: `catalog`.
- Nombre sin esquema: `division_territorial`.
- Número de columnas: 8.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `id` | `bigserial` | No | NOT NULL |
| `pais_id` | `int2` | No | NOT NULL |
| `parent_id` | `int8` | Sí | NULL |
| `nivel` | `int2` | No | NOT NULL |
| `codigo` | `varchar(20)` | No | NOT NULL |
| `nombre` | `varchar(150)` | No | NOT NULL |
| `abreviatura` | `varchar(30)` | Sí | NULL |
| `activo` | `bool` | No | DEFAULT true NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT chk_division_nivel CHECK ((nivel > 0))`
- `CONSTRAINT chk_division_no_self_parent CHECK ((id <> parent_id))`
- `CONSTRAINT division_territorial_pkey PRIMARY KEY (id)`
- `CONSTRAINT uq_division_pais_codigo UNIQUE (pais_id, codigo)`
- `CONSTRAINT division_territorial_pais_id_fkey FOREIGN KEY (pais_id) REFERENCES "catalog".pais(id)`
- `CONSTRAINT fk_division_parent FOREIGN KEY (parent_id) REFERENCES "catalog".division_territorial(id)`

#### Referencias entrantes

- `"catalog".division_territorial` (parent_id) → `"catalog".division_territorial` (id), restricción `fk_division_parent`.

#### DDL documentado

```sql
CREATE TABLE "catalog".division_territorial (
	id bigserial NOT NULL,
	pais_id int2 NOT NULL,
	parent_id int8 NULL,
	nivel int2 NOT NULL,
	codigo varchar(20) NOT NULL,
	nombre varchar(150) NOT NULL,
	abreviatura varchar(30) NULL,
	activo bool DEFAULT true NOT NULL,
	CONSTRAINT chk_division_nivel CHECK ((nivel > 0)),
	CONSTRAINT chk_division_no_self_parent CHECK ((id <> parent_id)),
	CONSTRAINT division_territorial_pkey PRIMARY KEY (id),
	CONSTRAINT uq_division_pais_codigo UNIQUE (pais_id, codigo),
	CONSTRAINT division_territorial_pais_id_fkey FOREIGN KEY (pais_id) REFERENCES "catalog".pais(id),
	CONSTRAINT fk_division_parent FOREIGN KEY (parent_id) REFERENCES "catalog".division_territorial(id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
CREATE INDEX idx_division_territorial_nivel ON catalog.division_territorial USING btree (nivel);
CREATE INDEX idx_division_territorial_pais ON catalog.division_territorial USING btree (pais_id);
CREATE INDEX idx_division_territorial_parent ON catalog.division_territorial USING btree (parent_id);

-- Permissions

ALTER TABLE "catalog".division_territorial OWNER TO adminboek;
GRANT ALL ON TABLE "catalog".division_territorial TO adminboek;
```

## Esquema `core`

### Tabla `core.ai_clients`

- Esquema: `core`.
- Nombre sin esquema: `ai_clients`.
- Número de columnas: 6.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `phone` | `varchar(20)` | No | NOT NULL |
| `customer_id` | `uuid` | Sí | NULL |
| `full_name` | `varchar(150)` | Sí | NULL |
| `alias` | `varchar(80)` | Sí | NULL |
| `is_active` | `bool` | Sí | NULL |
| `last_visit` | `date` | Sí | NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT ai_clients_pkey PRIMARY KEY (phone)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE core.ai_clients (
	phone varchar(20) NOT NULL,
	customer_id uuid NULL,
	full_name varchar(150) NULL,
	alias varchar(80) NULL,
	is_active bool NULL,
	last_visit date NULL,
	CONSTRAINT ai_clients_pkey PRIMARY KEY (phone)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.ai_clients OWNER TO adminboek;
GRANT ALL ON TABLE core.ai_clients TO adminboek;
```

### Tabla `core.ai_conversations`

- Esquema: `core`.
- Nombre sin esquema: `ai_conversations`.
- Número de columnas: 8.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `conversation_id` | `uuid` | No | NOT NULL |
| `phone` | `varchar(20)` | No | NOT NULL |
| `status` | `varchar(30)` | No | NOT NULL |
| `json_data` | `jsonb` | No | NOT NULL |
| `summary` | `text` | Sí | NULL |
| `created_at` | `timestamp` | No | NOT NULL |
| `updated_at` | `timestamp` | No | NOT NULL |
| `closed_at` | `timestamp` | Sí | NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT ai_conversations_pkey PRIMARY KEY (conversation_id)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE core.ai_conversations (
	conversation_id uuid NOT NULL,
	phone varchar(20) NOT NULL,
	status varchar(30) NOT NULL,
	json_data jsonb NOT NULL,
	summary text NULL,
	created_at timestamp NOT NULL,
	updated_at timestamp NOT NULL,
	closed_at timestamp NULL,
	CONSTRAINT ai_conversations_pkey PRIMARY KEY (conversation_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.ai_conversations OWNER TO adminboek;
GRANT ALL ON TABLE core.ai_conversations TO adminboek;
```

### Tabla `core.business`

- Esquema: `core`.
- Nombre sin esquema: `business`.
- Número de columnas: 4.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `business_id` | `uuid` | No | DEFAULT gen_random_uuid() NOT NULL |
| `business_slug` | `varchar(120)` | No | NOT NULL |
| `business_name` | `varchar(200)` | No | NOT NULL |
| `business_created_at` | `timestamp` | Sí | DEFAULT CURRENT_TIMESTAMP NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT business_business_slug_key UNIQUE (business_slug)`
- `CONSTRAINT business_pkey null`

#### Referencias entrantes

- `core."local"` (business_id) → `core.business` (business_id), restricción `fk_local_business_id`.
- `core.professional` (business_id) → `core.business` (business_id), restricción `fk_professional_business_ud`.
- `core.local_professional` (business_id) → `core.business` (business_id), restricción `fk_local_professional_business_id`.
- `core.professional_service` (business_id) → `core.business` (business_id), restricción `fk_professional_service_business`.

#### DDL documentado

```sql
CREATE TABLE core.business (
	business_id uuid DEFAULT gen_random_uuid() NOT NULL,
	business_slug varchar(120) NOT NULL,
	business_name varchar(200) NOT NULL,
	business_created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT business_business_slug_key UNIQUE (business_slug),
	CONSTRAINT business_pkey null
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.business OWNER TO adminboek;
GRANT ALL ON TABLE core.business TO adminboek;
```

### Tabla `core.businesses`

- Esquema: `core`.
- Nombre sin esquema: `businesses`.
- Número de columnas: 4.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `businesses_id` | `uuid` | No | DEFAULT gen_random_uuid() NOT NULL |
| `businesses_slug` | `varchar(120)` | No | NOT NULL |
| `businesses_name` | `varchar(200)` | No | NOT NULL |
| `businesses_created_at` | `timestamp` | Sí | DEFAULT CURRENT_TIMESTAMP NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT businesses_businesses_slug_key UNIQUE (businesses_slug)`
- `CONSTRAINT businesses_pkey PRIMARY KEY (businesses_id)`

#### Referencias entrantes

- `core.business_theme` (business_theme_businesses_id) → `core.businesses` (businesses_id), restricción `fk_business_theme_business`.

#### DDL documentado

```sql
CREATE TABLE core.businesses (
	businesses_id uuid DEFAULT gen_random_uuid() NOT NULL,
	businesses_slug varchar(120) NOT NULL,
	businesses_name varchar(200) NOT NULL,
	businesses_created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT businesses_businesses_slug_key UNIQUE (businesses_slug),
	CONSTRAINT businesses_pkey PRIMARY KEY (businesses_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.businesses OWNER TO adminboek;
GRANT ALL ON TABLE core.businesses TO adminboek;
```

### Tabla `core.customers_express_token_map`

- Esquema: `core`.
- Nombre sin esquema: `customers_express_token_map`.
- Número de columnas: 3.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `"token"` | `varchar(120)` | No | NOT NULL |
| `tenant_schema` | `varchar(100)` | No | NOT NULL |
| `created_at` | `timestamp` | Sí | DEFAULT now() NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT customers_express_token_map_pkey PRIMARY KEY (token)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE core.customers_express_token_map (
	"token" varchar(120) NOT NULL,
	tenant_schema varchar(100) NOT NULL,
	created_at timestamp DEFAULT now() NULL,
	CONSTRAINT customers_express_token_map_pkey PRIMARY KEY (token)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.customers_express_token_map OWNER TO adminboek;
GRANT ALL ON TABLE core.customers_express_token_map TO adminboek;
```

### Tabla `core."group"`

- Esquema: `core`.
- Nombre sin esquema: `"group"`.
- Número de columnas: 4.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `group_id` | `bigserial` | No | NOT NULL |
| `group_code` | `varchar(30)` | No | NOT NULL |
| `group_name` | `varchar(50)` | No | NOT NULL |
| `group_created_at` | `timestamp` | Sí | DEFAULT CURRENT_TIMESTAMP NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT group_group_code_key UNIQUE (group_code)`
- `CONSTRAINT group_pkey PRIMARY KEY (group_id)`

#### Referencias entrantes

- `core.group_role` (group_id) → `core."group"` (group_id), restricción `group_role_group_id_fkey`.
- `core."user"` (user_group_id) → `core."group"` (group_id), restricción `user_user_group_id_fkey`.

#### DDL documentado

```sql
CREATE TABLE core."group" (
	group_id bigserial NOT NULL,
	group_code varchar(30) NOT NULL,
	group_name varchar(50) NOT NULL,
	group_created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT group_group_code_key UNIQUE (group_code),
	CONSTRAINT group_pkey PRIMARY KEY (group_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core."group" OWNER TO adminboek;
GRANT ALL ON TABLE core."group" TO adminboek;
```

### Tabla `core."hour"`

- Esquema: `core`.
- Nombre sin esquema: `"hour"`.
- Número de columnas: 11.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `hour_id` | `bigserial` | No | NOT NULL |
| `local_id` | `uuid` | No | NOT NULL |
| `hour_schedule_name` | `varchar(100)` | No | DEFAULT 'Horario normal'::character varying NOT NULL |
| `hour_day_of_week` | `int2` | No | NOT NULL |
| `hour_start_time` | `time` | Sí | NULL |
| `hour_end_time` | `time` | Sí | NULL |
| `hour_is_closed` | `bool` | No | DEFAULT false NOT NULL |
| `hour_valid_from` | `date` | No | DEFAULT CURRENT_DATE NOT NULL |
| `hour_valid_to` | `date` | Sí | NULL |
| `hour_created_at` | `timestamptz` | No | DEFAULT now() NOT NULL |
| `hour_updated_at` | `timestamptz` | No | DEFAULT now() NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT chk_business_hour_time CHECK (((hour_is_closed = true) OR ((hour_start_time IS NOT NULL) AND (hour_end_time IS NOT NULL) AND (hour_start_time < hour_end_time))))`
- `CONSTRAINT chk_day_of_week CHECK (((hour_day_of_week >= 1) AND (hour_day_of_week <= 7)))`
- `CONSTRAINT chk_hour_valid_range CHECK (((hour_valid_to IS NULL) OR (hour_valid_to >= hour_valid_from)))`
- `CONSTRAINT hour_pkey PRIMARY KEY (hour_id)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE core."hour" (
	hour_id bigserial NOT NULL,
	local_id uuid NOT NULL,
	hour_schedule_name varchar(100) DEFAULT 'Horario normal'::character varying NOT NULL,
	hour_day_of_week int2 NOT NULL,
	hour_start_time time NULL,
	hour_end_time time NULL,
	hour_is_closed bool DEFAULT false NOT NULL,
	hour_valid_from date DEFAULT CURRENT_DATE NOT NULL,
	hour_valid_to date NULL,
	hour_created_at timestamptz DEFAULT now() NOT NULL,
	hour_updated_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT chk_business_hour_time CHECK (((hour_is_closed = true) OR ((hour_start_time IS NOT NULL) AND (hour_end_time IS NOT NULL) AND (hour_start_time < hour_end_time)))),
	CONSTRAINT chk_day_of_week CHECK (((hour_day_of_week >= 1) AND (hour_day_of_week <= 7))),
	CONSTRAINT chk_hour_valid_range CHECK (((hour_valid_to IS NULL) OR (hour_valid_to >= hour_valid_from))),
	CONSTRAINT hour_pkey PRIMARY KEY (hour_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core."hour" OWNER TO adminboek;
GRANT ALL ON TABLE core."hour" TO adminboek;
```

### Tabla `core.identifier_types`

- Esquema: `core`.
- Nombre sin esquema: `identifier_types`.
- Número de columnas: 13.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `identifier_types_id` | `uuid` | No | DEFAULT gen_random_uuid() NOT NULL |
| `identifier_types_code` | `varchar(50)` | No | NOT NULL |
| `identifier_types_name` | `varchar(120)` | No | NOT NULL |
| `identifier_types_description` | `text` | Sí | NULL |
| `identifier_types_validation_type` | `varchar(20)` | Sí | NULL |
| `identifier_types_validation_pattern` | `varchar(250)` | Sí | NULL |
| `identifier_types_validation_function` | `varchar(120)` | Sí | NULL |
| `identifier_types_min_length` | `int4` | Sí | NULL |
| `identifier_types_max_length` | `int4` | Sí | NULL |
| `identifier_types_is_unique` | `bool` | Sí | DEFAULT false NULL |
| `identifier_types_is_searchable` | `bool` | Sí | DEFAULT true NULL |
| `identifier_types_status` | `varchar(20)` | Sí | DEFAULT 'active'::character varying NULL |
| `identifier_types_created_at` | `timestamp` | Sí | DEFAULT CURRENT_TIMESTAMP NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT identifier_types_identifier_types_code_key UNIQUE (identifier_types_code)`
- `CONSTRAINT identifier_types_pkey PRIMARY KEY (identifier_types_id)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE core.identifier_types (
	identifier_types_id uuid DEFAULT gen_random_uuid() NOT NULL,
	identifier_types_code varchar(50) NOT NULL,
	identifier_types_name varchar(120) NOT NULL,
	identifier_types_description text NULL,
	identifier_types_validation_type varchar(20) NULL,
	identifier_types_validation_pattern varchar(250) NULL,
	identifier_types_validation_function varchar(120) NULL,
	identifier_types_min_length int4 NULL,
	identifier_types_max_length int4 NULL,
	identifier_types_is_unique bool DEFAULT false NULL,
	identifier_types_is_searchable bool DEFAULT true NULL,
	identifier_types_status varchar(20) DEFAULT 'active'::character varying NULL,
	identifier_types_created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT identifier_types_identifier_types_code_key UNIQUE (identifier_types_code),
	CONSTRAINT identifier_types_pkey PRIMARY KEY (identifier_types_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
CREATE INDEX idx_identifier_types_code ON core.identifier_types USING btree (identifier_types_code);

-- Permissions

ALTER TABLE core.identifier_types OWNER TO adminboek;
GRANT ALL ON TABLE core.identifier_types TO adminboek;
```

### Tabla `core."permission"`

- Esquema: `core`.
- Nombre sin esquema: `"permission"`.
- Número de columnas: 3.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `permission_id` | `bigserial` | No | NOT NULL |
| `permission_code` | `varchar(50)` | No | NOT NULL |
| `permission_description` | `varchar(100)` | Sí | NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT permission_permission_code_key UNIQUE (permission_code)`
- `CONSTRAINT permission_pkey PRIMARY KEY (permission_id)`

#### Referencias entrantes

- `core.role_permission` (permission_id) → `core."permission"` (permission_id), restricción `role_permission_permission_id_fkey`.

#### DDL documentado

```sql
CREATE TABLE core."permission" (
	permission_id bigserial NOT NULL,
	permission_code varchar(50) NOT NULL,
	permission_description varchar(100) NULL,
	CONSTRAINT permission_permission_code_key UNIQUE (permission_code),
	CONSTRAINT permission_pkey PRIMARY KEY (permission_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core."permission" OWNER TO adminboek;
GRANT ALL ON TABLE core."permission" TO adminboek;
```

### Tabla `core.person`

- Esquema: `core`.
- Nombre sin esquema: `person`.
- Número de columnas: 23.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `person_id` | `bigserial` | No | NOT NULL |
| `person_user_id` | `int8` | Sí | NULL |
| `person_document_type` | `varchar(20)` | Sí | NULL |
| `person_rut` | `varchar(20)` | Sí | NULL |
| `person_passport` | `varchar(30)` | Sí | NULL |
| `person_first_name` | `varchar(80)` | No | NOT NULL |
| `person_middle_name` | `varchar(80)` | Sí | NULL |
| `person_last_name` | `varchar(80)` | No | NOT NULL |
| `person_second_last_name` | `varchar(80)` | Sí | NULL |
| `person_birth_date` | `date` | Sí | NULL |
| `person_nationality` | `varchar(80)` | Sí | NULL |
| `person_email` | `varchar(150)` | Sí | NULL |
| `person_phone` | `varchar(30)` | Sí | NULL |
| `person_emergency_phone` | `varchar(30)` | Sí | NULL |
| `person_address` | `varchar(250)` | Sí | NULL |
| `person_commune` | `varchar(120)` | Sí | NULL |
| `person_city` | `varchar(120)` | Sí | NULL |
| `person_region` | `varchar(120)` | Sí | NULL |
| `person_country` | `varchar(120)` | Sí | NULL |
| `person_profile_photo_url` | `text` | Sí | NULL |
| `person_profile_visibility` | `bool` | Sí | DEFAULT false NULL |
| `person_created_at` | `timestamp` | Sí | DEFAULT now() NULL |
| `person_updated_at` | `timestamp` | Sí | NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT person_person_user_id_key UNIQUE (person_user_id)`
- `CONSTRAINT person_pkey PRIMARY KEY (person_id)`

#### Referencias entrantes

- `core.person_organization` (person_organization_person_id) → `core.person` (person_id), restricción `fk_person_organization_person`.
- `core.professional_profile` (professional_profile_person_id) → `core.person` (person_id), restricción `fk_professional_profile_person`.

#### DDL documentado

```sql
CREATE TABLE core.person (
	person_id bigserial NOT NULL,
	person_user_id int8 NULL,
	person_document_type varchar(20) NULL,
	person_rut varchar(20) NULL,
	person_passport varchar(30) NULL,
	person_first_name varchar(80) NOT NULL,
	person_middle_name varchar(80) NULL,
	person_last_name varchar(80) NOT NULL,
	person_second_last_name varchar(80) NULL,
	person_birth_date date NULL,
	person_nationality varchar(80) NULL,
	person_email varchar(150) NULL,
	person_phone varchar(30) NULL,
	person_emergency_phone varchar(30) NULL,
	person_address varchar(250) NULL,
	person_commune varchar(120) NULL,
	person_city varchar(120) NULL,
	person_region varchar(120) NULL,
	person_country varchar(120) NULL,
	person_profile_photo_url text NULL,
	person_profile_visibility bool DEFAULT false NULL,
	person_created_at timestamp DEFAULT now() NULL,
	person_updated_at timestamp NULL,
	CONSTRAINT person_person_user_id_key UNIQUE (person_user_id),
	CONSTRAINT person_pkey PRIMARY KEY (person_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.person OWNER TO adminboek;
GRANT ALL ON TABLE core.person TO adminboek;
```

### Tabla `core.prices`

- Esquema: `core`.
- Nombre sin esquema: `prices`.
- Número de columnas: 12.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `servicekey` | `varchar(200)` | No | NOT NULL |
| `"family"` | `varchar(50)` | Sí | NULL |
| `level2` | `varchar(50)` | Sí | NULL |
| `level3` | `varchar(50)` | Sí | NULL |
| `level4` | `varchar(50)` | Sí | NULL |
| `professionalprice` | `int4` | Sí | NULL |
| `listprice` | `int4` | Sí | NULL |
| `professionalpercentage` | `numeric(6, 2)` | Sí | NULL |
| `salonpercentage` | `numeric(6, 2)` | Sí | NULL |
| `service_id` | `varchar` | Sí | NULL |
| `duration` | `int2` | Sí | NULL |
| `deposit` | `int4` | Sí | NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT prijs_pkey PRIMARY KEY (servicekey)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE core.prices (
	servicekey varchar(200) NOT NULL,
	"family" varchar(50) NULL,
	level2 varchar(50) NULL,
	level3 varchar(50) NULL,
	level4 varchar(50) NULL,
	professionalprice int4 NULL,
	listprice int4 NULL,
	professionalpercentage numeric(6, 2) NULL,
	salonpercentage numeric(6, 2) NULL,
	service_id varchar NULL, -- Corresponde al codigo que identifica a un unico servicio.
	duration int2 NULL, -- duracion del servicio en minutos
	deposit int4 NULL, -- deposito u abono por el servicio
	CONSTRAINT prijs_pkey PRIMARY KEY (servicekey)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Column comments

COMMENT ON COLUMN core.prices.service_id IS 'Corresponde al codigo que identifica a un unico servicio.';
COMMENT ON COLUMN core.prices.duration IS 'duracion del servicio en minutos';
COMMENT ON COLUMN core.prices.deposit IS 'deposito u abono por el servicio';

-- Permissions

ALTER TABLE core.prices OWNER TO adminboek;
```

### Tabla `core."role"`

- Esquema: `core`.
- Nombre sin esquema: `"role"`.
- Número de columnas: 4.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `role_id` | `bigserial` | No | NOT NULL |
| `role_code` | `varchar(30)` | No | NOT NULL |
| `role_description` | `varchar(100)` | Sí | NULL |
| `role_created_at` | `timestamp` | Sí | DEFAULT CURRENT_TIMESTAMP NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT role_pkey PRIMARY KEY (role_id)`
- `CONSTRAINT role_role_code_key UNIQUE (role_code)`

#### Referencias entrantes

- `core.group_role` (role_id) → `core."role"` (role_id), restricción `group_role_role_id_fkey`.
- `core.role_menu` (role_id) → `core."role"` (role_id), restricción `role_menu_role_id_fkey`.

#### DDL documentado

```sql
CREATE TABLE core."role" (
	role_id bigserial NOT NULL,
	role_code varchar(30) NOT NULL,
	role_description varchar(100) NULL,
	role_created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT role_pkey PRIMARY KEY (role_id),
	CONSTRAINT role_role_code_key UNIQUE (role_code)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core."role" OWNER TO adminboek;
GRANT ALL ON TABLE core."role" TO adminboek;
```

### Tabla `core.service`

- Esquema: `core`.
- Nombre sin esquema: `service`.
- Número de columnas: 7.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `service_template_id` | `uuid` | No | DEFAULT gen_random_uuid() NOT NULL |
| `service_template_code` | `varchar(50)` | No | NOT NULL |
| `service_template_name` | `varchar(150)` | No | NOT NULL |
| `service_template_description` | `varchar(500)` | Sí | NULL |
| `service_template_status` | `varchar(20)` | No | DEFAULT 'ACTIVE'::character varying NOT NULL |
| `service_template_created_at` | `timestamptz` | No | DEFAULT now() NOT NULL |
| `service_template_updated_at` | `timestamptz` | No | DEFAULT now() NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT chk_service_status CHECK (((service_template_status)::text = ANY (ARRAY[('ACTIVE'::character varying)::text, ('INACTIVE'::character varying)::text])))`
- `CONSTRAINT service_pkey PRIMARY KEY (service_template_id)`
- `CONSTRAINT service_service_template_code_key UNIQUE (service_template_code)`

#### Referencias entrantes

- `core.service_catalog` (service_template_id) → `core.service` (service_template_id), restricción `fk_service_catalog`.

#### DDL documentado

```sql
CREATE TABLE core.service (
	service_template_id uuid DEFAULT gen_random_uuid() NOT NULL,
	service_template_code varchar(50) NOT NULL,
	service_template_name varchar(150) NOT NULL,
	service_template_description varchar(500) NULL,
	service_template_status varchar(20) DEFAULT 'ACTIVE'::character varying NOT NULL,
	service_template_created_at timestamptz DEFAULT now() NOT NULL,
	service_template_updated_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT chk_service_status CHECK (((service_template_status)::text = ANY (ARRAY[('ACTIVE'::character varying)::text, ('INACTIVE'::character varying)::text]))),
	CONSTRAINT service_pkey PRIMARY KEY (service_template_id),
	CONSTRAINT service_service_template_code_key UNIQUE (service_template_code)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.service OWNER TO adminboek;
GRANT ALL ON TABLE core.service TO adminboek;
```

### Tabla `core.service_template`

- Esquema: `core`.
- Nombre sin esquema: `service_template`.
- Número de columnas: 7.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `service_template_id` | `uuid` | No | DEFAULT gen_random_uuid() NOT NULL |
| `service_template_code` | `varchar(50)` | No | NOT NULL |
| `service_template_name` | `varchar(150)` | No | NOT NULL |
| `service_template_description` | `varchar(500)` | Sí | NULL |
| `service_template_status` | `varchar(20)` | No | DEFAULT 'ACTIVE'::character varying NOT NULL |
| `service_template_created_at` | `timestamptz` | No | DEFAULT now() NOT NULL |
| `service_template_updated_at` | `timestamptz` | No | DEFAULT now() NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT chk_service_template_status CHECK (((service_template_status)::text = ANY ((ARRAY['ACTIVE'::character varying, 'INACTIVE'::character varying])::text[])))`
- `CONSTRAINT service_template_pkey PRIMARY KEY (service_template_id)`
- `CONSTRAINT service_template_service_template_code_key UNIQUE (service_template_code)`

#### Referencias entrantes

- `core.service_template_catalog` (service_template_id) → `core.service_template` (service_template_id), restricción `fk_template_catalog_template`.

#### DDL documentado

```sql
CREATE TABLE core.service_template (
	service_template_id uuid DEFAULT gen_random_uuid() NOT NULL,
	service_template_code varchar(50) NOT NULL,
	service_template_name varchar(150) NOT NULL,
	service_template_description varchar(500) NULL,
	service_template_status varchar(20) DEFAULT 'ACTIVE'::character varying NOT NULL,
	service_template_created_at timestamptz DEFAULT now() NOT NULL,
	service_template_updated_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT chk_service_template_status CHECK (((service_template_status)::text = ANY ((ARRAY['ACTIVE'::character varying, 'INACTIVE'::character varying])::text[]))),
	CONSTRAINT service_template_pkey PRIMARY KEY (service_template_id),
	CONSTRAINT service_template_service_template_code_key UNIQUE (service_template_code)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.service_template OWNER TO adminboek;
GRANT ALL ON TABLE core.service_template TO adminboek;
```

### Tabla `core.skill`

- Esquema: `core`.
- Nombre sin esquema: `skill`.
- Número de columnas: 6.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `skill_id` | `bigserial` | No | NOT NULL |
| `skill_name` | `varchar(120)` | No | NOT NULL |
| `skill_description` | `text` | Sí | NULL |
| `skill_active` | `bool` | Sí | DEFAULT true NULL |
| `skill_created_at` | `timestamp` | Sí | DEFAULT now() NULL |
| `skill_updated_at` | `timestamp` | Sí | NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT skill_pkey PRIMARY KEY (skill_id)`
- `CONSTRAINT skill_skill_name_key UNIQUE (skill_name)`

#### Referencias entrantes

- `core.professional_skill` (professional_skill_skill_id) → `core.skill` (skill_id), restricción `fk_professional_skill_skill`.

#### DDL documentado

```sql
CREATE TABLE core.skill (
	skill_id bigserial NOT NULL,
	skill_name varchar(120) NOT NULL,
	skill_description text NULL,
	skill_active bool DEFAULT true NULL,
	skill_created_at timestamp DEFAULT now() NULL,
	skill_updated_at timestamp NULL,
	CONSTRAINT skill_pkey PRIMARY KEY (skill_id),
	CONSTRAINT skill_skill_name_key UNIQUE (skill_name)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.skill OWNER TO adminboek;
GRANT ALL ON TABLE core.skill TO adminboek;
```

### Tabla `core.stg_ventas_lyl`

- Esquema: `core`.
- Nombre sin esquema: `stg_ventas_lyl`.
- Número de columnas: 36.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `ventas_key` | `varchar(50)` | No | NOT NULL |
| `sp` | `varchar(20)` | Sí | NULL |
| `fecha_entrega` | `date` | Sí | NULL |
| `profesional` | `varchar(50)` | Sí | NULL |
| `fecha_recau` | `date` | Sí | NULL |
| `rut_celular` | `varchar(20)` | Sí | NULL |
| `nombre` | `varchar(100)` | Sí | NULL |
| `origen` | `varchar(50)` | Sí | NULL |
| `nro_formulario` | `varchar(50)` | Sí | NULL |
| `familia` | `varchar(50)` | Sí | NULL |
| `nivel_2` | `varchar(50)` | Sí | NULL |
| `nivel_3` | `varchar(50)` | Sí | NULL |
| `nivel_4` | `varchar(50)` | Sí | NULL |
| `precio_profesional` | `numeric(16, 4)` | Sí | NULL |
| `precio_web` | `numeric(16, 4)` | Sí | NULL |
| `porcentaje_profesional` | `numeric(4, 2)` | Sí | NULL |
| `abono` | `numeric(16, 4)` | Sí | NULL |
| `pagados` | `numeric(16, 4)` | Sí | NULL |
| `total` | `numeric(16, 4)` | Sí | NULL |
| `nro_getnet` | `varchar(20)` | Sí | NULL |
| `total_pw` | `numeric(16, 4)` | Sí | NULL |
| `valida_form` | `varchar(20)` | Sí | NULL |
| `abono_perdido` | `numeric(16, 4)` | Sí | NULL |
| `descuento` | `varchar(50)` | Sí | NULL |
| `descuentos` | `numeric(16, 4)` | Sí | NULL |
| `ganancia_prof` | `numeric(16, 4)` | Sí | NULL |
| `total_ganancia_prof` | `numeric(16, 4)` | Sí | NULL |
| `ganancia_salon` | `numeric(16, 4)` | Sí | NULL |
| `descprof_a_clientas` | `numeric(16, 4)` | Sí | NULL |
| `anio` | `int4` | Sí | NULL |
| `anio_mes` | `varchar(7)` | Sí | NULL |
| `obs` | `varchar(300)` | Sí | NULL |
| `archivo_origen` | `varchar(255)` | Sí | NULL |
| `hoja_origen` | `varchar(100)` | Sí | DEFAULT 'VENTAS'::character varying NULL |
| `fila_excel` | `int4` | Sí | NULL |
| `fecha_carga` | `timestamp` | Sí | DEFAULT CURRENT_TIMESTAMP NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT pk_stg_ventas_lyl PRIMARY KEY (ventas_key)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE core.stg_ventas_lyl (
	ventas_key varchar(50) NOT NULL,
	sp varchar(20) NULL,
	fecha_entrega date NULL,
	profesional varchar(50) NULL,
	fecha_recau date NULL,
	rut_celular varchar(20) NULL,
	nombre varchar(100) NULL,
	origen varchar(50) NULL,
	nro_formulario varchar(50) NULL,
	familia varchar(50) NULL,
	nivel_2 varchar(50) NULL,
	nivel_3 varchar(50) NULL,
	nivel_4 varchar(50) NULL,
	precio_profesional numeric(16, 4) NULL,
	precio_web numeric(16, 4) NULL,
	porcentaje_profesional numeric(4, 2) NULL,
	abono numeric(16, 4) NULL,
	pagados numeric(16, 4) NULL,
	total numeric(16, 4) NULL,
	nro_getnet varchar(20) NULL,
	total_pw numeric(16, 4) NULL,
	valida_form varchar(20) NULL,
	abono_perdido numeric(16, 4) NULL,
	descuento varchar(50) NULL,
	descuentos numeric(16, 4) NULL,
	ganancia_prof numeric(16, 4) NULL,
	total_ganancia_prof numeric(16, 4) NULL,
	ganancia_salon numeric(16, 4) NULL,
	descprof_a_clientas numeric(16, 4) NULL,
	anio int4 NULL,
	anio_mes varchar(7) NULL,
	obs varchar(300) NULL,
	archivo_origen varchar(255) NULL,
	hoja_origen varchar(100) DEFAULT 'VENTAS'::character varying NULL,
	fila_excel int4 NULL,
	fecha_carga timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT pk_stg_ventas_lyl PRIMARY KEY (ventas_key)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.stg_ventas_lyl OWNER TO adminboek;
```

### Tabla `core.template_hour`

- Esquema: `core`.
- Nombre sin esquema: `template_hour`.
- Número de columnas: 11.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `template_hour_id` | `bigserial` | No | NOT NULL |
| `template_hour_schedule_name` | `varchar(100)` | No | DEFAULT 'Horario normal'::character varying NOT NULL |
| `template_hour_day_of_week` | `int2` | No | NOT NULL |
| `template_hour_start_time` | `time` | Sí | NULL |
| `template_hour_end_time` | `time` | Sí | NULL |
| `template_hour_is_closed` | `bool` | No | DEFAULT false NOT NULL |
| `template_hour_valid_from` | `date` | No | DEFAULT CURRENT_DATE NOT NULL |
| `template_hour_valid_to` | `date` | Sí | NULL |
| `template_hour_created_at` | `timestamptz` | No | DEFAULT now() NOT NULL |
| `template_hour_updated_at` | `timestamptz` | No | DEFAULT now() NOT NULL |
| `template_hour_code` | `varchar(50)` | Sí | NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT chk_business_hour_time CHECK (((template_hour_is_closed = true) OR ((template_hour_start_time IS NOT NULL) AND (template_hour_end_time IS NOT NULL) AND (template_hour_start_time < template_hour_end_time))))`
- `CONSTRAINT chk_day_of_week CHECK (((template_hour_day_of_week >= 1) AND (template_hour_day_of_week <= 7)))`
- `CONSTRAINT chk_hour_valid_range CHECK (((template_hour_valid_to IS NULL) OR (template_hour_valid_to >= template_hour_valid_from)))`
- `CONSTRAINT template_hour_pkey PRIMARY KEY (template_hour_id)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE core.template_hour (
	template_hour_id bigserial NOT NULL,
	template_hour_schedule_name varchar(100) DEFAULT 'Horario normal'::character varying NOT NULL,
	template_hour_day_of_week int2 NOT NULL,
	template_hour_start_time time NULL,
	template_hour_end_time time NULL,
	template_hour_is_closed bool DEFAULT false NOT NULL,
	template_hour_valid_from date DEFAULT CURRENT_DATE NOT NULL,
	template_hour_valid_to date NULL,
	template_hour_created_at timestamptz DEFAULT now() NOT NULL,
	template_hour_updated_at timestamptz DEFAULT now() NOT NULL,
	template_hour_code varchar(50) NULL,
	CONSTRAINT chk_business_hour_time CHECK (((template_hour_is_closed = true) OR ((template_hour_start_time IS NOT NULL) AND (template_hour_end_time IS NOT NULL) AND (template_hour_start_time < template_hour_end_time)))),
	CONSTRAINT chk_day_of_week CHECK (((template_hour_day_of_week >= 1) AND (template_hour_day_of_week <= 7))),
	CONSTRAINT chk_hour_valid_range CHECK (((template_hour_valid_to IS NULL) OR (template_hour_valid_to >= template_hour_valid_from))),
	CONSTRAINT template_hour_pkey PRIMARY KEY (template_hour_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.template_hour OWNER TO adminboek;
```

### Tabla `core.tenant`

- Esquema: `core`.
- Nombre sin esquema: `tenant`.
- Número de columnas: 5.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `tenant_id` | `int4` | No | GENERATED ALWAYS AS IDENTITY NOT NULL |
| `tenant_name` | `varchar(100)` | No | NOT NULL |
| `tenant_db_schema` | `varchar(100)` | No | NOT NULL |
| `tenant_db_role` | `varchar(100)` | No | NOT NULL |
| `tenant_created_at` | `timestamptz` | No | DEFAULT CURRENT_TIMESTAMP NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT tenant_pkey PRIMARY KEY (tenant_id)`
- `CONSTRAINT tenant_tenant_name_key UNIQUE (tenant_name)`

#### Referencias entrantes

- `core.organization` (organization_tenant_id) → `core.tenant` (tenant_id), restricción `fk_organization_tenant`.
- `core.theme` (theme_tenant_id) → `core.tenant` (tenant_id), restricción `fk_theme`.

#### DDL documentado

```sql
CREATE TABLE core.tenant (
	tenant_id int4 GENERATED ALWAYS AS IDENTITY NOT NULL,
	tenant_name varchar(100) NOT NULL,
	tenant_db_schema varchar(100) NOT NULL,
	tenant_db_role varchar(100) NOT NULL,
	tenant_created_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT tenant_pkey PRIMARY KEY (tenant_id),
	CONSTRAINT tenant_tenant_name_key UNIQUE (tenant_name)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.tenant OWNER TO adminboek;
GRANT ALL ON TABLE core.tenant TO adminboek;
```

### Tabla `core.token_blacklist`

- Esquema: `core`.
- Nombre sin esquema: `token_blacklist`.
- Número de columnas: 2.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `jti` | `text` | No | NOT NULL |
| `revoked_at` | `timestamp` | Sí | DEFAULT now() NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT token_blacklist_pkey PRIMARY KEY (jti)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE core.token_blacklist (
	jti text NOT NULL,
	revoked_at timestamp DEFAULT now() NULL,
	CONSTRAINT token_blacklist_pkey PRIMARY KEY (jti)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.token_blacklist OWNER TO adminboek;
GRANT ALL ON TABLE core.token_blacklist TO adminboek;
```

### Tabla `core.user_session`

- Esquema: `core`.
- Nombre sin esquema: `user_session`.
- Número de columnas: 8.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `session_id` | `uuid` | No | NOT NULL |
| `user_name` | `text` | No | NOT NULL |
| `user_group_id` | `int4` | No | NOT NULL |
| `created_at` | `timestamp` | Sí | DEFAULT now() NULL |
| `expires_at` | `timestamp` | No | NOT NULL |
| `revoked` | `bool` | Sí | DEFAULT false NULL |
| `ip_address` | `text` | Sí | NULL |
| `user_agent` | `text` | Sí | NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT user_session_pkey PRIMARY KEY (session_id)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE core.user_session (
	session_id uuid NOT NULL,
	user_name text NOT NULL,
	user_group_id int4 NOT NULL,
	created_at timestamp DEFAULT now() NULL,
	expires_at timestamp NOT NULL,
	revoked bool DEFAULT false NULL,
	ip_address text NULL,
	user_agent text NULL,
	CONSTRAINT user_session_pkey PRIMARY KEY (session_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.user_session OWNER TO adminboek;
GRANT ALL ON TABLE core.user_session TO adminboek;
```

### Tabla `core.business_theme`

- Esquema: `core`.
- Nombre sin esquema: `business_theme`.
- Número de columnas: 14.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `business_theme_id` | `uuid` | No | DEFAULT gen_random_uuid() NOT NULL |
| `business_theme_businesses_id` | `uuid` | No | NOT NULL |
| `business_theme_base_color` | `varchar(20)` | Sí | NULL |
| `business_theme_primary_color` | `varchar(20)` | Sí | NULL |
| `business_theme_secondary_color` | `varchar(20)` | Sí | NULL |
| `business_theme_background_color` | `varchar(20)` | Sí | NULL |
| `business_theme_success_color` | `varchar(20)` | Sí | NULL |
| `business_theme_error_color` | `varchar(20)` | Sí | NULL |
| `business_theme_white_color` | `varchar(20)` | Sí | NULL |
| `business_theme_border_color` | `varchar(20)` | Sí | NULL |
| `business_theme_shadow_soft` | `varchar(200)` | Sí | NULL |
| `business_theme_radius` | `varchar(20)` | Sí | NULL |
| `business_theme_created_at` | `timestamp` | Sí | DEFAULT CURRENT_TIMESTAMP NULL |
| `business_theme_logo_url` | `varchar(500)` | Sí | NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT business_theme_pkey PRIMARY KEY (business_theme_id)`
- `CONSTRAINT fk_business_theme_business FOREIGN KEY (business_theme_businesses_id) REFERENCES core.businesses(businesses_id)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE core.business_theme (
	business_theme_id uuid DEFAULT gen_random_uuid() NOT NULL,
	business_theme_businesses_id uuid NOT NULL,
	business_theme_base_color varchar(20) NULL,
	business_theme_primary_color varchar(20) NULL,
	business_theme_secondary_color varchar(20) NULL,
	business_theme_background_color varchar(20) NULL,
	business_theme_success_color varchar(20) NULL,
	business_theme_error_color varchar(20) NULL,
	business_theme_white_color varchar(20) NULL,
	business_theme_border_color varchar(20) NULL,
	business_theme_shadow_soft varchar(200) NULL,
	business_theme_radius varchar(20) NULL,
	business_theme_created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	business_theme_logo_url varchar(500) NULL,
	CONSTRAINT business_theme_pkey PRIMARY KEY (business_theme_id),
	CONSTRAINT fk_business_theme_business FOREIGN KEY (business_theme_businesses_id) REFERENCES core.businesses(businesses_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.business_theme OWNER TO adminboek;
GRANT ALL ON TABLE core.business_theme TO adminboek;
```

### Tabla `core.group_role`

- Esquema: `core`.
- Nombre sin esquema: `group_role`.
- Número de columnas: 2.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `group_id` | `int8` | No | NOT NULL |
| `role_id` | `int8` | No | NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT group_role_pkey PRIMARY KEY (group_id, role_id)`
- `CONSTRAINT group_role_group_id_fkey FOREIGN KEY (group_id) REFERENCES core."group"(group_id) ON DELETE CASCADE`
- `CONSTRAINT group_role_role_id_fkey FOREIGN KEY (role_id) REFERENCES core."role"(role_id) ON DELETE CASCADE`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE core.group_role (
	group_id int8 NOT NULL,
	role_id int8 NOT NULL,
	CONSTRAINT group_role_pkey PRIMARY KEY (group_id, role_id),
	CONSTRAINT group_role_group_id_fkey FOREIGN KEY (group_id) REFERENCES core."group"(group_id) ON DELETE CASCADE,
	CONSTRAINT group_role_role_id_fkey FOREIGN KEY (role_id) REFERENCES core."role"(role_id) ON DELETE CASCADE
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.group_role OWNER TO adminboek;
GRANT ALL ON TABLE core.group_role TO adminboek;
```

### Tabla `core."local"`

- Esquema: `core`.
- Nombre sin esquema: `"local"`.
- Número de columnas: 4.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `local_id` | `uuid` | No | DEFAULT gen_random_uuid() NOT NULL |
| `business_id` | `uuid` | No | NOT NULL |
| `local_name` | `varchar(200)` | No | NOT NULL |
| `local_created_at` | `timestamp` | Sí | DEFAULT CURRENT_TIMESTAMP NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT local_pkey PRIMARY KEY (local_id)`
- `CONSTRAINT fk_local_business_id FOREIGN KEY (business_id) REFERENCES core.business(business_id)`

#### Referencias entrantes

- `core.local_professional` (local_id) → `core."local"` (local_id), restricción `fk_local_professional_local_id`.
- `core.professional_service` (local_id) → `core."local"` (local_id), restricción `fk_professional_service_local`.

#### DDL documentado

```sql
CREATE TABLE core."local" (
	local_id uuid DEFAULT gen_random_uuid() NOT NULL,
	business_id uuid NOT NULL,
	local_name varchar(200) NOT NULL,
	local_created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT local_pkey PRIMARY KEY (local_id),
	CONSTRAINT fk_local_business_id FOREIGN KEY (business_id) REFERENCES core.business(business_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core."local" OWNER TO adminboek;
GRANT ALL ON TABLE core."local" TO adminboek;
```

### Tabla `core.menu`

- Esquema: `core`.
- Nombre sin esquema: `menu`.
- Número de columnas: 9.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `menu_id` | `bigserial` | No | NOT NULL |
| `menu_name` | `varchar(100)` | No | NOT NULL |
| `menu_path` | `varchar(150)` | Sí | NULL |
| `menu_parent_id` | `int8` | Sí | NULL |
| `menu_order` | `int4` | Sí | DEFAULT 1 NULL |
| `menu_active` | `bool` | Sí | DEFAULT true NULL |
| `menu_icon` | `varchar(50)` | Sí | NULL |
| `menu_level` | `int4` | Sí | DEFAULT 1 NULL |
| `menu_created_at` | `timestamp` | Sí | DEFAULT CURRENT_TIMESTAMP NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT menu_pkey PRIMARY KEY (menu_id)`
- `CONSTRAINT fk_menu_parent FOREIGN KEY (menu_parent_id) REFERENCES core.menu(menu_id) ON DELETE CASCADE`

#### Referencias entrantes

- `core.menu` (menu_parent_id) → `core.menu` (menu_id), restricción `fk_menu_parent`.
- `core.role_menu` (menu_id) → `core.menu` (menu_id), restricción `role_menu_menu_id_fkey`.

#### DDL documentado

```sql
CREATE TABLE core.menu (
	menu_id bigserial NOT NULL,
	menu_name varchar(100) NOT NULL,
	menu_path varchar(150) NULL,
	menu_parent_id int8 NULL,
	menu_order int4 DEFAULT 1 NULL,
	menu_active bool DEFAULT true NULL,
	menu_icon varchar(50) NULL,
	menu_level int4 DEFAULT 1 NULL,
	menu_created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT menu_pkey PRIMARY KEY (menu_id),
	CONSTRAINT fk_menu_parent FOREIGN KEY (menu_parent_id) REFERENCES core.menu(menu_id) ON DELETE CASCADE
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.menu OWNER TO adminboek;
GRANT ALL ON TABLE core.menu TO adminboek;
```

### Tabla `core.organization`

- Esquema: `core`.
- Nombre sin esquema: `organization`.
- Número de columnas: 8.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `organization_id` | `int4` | No | GENERATED ALWAYS AS IDENTITY NOT NULL |
| `organization_parent_id` | `int4` | Sí | NULL |
| `organization_code` | `varchar(20)` | No | NOT NULL |
| `organization_name` | `varchar(150)` | No | NOT NULL |
| `organization_type` | `varchar(10)` | No | DEFAULT 'TN'::character varying NOT NULL |
| `organization_type_code` | `varchar(10)` | No | NOT NULL |
| `organization_tenant_id` | `int4` | Sí | NULL |
| `orgatization_created_at` | `timestamptz` | No | DEFAULT CURRENT_TIMESTAMP NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT chk_organization_type_fixed CHECK (((organization_type)::text = 'TN'::text))`
- `CONSTRAINT organization_pkey PRIMARY KEY (organization_id)`
- `CONSTRAINT fk_organization_tenant FOREIGN KEY (organization_tenant_id) REFERENCES core.tenant(tenant_id)`
- `CONSTRAINT fk_organization_type FOREIGN KEY (organization_type,organization_type_code) REFERENCES "catalog".parameters("type",code)`
- `CONSTRAINT organization_organization_parent_id_fkey FOREIGN KEY (organization_parent_id) REFERENCES core.organization(organization_id)`

#### Referencias entrantes

- `core.organization` (organization_parent_id) → `core.organization` (organization_id), restricción `organization_organization_parent_id_fkey`.
- `core.person_organization` (person_organization_organization_id) → `core.organization` (organization_id), restricción `fk_person_organization_organization`.
- `core.professional` (organization_id) → `core.organization` (organization_id), restricción `fk_professional_organization`.
- `lindasylunaticas.product_family` (organization_id) → `core.organization` (organization_id), restricción `fk_product_family_organization`.
- `lindasylunaticas.ticket` (organization_id) → `core.organization` (organization_id), restricción `fk_ticket_organization`.

#### DDL documentado

```sql
CREATE TABLE core.organization (
	organization_id int4 GENERATED ALWAYS AS IDENTITY NOT NULL, -- Identificador único interno del nodo organizacional. Llave primaria técnica.
	organization_parent_id int4 NULL, -- Identificador del nodo organizacional padre dentro de la jerarquía. Permite modelar relaciones de matriz-filial-sucursal en múltiples niveles.
	organization_code varchar(20) NOT NULL, -- Código funcional único del nodo organizacional dentro del sistema. Identifica empresas, filiales o sucursales.
	organization_name varchar(150) NOT NULL, -- Nombre descriptivo del nodo organizacional. Ejemplo: Casa Matriz, Sucursal Providencia, Filial Norte, etc.
	organization_type varchar(10) DEFAULT 'TN'::character varying NOT NULL, -- Tipo lógico del nodo organizacional. Se fija en 'TN' para indicar clasificación por tipo de nodo.
	organization_type_code varchar(10) NOT NULL, -- Código del tipo de nodo organizacional según parámetros. Ejemplos: TN001 = Matriz, TN002 = Sucursal, TN003 = Filial.
	organization_tenant_id int4 NULL,
	orgatization_created_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT chk_organization_type_fixed CHECK (((organization_type)::text = 'TN'::text)),
	CONSTRAINT organization_pkey PRIMARY KEY (organization_id),
	CONSTRAINT fk_organization_tenant FOREIGN KEY (organization_tenant_id) REFERENCES core.tenant(tenant_id),
	CONSTRAINT fk_organization_type FOREIGN KEY (organization_type,organization_type_code) REFERENCES "catalog".parameters("type",code),
	CONSTRAINT organization_organization_parent_id_fkey FOREIGN KEY (organization_parent_id) REFERENCES core.organization(organization_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
CREATE INDEX idx_organization_parent ON core.organization USING btree (organization_parent_id);
CREATE INDEX idx_organization_tenant ON core.organization USING btree (organization_tenant_id);
CREATE INDEX idx_organization_type ON core.organization USING btree (organization_type_code);
CREATE UNIQUE INDEX ux_organization_code ON core.organization USING btree (organization_code);
COMMENT ON TABLE core.organization IS 'Tabla maestra de la estructura organizacional del negocio. Permite modelar jerarquías de empresas, matrices, filiales y sucursales en múltiples niveles. Cada nodo de la estructura se clasifica mediante un tipo controlado por parámetros.';

-- Column comments

COMMENT ON COLUMN core.organization.organization_id IS 'Identificador único interno del nodo organizacional. Llave primaria técnica.';
COMMENT ON COLUMN core.organization.organization_parent_id IS 'Identificador del nodo organizacional padre dentro de la jerarquía. Permite modelar relaciones de matriz-filial-sucursal en múltiples niveles.';
COMMENT ON COLUMN core.organization.organization_code IS 'Código funcional único del nodo organizacional dentro del sistema. Identifica empresas, filiales o sucursales.';
COMMENT ON COLUMN core.organization.organization_name IS 'Nombre descriptivo del nodo organizacional. Ejemplo: Casa Matriz, Sucursal Providencia, Filial Norte, etc.';
COMMENT ON COLUMN core.organization.organization_type IS 'Tipo lógico del nodo organizacional. Se fija en ''TN'' para indicar clasificación por tipo de nodo.';
COMMENT ON COLUMN core.organization.organization_type_code IS 'Código del tipo de nodo organizacional según parámetros. Ejemplos: TN001 = Matriz, TN002 = Sucursal, TN003 = Filial.';

-- Constraint comments

COMMENT ON CONSTRAINT chk_organization_type_fixed ON core.organization IS 'Restricción que fuerza el uso del tipo lógico ''TN'' para clasificar nodos organizacionales.';
COMMENT ON CONSTRAINT fk_organization_type ON core.organization IS 'Restricción de integridad que valida el tipo de nodo organizacional contra la tabla central de parámetros.';

-- Permissions

ALTER TABLE core.organization OWNER TO adminboek;
GRANT ALL ON TABLE core.organization TO adminboek;
```

### Tabla `core.person_organization`

- Esquema: `core`.
- Nombre sin esquema: `person_organization`.
- Número de columnas: 7.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `person_organization_id` | `bigserial` | No | NOT NULL |
| `person_organization_person_id` | `int8` | No | NOT NULL |
| `person_organization_organization_id` | `int4` | No | NOT NULL |
| `person_organization_is_default` | `bool` | No | DEFAULT false NOT NULL |
| `person_organization_active` | `bool` | No | DEFAULT true NOT NULL |
| `person_organization_created_at` | `timestamp` | Sí | DEFAULT now() NULL |
| `person_organization_updated_at` | `timestamp` | Sí | NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT person_organization_pkey PRIMARY KEY (person_organization_id)`
- `CONSTRAINT ux_person_organization UNIQUE (person_organization_person_id, person_organization_organization_id)`
- `CONSTRAINT fk_person_organization_organization FOREIGN KEY (person_organization_organization_id) REFERENCES core.organization(organization_id)`
- `CONSTRAINT fk_person_organization_person FOREIGN KEY (person_organization_person_id) REFERENCES core.person(person_id)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE core.person_organization (
	person_organization_id bigserial NOT NULL,
	person_organization_person_id int8 NOT NULL,
	person_organization_organization_id int4 NOT NULL,
	person_organization_is_default bool DEFAULT false NOT NULL,
	person_organization_active bool DEFAULT true NOT NULL,
	person_organization_created_at timestamp DEFAULT now() NULL,
	person_organization_updated_at timestamp NULL,
	CONSTRAINT person_organization_pkey PRIMARY KEY (person_organization_id),
	CONSTRAINT ux_person_organization UNIQUE (person_organization_person_id, person_organization_organization_id),
	CONSTRAINT fk_person_organization_organization FOREIGN KEY (person_organization_organization_id) REFERENCES core.organization(organization_id),
	CONSTRAINT fk_person_organization_person FOREIGN KEY (person_organization_person_id) REFERENCES core.person(person_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
CREATE INDEX idx_person_organization_organization ON core.person_organization USING btree (person_organization_organization_id);
CREATE INDEX idx_person_organization_person ON core.person_organization USING btree (person_organization_person_id);
CREATE UNIQUE INDEX ux_person_organization_default ON core.person_organization USING btree (person_organization_person_id) WHERE (person_organization_is_default = true);

-- Permissions

ALTER TABLE core.person_organization OWNER TO adminboek;
GRANT ALL ON TABLE core.person_organization TO adminboek;
```

### Tabla `core.professional`

- Esquema: `core`.
- Nombre sin esquema: `professional`.
- Número de columnas: 6.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `professional_id` | `int4` | No | GENERATED ALWAYS AS IDENTITY NOT NULL |
| `business_id` | `int4` | No | NOT NULL |
| `professional_nickname` | `varchar(50)` | No | NOT NULL |
| `professional_name` | `varchar(120)` | No | NOT NULL |
| `organization_id` | `int4` | No | DEFAULT 2 NOT NULL |
| `professional_created_at` | `timestamptz` | No | DEFAULT CURRENT_TIMESTAMP NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT professional_pkey PRIMARY KEY (professional_id)`
- `CONSTRAINT fk_professional_business_ud FOREIGN KEY (business_id) REFERENCES core.business(business_id)`
- `CONSTRAINT fk_professional_organization FOREIGN KEY (organization_id) REFERENCES core.organization(organization_id)`

#### Referencias entrantes

- `lindasylunaticas.ticket_professional` (professional_id) → `core.professional` (professional_id), restricción `fk_tp_professional`.
- `core.local_professional` (professional_id) → `core.professional` (professional_id), restricción `fk_local_professional_professional_id`.
- `core.professional_service` (professional_id) → `core.professional` (professional_id), restricción `fk_professional_service_professional`.

#### DDL documentado

```sql
CREATE TABLE core.professional (
    professional_id int4 GENERATED ALWAYS AS IDENTITY NOT NULL,
    business_id int4 NOT NULL,
    professional_nickname varchar(50) NOT NULL,
    professional_name varchar(120) NOT NULL,
    organization_id int4 DEFAULT 2 NOT NULL,
    professional_created_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT professional_pkey PRIMARY KEY (professional_id),
    CONSTRAINT fk_professional_business_ud FOREIGN KEY (business_id) REFERENCES core.business(business_id),
    CONSTRAINT fk_professional_organization FOREIGN KEY (organization_id) REFERENCES core.organization(organization_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
ALTER TABLE core.professional OWNER TO adminboek;
GRANT UPDATE, INSERT, REFERENCES, TRUNCATE, DELETE, SELECT, TRIGGER
ON TABLE core.professional TO adminboek;
```

### Tabla `core.professional_profile`

- Esquema: `core`.
- Nombre sin esquema: `professional_profile`.
- Número de columnas: 24.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `professional_profile_id` | `bigserial` | No | NOT NULL |
| `professional_profile_person_id` | `int8` | No | NOT NULL |
| `professional_profile_status` | `varchar(20)` | Sí | DEFAULT 'active'::character varying NULL |
| `professional_profile_title` | `varchar(150)` | Sí | NULL |
| `professional_profile_bio` | `text` | Sí | NULL |
| `professional_profile_years_experience_total` | `int4` | Sí | NULL |
| `professional_profile_level` | `varchar(20)` | Sí | NULL |
| `professional_profile_visible_in_marketplace` | `bool` | Sí | DEFAULT false NULL |
| `professional_profile_available_for_work` | `bool` | Sí | DEFAULT false NULL |
| `professional_profile_available_for_freelance` | `bool` | Sí | DEFAULT false NULL |
| `professional_profile_accepts_private_clients` | `bool` | Sí | DEFAULT false NULL |
| `professional_profile_own_tools` | `bool` | Sí | DEFAULT false NULL |
| `professional_profile_travels_to_client` | `bool` | Sí | DEFAULT false NULL |
| `professional_profile_has_workspace` | `bool` | Sí | DEFAULT false NULL |
| `professional_profile_instagram` | `varchar(150)` | Sí | NULL |
| `professional_profile_facebook` | `varchar(150)` | Sí | NULL |
| `professional_profile_tiktok` | `varchar(150)` | Sí | NULL |
| `professional_profile_portfolio_url` | `text` | Sí | NULL |
| `professional_profile_public_profile_slug` | `varchar(200)` | Sí | NULL |
| `professional_profile_verified` | `bool` | Sí | DEFAULT false NULL |
| `professional_profile_verified_at` | `timestamp` | Sí | NULL |
| `professional_profile_created_at` | `timestamp` | Sí | DEFAULT now() NULL |
| `professional_profile_updated_at` | `timestamp` | Sí | NULL |
| `professional_profile_deleted_at` | `timestamp` | Sí | NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT professional_profile_pkey PRIMARY KEY (professional_profile_id)`
- `CONSTRAINT professional_profile_professional_profile_person_id_key UNIQUE (professional_profile_person_id)`
- `CONSTRAINT fk_professional_profile_person FOREIGN KEY (professional_profile_person_id) REFERENCES core.person(person_id)`

#### Referencias entrantes

- `core.professional_skill` (professional_skill_professional_profile_id) → `core.professional_profile` (professional_profile_id), restricción `fk_professional_skill_profile`.
- `core.professional_certification` (professional_certification_professional_profile_id) → `core.professional_profile` (professional_profile_id), restricción `fk_professional_certification_profile`.
- `core.professional_location` (professional_location_professional_profile_id) → `core.professional_profile` (professional_profile_id), restricción `fk_professional_location_profile`.

#### DDL documentado

```sql
CREATE TABLE core.professional_profile (
	professional_profile_id bigserial NOT NULL,
	professional_profile_person_id int8 NOT NULL,
	professional_profile_status varchar(20) DEFAULT 'active'::character varying NULL,
	professional_profile_title varchar(150) NULL,
	professional_profile_bio text NULL,
	professional_profile_years_experience_total int4 NULL,
	professional_profile_level varchar(20) NULL,
	professional_profile_visible_in_marketplace bool DEFAULT false NULL,
	professional_profile_available_for_work bool DEFAULT false NULL,
	professional_profile_available_for_freelance bool DEFAULT false NULL,
	professional_profile_accepts_private_clients bool DEFAULT false NULL,
	professional_profile_own_tools bool DEFAULT false NULL,
	professional_profile_travels_to_client bool DEFAULT false NULL,
	professional_profile_has_workspace bool DEFAULT false NULL,
	professional_profile_instagram varchar(150) NULL,
	professional_profile_facebook varchar(150) NULL,
	professional_profile_tiktok varchar(150) NULL,
	professional_profile_portfolio_url text NULL,
	professional_profile_public_profile_slug varchar(200) NULL,
	professional_profile_verified bool DEFAULT false NULL,
	professional_profile_verified_at timestamp NULL,
	professional_profile_created_at timestamp DEFAULT now() NULL,
	professional_profile_updated_at timestamp NULL,
	professional_profile_deleted_at timestamp NULL,
	CONSTRAINT professional_profile_pkey PRIMARY KEY (professional_profile_id),
	CONSTRAINT professional_profile_professional_profile_person_id_key UNIQUE (professional_profile_person_id),
	CONSTRAINT fk_professional_profile_person FOREIGN KEY (professional_profile_person_id) REFERENCES core.person(person_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.professional_profile OWNER TO adminboek;
GRANT ALL ON TABLE core.professional_profile TO adminboek;
```

### Tabla `core.professional_skill`

- Esquema: `core`.
- Nombre sin esquema: `professional_skill`.
- Número de columnas: 8.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `professional_skill_id` | `bigserial` | No | NOT NULL |
| `professional_skill_professional_profile_id` | `int8` | No | NOT NULL |
| `professional_skill_skill_id` | `int4` | No | NOT NULL |
| `professional_skill_years_experience` | `int4` | Sí | NULL |
| `professional_skill_level` | `varchar(20)` | Sí | NULL |
| `professional_skill_is_primary` | `bool` | Sí | DEFAULT false NULL |
| `professional_skill_created_at` | `timestamp` | Sí | DEFAULT now() NULL |
| `professional_skill_updated_at` | `timestamp` | Sí | NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT professional_skill_pkey PRIMARY KEY (professional_skill_id)`
- `CONSTRAINT fk_professional_skill_profile FOREIGN KEY (professional_skill_professional_profile_id) REFERENCES core.professional_profile(professional_profile_id)`
- `CONSTRAINT fk_professional_skill_skill FOREIGN KEY (professional_skill_skill_id) REFERENCES core.skill(skill_id)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE core.professional_skill (
	professional_skill_id bigserial NOT NULL,
	professional_skill_professional_profile_id int8 NOT NULL,
	professional_skill_skill_id int4 NOT NULL,
	professional_skill_years_experience int4 NULL,
	professional_skill_level varchar(20) NULL,
	professional_skill_is_primary bool DEFAULT false NULL,
	professional_skill_created_at timestamp DEFAULT now() NULL,
	professional_skill_updated_at timestamp NULL,
	CONSTRAINT professional_skill_pkey PRIMARY KEY (professional_skill_id),
	CONSTRAINT fk_professional_skill_profile FOREIGN KEY (professional_skill_professional_profile_id) REFERENCES core.professional_profile(professional_profile_id),
	CONSTRAINT fk_professional_skill_skill FOREIGN KEY (professional_skill_skill_id) REFERENCES core.skill(skill_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.professional_skill OWNER TO adminboek;
GRANT ALL ON TABLE core.professional_skill TO adminboek;
```

### Tabla `core.role_menu`

- Esquema: `core`.
- Nombre sin esquema: `role_menu`.
- Número de columnas: 2.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `role_id` | `int8` | No | NOT NULL |
| `menu_id` | `int8` | No | NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT role_menu_pkey PRIMARY KEY (role_id, menu_id)`
- `CONSTRAINT role_menu_menu_id_fkey FOREIGN KEY (menu_id) REFERENCES core.menu(menu_id) ON DELETE CASCADE`
- `CONSTRAINT role_menu_role_id_fkey FOREIGN KEY (role_id) REFERENCES core."role"(role_id) ON DELETE CASCADE`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE core.role_menu (
	role_id int8 NOT NULL,
	menu_id int8 NOT NULL,
	CONSTRAINT role_menu_pkey PRIMARY KEY (role_id, menu_id),
	CONSTRAINT role_menu_menu_id_fkey FOREIGN KEY (menu_id) REFERENCES core.menu(menu_id) ON DELETE CASCADE,
	CONSTRAINT role_menu_role_id_fkey FOREIGN KEY (role_id) REFERENCES core."role"(role_id) ON DELETE CASCADE
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.role_menu OWNER TO adminboek;
GRANT ALL ON TABLE core.role_menu TO adminboek;
```

### Tabla `core.role_permission`

- Esquema: `core`.
- Nombre sin esquema: `role_permission`.
- Número de columnas: 2.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `role_id` | `int8` | No | NOT NULL |
| `permission_id` | `int8` | No | NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT role_permission_pkey PRIMARY KEY (role_id, permission_id)`
- `CONSTRAINT role_permission_permission_id_fkey FOREIGN KEY (permission_id) REFERENCES core."permission"(permission_id) ON DELETE CASCADE`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE core.role_permission (
	role_id int8 NOT NULL,
	permission_id int8 NOT NULL,
	CONSTRAINT role_permission_pkey PRIMARY KEY (role_id, permission_id),
	CONSTRAINT role_permission_permission_id_fkey FOREIGN KEY (permission_id) REFERENCES core."permission"(permission_id) ON DELETE CASCADE
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.role_permission OWNER TO adminboek;
GRANT ALL ON TABLE core.role_permission TO adminboek;
```

### Tabla `core.service_catalog`

- Esquema: `core`.
- Nombre sin esquema: `service_catalog`.
- Número de columnas: 14.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `template_catalog_node_id` | `uuid` | No | DEFAULT gen_random_uuid() NOT NULL |
| `service_template_id` | `uuid` | No | NOT NULL |
| `parent_node_id` | `uuid` | Sí | NULL |
| `node_type` | `varchar(20)` | No | NOT NULL |
| `node_name` | `varchar(150)` | No | NOT NULL |
| `service_short_name` | `varchar(50)` | Sí | NULL |
| `service_professional_price` | `numeric(14, 2)` | Sí | NULL |
| `service_list_price` | `numeric(14, 2)` | Sí | NULL |
| `service_deposit` | `numeric(14, 2)` | Sí | NULL |
| `service_duration_minutes` | `int4` | Sí | NULL |
| `node_order` | `int4` | No | DEFAULT 0 NOT NULL |
| `node_status` | `varchar(20)` | No | DEFAULT 'ACTIVE'::character varying NOT NULL |
| `node_created_at` | `timestamptz` | No | DEFAULT now() NOT NULL |
| `node_updated_at` | `timestamptz` | No | DEFAULT now() NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT chk_service_catalog_category_without_service_data CHECK ((((node_type)::text <> 'CATEGORY'::text) OR ((service_short_name IS NULL) AND (service_list_price IS NULL) AND (service_professional_price IS NULL) AND (service_deposit IS NULL) AND (service_duration_minutes IS NULL))))`
- `CONSTRAINT chk_service_catalog_node_type CHECK (((node_type)::text = ANY (ARRAY[('CATEGORY'::character varying)::text, ('SERVICE'::character varying)::text])))`
- `CONSTRAINT chk_service_catalog_non_negative_values CHECK (((COALESCE(service_professional_price, (0)::numeric) >= (0)::numeric) AND (COALESCE(service_list_price, (0)::numeric) >= (0)::numeric) AND (COALESCE(service_deposit, (0)::numeric) >= (0)::numeric) AND (COALESCE(service_duration_minutes, 0) >= 0)))`
- `CONSTRAINT chk_service_catalog_service_required_data CHECK ((((node_type)::text <> 'SERVICE'::text) OR ((service_short_name IS NOT NULL) AND (service_list_price IS NOT NULL) AND (service_professional_price IS NOT NULL) AND (service_duration_minutes IS NOT NULL))))`
- `CONSTRAINT chk_service_catalog_status CHECK (((node_status)::text = ANY (ARRAY[('ACTIVE'::character varying)::text, ('INACTIVE'::character varying)::text])))`
- `CONSTRAINT service_catalog_pkey PRIMARY KEY (template_catalog_node_id)`
- `CONSTRAINT fk_service_catalog FOREIGN KEY (service_template_id) REFERENCES core.service(service_template_id)`
- `CONSTRAINT fk_service_catalog_parent FOREIGN KEY (parent_node_id) REFERENCES core.service_catalog(template_catalog_node_id)`

#### Referencias entrantes

- `core.service_catalog` (parent_node_id) → `core.service_catalog` (template_catalog_node_id), restricción `fk_service_catalog_parent`.
- `core.professional_service` (service_catalog_node_id) → `core.service_catalog` (template_catalog_node_id), restricción `fk_professional_service_catalog`.

#### DDL documentado

```sql
CREATE TABLE core.service_catalog (
	template_catalog_node_id uuid DEFAULT gen_random_uuid() NOT NULL,
	service_template_id uuid NOT NULL,
	parent_node_id uuid NULL,
	node_type varchar(20) NOT NULL,
	node_name varchar(150) NOT NULL,
	service_short_name varchar(50) NULL,
	service_professional_price numeric(14, 2) NULL,
	service_list_price numeric(14, 2) NULL,
	service_deposit numeric(14, 2) NULL,
	service_duration_minutes int4 NULL,
	node_order int4 DEFAULT 0 NOT NULL,
	node_status varchar(20) DEFAULT 'ACTIVE'::character varying NOT NULL,
	node_created_at timestamptz DEFAULT now() NOT NULL,
	node_updated_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT chk_service_catalog_category_without_service_data CHECK ((((node_type)::text <> 'CATEGORY'::text) OR ((service_short_name IS NULL) AND (service_list_price IS NULL) AND (service_professional_price IS NULL) AND (service_deposit IS NULL) AND (service_duration_minutes IS NULL)))),
	CONSTRAINT chk_service_catalog_node_type CHECK (((node_type)::text = ANY (ARRAY[('CATEGORY'::character varying)::text, ('SERVICE'::character varying)::text]))),
	CONSTRAINT chk_service_catalog_non_negative_values CHECK (((COALESCE(service_professional_price, (0)::numeric) >= (0)::numeric) AND (COALESCE(service_list_price, (0)::numeric) >= (0)::numeric) AND (COALESCE(service_deposit, (0)::numeric) >= (0)::numeric) AND (COALESCE(service_duration_minutes, 0) >= 0))),
	CONSTRAINT chk_service_catalog_service_required_data CHECK ((((node_type)::text <> 'SERVICE'::text) OR ((service_short_name IS NOT NULL) AND (service_list_price IS NOT NULL) AND (service_professional_price IS NOT NULL) AND (service_duration_minutes IS NOT NULL)))),
	CONSTRAINT chk_service_catalog_status CHECK (((node_status)::text = ANY (ARRAY[('ACTIVE'::character varying)::text, ('INACTIVE'::character varying)::text]))),
	CONSTRAINT service_catalog_pkey PRIMARY KEY (template_catalog_node_id),
	CONSTRAINT fk_service_catalog FOREIGN KEY (service_template_id) REFERENCES core.service(service_template_id),
	CONSTRAINT fk_service_catalog_parent FOREIGN KEY (parent_node_id) REFERENCES core.service_catalog(template_catalog_node_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.service_catalog OWNER TO adminboek;
```

### Tabla `core.service_template_catalog`

- Esquema: `core`.
- Nombre sin esquema: `service_template_catalog`.
- Número de columnas: 14.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `template_catalog_node_id` | `uuid` | No | DEFAULT gen_random_uuid() NOT NULL |
| `service_template_id` | `uuid` | No | NOT NULL |
| `parent_node_id` | `uuid` | Sí | NULL |
| `node_type` | `varchar(20)` | No | NOT NULL |
| `node_name` | `varchar(150)` | No | NOT NULL |
| `service_short_name` | `varchar(50)` | Sí | NULL |
| `service_professional_price` | `numeric(14, 2)` | Sí | NULL |
| `service_list_price` | `numeric(14, 2)` | Sí | NULL |
| `service_deposit` | `numeric(14, 2)` | Sí | NULL |
| `service_duration_minutes` | `int4` | Sí | NULL |
| `node_order` | `int4` | No | DEFAULT 0 NOT NULL |
| `node_status` | `varchar(20)` | No | DEFAULT 'ACTIVE'::character varying NOT NULL |
| `node_created_at` | `timestamptz` | No | DEFAULT now() NOT NULL |
| `node_updated_at` | `timestamptz` | No | DEFAULT now() NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT chk_template_catalog_category_without_service_data CHECK ((((node_type)::text <> 'CATEGORY'::text) OR ((service_short_name IS NULL) AND (service_list_price IS NULL) AND (service_professional_price IS NULL) AND (service_deposit IS NULL) AND (service_duration_minutes IS NULL))))`
- `CONSTRAINT chk_template_catalog_node_type CHECK (((node_type)::text = ANY ((ARRAY['CATEGORY'::character varying, 'SERVICE'::character varying])::text[])))`
- `CONSTRAINT chk_template_catalog_non_negative_values CHECK (((COALESCE(service_professional_price, (0)::numeric) >= (0)::numeric) AND (COALESCE(service_list_price, (0)::numeric) >= (0)::numeric) AND (COALESCE(service_deposit, (0)::numeric) >= (0)::numeric) AND (COALESCE(service_duration_minutes, 0) >= 0)))`
- `CONSTRAINT chk_template_catalog_service_required_data CHECK ((((node_type)::text <> 'SERVICE'::text) OR ((service_short_name IS NOT NULL) AND (service_list_price IS NOT NULL) AND (service_professional_price IS NOT NULL) AND (service_duration_minutes IS NOT NULL))))`
- `CONSTRAINT chk_template_catalog_status CHECK (((node_status)::text = ANY ((ARRAY['ACTIVE'::character varying, 'INACTIVE'::character varying])::text[])))`
- `CONSTRAINT service_template_catalog_pkey PRIMARY KEY (template_catalog_node_id)`
- `CONSTRAINT fk_template_catalog_parent FOREIGN KEY (parent_node_id) REFERENCES core.service_template_catalog(template_catalog_node_id)`
- `CONSTRAINT fk_template_catalog_template FOREIGN KEY (service_template_id) REFERENCES core.service_template(service_template_id)`

#### Referencias entrantes

- `core.service_template_catalog` (parent_node_id) → `core.service_template_catalog` (template_catalog_node_id), restricción `fk_template_catalog_parent`.

#### DDL documentado

```sql
CREATE TABLE core.service_template_catalog (
	template_catalog_node_id uuid DEFAULT gen_random_uuid() NOT NULL,
	service_template_id uuid NOT NULL,
	parent_node_id uuid NULL,
	node_type varchar(20) NOT NULL,
	node_name varchar(150) NOT NULL,
	service_short_name varchar(50) NULL,
	service_professional_price numeric(14, 2) NULL,
	service_list_price numeric(14, 2) NULL,
	service_deposit numeric(14, 2) NULL,
	service_duration_minutes int4 NULL,
	node_order int4 DEFAULT 0 NOT NULL,
	node_status varchar(20) DEFAULT 'ACTIVE'::character varying NOT NULL,
	node_created_at timestamptz DEFAULT now() NOT NULL,
	node_updated_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT chk_template_catalog_category_without_service_data CHECK ((((node_type)::text <> 'CATEGORY'::text) OR ((service_short_name IS NULL) AND (service_list_price IS NULL) AND (service_professional_price IS NULL) AND (service_deposit IS NULL) AND (service_duration_minutes IS NULL)))),
	CONSTRAINT chk_template_catalog_node_type CHECK (((node_type)::text = ANY ((ARRAY['CATEGORY'::character varying, 'SERVICE'::character varying])::text[]))),
	CONSTRAINT chk_template_catalog_non_negative_values CHECK (((COALESCE(service_professional_price, (0)::numeric) >= (0)::numeric) AND (COALESCE(service_list_price, (0)::numeric) >= (0)::numeric) AND (COALESCE(service_deposit, (0)::numeric) >= (0)::numeric) AND (COALESCE(service_duration_minutes, 0) >= 0))),
	CONSTRAINT chk_template_catalog_service_required_data CHECK ((((node_type)::text <> 'SERVICE'::text) OR ((service_short_name IS NOT NULL) AND (service_list_price IS NOT NULL) AND (service_professional_price IS NOT NULL) AND (service_duration_minutes IS NOT NULL)))),
	CONSTRAINT chk_template_catalog_status CHECK (((node_status)::text = ANY ((ARRAY['ACTIVE'::character varying, 'INACTIVE'::character varying])::text[]))),
	CONSTRAINT service_template_catalog_pkey PRIMARY KEY (template_catalog_node_id),
	CONSTRAINT fk_template_catalog_parent FOREIGN KEY (parent_node_id) REFERENCES core.service_template_catalog(template_catalog_node_id),
	CONSTRAINT fk_template_catalog_template FOREIGN KEY (service_template_id) REFERENCES core.service_template(service_template_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.service_template_catalog OWNER TO adminboek;
GRANT ALL ON TABLE core.service_template_catalog TO adminboek;
```

### Tabla `core.theme`

- Esquema: `core`.
- Nombre sin esquema: `theme`.
- Número de columnas: 7.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `theme_id` | `int8` | No | GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL |
| `theme_tenant_id` | `int4` | No | NOT NULL |
| `theme_name` | `varchar(100)` | No | DEFAULT 'default'::character varying NOT NULL |
| `theme_tokens` | `jsonb` | No | NOT NULL |
| `theme_is_active` | `bool` | No | DEFAULT true NOT NULL |
| `theme_created_at` | `timestamp` | No | DEFAULT CURRENT_TIMESTAMP NOT NULL |
| `theme_updated_at` | `timestamp` | No | DEFAULT CURRENT_TIMESTAMP NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT theme_pkey PRIMARY KEY (theme_id)`
- `CONSTRAINT uq_theme_name UNIQUE (theme_tenant_id, theme_name)`
- `CONSTRAINT fk_theme FOREIGN KEY (theme_tenant_id) REFERENCES core.tenant(tenant_id) ON DELETE CASCADE`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE core.theme (
	theme_id int8 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL,
	theme_tenant_id int4 NOT NULL,
	theme_name varchar(100) DEFAULT 'default'::character varying NOT NULL,
	theme_tokens jsonb NOT NULL,
	theme_is_active bool DEFAULT true NOT NULL,
	theme_created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	theme_updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT theme_pkey PRIMARY KEY (theme_id),
	CONSTRAINT uq_theme_name UNIQUE (theme_tenant_id, theme_name),
	CONSTRAINT fk_theme FOREIGN KEY (theme_tenant_id) REFERENCES core.tenant(tenant_id) ON DELETE CASCADE
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.theme OWNER TO adminboek;
GRANT ALL ON TABLE core.theme TO adminboek;
```

### Tabla `core."user"`

- Esquema: `core`.
- Nombre sin esquema: `"user"`.
- Número de columnas: 9.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `user_id` | `int4` | No | GENERATED ALWAYS AS IDENTITY NOT NULL |
| `user_name` | `varchar(100)` | No | NOT NULL |
| `user_password_hash` | `text` | No | NOT NULL |
| `user_active` | `bool` | No | DEFAULT true NOT NULL |
| `user_created_at` | `timestamptz` | No | DEFAULT CURRENT_TIMESTAMP NOT NULL |
| `user_nickname` | `varchar(20)` | Sí | NULL |
| `user_firstname` | `varchar(100)` | Sí | NULL |
| `user_lastname` | `varchar(100)` | Sí | NULL |
| `user_group_id` | `int8` | No | NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT user_pkey PRIMARY KEY (user_id)`
- `CONSTRAINT user_user_name_key UNIQUE (user_name)`
- `CONSTRAINT user_user_group_id_fkey FOREIGN KEY (user_group_id) REFERENCES core."group"(group_id) ON DELETE RESTRICT`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE core."user" (
	user_id int4 GENERATED ALWAYS AS IDENTITY NOT NULL,
	user_name varchar(100) NOT NULL,
	user_password_hash text NOT NULL,
	user_active bool DEFAULT true NOT NULL,
	user_created_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	user_nickname varchar(20) NULL,
	user_firstname varchar(100) NULL,
	user_lastname varchar(100) NULL,
	user_group_id int8 NOT NULL,
	CONSTRAINT user_pkey PRIMARY KEY (user_id),
	CONSTRAINT user_user_name_key UNIQUE (user_name),
	CONSTRAINT user_user_group_id_fkey FOREIGN KEY (user_group_id) REFERENCES core."group"(group_id) ON DELETE RESTRICT
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core."user" OWNER TO adminboek;
GRANT ALL ON TABLE core."user" TO adminboek;
```

### Tabla `core.local_professional`

- Esquema: `core`.
- Nombre sin esquema: `local_professional`.
- Número de columnas: 4.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `local_professional_id` | `uuid` | No | DEFAULT gen_random_uuid() NOT NULL |
| `business_id` | `uuid` | No | NOT NULL |
| `local_id` | `uuid` | No | NOT NULL |
| `professional_id` | `uuid` | No | NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT local_professional_id_pkey PRIMARY KEY (local_professional_id)`
- `CONSTRAINT uq_local_professional UNIQUE (business_id, local_id, professional_id)`
- `CONSTRAINT fk_local_professional_business_id FOREIGN KEY (business_id) REFERENCES core.business(business_id)`
- `CONSTRAINT fk_local_professional_local_id FOREIGN KEY (local_id) REFERENCES core."local"(local_id)`
- `CONSTRAINT fk_local_professional_professional_id FOREIGN KEY (professional_id) REFERENCES core.professional(professional_id)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE core.local_professional (
	local_professional_id uuid DEFAULT gen_random_uuid() NOT NULL,
	business_id uuid NOT NULL,
	local_id uuid NOT NULL,
	professional_id uuid NOT NULL,
	CONSTRAINT local_professional_id_pkey PRIMARY KEY (local_professional_id),
	CONSTRAINT uq_local_professional UNIQUE (business_id, local_id, professional_id),
	CONSTRAINT fk_local_professional_business_id FOREIGN KEY (business_id) REFERENCES core.business(business_id),
	CONSTRAINT fk_local_professional_local_id FOREIGN KEY (local_id) REFERENCES core."local"(local_id),
	CONSTRAINT fk_local_professional_professional_id FOREIGN KEY (professional_id) REFERENCES core.professional(professional_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.local_professional OWNER TO adminboek;
```

### Tabla `core.professional_certification`

- Esquema: `core`.
- Nombre sin esquema: `professional_certification`.
- Número de columnas: 8.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `professional_certification_id` | `bigserial` | No | NOT NULL |
| `professional_certification_professional_profile_id` | `int8` | No | NOT NULL |
| `professional_certification_name` | `varchar(200)` | No | NOT NULL |
| `professional_certification_institution` | `varchar(200)` | Sí | NULL |
| `professional_certification_year` | `int4` | Sí | NULL |
| `professional_certification_description` | `text` | Sí | NULL |
| `professional_certification_created_at` | `timestamp` | Sí | DEFAULT now() NULL |
| `professional_certification_updated_at` | `timestamp` | Sí | NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT professional_certification_pkey PRIMARY KEY (professional_certification_id)`
- `CONSTRAINT fk_professional_certification_profile FOREIGN KEY (professional_certification_professional_profile_id) REFERENCES core.professional_profile(professional_profile_id)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE core.professional_certification (
	professional_certification_id bigserial NOT NULL,
	professional_certification_professional_profile_id int8 NOT NULL,
	professional_certification_name varchar(200) NOT NULL,
	professional_certification_institution varchar(200) NULL,
	professional_certification_year int4 NULL,
	professional_certification_description text NULL,
	professional_certification_created_at timestamp DEFAULT now() NULL,
	professional_certification_updated_at timestamp NULL,
	CONSTRAINT professional_certification_pkey PRIMARY KEY (professional_certification_id),
	CONSTRAINT fk_professional_certification_profile FOREIGN KEY (professional_certification_professional_profile_id) REFERENCES core.professional_profile(professional_profile_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.professional_certification OWNER TO adminboek;
GRANT ALL ON TABLE core.professional_certification TO adminboek;
```

### Tabla `core.professional_location`

- Esquema: `core`.
- Nombre sin esquema: `professional_location`.
- Número de columnas: 9.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `professional_location_id` | `bigserial` | No | NOT NULL |
| `professional_location_professional_profile_id` | `int8` | No | NOT NULL |
| `professional_location_commune` | `varchar(120)` | No | NOT NULL |
| `professional_location_city` | `varchar(120)` | No | NOT NULL |
| `professional_location_region` | `varchar(120)` | No | NOT NULL |
| `professional_location_country` | `varchar(120)` | Sí | DEFAULT 'Chile'::character varying NULL |
| `professional_location_is_primary` | `bool` | Sí | DEFAULT false NULL |
| `professional_location_created_at` | `timestamp` | Sí | DEFAULT now() NULL |
| `professional_location_updated_at` | `timestamp` | Sí | NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT professional_location_pkey PRIMARY KEY (professional_location_id)`
- `CONSTRAINT fk_professional_location_profile FOREIGN KEY (professional_location_professional_profile_id) REFERENCES core.professional_profile(professional_profile_id)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE core.professional_location (
	professional_location_id bigserial NOT NULL,
	professional_location_professional_profile_id int8 NOT NULL,
	professional_location_commune varchar(120) NOT NULL,
	professional_location_city varchar(120) NOT NULL,
	professional_location_region varchar(120) NOT NULL,
	professional_location_country varchar(120) DEFAULT 'Chile'::character varying NULL,
	professional_location_is_primary bool DEFAULT false NULL,
	professional_location_created_at timestamp DEFAULT now() NULL,
	professional_location_updated_at timestamp NULL,
	CONSTRAINT professional_location_pkey PRIMARY KEY (professional_location_id),
	CONSTRAINT fk_professional_location_profile FOREIGN KEY (professional_location_professional_profile_id) REFERENCES core.professional_profile(professional_profile_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.professional_location OWNER TO adminboek;
GRANT ALL ON TABLE core.professional_location TO adminboek;
```

### Tabla `core.professional_service`

- Esquema: `core`.
- Nombre sin esquema: `professional_service`.
- Número de columnas: 8.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `professional_service_id` | `uuid` | No | DEFAULT gen_random_uuid() NOT NULL |
| `business_id` | `uuid` | No | NOT NULL |
| `local_id` | `uuid` | No | NOT NULL |
| `professional_id` | `uuid` | No | NOT NULL |
| `service_catalog_node_id` | `uuid` | No | NOT NULL |
| `professional_service_status` | `varchar(20)` | No | DEFAULT 'ACTIVE'::character varying NOT NULL |
| `professional_service_created_at` | `timestamptz` | No | DEFAULT now() NOT NULL |
| `professional_service_updated_at` | `timestamptz` | No | DEFAULT now() NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT chk_professional_service_status CHECK (((professional_service_status)::text = ANY ((ARRAY['ACTIVE'::character varying, 'INACTIVE'::character varying])::text[])))`
- `CONSTRAINT professional_service_pkey PRIMARY KEY (professional_service_id)`
- `CONSTRAINT uq_professional_service UNIQUE (business_id, local_id, professional_id, service_catalog_node_id)`
- `CONSTRAINT fk_professional_service_business FOREIGN KEY (business_id) REFERENCES core.business(business_id)`
- `CONSTRAINT fk_professional_service_catalog FOREIGN KEY (service_catalog_node_id) REFERENCES core.service_catalog(template_catalog_node_id)`
- `CONSTRAINT fk_professional_service_local FOREIGN KEY (local_id) REFERENCES core."local"(local_id)`
- `CONSTRAINT fk_professional_service_professional FOREIGN KEY (professional_id) REFERENCES core.professional(professional_id)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE core.professional_service (
	professional_service_id uuid DEFAULT gen_random_uuid() NOT NULL,
	business_id uuid NOT NULL,
	local_id uuid NOT NULL,
	professional_id uuid NOT NULL,
	service_catalog_node_id uuid NOT NULL,
	professional_service_status varchar(20) DEFAULT 'ACTIVE'::character varying NOT NULL,
	professional_service_created_at timestamptz DEFAULT now() NOT NULL,
	professional_service_updated_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT chk_professional_service_status CHECK (((professional_service_status)::text = ANY ((ARRAY['ACTIVE'::character varying, 'INACTIVE'::character varying])::text[]))),
	CONSTRAINT professional_service_pkey PRIMARY KEY (professional_service_id),
	CONSTRAINT uq_professional_service UNIQUE (business_id, local_id, professional_id, service_catalog_node_id),
	CONSTRAINT fk_professional_service_business FOREIGN KEY (business_id) REFERENCES core.business(business_id),
	CONSTRAINT fk_professional_service_catalog FOREIGN KEY (service_catalog_node_id) REFERENCES core.service_catalog(template_catalog_node_id),
	CONSTRAINT fk_professional_service_local FOREIGN KEY (local_id) REFERENCES core."local"(local_id),
	CONSTRAINT fk_professional_service_professional FOREIGN KEY (professional_id) REFERENCES core.professional(professional_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE core.professional_service OWNER TO adminboek;
GRANT ALL ON TABLE core.professional_service TO adminboek;
```

## Esquema `lindasylunaticas`

### Tabla `lindasylunaticas.ai_whatsapp_conversation`

- Esquema: `lindasylunaticas`.
- Nombre sin esquema: `ai_whatsapp_conversation`.
- Número de columnas: 25.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `phone` | `varchar(20)` | No | NOT NULL |
| `whatsapp_name` | `varchar(120)` | Sí | NULL |
| `customer_id` | `uuid` | Sí | NULL |
| `customer_name` | `varchar(200)` | Sí | NULL |
| `state` | `varchar(60)` | No | DEFAULT 'NUEVA'::character varying NOT NULL |
| `control_state` | `varchar(60)` | No | DEFAULT 'BOT_ACTIVO'::character varying NOT NULL |
| `bot_enabled` | `bool` | No | DEFAULT true NOT NULL |
| `intent` | `varchar(80)` | Sí | NULL |
| `service_family` | `varchar(100)` | Sí | NULL |
| `service_detail` | `text` | Sí | NULL |
| `preferred_day` | `text` | Sí | NULL |
| `preferred_time` | `text` | Sí | NULL |
| `preferred_professional` | `text` | Sí | NULL |
| `notes` | `text` | Sí | NULL |
| `has_photos` | `bool` | No | DEFAULT false NOT NULL |
| `summary_json` | `jsonb` | Sí | NULL |
| `last_customer_message` | `text` | Sí | NULL |
| `last_customer_message_at` | `timestamp` | Sí | NULL |
| `window_expires_at` | `timestamp` | Sí | NULL |
| `last_bot_message_at` | `timestamp` | Sí | NULL |
| `last_human_message_at` | `timestamp` | Sí | NULL |
| `assigned_to` | `varchar(100)` | Sí | NULL |
| `handoff_reason` | `text` | Sí | NULL |
| `created_at` | `timestamp` | No | DEFAULT now() NOT NULL |
| `updated_at` | `timestamp` | No | DEFAULT now() NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT ai_whatsapp_conversation_pkey PRIMARY KEY (phone)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE lindasylunaticas.ai_whatsapp_conversation (
	phone varchar(20) NOT NULL,
	whatsapp_name varchar(120) NULL,
	customer_id uuid NULL,
	customer_name varchar(200) NULL,
	state varchar(60) DEFAULT 'NUEVA'::character varying NOT NULL,
	control_state varchar(60) DEFAULT 'BOT_ACTIVO'::character varying NOT NULL,
	bot_enabled bool DEFAULT true NOT NULL,
	intent varchar(80) NULL,
	service_family varchar(100) NULL,
	service_detail text NULL,
	preferred_day text NULL,
	preferred_time text NULL,
	preferred_professional text NULL,
	notes text NULL,
	has_photos bool DEFAULT false NOT NULL,
	summary_json jsonb NULL,
	last_customer_message text NULL,
	last_customer_message_at timestamp NULL,
	window_expires_at timestamp NULL,
	last_bot_message_at timestamp NULL,
	last_human_message_at timestamp NULL,
	assigned_to varchar(100) NULL,
	handoff_reason text NULL,
	created_at timestamp DEFAULT now() NOT NULL,
	updated_at timestamp DEFAULT now() NOT NULL,
	CONSTRAINT ai_whatsapp_conversation_pkey PRIMARY KEY (phone)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE lindasylunaticas.ai_whatsapp_conversation OWNER TO adminboek;
GRANT ALL ON TABLE lindasylunaticas.ai_whatsapp_conversation TO adminboek;
```

### Tabla `lindasylunaticas.ai_whatsapp_state`

- Esquema: `lindasylunaticas`.
- Nombre sin esquema: `ai_whatsapp_state`.
- Número de columnas: 5.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `phone` | `varchar(20)` | No | NOT NULL |
| `state` | `varchar(50)` | No | NOT NULL |
| `last_message` | `text` | Sí | NULL |
| `created_at` | `timestamp` | No | DEFAULT now() NOT NULL |
| `updated_at` | `timestamp` | No | DEFAULT now() NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT ai_whatsapp_state_pkey PRIMARY KEY (phone)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE lindasylunaticas.ai_whatsapp_state (
	phone varchar(20) NOT NULL,
	state varchar(50) NOT NULL,
	last_message text NULL,
	created_at timestamp DEFAULT now() NOT NULL,
	updated_at timestamp DEFAULT now() NOT NULL,
	CONSTRAINT ai_whatsapp_state_pkey PRIMARY KEY (phone)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE lindasylunaticas.ai_whatsapp_state OWNER TO adminboek;
GRANT ALL ON TABLE lindasylunaticas.ai_whatsapp_state TO adminboek;
```

### Tabla `lindasylunaticas.customer`

- Esquema: `lindasylunaticas`.
- Nombre sin esquema: `customer`.
- Número de columnas: 27.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `customer_id` | `int4` | No | GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL |
| `customer_type` | `varchar(20)` | No | DEFAULT 'person'::character varying NOT NULL |
| `customer_first_name` | `varchar(120)` | Sí | NULL |
| `customer_middle_name` | `varchar(50)` | Sí | NULL |
| `customer_last_name` | `varchar(120)` | Sí | NULL |
| `customer_second_name` | `varchar(50)` | Sí | NULL |
| `customer_full_name` | `varchar(250)` | Sí | NULL |
| `customer_nickname` | `varchar(120)` | Sí | NULL |
| `customer_mobile` | `varchar(20)` | Sí | NULL |
| `customer_idtype` | `varchar(20)` | Sí | NULL |
| `customer_personalid` | `varchar(30)` | Sí | NULL |
| `customer_company_name` | `varchar(250)` | Sí | NULL |
| `customer_trade_name` | `varchar(250)` | Sí | NULL |
| `customer_birth_date` | `date` | Sí | NULL |
| `customer_gender` | `varchar(20)` | Sí | NULL |
| `customer_taxpayer_type` | `varchar(20)` | Sí | NULL |
| `customer_tax_document_preference` | `varchar(20)` | Sí | NULL |
| `customer_status` | `varchar(20)` | Sí | DEFAULT 'active'::character varying NULL |
| `customer_category` | `varchar(50)` | Sí | NULL |
| `customer_notes` | `text` | Sí | NULL |
| `customer_created_at` | `timestamptz` | Sí | DEFAULT CURRENT_TIMESTAMP NULL |
| `customer_updated_at` | `timestamptz` | Sí | DEFAULT CURRENT_TIMESTAMP NULL |
| `customer_deleted_at` | `timestamptz` | Sí | NULL |
| `customer_email` | `varchar(50)` | Sí | NULL |
| `customer_idtypekey` | `varchar(60)` | Sí | GENERATED ALWAYS AS ((((COALESCE(customer_idtype, ''::character varying)::text &#124;&#124; '&#124;'::text) &#124;&#124; COALESCE(customer_personalid, ''::character varying)::text))) STORED NULL |
| `customer_fullsearch` | `varchar(500)` | Sí | GENERATED ALWAYS AS (upper((((((((COALESCE(customer_full_name, ''::character varying)::text &#124;&#124; ' '::text) &#124;&#124; COALESCE(customer_nickname, ''::character varying)::text) &#124;&#124; ' '::text) &#124;&#124; COALESCE(customer_mobile, ''::character varying)::text) &#124;&#124; ' '::text) &#124;&#124; COALESCE(customer_idtype, ''::character varying)::text) &#124;&#124; ' '::text) &#124;&#124; COALESCE(customer_personalid, ''::character varying)::text)) STORED NULL |
| `organization_id` | `int4` | Sí | DEFAULT 2 NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT customer_pkey PRIMARY KEY (customer_id)`

#### Referencias entrantes

- `lindasylunaticas.ticket_professional_customer` (customer_id) → `lindasylunaticas.customer` (customer_id), restricción `fk_tpc_customer`.

#### DDL documentado

```sql
CREATE TABLE lindasylunaticas.customer (
	customer_id int4 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL,
	customer_type varchar(20) DEFAULT 'person'::character varying NOT NULL,
	customer_first_name varchar(120) NULL,
	customer_middle_name varchar(50) NULL,
	customer_last_name varchar(120) NULL,
	customer_second_name varchar(50) NULL,
	customer_full_name varchar(250) NULL,
	customer_nickname varchar(120) NULL,
	customer_mobile varchar(20) NULL,
	customer_idtype varchar(20) NULL,
	customer_personalid varchar(30) NULL,
	customer_company_name varchar(250) NULL,
	customer_trade_name varchar(250) NULL,
	customer_birth_date date NULL,
	customer_gender varchar(20) NULL,
	customer_taxpayer_type varchar(20) NULL,
	customer_tax_document_preference varchar(20) NULL,
	customer_status varchar(20) DEFAULT 'active'::character varying NULL,
	customer_category varchar(50) NULL,
	customer_notes text NULL,
	customer_created_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	customer_updated_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	customer_deleted_at timestamptz NULL,
	customer_email varchar(50) NULL,
	customer_idtypekey varchar(60) GENERATED ALWAYS AS ((((COALESCE(customer_idtype, ''::character varying)::text || '|'::text) || COALESCE(customer_personalid, ''::character varying)::text))) STORED NULL,
	customer_fullsearch varchar(500) GENERATED ALWAYS AS (upper((((((((COALESCE(customer_full_name, ''::character varying)::text || ' '::text) || COALESCE(customer_nickname, ''::character varying)::text) || ' '::text) || COALESCE(customer_mobile, ''::character varying)::text) || ' '::text) || COALESCE(customer_idtype, ''::character varying)::text) || ' '::text) || COALESCE(customer_personalid, ''::character varying)::text)) STORED NULL,
	organization_id int4 DEFAULT 2 NULL,
	CONSTRAINT customer_pkey PRIMARY KEY (customer_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
CREATE INDEX ix_customer_fullsearch ON lindasylunaticas.customer USING gin (customer_fullsearch gin_trgm_ops);
CREATE UNIQUE INDEX ux_customer_idtypekey ON lindasylunaticas.customer USING btree (customer_idtypekey);

-- Table Triggers

create trigger trg_customer_updated_at before
update
    on
    lindasylunaticas.customer for each row execute function lindasylunaticas.set_customer_updated_at();

-- Permissions

ALTER TABLE lindasylunaticas.customer OWNER TO adminboek;
GRANT ALL ON TABLE lindasylunaticas.customer TO adminboek;
```

### Tabla `lindasylunaticas.customer_capture_settings`

- Esquema: `lindasylunaticas`.
- Nombre sin esquema: `customer_capture_settings`.
- Número de columnas: 13.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `customer_capture_settings_id` | `uuid` | No | DEFAULT gen_random_uuid() NOT NULL |
| `customer_capture_settings_field` | `varchar(50)` | No | NOT NULL |
| `customer_capture_settings_is_primary` | `bool` | Sí | DEFAULT false NULL |
| `customer_capture_settings_is_required` | `bool` | Sí | DEFAULT false NULL |
| `customer_capture_settings_display_order` | `int4` | Sí | NULL |
| `customer_capture_settings_is_active` | `bool` | Sí | DEFAULT true NULL |
| `customer_capture_settings_created_at` | `timestamp` | Sí | DEFAULT CURRENT_TIMESTAMP NULL |
| `customer_capture_settings_label` | `varchar(120)` | Sí | NULL |
| `customer_capture_settings_input_type` | `varchar(30)` | Sí | NULL |
| `customer_capture_settings_validation` | `varchar(50)` | Sí | NULL |
| `customer_capture_settings_validation_config` | `jsonb` | Sí | NULL |
| `customer_capture_settings_validation_type` | `varchar(50)` | Sí | NULL |
| `customer_capture_settings_placeholder` | `varchar(150)` | Sí | NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT customer_capture_settings_pkey PRIMARY KEY (customer_capture_settings_id)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE lindasylunaticas.customer_capture_settings (
	customer_capture_settings_id uuid DEFAULT gen_random_uuid() NOT NULL,
	customer_capture_settings_field varchar(50) NOT NULL,
	customer_capture_settings_is_primary bool DEFAULT false NULL,
	customer_capture_settings_is_required bool DEFAULT false NULL,
	customer_capture_settings_display_order int4 NULL,
	customer_capture_settings_is_active bool DEFAULT true NULL,
	customer_capture_settings_created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	customer_capture_settings_label varchar(120) NULL,
	customer_capture_settings_input_type varchar(30) NULL,
	customer_capture_settings_validation varchar(50) NULL,
	customer_capture_settings_validation_config jsonb NULL,
	customer_capture_settings_validation_type varchar(50) NULL,
	customer_capture_settings_placeholder varchar(150) NULL,
	CONSTRAINT customer_capture_settings_pkey PRIMARY KEY (customer_capture_settings_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE lindasylunaticas.customer_capture_settings OWNER TO adminboek;
GRANT ALL ON TABLE lindasylunaticas.customer_capture_settings TO adminboek;
```

### Tabla `lindasylunaticas.customer_identifiers`

- Esquema: `lindasylunaticas`.
- Nombre sin esquema: `customer_identifiers`.
- Número de columnas: 6.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `customer_identifiers_id` | `uuid` | No | DEFAULT gen_random_uuid() NOT NULL |
| `customer_identifiers_customers_id` | `uuid` | No | NOT NULL |
| `customer_identifiers_type_code` | `varchar(50)` | No | NOT NULL |
| `customer_identifiers_value` | `varchar(200)` | No | NOT NULL |
| `customer_identifiers_is_primary` | `bool` | Sí | DEFAULT false NULL |
| `customer_identifiers_created_at` | `timestamp` | Sí | DEFAULT CURRENT_TIMESTAMP NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT customer_identifiers_pkey PRIMARY KEY (customer_identifiers_id)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE lindasylunaticas.customer_identifiers (
	customer_identifiers_id uuid DEFAULT gen_random_uuid() NOT NULL,
	customer_identifiers_customers_id uuid NOT NULL,
	customer_identifiers_type_code varchar(50) NOT NULL,
	customer_identifiers_value varchar(200) NOT NULL,
	customer_identifiers_is_primary bool DEFAULT false NULL,
	customer_identifiers_created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT customer_identifiers_pkey PRIMARY KEY (customer_identifiers_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
CREATE UNIQUE INDEX ux_customer_identifier_unique ON lindasylunaticas.customer_identifiers USING btree (customer_identifiers_type_code, customer_identifiers_value);
CREATE UNIQUE INDEX ux_customer_identifiers_rut ON lindasylunaticas.customer_identifiers USING btree (customer_identifiers_value) WHERE ((customer_identifiers_type_code)::text = 'RUT'::text);

-- Permissions

ALTER TABLE lindasylunaticas.customer_identifiers OWNER TO adminboek;
GRANT ALL ON TABLE lindasylunaticas.customer_identifiers TO adminboek;
```

### Tabla `lindasylunaticas.customers`

- Esquema: `lindasylunaticas`.
- Nombre sin esquema: `customers`.
- Número de columnas: 27.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `customers_id` | `uuid` | No | DEFAULT gen_random_uuid() NOT NULL |
| `customers_type` | `varchar(20)` | No | DEFAULT 'person'::character varying NOT NULL |
| `customers_first_name` | `varchar(120)` | Sí | NULL |
| `customers_middle_name` | `varchar(50)` | Sí | NULL |
| `customers_last_name` | `varchar(120)` | Sí | NULL |
| `customers_second_name` | `varchar(50)` | Sí | NULL |
| `customers_full_name` | `varchar(250)` | Sí | NULL |
| `customers_nickname` | `varchar(120)` | Sí | NULL |
| `customers_mobile` | `varchar(20)` | Sí | NULL |
| `customers_idtype` | `varchar(20)` | Sí | NULL |
| `customers_personalid` | `varchar(30)` | Sí | NULL |
| `customers_company_name` | `varchar(250)` | Sí | NULL |
| `customers_trade_name` | `varchar(250)` | Sí | NULL |
| `customers_birth_date` | `date` | Sí | NULL |
| `customers_gender` | `varchar(20)` | Sí | NULL |
| `customers_taxpayer_type` | `varchar(20)` | Sí | NULL |
| `customers_tax_document_preference` | `varchar(20)` | Sí | NULL |
| `customers_status` | `varchar(20)` | Sí | DEFAULT 'active'::character varying NULL |
| `customers_category` | `varchar(50)` | Sí | NULL |
| `customers_notes` | `text` | Sí | NULL |
| `customers_created_at` | `timestamp` | Sí | DEFAULT CURRENT_TIMESTAMP NULL |
| `customers_updated_at` | `timestamp` | Sí | DEFAULT CURRENT_TIMESTAMP NULL |
| `customers_deleted_at` | `timestamp` | Sí | NULL |
| `customers_email` | `varchar(50)` | Sí | NULL |
| `customers_idtypekey` | `varchar(60)` | Sí | GENERATED ALWAYS AS ((((COALESCE(customers_idtype, ''::character varying)::text &#124;&#124; '&#124;'::text) &#124;&#124; COALESCE(customers_personalid, ''::character varying)::text))) STORED NULL |
| `customers_fullsearch` | `varchar(500)` | Sí | GENERATED ALWAYS AS (upper((((((((COALESCE(customers_full_name, ''::character varying)::text &#124;&#124; ' '::text) &#124;&#124; COALESCE(customers_nickname, ''::character varying)::text) &#124;&#124; ' '::text) &#124;&#124; COALESCE(customers_mobile, ''::character varying)::text) &#124;&#124; ' '::text) &#124;&#124; COALESCE(customers_idtype, ''::character varying)::text) &#124;&#124; ' '::text) &#124;&#124; COALESCE(customers_personalid, ''::character varying)::text)) STORED NULL |
| `organization_id` | `int4` | Sí | NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT customers_pkey PRIMARY KEY (customers_id)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE lindasylunaticas.customers (
	customers_id uuid DEFAULT gen_random_uuid() NOT NULL,
	customers_type varchar(20) DEFAULT 'person'::character varying NOT NULL,
	customers_first_name varchar(120) NULL,
	customers_middle_name varchar(50) NULL,
	customers_last_name varchar(120) NULL,
	customers_second_name varchar(50) NULL,
	customers_full_name varchar(250) NULL,
	customers_nickname varchar(120) NULL,
	customers_mobile varchar(20) NULL,
	customers_idtype varchar(20) NULL,
	customers_personalid varchar(30) NULL,
	customers_company_name varchar(250) NULL,
	customers_trade_name varchar(250) NULL,
	customers_birth_date date NULL,
	customers_gender varchar(20) NULL,
	customers_taxpayer_type varchar(20) NULL,
	customers_tax_document_preference varchar(20) NULL,
	customers_status varchar(20) DEFAULT 'active'::character varying NULL,
	customers_category varchar(50) NULL,
	customers_notes text NULL,
	customers_created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	customers_updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	customers_deleted_at timestamp NULL,
	customers_email varchar(50) NULL,
	customers_idtypekey varchar(60) GENERATED ALWAYS AS ((((COALESCE(customers_idtype, ''::character varying)::text || '|'::text) || COALESCE(customers_personalid, ''::character varying)::text))) STORED NULL,
	customers_fullsearch varchar(500) GENERATED ALWAYS AS (upper((((((((COALESCE(customers_full_name, ''::character varying)::text || ' '::text) || COALESCE(customers_nickname, ''::character varying)::text) || ' '::text) || COALESCE(customers_mobile, ''::character varying)::text) || ' '::text) || COALESCE(customers_idtype, ''::character varying)::text) || ' '::text) || COALESCE(customers_personalid, ''::character varying)::text)) STORED NULL,
	organization_id int4 NULL,
	CONSTRAINT customers_pkey PRIMARY KEY (customers_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
CREATE INDEX ix_customers_fullsearch ON lindasylunaticas.customers USING gin (customers_fullsearch gin_trgm_ops);
CREATE UNIQUE INDEX ux_customers_idtypekey ON lindasylunaticas.customers USING btree (customers_idtypekey);

-- Permissions

ALTER TABLE lindasylunaticas.customers OWNER TO adminboek;
GRANT UPDATE, INSERT, REFERENCES, TRUNCATE, DELETE, SELECT, TRIGGER ON TABLE lindasylunaticas.customers TO adminboek;
```

### Tabla `lindasylunaticas.customers_express`

- Esquema: `lindasylunaticas`.
- Nombre sin esquema: `customers_express`.
- Número de columnas: 11.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `customers_express_uuid` | `varchar(50)` | Sí | DEFAULT gen_random_uuid()::character varying(50) NULL |
| `customers_express_mobile` | `varchar(20)` | Sí | NULL |
| `customers_express_first_name` | `varchar(50)` | Sí | NULL |
| `customers_express_middle_name` | `varchar(50)` | Sí | NULL |
| `customers_express_last_name` | `varchar(50)` | Sí | NULL |
| `customers_express_second_name` | `varchar(50)` | Sí | NULL |
| `customers_express_identifier_type_code` | `varchar(50)` | Sí | NULL |
| `customers_express_identifier_value` | `int4` | Sí | NULL |
| `customers_express_status` | `varchar(50)` | Sí | NULL |
| `customers_express_link_status` | `varchar(50)` | Sí | NULL |
| `customers_express_full_search` | `varchar(50)` | Sí | NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

No se declararon restricciones de tabla en este CREATE TABLE.

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE lindasylunaticas.customers_express (
	customers_express_uuid varchar(50) DEFAULT gen_random_uuid()::character varying(50) NULL,
	customers_express_mobile varchar(20) NULL,
	customers_express_first_name varchar(50) NULL,
	customers_express_middle_name varchar(50) NULL,
	customers_express_last_name varchar(50) NULL,
	customers_express_second_name varchar(50) NULL,
	customers_express_identifier_type_code varchar(50) NULL,
	customers_express_identifier_value int4 NULL,
	customers_express_status varchar(50) NULL,
	customers_express_link_status varchar(50) NULL,
	customers_express_full_search varchar(50) NULL
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE lindasylunaticas.customers_express OWNER TO adminboek;
GRANT ALL ON TABLE lindasylunaticas.customers_express TO adminboek;
```

### Tabla `lindasylunaticas.field_type_settings`

- Esquema: `lindasylunaticas`.
- Nombre sin esquema: `field_type_settings`.
- Número de columnas: 6.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `identifier_type_settings_code` | `varchar(50)` | No | NOT NULL |
| `identifier_type_settings_label` | `varchar(120)` | No | NOT NULL |
| `identifier_type_settings_is_active` | `bool` | Sí | DEFAULT true NULL |
| `identifier_type_settings_validation` | `varchar(50)` | Sí | NULL |
| `identifier_type_settings_validation_config` | `jsonb` | Sí | NULL |
| `identifier_type_settings_display_order` | `int4` | Sí | NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT identifier_type_settings_pkey PRIMARY KEY (identifier_type_settings_code)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE lindasylunaticas.field_type_settings (
	identifier_type_settings_code varchar(50) NOT NULL,
	identifier_type_settings_label varchar(120) NOT NULL,
	identifier_type_settings_is_active bool DEFAULT true NULL,
	identifier_type_settings_validation varchar(50) NULL,
	identifier_type_settings_validation_config jsonb NULL,
	identifier_type_settings_display_order int4 NULL,
	CONSTRAINT identifier_type_settings_pkey PRIMARY KEY (identifier_type_settings_code)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE lindasylunaticas.field_type_settings OWNER TO adminboek;
GRANT ALL ON TABLE lindasylunaticas.field_type_settings TO adminboek;
```

### Tabla `lindasylunaticas.precios_servicios`

- Esquema: `lindasylunaticas`.
- Nombre sin esquema: `precios_servicios`.
- Número de columnas: 15.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `clave` | `varchar(150)` | No | NOT NULL |
| `familia` | `varchar(50)` | Sí | NULL |
| `nivel_2` | `varchar(50)` | Sí | NULL |
| `nivel_3` | `varchar(50)` | Sí | NULL |
| `nivel_4` | `varchar(50)` | Sí | NULL |
| `precio_profesional` | `numeric(16, 4)` | Sí | NULL |
| `precio_lista` | `numeric(16, 4)` | Sí | NULL |
| `nombre_corto` | `varchar(50)` | Sí | NULL |
| `duracion_horas` | `numeric(4, 1)` | Sí | NULL |
| `porcentaje_profesional` | `numeric(4, 2)` | Sí | NULL |
| `servicio` | `bool` | Sí | NULL |
| `archivo_origen` | `varchar(255)` | Sí | NULL |
| `hoja_origen` | `varchar(100)` | Sí | DEFAULT 'PRECIOS'::character varying NULL |
| `fila_excel` | `int4` | Sí | NULL |
| `fecha_carga` | `timestamp` | Sí | DEFAULT CURRENT_TIMESTAMP NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT pk_precios_servicios PRIMARY KEY (clave)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE lindasylunaticas.precios_servicios (
	clave varchar(150) NOT NULL,
	familia varchar(50) NULL,
	nivel_2 varchar(50) NULL,
	nivel_3 varchar(50) NULL,
	nivel_4 varchar(50) NULL,
	precio_profesional numeric(16, 4) NULL,
	precio_lista numeric(16, 4) NULL,
	nombre_corto varchar(50) NULL,
	duracion_horas numeric(4, 1) NULL,
	porcentaje_profesional numeric(4, 2) NULL,
	servicio bool NULL,
	archivo_origen varchar(255) NULL,
	hoja_origen varchar(100) DEFAULT 'PRECIOS'::character varying NULL,
	fila_excel int4 NULL,
	fecha_carga timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT pk_precios_servicios PRIMARY KEY (clave)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE lindasylunaticas.precios_servicios OWNER TO adminboek;
GRANT ALL ON TABLE lindasylunaticas.precios_servicios TO adminboek;
```

### Tabla `lindasylunaticas.price`

- Esquema: `lindasylunaticas`.
- Nombre sin esquema: `price`.
- Número de columnas: 11.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `price_family` | `varchar(100)` | Sí | NULL |
| `price_level2` | `varchar(100)` | Sí | NULL |
| `price_level3` | `varchar(100)` | Sí | NULL |
| `price_level4` | `varchar(100)` | Sí | NULL |
| `price_professionalpercentage` | `numeric(6, 2)` | Sí | NULL |
| `price_professionalprice` | `int4` | Sí | NULL |
| `price_listprice` | `int4` | Sí | NULL |
| `price_service_id` | `varchar(20)` | No | NOT NULL |
| `price_duration` | `int2` | Sí | NULL |
| `price_deposit` | `int4` | Sí | NULL |
| `price_servicekey` | `varchar(400)` | Sí | GENERATED ALWAYS AS (upper((((((COALESCE(price_family, ''::character varying)::text &#124;&#124; '&#124;'::text) &#124;&#124; COALESCE(price_level2, ''::character varying)::text) &#124;&#124; '&#124;'::text) &#124;&#124; COALESCE(price_level3, ''::character varying)::text) &#124;&#124; '&#124;'::text) &#124;&#124; COALESCE(price_level4, ''::character varying)::text)) STORED NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT uq_price_service_id UNIQUE (price_service_id)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE lindasylunaticas.price (
	price_family varchar(100) NULL,
	price_level2 varchar(100) NULL,
	price_level3 varchar(100) NULL,
	price_level4 varchar(100) NULL,
	price_professionalpercentage numeric(6, 2) NULL,
	price_professionalprice int4 NULL,
	price_listprice int4 NULL,
	price_service_id varchar(20) NOT NULL,
	price_duration int2 NULL,
	price_deposit int4 NULL,
	price_servicekey varchar(400) GENERATED ALWAYS AS (upper((((((COALESCE(price_family, ''::character varying)::text || '|'::text) || COALESCE(price_level2, ''::character varying)::text) || '|'::text) || COALESCE(price_level3, ''::character varying)::text) || '|'::text) || COALESCE(price_level4, ''::character varying)::text)) STORED NULL,
	CONSTRAINT uq_price_service_id UNIQUE (price_service_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
CREATE INDEX ix_price_hierarchy ON lindasylunaticas.price USING btree (price_family, price_level2, price_level3, price_level4);

-- Permissions

ALTER TABLE lindasylunaticas.price OWNER TO adminboek;
GRANT UPDATE, INSERT, REFERENCES, TRUNCATE, DELETE, SELECT, TRIGGER ON TABLE lindasylunaticas.price TO adminboek;
```

### Tabla `lindasylunaticas.prices`

- Esquema: `lindasylunaticas`.
- Nombre sin esquema: `prices`.
- Número de columnas: 11.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `prices_family` | `varchar(100)` | Sí | NULL |
| `prices_level2` | `varchar(100)` | Sí | NULL |
| `prices_level3` | `varchar(100)` | Sí | NULL |
| `prices_level4` | `varchar(100)` | Sí | NULL |
| `prices_professionalpercentage` | `numeric(6, 2)` | Sí | NULL |
| `prices_professionalprice` | `int4` | Sí | NULL |
| `prices_listprice` | `int4` | Sí | NULL |
| `prices_service_id` | `varchar(20)` | No | NOT NULL |
| `prices_duration` | `int2` | Sí | NULL |
| `prices_deposit` | `int4` | Sí | NULL |
| `prices_servicekey` | `varchar(400)` | Sí | GENERATED ALWAYS AS (upper((((((COALESCE(prices_family, ''::character varying)::text &#124;&#124; '&#124;'::text) &#124;&#124; COALESCE(prices_level2, ''::character varying)::text) &#124;&#124; '&#124;'::text) &#124;&#124; COALESCE(prices_level3, ''::character varying)::text) &#124;&#124; '&#124;'::text) &#124;&#124; COALESCE(prices_level4, ''::character varying)::text)) STORED NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT uq_prices_service_id UNIQUE (prices_service_id)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE lindasylunaticas.prices (
	prices_family varchar(100) NULL,
	prices_level2 varchar(100) NULL,
	prices_level3 varchar(100) NULL,
	prices_level4 varchar(100) NULL,
	prices_professionalpercentage numeric(6, 2) NULL,
	prices_professionalprice int4 NULL,
	prices_listprice int4 NULL,
	prices_service_id varchar(20) NOT NULL,
	prices_duration int2 NULL,
	prices_deposit int4 NULL,
	prices_servicekey varchar(400) GENERATED ALWAYS AS (upper((((((COALESCE(prices_family, ''::character varying)::text || '|'::text) || COALESCE(prices_level2, ''::character varying)::text) || '|'::text) || COALESCE(prices_level3, ''::character varying)::text) || '|'::text) || COALESCE(prices_level4, ''::character varying)::text)) STORED NULL,
	CONSTRAINT uq_prices_service_id UNIQUE (prices_service_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
CREATE INDEX ix_prices_hierarchy ON lindasylunaticas.prices USING btree (prices_family, prices_level2, prices_level3, prices_level4);

-- Permissions

ALTER TABLE lindasylunaticas.prices OWNER TO adminboek;
GRANT ALL ON TABLE lindasylunaticas.prices TO adminboek;
```

### Tabla `lindasylunaticas.sales`

- Esquema: `lindasylunaticas`.
- Nombre sin esquema: `sales`.
- Número de columnas: 36.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `idtype` | `varchar(10)` | Sí | NULL |
| `"date"` | `date` | Sí | NULL |
| `"year"` | `int4` | Sí | NULL |
| `"day"` | `int4` | Sí | NULL |
| `"month"` | `int4` | Sí | NULL |
| `"period"` | `int4` | Sí | NULL |
| `formattedperiod` | `varchar(7)` | Sí | NULL |
| `weekday` | `int4` | Sí | NULL |
| `dayname` | `varchar(10)` | Sí | NULL |
| `collectiondate` | `date` | Sí | NULL |
| `collectionperiod` | `int4` | Sí | NULL |
| `professional` | `varchar(20)` | Sí | NULL |
| `id` | `varchar(30)` | Sí | NULL |
| `names` | `varchar(100)` | Sí | NULL |
| `origin` | `varchar(50)` | Sí | NULL |
| `form` | `varchar(100)` | Sí | NULL |
| `"family"` | `varchar(50)` | Sí | NULL |
| `level2` | `varchar(50)` | Sí | NULL |
| `level3` | `varchar(50)` | Sí | NULL |
| `level4` | `varchar(50)` | Sí | NULL |
| `professionalprice` | `float8` | Sí | NULL |
| `listprice` | `float8` | Sí | NULL |
| `professionalpercentage` | `float8` | Sí | NULL |
| `advancepayment` | `float8` | Sí | NULL |
| `salonpayment` | `float8` | Sí | NULL |
| `total` | `float8` | Sí | NULL |
| `getnet` | `varchar(20)` | Sí | NULL |
| `totalpw` | `float8` | Sí | NULL |
| `lostadvancepayment` | `float8` | Sí | NULL |
| `discounttext` | `varchar(100)` | Sí | NULL |
| `salondiscount` | `float8` | Sí | NULL |
| `amounttopayprofessional` | `float8` | Sí | NULL |
| `totalprofessionalearnings` | `float8` | Sí | NULL |
| `amounttopaysalon` | `float8` | Sí | NULL |
| `production` | `float8` | Sí | NULL |
| `servicekey` | `varchar(200)` | Sí | GENERATED ALWAYS AS (((((family::text &#124;&#124; level2::text) &#124;&#124; level3::text) &#124;&#124; level4::text))) STORED NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

No se declararon restricciones de tabla en este CREATE TABLE.

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE lindasylunaticas.sales (
	idtype varchar(10) NULL,
	"date" date NULL,
	"year" int4 NULL,
	"day" int4 NULL,
	"month" int4 NULL,
	"period" int4 NULL,
	formattedperiod varchar(7) NULL,
	weekday int4 NULL,
	dayname varchar(10) NULL,
	collectiondate date NULL,
	collectionperiod int4 NULL,
	professional varchar(20) NULL,
	id varchar(30) NULL,
	names varchar(100) NULL,
	origin varchar(50) NULL,
	form varchar(100) NULL,
	"family" varchar(50) NULL,
	level2 varchar(50) NULL,
	level3 varchar(50) NULL,
	level4 varchar(50) NULL,
	professionalprice float8 NULL,
	listprice float8 NULL,
	professionalpercentage float8 NULL,
	advancepayment float8 NULL,
	salonpayment float8 NULL,
	total float8 NULL,
	getnet varchar(20) NULL,
	totalpw float8 NULL,
	lostadvancepayment float8 NULL,
	discounttext varchar(100) NULL,
	salondiscount float8 NULL,
	amounttopayprofessional float8 NULL,
	totalprofessionalearnings float8 NULL,
	amounttopaysalon float8 NULL,
	production float8 NULL,
	servicekey varchar(200) GENERATED ALWAYS AS (((((family::text || level2::text) || level3::text) || level4::text))) STORED NULL
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE lindasylunaticas.sales OWNER TO adminboek;
GRANT UPDATE, INSERT, REFERENCES, TRUNCATE, DELETE, SELECT, TRIGGER ON TABLE lindasylunaticas.sales TO adminboek;
GRANT UPDATE, INSERT, DELETE, SELECT ON TABLE lindasylunaticas.sales TO app_lindasylunaticas;
```

### Tabla `lindasylunaticas.product_family`

- Esquema: `lindasylunaticas`.
- Nombre sin esquema: `product_family`.
- Número de columnas: 6.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `product_family_id` | `int4` | No | GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL |
| `product_family_name` | `varchar(100)` | No | NOT NULL |
| `product_family_active` | `bool` | No | DEFAULT true NOT NULL |
| `product_family_created_at` | `timestamptz` | No | DEFAULT CURRENT_TIMESTAMP NOT NULL |
| `organization_id` | `int4` | No | DEFAULT 2 NOT NULL |
| `sort` | `int4` | No | DEFAULT 0 NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT chk_product_family_name CHECK ((btrim((product_family_name)::text) <> ''::text))`
- `CONSTRAINT product_family_pkey PRIMARY KEY (product_family_id)`
- `CONSTRAINT uq_product_family_name UNIQUE (organization_id, product_family_name)`
- `CONSTRAINT fk_product_family_organization FOREIGN KEY (organization_id) REFERENCES core.organization(organization_id)`

#### Referencias entrantes

- `lindasylunaticas.product_subfamily` (product_family_id) → `lindasylunaticas.product_family` (product_family_id), restricción `fk_product_subfamily_family`.

#### DDL documentado

```sql
CREATE TABLE lindasylunaticas.product_family (
	product_family_id int4 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL,
	product_family_name varchar(100) NOT NULL,
	product_family_active bool DEFAULT true NOT NULL,
	product_family_created_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	organization_id int4 DEFAULT 2 NOT NULL,
	sort int4 DEFAULT 0 NOT NULL,
	CONSTRAINT chk_product_family_name CHECK ((btrim((product_family_name)::text) <> ''::text)),
	CONSTRAINT product_family_pkey PRIMARY KEY (product_family_id),
	CONSTRAINT uq_product_family_name UNIQUE (organization_id, product_family_name),
	CONSTRAINT fk_product_family_organization FOREIGN KEY (organization_id) REFERENCES core.organization(organization_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE lindasylunaticas.product_family OWNER TO adminboek;
GRANT ALL ON TABLE lindasylunaticas.product_family TO adminboek;
```

### Tabla `lindasylunaticas.product_subfamily`

- Esquema: `lindasylunaticas`.
- Nombre sin esquema: `product_subfamily`.
- Número de columnas: 6.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `product_subfamily_id` | `int4` | No | GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL |
| `product_family_id` | `int4` | No | NOT NULL |
| `product_subfamily_name` | `varchar(100)` | No | NOT NULL |
| `product_subfamily_active` | `bool` | No | DEFAULT true NOT NULL |
| `product_subfamily_created_at` | `timestamptz` | No | DEFAULT CURRENT_TIMESTAMP NOT NULL |
| `sort` | `int4` | No | DEFAULT 0 NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT chk_product_subfamily_name CHECK ((btrim((product_subfamily_name)::text) <> ''::text))`
- `CONSTRAINT product_subfamily_pkey PRIMARY KEY (product_subfamily_id)`
- `CONSTRAINT uq_product_subfamily_name UNIQUE (product_family_id, product_subfamily_name)`
- `CONSTRAINT fk_product_subfamily_family FOREIGN KEY (product_family_id) REFERENCES lindasylunaticas.product_family(product_family_id)`

#### Referencias entrantes

- `lindasylunaticas.product` (product_subfamily_id) → `lindasylunaticas.product_subfamily` (product_subfamily_id), restricción `fk_product_subfamily`.

#### DDL documentado

```sql
CREATE TABLE lindasylunaticas.product_subfamily (
	product_subfamily_id int4 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL,
	product_family_id int4 NOT NULL,
	product_subfamily_name varchar(100) NOT NULL,
	product_subfamily_active bool DEFAULT true NOT NULL,
	product_subfamily_created_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	sort int4 DEFAULT 0 NOT NULL,
	CONSTRAINT chk_product_subfamily_name CHECK ((btrim((product_subfamily_name)::text) <> ''::text)),
	CONSTRAINT product_subfamily_pkey PRIMARY KEY (product_subfamily_id),
	CONSTRAINT uq_product_subfamily_name UNIQUE (product_family_id, product_subfamily_name),
	CONSTRAINT fk_product_subfamily_family FOREIGN KEY (product_family_id) REFERENCES lindasylunaticas.product_family(product_family_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE lindasylunaticas.product_subfamily OWNER TO adminboek;
GRANT ALL ON TABLE lindasylunaticas.product_subfamily TO adminboek;
```

### Tabla `lindasylunaticas.ticket`

- Esquema: `lindasylunaticas`.
- Nombre sin esquema: `ticket`.
- Número de columnas: 6.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `ticket_id` | `int8` | No | GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL |
| `organization_id` | `int4` | No | DEFAULT 2 NOT NULL |
| `ticket_status` | `varchar(10)` | No | DEFAULT 'Inactive'::character varying NOT NULL |
| `ticket_created_at` | `timestamptz` | No | DEFAULT CURRENT_TIMESTAMP NOT NULL |
| `ticket_updated_at` | `timestamptz` | No | DEFAULT CURRENT_TIMESTAMP NOT NULL |
| `ticket_status_changed_at` | `timestamptz` | No | DEFAULT CURRENT_TIMESTAMP NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT chk_ticket_status CHECK (((ticket_status)::text = ANY ((ARRAY['Inactive'::character varying, 'Assigned'::character varying, 'Started'::character varying, 'Completed'::character varying, 'Approved'::character varying, 'Finished'::character varying])::text[])))`
- `CONSTRAINT ticket_pkey PRIMARY KEY (ticket_id)`
- `CONSTRAINT fk_ticket_organization FOREIGN KEY (organization_id) REFERENCES core.organization(organization_id)`

#### Referencias entrantes

- `lindasylunaticas.ticket_professional` (ticket_id) → `lindasylunaticas.ticket` (ticket_id), restricción `fk_tp_ticket`.

#### DDL documentado

```sql
CREATE TABLE lindasylunaticas.ticket (
	ticket_id int8 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL,
	organization_id int4 DEFAULT 2 NOT NULL,
	ticket_status varchar(10) DEFAULT 'Inactive'::character varying NOT NULL,
	ticket_created_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	ticket_updated_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	ticket_status_changed_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT chk_ticket_status CHECK (((ticket_status)::text = ANY ((ARRAY['Inactive'::character varying, 'Assigned'::character varying, 'Started'::character varying, 'Completed'::character varying, 'Approved'::character varying, 'Finished'::character varying])::text[]))),
	CONSTRAINT ticket_pkey PRIMARY KEY (ticket_id),
	CONSTRAINT fk_ticket_organization FOREIGN KEY (organization_id) REFERENCES core.organization(organization_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
-- Permissions

ALTER TABLE lindasylunaticas.ticket OWNER TO adminboek;
GRANT ALL ON TABLE lindasylunaticas.ticket TO adminboek;
```

### Tabla `lindasylunaticas.ticket_professional`

- Esquema: `lindasylunaticas`.
- Nombre sin esquema: `ticket_professional`.
- Número de columnas: 6.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `ticket_professional_id` | `int4` | No | GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL |
| `ticket_id` | `int8` | No | NOT NULL |
| `professional_id` | `int4` | No | NOT NULL |
| `ticket_professional_created_at` | `timestamptz` | No | DEFAULT CURRENT_TIMESTAMP NOT NULL |
| `ticket_professional_updated_at` | `timestamptz` | No | DEFAULT CURRENT_TIMESTAMP NOT NULL |
| `ticket_professional_status_changed_at` | `timestamptz` | Sí | NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT ticket_professional_pkey PRIMARY KEY (ticket_professional_id)`
- `CONSTRAINT uq_ticket_professional UNIQUE (ticket_id, professional_id)`
- `CONSTRAINT fk_tp_professional FOREIGN KEY (professional_id) REFERENCES core.professional(professional_id)`
- `CONSTRAINT fk_tp_ticket FOREIGN KEY (ticket_id) REFERENCES lindasylunaticas.ticket(ticket_id)`

#### Referencias entrantes

- `lindasylunaticas.ticket_professional_customer` (ticket_professional_id) → `lindasylunaticas.ticket_professional` (ticket_professional_id), restricción `fk_tpc_ticket_professional`.

#### DDL documentado

```sql
CREATE TABLE lindasylunaticas.ticket_professional (
	ticket_professional_id int4 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL,
	ticket_id int8 NOT NULL,
	professional_id int4 NOT NULL,
	ticket_professional_created_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	ticket_professional_updated_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	ticket_professional_status_changed_at timestamptz NULL,
	CONSTRAINT ticket_professional_pkey PRIMARY KEY (ticket_professional_id),
	CONSTRAINT uq_ticket_professional UNIQUE (ticket_id, professional_id),
	CONSTRAINT fk_tp_professional FOREIGN KEY (professional_id) REFERENCES core.professional(professional_id),
	CONSTRAINT fk_tp_ticket FOREIGN KEY (ticket_id) REFERENCES lindasylunaticas.ticket(ticket_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
CREATE INDEX idx_tp_professional ON lindasylunaticas.ticket_professional USING btree (professional_id);

-- Permissions

ALTER TABLE lindasylunaticas.ticket_professional OWNER TO adminboek;
GRANT ALL ON TABLE lindasylunaticas.ticket_professional TO adminboek;
```

### Tabla `lindasylunaticas.ticket_professional_customer`

- Esquema: `lindasylunaticas`.
- Nombre sin esquema: `ticket_professional_customer`.
- Número de columnas: 7.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `ticket_professional_customer_id` | `int4` | No | GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL |
| `ticket_professional_id` | `int4` | No | NOT NULL |
| `customer_id` | `int4` | No | NOT NULL |
| `ticket_professional_customer_status` | `varchar(10)` | No | DEFAULT 'Inactive'::character varying NOT NULL |
| `ticket_professional_customer_created_at` | `timestamptz` | No | DEFAULT CURRENT_TIMESTAMP NOT NULL |
| `ticket_professional_customer_updated_at` | `timestamptz` | No | DEFAULT CURRENT_TIMESTAMP NOT NULL |
| `ticket_professional_customer_status_changed_at` | `timestamptz` | No | DEFAULT CURRENT_TIMESTAMP NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT chk_tpc_status CHECK (((ticket_professional_customer_status)::text = ANY ((ARRAY['Inactive'::character varying, 'Assigned'::character varying, 'Started'::character varying, 'Completed'::character varying])::text[])))`
- `CONSTRAINT ticket_professional_customer_pkey PRIMARY KEY (ticket_professional_customer_id)`
- `CONSTRAINT uq_ticket_professional_customer UNIQUE (ticket_professional_id, customer_id)`
- `CONSTRAINT fk_tpc_customer FOREIGN KEY (customer_id) REFERENCES lindasylunaticas.customer(customer_id)`
- `CONSTRAINT fk_tpc_ticket_professional FOREIGN KEY (ticket_professional_id) REFERENCES lindasylunaticas.ticket_professional(ticket_professional_id)`

#### Referencias entrantes

- `lindasylunaticas.ticket_professional_customer_product` (ticket_professional_customer_id) → `lindasylunaticas.ticket_professional_customer` (ticket_professional_customer_id), restricción `fk_tpcp_attention`.

#### DDL documentado

```sql
CREATE TABLE lindasylunaticas.ticket_professional_customer (
	ticket_professional_customer_id int4 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL,
	ticket_professional_id int4 NOT NULL,
	customer_id int4 NOT NULL,
	ticket_professional_customer_status varchar(10) DEFAULT 'Inactive'::character varying NOT NULL,
	ticket_professional_customer_created_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	ticket_professional_customer_updated_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	ticket_professional_customer_status_changed_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT chk_tpc_status CHECK (((ticket_professional_customer_status)::text = ANY ((ARRAY['Inactive'::character varying, 'Assigned'::character varying, 'Started'::character varying, 'Completed'::character varying])::text[]))),
	CONSTRAINT ticket_professional_customer_pkey PRIMARY KEY (ticket_professional_customer_id),
	CONSTRAINT uq_ticket_professional_customer UNIQUE (ticket_professional_id, customer_id),
	CONSTRAINT fk_tpc_customer FOREIGN KEY (customer_id) REFERENCES lindasylunaticas.customer(customer_id),
	CONSTRAINT fk_tpc_ticket_professional FOREIGN KEY (ticket_professional_id) REFERENCES lindasylunaticas.ticket_professional(ticket_professional_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
CREATE INDEX idx_tpc_customer ON lindasylunaticas.ticket_professional_customer USING btree (customer_id);

-- Permissions

ALTER TABLE lindasylunaticas.ticket_professional_customer OWNER TO adminboek;
GRANT ALL ON TABLE lindasylunaticas.ticket_professional_customer TO adminboek;
```

### Tabla `lindasylunaticas.product`

- Esquema: `lindasylunaticas`.
- Nombre sin esquema: `product`.
- Número de columnas: 11.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `product_id` | `int4` | No | GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL |
| `product_subfamily_id` | `int4` | No | NOT NULL |
| `product_code` | `varchar(30)` | No | NOT NULL |
| `product_name` | `varchar(250)` | No | NOT NULL |
| `product_type` | `varchar(10)` | No | DEFAULT 'service'::character varying NOT NULL |
| `product_price` | `numeric(12, 2)` | No | NOT NULL |
| `product_professional_price` | `numeric(12, 2)` | No | NOT NULL |
| `product_active` | `bool` | No | DEFAULT true NOT NULL |
| `product_created_at` | `timestamptz` | No | DEFAULT CURRENT_TIMESTAMP NOT NULL |
| `product_updated_at` | `timestamptz` | No | DEFAULT CURRENT_TIMESTAMP NOT NULL |
| `sort` | `int4` | No | DEFAULT 0 NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT chk_product_code CHECK ((btrim((product_code)::text) <> ''::text))`
- `CONSTRAINT chk_product_name CHECK ((btrim((product_name)::text) <> ''::text))`
- `CONSTRAINT chk_product_price CHECK ((product_price >= (0)::numeric))`
- `CONSTRAINT chk_product_professional_price CHECK ((product_professional_price >= (0)::numeric))`
- `CONSTRAINT chk_product_type CHECK (((product_type)::text = ANY ((ARRAY['product'::character varying, 'service'::character varying])::text[])))`
- `CONSTRAINT product_pkey PRIMARY KEY (product_id)`
- `CONSTRAINT product_product_code_key UNIQUE (product_code)`
- `CONSTRAINT fk_product_subfamily FOREIGN KEY (product_subfamily_id) REFERENCES lindasylunaticas.product_subfamily(product_subfamily_id)`

#### Referencias entrantes

- `lindasylunaticas.ticket_professional_customer_product` (product_id) → `lindasylunaticas.product` (product_id), restricción `fk_tpcp_product`.

#### DDL documentado

```sql
CREATE TABLE lindasylunaticas.product (
	product_id int4 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL,
	product_subfamily_id int4 NOT NULL,
	product_code varchar(30) NOT NULL,
	product_name varchar(250) NOT NULL,
	product_type varchar(10) DEFAULT 'service'::character varying NOT NULL,
	product_price numeric(12, 2) NOT NULL,
	product_professional_price numeric(12, 2) NOT NULL,
	product_active bool DEFAULT true NOT NULL,
	product_created_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	product_updated_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	sort int4 DEFAULT 0 NOT NULL,
	CONSTRAINT chk_product_code CHECK ((btrim((product_code)::text) <> ''::text)),
	CONSTRAINT chk_product_name CHECK ((btrim((product_name)::text) <> ''::text)),
	CONSTRAINT chk_product_price CHECK ((product_price >= (0)::numeric)),
	CONSTRAINT chk_product_professional_price CHECK ((product_professional_price >= (0)::numeric)),
	CONSTRAINT chk_product_type CHECK (((product_type)::text = ANY ((ARRAY['product'::character varying, 'service'::character varying])::text[]))),
	CONSTRAINT product_pkey PRIMARY KEY (product_id),
	CONSTRAINT product_product_code_key UNIQUE (product_code),
	CONSTRAINT fk_product_subfamily FOREIGN KEY (product_subfamily_id) REFERENCES lindasylunaticas.product_subfamily(product_subfamily_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
CREATE INDEX idx_product_subfamily ON lindasylunaticas.product USING btree (product_subfamily_id);

-- Permissions

ALTER TABLE lindasylunaticas.product OWNER TO adminboek;
GRANT ALL ON TABLE lindasylunaticas.product TO adminboek;
```

### Tabla `lindasylunaticas.ticket_professional_customer_product`

- Esquema: `lindasylunaticas`.
- Nombre sin esquema: `ticket_professional_customer_product`.
- Número de columnas: 9.

#### Columnas

| Columna | Tipo original | Admite NULL | Valor por defecto / generación / reglas locales |
|---|---|---|---|
| `ticket_professional_customer_product_id` | `int4` | No | GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL |
| `ticket_professional_customer_id` | `int4` | No | NOT NULL |
| `product_id` | `int4` | No | NOT NULL |
| `product_name` | `varchar(250)` | No | NOT NULL |
| `quantity` | `numeric(10, 2)` | No | DEFAULT 1 NOT NULL |
| `unit_price` | `numeric(12, 2)` | No | NOT NULL |
| `professional_unit_price` | `numeric(12, 2)` | No | NOT NULL |
| `ticket_professional_customer_product_created_at` | `timestamptz` | No | DEFAULT CURRENT_TIMESTAMP NOT NULL |
| `ticket_professional_customer_product_updated_at` | `timestamptz` | No | DEFAULT CURRENT_TIMESTAMP NOT NULL |

La nulabilidad anterior corresponde a la declaración de columna; la PK también implica NOT NULL. El DDL siguiente conserva la declaración documentada; en core.professional se integraron las FK del ALTER TABLE dentro del CREATE TABLE, sin cambiar su significado.

#### Restricciones declaradas

- `CONSTRAINT chk_tpcp_professional_price CHECK ((professional_unit_price >= (0)::numeric))`
- `CONSTRAINT chk_tpcp_quantity CHECK ((quantity > (0)::numeric))`
- `CONSTRAINT chk_tpcp_unit_price CHECK ((unit_price >= (0)::numeric))`
- `CONSTRAINT ticket_professional_customer_product_pkey PRIMARY KEY (ticket_professional_customer_product_id)`
- `CONSTRAINT fk_tpcp_attention FOREIGN KEY (ticket_professional_customer_id) REFERENCES lindasylunaticas.ticket_professional_customer(ticket_professional_customer_id)`
- `CONSTRAINT fk_tpcp_product FOREIGN KEY (product_id) REFERENCES lindasylunaticas.product(product_id)`

#### Referencias entrantes

No se identificaron claves foráneas entrantes en este archivo. No implica ausencia de relaciones lógicas o referencias externas.

#### DDL documentado

```sql
CREATE TABLE lindasylunaticas.ticket_professional_customer_product (
	ticket_professional_customer_product_id int4 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL,
	ticket_professional_customer_id int4 NOT NULL,
	product_id int4 NOT NULL,
	product_name varchar(250) NOT NULL,
	quantity numeric(10, 2) DEFAULT 1 NOT NULL,
	unit_price numeric(12, 2) NOT NULL,
	professional_unit_price numeric(12, 2) NOT NULL,
	ticket_professional_customer_product_created_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	ticket_professional_customer_product_updated_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT chk_tpcp_professional_price CHECK ((professional_unit_price >= (0)::numeric)),
	CONSTRAINT chk_tpcp_quantity CHECK ((quantity > (0)::numeric)),
	CONSTRAINT chk_tpcp_unit_price CHECK ((unit_price >= (0)::numeric)),
	CONSTRAINT ticket_professional_customer_product_pkey PRIMARY KEY (ticket_professional_customer_product_id),
	CONSTRAINT fk_tpcp_attention FOREIGN KEY (ticket_professional_customer_id) REFERENCES lindasylunaticas.ticket_professional_customer(ticket_professional_customer_id),
	CONSTRAINT fk_tpcp_product FOREIGN KEY (product_id) REFERENCES lindasylunaticas.product(product_id)
);
```

#### Índices, comentarios, triggers y permisos exportados

```sql
CREATE INDEX idx_tpcp_attention ON lindasylunaticas.ticket_professional_customer_product USING btree (ticket_professional_customer_id);
CREATE INDEX idx_tpcp_product ON lindasylunaticas.ticket_professional_customer_product USING btree (product_id);

-- Permissions

ALTER TABLE lindasylunaticas.ticket_professional_customer_product OWNER TO adminboek;
GRANT ALL ON TABLE lindasylunaticas.ticket_professional_customer_product TO adminboek;
```

## Verificación de cobertura

- CREATE TABLE detectados y documentados: 64.
- Columnas documentadas: 635.
- Claves foráneas documentadas: 46.
- Se conserva un bloque DDL original por tabla, además de sus declaraciones complementarias disponibles.
- No se agregaron tablas o relaciones de diseños previos que no figuren en la fuente.
