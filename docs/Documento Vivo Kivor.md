# DOCUMENTO_VIVO_KIVOR.md

Versión: 1.1

Estado: Proyecto en curso

---

# 1. VISIÓN DEL PROYECTO

KIVOR es una plataforma orientada a la administración y operación de negocios de servicios, diseñada bajo una arquitectura modular y preparada para soportar múltiples organizaciones mediante un esquema multi-tenant.

El objetivo es construir una plataforma escalable y mantenible, evitando deuda técnica innecesaria y privilegiando componentes reutilizables.

---

# 2. PRINCIPIOS DEL PROYECTO

## Modularidad

Cada componente debe tener una responsabilidad claramente definida.

## Desacoplamiento

Frontend y backend se encuentran separados.

## Multiempresa

La información de negocio puede residir en distintos esquemas (tenant_schema).

## Reutilización

Los componentes deben ser reutilizables.

## Escalabilidad

La arquitectura debe permitir incorporar nuevos módulos sin alterar la estructura existente.

## Simplicidad

Se privilegia la claridad sobre la complejidad.

---

# 3. STACK TECNOLÓGICO

## Backend

Python 3.11.9

Framework:

FastAPI

Servidor:

uvicorn

Acceso a datos:

psycopg3

Autenticación:

JWT

Base de datos:

PostgreSQL

Dependencias principales:

```txt
fastapi==0.115.8
uvicorn[standard]==0.34.0
psycopg[binary]==3.3.2
python-jose[cryptography]==3.3.0
bcrypt
pandas
openpyxl
python-multipart
```

---

## Frontend

HTML

CSS

JavaScript Vanilla

---

# 4. ESTRUCTURA GENERAL

## Backend

```text
kivor-main

core/
routes/
services/
schemas/

main.py
requirements.txt
runtime.txt
Dockerfile
```

## Frontend

```text
kivor-frontend-main

assets/
css/
js/

login.html
dashboard.html
customer-express.html
ganancias-mensuales.html
mantenedor-precios.html
ventas-lyl-carga.html
```

---

# 5. PATRÓN DE DESARROLLO

Backend:

```text
Route
↓
Service
↓
DB
```

Frontend:

```text
HTML
↓
Javascript
↓
api.js
↓
Endpoint
↓
Route
↓
Service
↓
PostgreSQL
```

---

# 6. ESTRUCTURA BACKEND

## core

Responsabilidad:

Infraestructura común.

Archivos:

```text
core/db.py
core/exceptions.py
core/security.py
```

---

## routes

Responsabilidad:

Exponer endpoints HTTP.

Archivos:

```text
routes/auth.py
routes/customers_express.py
routes/users.py
routes/analytics.py
routes/menu.py
routes/ventas_lyl.py
```

---

## services

Responsabilidad:

Implementar reglas de negocio.

Archivos:

```text
services/auth_service.py
services/customer_express_service.py
services/user_service.py
services/analytics_service.py
services/ventas_lyl_service.py
```

---

## schemas

Responsabilidad:

Modelos Pydantic.

Archivos:

```text
schemas/auth_schema.py
schemas/user_schema.py
schemas/ventas_lyl_schema.py
```

---

# 7. ESTRUCTURA FRONTEND

## HTML

Pantallas:

```text
login.html
login-v2.html
login-v3.html

dashboard.html

customer-express.html
customer-express-generate.html
cx.html

ganancias-mensuales.html

mantenedor-precios.html
```

---

## CSS

```text
customer-express.css
login.css
login-v2.css
login-v3.css
menu.css
styles.css
theme.css
mantenedor-precios.css
ventas-lyl.css
```

---

## JavaScript

```text
api.js

customer-express.js
dashboard.js
ganancias-mensuales.js

login.js
login-v2.js
login-v3.js

mantenedor-precios.js
ventas-lyl-carga.js

i18n.js
```

---

## Componentes

```text
js/components/

input-base.js
input-rut.js
input-text.js
```

---

## CRT

```text
js/crt/

crt-init.js
muevo.js
```

---

# 8. MODELO GENERAL DE EJECUCIÓN

Frontend

↓

Javascript

↓

API REST

↓

Routes

↓

Services

↓

core/db.py

↓

PostgreSQL

---

# 9. PRINCIPIOS DE DESARROLLO

* No generar código sin arquitectura definida.
* Evitar duplicación.
* Priorizar componentes reutilizables.
* Mantener consistencia.
* Evitar sobreingeniería.
* Mantener separación entre presentación y lógica.
* El DOCUMENTO VIVO es la referencia oficial del proyecto.

FIN PARTE 1

# DOCUMENTO_VIVO_KIVOR.md

# PARTE 2 – BACKEND

---

# 10. BACKEND

El backend de KIVOR está construido en Python utilizando FastAPI y sigue una arquitectura basada en:

```text
Route
↓
Service
↓
core/db.py
↓
PostgreSQL
```

Su objetivo es mantener desacoplada la exposición HTTP de la lógica de negocio.

---

# 11. MAIN.PY

Archivo:

```text
main.py
```

Responsabilidad:

Inicialización de FastAPI.

Funciones:

* Configuración CORS.
* Registro de routers.
* Manejo global de excepciones.
* Endpoints de monitoreo.
* Configuración general.

Routers registrados:

```text
auth.router
customers_express.router
users.router
analytics.router
menu.router
ventas_lyl.router
```

Endpoints internos:

```text
GET /
GET /health
GET /test-db
GET /config
```

Función auxiliar:

```python
validar_rut()
```

Propósito:

Validar RUT chileno.

---

# 12. CORE

## db.py

Responsabilidad:

Administrar conexiones PostgreSQL.

Funciones:

### get_connection()

Obtiene conexión utilizando DATABASE_URL.

Responsabilidades:

* Parsear URL.
* Eliminar parámetros incompatibles.
* Abrir conexión mediante psycopg.

### set_tenant_schema()

Permite trabajar con distintos esquemas.

Ejemplo:

```python
set_tenant_schema(conn, tenant_schema)
```

Arquitectura Multi Tenant.

---

## exceptions.py

Responsabilidad:

Centralizar excepciones.

Jerarquía:

```text
AppException
│
├── NotFoundError
├── UnauthorizedError
├── ForbiddenError
├── InvalidDataError
└── InternalServerError
```

Objetivo:

Estandarizar respuestas de error.

---

## security.py

Responsabilidad:

Autenticación JWT.

Variables:

```text
SECRET_KEY
ALGORITHM
ACCESS_TOKEN_EXPIRE_MINUTES
```

Funciones:

### create_access_token()

Genera JWT.

Incluye:

* sub
* group_id
* session_id
* tenant_id
* tenant_schema
* organization_id
* person_id

### verify_token()

Valida:

* token JWT
* sesión activa
* expiración
* revocación

Tabla utilizada:

```text
core.user_session
```

Devuelve:

```python
{
    username,
    group_id,
    session_id,
    tenant_schema,
    organization_id,
    person_id
}
```

---

# 13. ROUTES

Su responsabilidad es exponer APIs REST.

No deben contener lógica compleja.

---

## auth.py

Responsabilidad:

Autenticación.

Endpoints:

### POST /login

Delega en:

```text
auth_service.login_user()
```

---

### POST /login-username

Valida existencia de usuario.

Tabla:

```text
core.user
```

---

### POST /logout

Revoca sesión.

Tabla:

```text
core.user_session
```

---

### POST /logout-session

Cierra sesión específica.

---

Dependencia:

```text
core.security.verify_token()
```

---

## customers_express.py

Prefijo:

```text
/customers-express
```

Endpoints:

### POST /generate

Genera enlace de captura.

Servicio:

```text
generate_customer_express_service()
```

---

### GET /{token}

Obtiene formulario.

Servicio:

```text
get_customer_express_service()
```

---

### POST /{token}

Guarda información.

Servicio:

```text
save_customer_express_service()
```

---

### GET /by-mobile/{mobile}

Busca clientes por teléfono.

Servicio:

```text
search_customer_express_by_mobile_service()
```

---

## analytics.py

Responsabilidad:

Indicadores.

Endpoints:

### GET /ganancias-por-mes

Servicio:

```text
get_ganancias_por_mes_service()
```

---

### GET /familias

Servicio:

```text
get_familias_service()
```

---

### GET /niveles2

Servicio:

```text
get_nivel2_service()
```

---

### GET /niveles3

Servicio:

```text
get_nivel3_service()
```

---

### GET /niveles4

Servicio:

```text
get_nivel4_service()
```

---

### GET /precios

Servicio:

```text
get_precios_service()
```

---

### GET /preciosGS

Servicio:

```text
get_precios_service_GS()
```

---

## menu.py

Responsabilidad:

Construcción dinámica del menú.

Endpoints:

### GET /menu

Tablas:

```text
core.menu
core.role_menu
core.group_role
```

Utiliza consulta recursiva.

---

### GET /sessions

Tabla:

```text
core.user_session
```

Obtiene sesiones activas.

---

## users.py

Endpoint:

### POST /users

Crea usuario.

Servicios:

```text
create_user_service()
```

Tabla:

```text
core.user
```

---

# 14. SCHEMAS

Responsabilidad:

Modelos Pydantic.

---

## auth_schema.py

### LoginRequest

Campos:

```text
username
password
```

---

### LoginUsernameRequest

Campo:

```text
username
```

---

### LogoutSessionRequest

Campo:

```text
session_id
```

---

## user_schema.py

### CreateUserRequest

Campos:

```text
user_nickname
user_name
user_password
user_firstname
user_lastname
user_group_id
```

---

### CreateUserResponse

Campo:

```text
user_id
```

---

# 15. SERVICES

Contienen la lógica de negocio.

---

## auth_service.py

Responsabilidad:

Autenticación.

Proceso:

```text
Usuario
↓
core.user
↓
core.person
↓
core.person_organization
↓
core.organization
↓
core.tenant
↓
core.user_session
↓
JWT
```

Tablas utilizadas:

```text
core.user
core.person
core.person_organization
core.organization
core.tenant
core.user_session
```

Función:

```python
login_user()
```

---

## analytics_service.py

Responsabilidad:

Indicadores.

Funciones:

```python
get_ganancias_por_mes_service()

get_familias_service()

get_nivel2_service()

get_nivel3_service()

get_nivel4_service()

get_precios_service()

get_precios_service_GS()
```

Tabla principal:

```text
core.prices
```

Tabla tenant:

```text
sales
```

---

## customer_express_service.py

Responsabilidad:

Captura dinámica de clientes.

Funciones:

### generate_customer_express_service()

Genera token.

Tablas:

```text
customers_express
core.customers_express_token_map
```

---

### get_customer_express_service()

Obtiene estructura dinámica.

Tablas:

```text
customer_capture_settings
identifier_type_settings
customers_express
```

---

### save_customer_express_service()

Actualiza datos.

---

### search_customer_express_by_mobile_service()

Busca clientes.

---

Arquitectura:

```text
token
↓
tenant_schema
↓
customers_express
↓
campos dinámicos
↓
persistencia
```

---

## user_service.py

Responsabilidad:

Creación de usuarios.

Función:

```python
create_user_service()
```

Tabla:

```text
core.user
```

Algoritmo:

SHA256.

---

# 16. ARQUITECTURA BACKEND

```text
main.py
│
├── routes
│
├── auth.py
├── users.py
├── analytics.py
├── customers_express.py
└── menu.py
        │
        ↓
services
        │
        ↓
core/db.py
        │
        ↓
PostgreSQL
```

---

# 17. OBSERVACIONES

El backend presenta una arquitectura consistente.

Patrón predominante:

```text
Route
↓
Service
↓
DB
```

La lógica de negocio está correctamente separada de la exposición HTTP.

La arquitectura soporta crecimiento mediante nuevos módulos.

FIN PARTE 2


# DOCUMENTO_VIVO_KIVOR.md

# PARTE 3 – FRONTEND

---

# 18. FRONTEND

KIVOR utiliza HTML, CSS y JavaScript Vanilla.

La arquitectura del frontend se basa en:

```text
HTML
↓
Javascript
↓
api.js
↓
Backend
↓
PostgreSQL
```

Objetivos:

* Mantener independencia respecto del backend.
* Reutilizar componentes.
* Minimizar dependencias externas.
* Facilitar crecimiento por módulos.

---

# 19. PÁGINAS HTML

---

## index.html

Responsabilidad:

Página principal del sistema.

Punto de entrada.

---

## login.html

Responsabilidad:

Autenticación tradicional.

Javascript asociado:

```text
js/login.js
```

CSS asociado:

```text
css/login.css
```

---

## login-v2.html

Responsabilidad:

Versión alternativa del login.

Javascript:

```text
login-v2.js
```

CSS:

```text
login-v2.css
```

---

## login-v3.html

Responsabilidad:

Versión evolucionada del login.

Javascript:

```text
login-v3.js
```

CSS:

```text
login-v3.css
```

---

## dashboard.html

Responsabilidad:

Pantalla principal posterior al login.

Javascript:

```text
dashboard.js
```

Funciones:

* Cargar menú.
* Inicializar sesión.
* Mostrar indicadores.

---

## customer-express.html

Responsabilidad:

Captura dinámica de clientes.

Javascript:

```text
customer-express.js
```

CSS:

```text
customer-express.css
```

Flujo:

```text
customer-express.html
↓
customer-express.js
↓
api.js
↓
customers_express.py
↓
customer_express_service.py
```

---

## customer-express-generate.html

Responsabilidad:

Generación de enlaces Customer Express.

---

## cx.html

Responsabilidad:

Pantalla alternativa Customer Express.

---

## ganancias-mensuales.html

Responsabilidad:

Visualización de indicadores de ganancias.

Javascript:

```text
ganancias-mensuales.js
```

Flujo:

```text
ganancias-mensuales.html
↓
ganancias-mensuales.js
↓
analytics.py
↓
analytics_service.py
```

---

## mantenedor-precios.html

Responsabilidad:

Mantención de precios.

Javascript:

```text
mantenedor-precios.js
```

CSS:

```text
mantenedor-precios.css
```

---

# 20. CSS

---

## styles.css

Responsabilidad:

Estilos globales.

Define:

* layout general
* colores
* espaciados
* tipografía

---

## theme.css

Responsabilidad:

Tema visual.

Permite personalización.

---

## menu.css

Responsabilidad:

Estilos del menú.

---

## login.css

Responsabilidad:

Estilos login clásico.

---

## login-v2.css

Responsabilidad:

Versión alternativa.

---

## login-v3.css

Responsabilidad:

Versión moderna.

---

## customer-express.css

Responsabilidad:

Formulario Customer Express.

---

## mantenedor-precios.css

Responsabilidad:

Pantalla de precios.

---

# 21. JAVASCRIPT

---

## api.js

Responsabilidad:

Capa de comunicación con el backend.

Centraliza:

* GET
* POST
* PUT
* DELETE

Objetivo:

Evitar llamadas directas desde las pantallas.

Arquitectura:

```text
Pantalla
↓
api.js
↓
Backend
```

---

## login.js

Responsabilidad:

Autenticación.

Flujo:

```text
usuario
↓
login.js
↓
/login
↓
JWT
↓
dashboard
```

---

## login-v2.js

Versión alternativa.

---

## login-v3.js

Versión evolucionada.

---

## dashboard.js

Responsabilidad:

Inicialización del dashboard.

Funciones:

* carga menú
* obtiene sesión
* navegación

---

## customer-express.js

Responsabilidad:

Captura dinámica.

Funciones:

* generar formularios
* enviar datos
* validaciones

Flujo:

```text
HTML
↓
customer-express.js
↓
api.js
↓
customers_express
```

---

## ganancias-mensuales.js

Responsabilidad:

Gráficos e indicadores.

Obtiene:

* ganancias
* familias
* niveles
* precios

---

## mantenedor-precios.js

Responsabilidad:

Administración de precios.

---

## i18n.js

Responsabilidad:

Internacionalización.

Preparación para múltiples idiomas.

---

# 22. COMPONENTES REUTILIZABLES

Carpeta:

```text
js/components/
```

Objetivo:

Centralizar componentes reutilizables.

---

## input-base.js

Componente base.

Responsabilidad:

Estandarizar comportamiento.

Características:

* etiquetas
* eventos
* validaciones

---

## input-rut.js

Especialización para RUT.

Funciones:

* formato
* validación

---

## input-text.js

Entrada estándar de texto.

---

# 23. CRT

Carpeta:

```text
js/crt/
```

---

## crt-init.js

Responsabilidad:

Inicialización de CRT.

---

## muevo.js

Responsabilidad:

Funciones auxiliares.

---

# 24. ASSETS

Carpeta:

```text
assets/images
```

Imágenes:

```text
logo-kivor.png
logo-kivor.svg
logo-lindasylunaticas.png
MARCA LITTLE.png
```

Responsabilidad:

Identidad visual.

---

# 25. ARQUITECTURA FRONTEND

```text
HTML
│
├── login
├── dashboard
├── customer-express
├── ganancias
└── precios
        │
        ↓
Javascript
        │
        ↓
api.js
        │
        ↓
FastAPI
        │
        ↓
PostgreSQL
```

---

# 26. PATRÓN DE DESARROLLO FRONTEND

Pantalla

↓

Javascript

↓

api.js

↓

Endpoint

↓

Route

↓

Service

↓

Base de datos

---

# 27. PRINCIPIOS FRONTEND

* Separar presentación y lógica.
* Centralizar llamadas mediante api.js.
* Reutilizar componentes.
* Evitar lógica duplicada.
* Mantener consistencia visual.
* Facilitar incorporación de nuevos módulos.

---

# 28. MÓDULOS FUTUROS FRONTEND

Pendientes:

```text
ventas-lyl.html

agenda.html

servicios.html

profesionales.html

clientes.html

caja.html

comisiones.html

reportes.html
```

Cada módulo deberá respetar la arquitectura actual:

```text
HTML
↓
JS
↓
api.js
↓
Backend
```

FIN PARTE 3

# DOCUMENTO_VIVO_KIVOR.md

# PARTE 4 – FLUJOS, ENDPOINTS, DEPENDENCIAS Y MODELO FUNCIONAL

---

# 29. MODELO GENERAL DE OPERACIÓN

KIVOR implementa una arquitectura desacoplada basada en:

```text id="mj9u8g"
Frontend
↓
Javascript
↓
api.js
↓
FastAPI
↓
Routes
↓
Services
↓
core/db.py
↓
PostgreSQL
```

Principios:

* La lógica de negocio reside exclusivamente en Services.
* Las Routes sólo exponen endpoints.
* api.js centraliza el acceso desde el frontend.
* PostgreSQL es la capa persistente.

---

# 30. INVENTARIO DE ENDPOINTS

## AUTH

Archivo:

```text id="1uc3h0"
routes/auth.py
```

### POST /login

Objetivo:

Autenticar usuario.

Servicio:

```text id="x5prfo"
auth_service.login_user()
```

---

### POST /login-username

Objetivo:

Validar existencia de usuario.

---

### POST /logout

Objetivo:

Cerrar sesión.

---

### POST /logout-session

Objetivo:

Cerrar una sesión específica.

---

# CUSTOMER EXPRESS

Archivo:

```text id="8py3l5"
routes/customers_express.py
```

Prefijo:

```text id="jbk66r"
/customers-express
```

---

### POST /generate

Genera token para captura.

Servicio:

```text id="ob8m2j"
generate_customer_express_service()
```

---

### GET /{token}

Obtiene formulario dinámico.

Servicio:

```text id="mhyn2h"
get_customer_express_service()
```

---

### POST /{token}

Guarda información.

Servicio:

```text id="g2o9yy"
save_customer_express_service()
```

---

### GET /by-mobile/{mobile}

Busca clientes.

Servicio:

```text id="tnvk2w"
search_customer_express_by_mobile_service()
```

---

# ANALYTICS

Archivo:

```text id="a85e89"
routes/analytics.py
```

---

### GET /ganancias-por-mes

Servicio:

```text id="sq4jbm"
get_ganancias_por_mes_service()
```

---

### GET /familias

Servicio:

```text id="6diyzt"
get_familias_service()
```

---

### GET /niveles2

Servicio:

```text id="5irjlwm"
get_nivel2_service()
```

---

### GET /niveles3

Servicio:

```text id="t0iyva"
get_nivel3_service()
```

---

### GET /niveles4

Servicio:

```text id="6vyxmy"
get_nivel4_service()
```

---

### GET /precios

Servicio:

```text id="spc0yn"
get_precios_service()
```

---

### GET /preciosGS

Servicio:

```text id="0svq93"
get_precios_service_GS()
```

---

# USERS

Archivo:

```text id="wsvf8v"
routes/users.py
```

---

### POST /users

Servicio:

```text id="gd54o9"
create_user_service()
```

---

# MENU

Archivo:

```text id="7f5zts"
routes/menu.py
```

---

### GET /menu

Construcción dinámica del menú.

---

### GET /sessions

Sesiones activas.

---

# 31. FLUJO DE AUTENTICACIÓN

```text id="l7c7cs"
login.html
↓
login.js
↓
api.js
↓
POST /login
↓
auth.py
↓
auth_service.py
↓
core.user
↓
core.user_session
↓
JWT
↓
dashboard.html
```

---

# 32. FLUJO CUSTOMER EXPRESS

Generación:

```text id="u8olr0"
customer-express-generate.html
↓
customer-express.js
↓
POST /generate
↓
generate_customer_express_service()
↓
core.customers_express_token_map
```

Captura:

```text id="dnr8c0"
Cliente
↓
GET /{token}
↓
customer_capture_settings
↓
Formulario dinámico
↓
POST /{token}
↓
customers_express
```

---

# 33. FLUJO ANALYTICS

```text id="vut7r2"
ganancias-mensuales.html
↓
ganancias-mensuales.js
↓
api.js
↓
analytics.py
↓
analytics_service.py
↓
sales
↓
Indicadores
```

Indicadores disponibles:

* Ganancias.
* Familias.
* Nivel 2.
* Nivel 3.
* Nivel 4.
* Precios.

---

# 34. FLUJO MENÚ

```text id="lcnx55"
dashboard.html
↓
dashboard.js
↓
GET /menu
↓
menu.py
↓
core.menu
↓
core.role_menu
↓
core.group_role
↓
Construcción menú
```

---

# 35. DEPENDENCIAS PRINCIPALES

## Login

```text id="h98w0g"
login.html
↓
login.js
↓
api.js
↓
auth.py
↓
auth_service.py
↓
core.user
```

---

## Customer Express

```text id="o4jyp0"
customer-express.html
↓
customer-express.js
↓
api.js
↓
customers_express.py
↓
customer_express_service.py
↓
customers_express
```

---

## Dashboard

```text id="c31jxf"
dashboard.html
↓
dashboard.js
↓
menu.py
↓
core.menu
```

---

## Analytics

```text id="wbblwo"
ganancias-mensuales.html
↓
ganancias-mensuales.js
↓
analytics.py
↓
analytics_service.py
↓
sales
```

---

# 36. TABLAS UTILIZADAS

## Core

```text id="7g72v0"
core.user

core.person

core.person_organization

core.organization

core.tenant

core.user_session

core.menu

core.role_menu

core.group_role

core.customers_express_token_map
```

---

## Tenant

```text id="g8g7jd"
customers_express

customer_capture_settings

identifier_type_settings

sales
```

---

# 37. ARQUITECTURA DE DEPENDENCIAS

```text id="y76d2r"
Frontend
│
├── login
├── dashboard
├── customer express
├── analytics
└── precios
        │
        ↓
api.js
        │
        ↓
Routes
        │
        ↓
Services
        │
        ↓
core/db.py
        │
        ↓
PostgreSQL
```

---

# 38. PATRÓN GENERAL

Toda nueva funcionalidad debe respetar:

```text id="u5w3lp"
HTML

↓

Javascript

↓

api.js

↓

Route

↓

Service

↓

DB

↓

PostgreSQL
```

No debe existir:

* HTML llamando directamente a la base.
* Routes con lógica compleja.
* Services accediendo al frontend.

---

# 39. NUEVO MÓDULO VENTAS LYL

Arquitectura propuesta:

```text id="pb50rn"
ventas-lyl.html
↓
ventas-lyl.js
↓
api.js
↓
routes/ventas_lyl.py
↓
services/ventas_lyl_service.py
↓
core.stg_ventas_lyl
```

Endpoints previstos:

### POST /ventas-lyl/upload

Carga Excel.

---

### GET /ventas-lyl

Consulta registros.

---

### PUT /ventas-lyl

Edición de registros.

---

Proceso:

```text id="qof1zv"
Seleccionar año

↓

Seleccionar mes

↓

Subir Excel

↓

Leer hoja VENTAS

↓

Eliminar registros existentes

↓

Insertar datos

↓

Mostrar información

↓

Editar ventas
```

---

# 40. OBSERVACIONES

La arquitectura actual es consistente.

El patrón:

```text id="qqh61i"
Frontend
↓
api.js
↓
Route
↓
Service
↓
DB
```

se encuentra correctamente definido y permite incorporar nuevos módulos sin alterar los existentes.

FIN PARTE 4


# DOCUMENTO_VIVO_KIVOR.md

# PARTE 5 – ESTADO ACTUAL, DEUDA TÉCNICA Y ROADMAP

---

# 41. ESTADO ACTUAL DEL PROYECTO

KIVOR es un proyecto activo en evolución.

La arquitectura base se encuentra implementada y estable.

El sistema presenta una separación consistente entre:

* Frontend.
* Backend.
* Servicios.
* Base de datos.

---

# 42. MÓDULOS EXISTENTES

## Autenticación

Estado:

✅ COMPLETO

Componentes:

```text
login.html

login.js

routes/auth.py

services/auth_service.py

core/security.py
```

Funciones:

* Login.
* Logout.
* JWT.
* Validación de sesión.

---

## Usuarios

Estado:

✅ COMPLETO

Archivos:

```text
routes/users.py

services/user_service.py

schemas/user_schema.py
```

Funciones:

* Creación de usuarios.

---

## Menú Dinámico

Estado:

✅ COMPLETO

Archivos:

```text
dashboard.js

routes/menu.py
```

Funciones:

* Construcción dinámica.
* Control por roles.

---

## Customer Express

Estado:

✅ COMPLETO

Archivos:

```text
customer-express.html

customer-express.js

routes/customers_express.py

services/customer_express_service.py
```

Funciones:

* Formularios dinámicos.
* Captura de clientes.
* Tokens de acceso.

---

## Dashboard

Estado:

✅ COMPLETO

Funciones:

* Navegación principal.
* Carga de menú.

---

## Analytics

Estado:

✅ COMPLETO

Archivos:

```text
ganancias-mensuales.html

ganancias-mensuales.js

routes/analytics.py

services/analytics_service.py
```

Funciones:

* Ganancias mensuales.
* Familias.
* Niveles.
* Precios.

---

# 43. MÓDULOS EN DESARROLLO

## Mantenedor de Precios

Estado:

🟡 EN DESARROLLO

Archivos:

```text
mantenedor-precios.html

mantenedor-precios.js
```

Pendiente:

* CRUD completo.
* Persistencia.

---

## Ganancias Mensuales

Estado:

🟡 EN EVOLUCIÓN

Pendiente:

* Mejorar visualizaciones.
* Indicadores avanzados.

---

# 44. MÓDULOS PLANIFICADOS

## Ventas LYL

Estado:

🟢 INCORPORADO / EN VALIDACIÓN

Objetivo:

Carga masiva de ventas desde archivo Excel hacia tabla staging PostgreSQL.

Alcance actual:

* Pantalla de carga de ventas.
* Selección obligatoria de año, mes y archivo Excel.
* Confirmación explícita antes de reemplazar datos del período.
* Lectura de hoja `VENTAS`.
* Validación de columnas obligatorias.
* Filtrado por período seleccionado.
* Eliminación previa de registros existentes del período.
* Inserción de registros normalizados en tabla staging.
* Respuesta con período, registros eliminados e insertados.

Tabla:

```text
core.stg_ventas_lyl
```

Backend:

```text
routes/ventas_lyl.py

services/ventas_lyl_service.py

schemas/ventas_lyl_schema.py
```

Frontend:

```text
ventas-lyl-carga.html

ventas-lyl-carga.js

ventas-lyl.css
```

Endpoint:

```text
POST /ventas-lyl/upload
```

---

## Agenda

Estado:

🔵 PLANIFICADO

---

## Servicios

Estado:

🔵 PLANIFICADO

---

## Profesionales

Estado:

🔵 PLANIFICADO

---

## Clientes

Estado:

🔵 PLANIFICADO

---

## Caja

Estado:

🔵 PLANIFICADO

---

## Comisiones

Estado:

🔵 PLANIFICADO

---

## Reportes

Estado:

🔵 PLANIFICADO

---

## Analytics Avanzado

Estado:

🔵 PLANIFICADO

---

# 45. DEUDA TÉCNICA

---

## ALTA

### role_menu

Situación:

Acoplado directamente a roles.

Problema:

Escalabilidad limitada.

Objetivo futuro:

Migrar hacia permisos granulares.

---

### user_session

Situación:

Implementación básica.

Pendientes:

* Expiración configurable.
* Revocación avanzada.

---

### Auditoría transversal

Situación:

No implementada.

Objetivo:

Registrar:

* inserciones;
* modificaciones;
* eliminaciones.

---

# MEDIA

## catalog.parameters

Situación:

Poco utilizado.

Pendiente:

Centralizar parámetros.

---

## business_theme

Situación:

No completamente integrado.

Pendiente:

Temas visuales dinámicos.

---

## customer_identifiers

Situación:

Requiere revisión.

Objetivo:

Mayor flexibilidad internacional.

---

# BAJA

## Internacionalización

Archivo:

```text
i18n.js
```

Pendiente:

Implementación completa.

---

# 46. PRINCIPIOS DE DESARROLLO

Todo nuevo desarrollo debe cumplir:

### No generar código sin arquitectura.

### No duplicar lógica.

### Reutilizar componentes.

### Mantener separación entre:

```text
Frontend

↓

API

↓

Route

↓

Service

↓

Base de datos
```

### Priorizar simplicidad.

### Evitar sobreingeniería.

### Mantener consistencia.

---

# 47. ROADMAP

---

## FASE 1

### Ventas LYL

Carga Excel.

Tabla staging.

Editor de ventas.

Transformación posterior.

---

### Mantenedor de Precios

Finalización.

---

### Mejoras Analytics

Indicadores adicionales.

---

## FASE 2

### Servicios

CRUD.

---

### Profesionales

CRUD.

---

### Locales

Administración.

---

### Agenda

Gestión de reservas.

---

## FASE 3

### Clientes

CRM.

---

### Caja

Movimientos.

---

### Comisiones

Liquidaciones.

---

### Reportería

Informes.

---

## FASE 4

### Analytics Avanzado

KPIs.

Rentabilidad.

Comparativos.

---

### Inteligencia Artificial

Asistentes.

Automatización.

Predicciones.

---

# 48. COMPONENTES FUTUROS

Objetivo:

Estandarizar interfaz.

---

## GRID_KIVOR

Tablas reutilizables.

---

## MODAL_KIVOR

Ventanas modales.

---

## BUTTON_KIVOR

Botones estándar.

---

## SELECT_KIVOR

Combos reutilizables.

---

## ALERT_KIVOR

Mensajes y notificaciones.

---

## DATE_KIVOR

Fechas estandarizadas.

---

## UPLOAD_KIVOR

Carga de archivos.

---

# 49. MODELO DE CRECIMIENTO

Toda nueva funcionalidad debe seguir:

```text
Pantalla HTML

↓

Javascript

↓

api.js

↓

Route

↓

Service

↓

core/db.py

↓

PostgreSQL
```

No se permiten:

* accesos directos desde HTML a la base;
* lógica compleja en Routes;
* SQL dentro del frontend.

---

# 50. OBJETIVO DEL DOCUMENTO VIVO

Este documento constituye la memoria técnica oficial de KIVOR.

Debe evolucionar junto al sistema.

Todo nuevo módulo debe ser incorporado a este documento.

La consistencia arquitectónica tiene prioridad sobre la velocidad de desarrollo.

FIN PARTE 5

# DOCUMENTO_VIVO_KIVOR.md

# PARTE 6 – MODELO DE DATOS Y ARQUITECTURA DE BASE DE DATOS

---

# 51. BASE DE DATOS

Nombre:

```text
boekdb
```

Motor:

```text
PostgreSQL
```

Arquitectura:

Multi Tenant mediante esquemas.

Principio:

```text
Core compartido
+
Schemas por empresa
```

---

# 52. ESQUEMAS

## core

Responsabilidad:

Infraestructura transversal.

Contiene:

* usuarios
* organizaciones
* tenants
* menús
* sesiones
* autenticación
* configuraciones globales

---

## catalog

Responsabilidad:

Catálogos reutilizables.

Ejemplos:

* parámetros
* tipos

---

## audit

Responsabilidad:

Auditoría.

Objetivo:

Registrar operaciones.

---

## tenant schemas

Ejemplo:

```text
lindasylunaticas
```

Contienen:

Información específica del negocio.

---

# 53. MODELO MULTI TENANT

Arquitectura:

```text
core
│
├── tenant
│
└── organization
          │
          ↓
tenant_schema
          │
          ↓
lindasylunaticas
```

Cambio dinámico:

```python
set_tenant_schema()
```

Permite que distintos clientes compartan la misma plataforma.

---

# 54. TABLAS CORE

---

## core.user

Responsabilidad:

Usuarios del sistema.

Relaciones:

```text
user
↓
person
↓
organization
↓
tenant
```

Utilizado por:

* auth_service.py
* user_service.py

---

## core.person

Responsabilidad:

Personas.

Relaciones:

```text
person
↓
person_organization
```

---

## core.person_organization

Responsabilidad:

Asociar personas con organizaciones.

---

## core.organization

Responsabilidad:

Empresas.

---

## core.tenant

Responsabilidad:

Configuración tenant.

Campo principal:

```text
tenant_schema
```

---

## core.user_session

Responsabilidad:

Sesiones.

Utilizado por:

* security.py
* auth_service.py
* menu.py

---

## core.menu

Responsabilidad:

Menú dinámico.

Relaciones:

```text
menu
↓
role_menu
↓
group_role
```

---

## core.role_menu

Responsabilidad:

Asignación menú-rol.

---

## core.group_role

Responsabilidad:

Roles.

---

## core.customers_express_token_map

Responsabilidad:

Tokens Customer Express.

Utilizado por:

```text
customer_express_service.py
```

---

## core.identifier_types

Responsabilidad:

Tipos de identificadores.

Ejemplos:

* RUT
* DNI
* PASAPORTE

---

## core.business_theme

Responsabilidad:

Temas visuales.

Pendiente de integración completa.

---

# 55. ESQUEMA CATALOG

---

## catalog.parameters

Responsabilidad:

Catálogos reutilizables.

Uso futuro:

Centralizar configuraciones.

---

# 56. ESQUEMA AUDIT

Objetivo:

Registrar:

* INSERT
* UPDATE
* DELETE

Estado:

Pendiente evolución.

---

# 57. TABLAS TENANT

Ejemplo:

```text
lindasylunaticas
```

---

## customers_express

Responsabilidad:

Captura de clientes.

Utilizado por:

```text
customer_express_service.py
```

---

## customer_capture_settings

Responsabilidad:

Configuración dinámica.

Permite:

* agregar campos
* cambiar obligatoriedad
* controlar formularios

---

## identifier_type_settings

Responsabilidad:

Configuración identificadores.

---

## sales

Responsabilidad:

Información comercial.

Utilizado por:

```text
analytics_service.py
```

Indicadores:

* ganancias
* familias
* niveles
* precios

---

# 58. RELACIONES PRINCIPALES

Autenticación:

```text
tenant
↓
organization
↓
person
↓
user
↓
user_session
```

---

Menú:

```text
group_role
↓
role_menu
↓
menu
```

---

Customer Express:

```text
customers_express_token_map
↓
customers_express
↓
customer_capture_settings
↓
identifier_type_settings
```

---

Analytics:

```text
sales
↓
familias
↓
niveles
↓
precios
```

---

# 59. NUEVO MÓDULO VENTAS LYL

Tabla staging:

```text
core.stg_ventas_lyl
```

Objetivo:

Mantener una copia fiel del Excel, con excepción de `ventas_key` (ver nota más abajo).

---

## Estructura

Clave primaria:

```text
ventas_key
```

Tipo:

```text
VARCHAR(50)
```

Nota:

```text
ventas_key ya no se copia del Excel: se calcula en el proceso de carga
(rut_celular & "-" & MM & DD & "-" & correlativo). Ver fórmula y regla
de validación completas en la sección 92.2 REGLAS DE NEGOCIO.
```

---

Campos principales:

```text
sp
fecha_entrega
profesional
fecha_recau
rut_celular
nombre
origen
nro_formulario
familia
nivel_2
nivel_3
nivel_4
precio_profesional
precio_web
porcentaje_profesional
abono
pagados
total
nro_getnet
total_pw
valida_form
abono_perdido
descuento
descuentos
ganancia_prof
total_ganancia_prof
ganancia_salon
descprof_a_clientas
anio
anio_mes
obs
```

Campos técnicos:

```text
archivo_origen
hoja_origen
fila_excel
fecha_carga
```

---

DDL oficial (confirmado 2026-08-17):

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

Nota: `descuento` es texto (`varchar(50)`), no numérico; el monto de descuento calculado vive en `descuentos` (`numeric(16,4)`). No confundir ambas columnas.

---

# 60. MODELO FUTURO DE VENTAS

Arquitectura recomendada:

```text
Excel
↓
core.stg_ventas_lyl
↓
ETL
↓
sales
↓
analytics
```

Principio:

La tabla staging nunca reemplaza el modelo de negocio.

---

# 61. PRINCIPIOS DEL MODELO DE DATOS

Separar:

```text
Core
↓
Infraestructura

Tenant
↓
Negocio
```

Mantener:

* multiempresa
* independencia entre clientes
* reutilización

---

# 62. EVOLUCIÓN FUTURA

## Servicios

Tablas:

```text
services
service_family
service_category
```

---

## Profesionales

Tablas:

```text
professionals
professional_commissions
```

---

## Agenda

Tablas:

```text
appointments
appointment_status
appointment_services
```

---

## Caja

Tablas:

```text
cash_movements
cash_closing
payment_methods
```

---

## Comisiones

Tablas:

```text
commissions
commission_details
```

---

## Reportes

Tablas:

```text
report_templates
```

---

# 63. PRINCIPIOS DE CRECIMIENTO

No modificar:

```text
core
```

para resolver reglas específicas del negocio.

Las nuevas funcionalidades deben implementarse preferentemente en los schemas tenant.

---

# 64. OBJETIVO DEL MODELO

Garantizar:

* escalabilidad
* multiempresa
* independencia
* mantenibilidad

sin romper la arquitectura actual.

FIN PARTE 6


# DOCUMENTO_VIVO_KIVOR.md

# PARTE 7 – COMPONENTES, ESTÁNDARES, CONVENCIONES Y ARQUITECTURA DE EVOLUCIÓN

---

# 65. FILOSOFÍA DE DESARROLLO

KIVOR está diseñado para evolucionar mediante módulos independientes.

Principios:

* Simplicidad.
* Reutilización.
* Consistencia.
* Bajo acoplamiento.
* Escalabilidad.

Toda nueva funcionalidad debe adaptarse a la arquitectura existente y no al revés.

---

# 66. COMPONENTES REUTILIZABLES ACTUALES

Carpeta:

```text id="79rqdc"
js/components/
```

---

## INPUT_BASE

Archivo:

```text id="xv39au"
input-base.js
```

Responsabilidad:

Componente base para entradas.

Objetivos:

* Uniformidad.
* Validación.
* Eventos comunes.

---

## INPUT_RUT

Archivo:

```text id="n20drk"
input-rut.js
```

Responsabilidad:

Captura de RUT.

Funciones:

* Validación.
* Formateo.
* Normalización.

---

## INPUT_TEXT

Archivo:

```text id="e7tjlwm"
input-text.js
```

Responsabilidad:

Entrada estándar de texto.

---

# 67. COMPONENTES FUTUROS

El objetivo es construir una biblioteca KIVOR.

---

## GRID_KIVOR

Responsabilidad:

Tablas reutilizables.

Características:

* Ordenamiento.
* Paginación.
* Filtros.
* Exportación.

---

## BUTTON_KIVOR

Botones estándar.

Tipos:

```text id="qhhpyj"
primary
secondary
success
danger
warning
```

---

## MODAL_KIVOR

Ventanas emergentes.

Funciones:

* confirmación
* mensajes
* edición

---

## SELECT_KIVOR

Combos reutilizables.

Características:

* búsqueda
* carga dinámica

---

## DATE_KIVOR

Selector de fechas.

---

## UPLOAD_KIVOR

Carga de archivos.

Funciones:

* drag & drop
* validación
* progreso

---

## ALERT_KIVOR

Mensajes estándar.

Tipos:

```text id="ydj2e2"
success
warning
error
info
```

---

# 68. ESTÁNDARES DE DESARROLLO

---

## Backend

Arquitectura obligatoria:

```text id="kpxwqv"
Route
↓
Service
↓
DB
```

Restricciones:

No incluir SQL en Routes.

No incluir lógica compleja en main.py.

No acceder directamente a PostgreSQL desde el frontend.

---

## Frontend

Arquitectura:

```text id="3crh2x"
HTML
↓
JS
↓
api.js
↓
Backend
```

Restricciones:

Evitar lógica en HTML.

Centralizar llamadas HTTP.

Reutilizar componentes.

---

# 69. CONVENCIONES DE NOMENCLATURA

---

## Backend

Routes:

```text id="sr24eq"
nombre_modulo.py
```

Ejemplo:

```text id="ifd2jr"
ventas_lyl.py
```

---

Services:

```text id="dt1tcy"
nombre_modulo_service.py
```

Ejemplo:

```text id="7sp3l2"
ventas_lyl_service.py
```

---

Schemas:

```text id="70pyhh"
nombre_modulo_schema.py
```

---

## Frontend

Pantallas:

```text id="2xmbvg"
ventas-lyl.html
```

Javascript:

```text id="w7e3zr"
ventas-lyl.js
```

CSS:

```text id="8xqvji"
ventas-lyl.css
```

---

# 70. PATRÓN DE MÓDULOS

Todo módulo nuevo debe seguir:

```text id="ln0wwk"
HTML

↓

JS

↓

api.js

↓

Route

↓

Service

↓

PostgreSQL
```

---

# 71. PATRÓN CRUD

Arquitectura recomendada:

```text id="t4uql7"
LISTAR

GET

↓

CREAR

POST

↓

ACTUALIZAR

PUT

↓

ELIMINAR

DELETE
```

---

# 72. PATRÓN DE TABLAS STAGING

Objetivo:

Mantener copias fieles de información externa.

Ejemplo:

```text id="xyb18v"
core.stg_ventas_lyl
```

Flujo:

```text id="6l9a4k"
Archivo externo

↓

Tabla staging

↓

ETL

↓

Modelo de negocio
```

---

# 73. PRINCIPIOS DE BASE DE DATOS

Separar:

```text id="e74tx5"
core
↓
infraestructura

tenant schema
↓
negocio
```

Evitar:

* lógica en triggers innecesarios.
* duplicación.
* dependencias circulares.

---

# 74. ESTÁNDAR DE API

Formato respuesta:

Éxito:

```json id="9r5h2j"
{
  "success": true,
  "data": {}
}
```

Error:

```json id="8q3z4m"
{
  "success": false,
  "message": ""
}
```

---

# 75. ESTÁNDAR DE AUDITORÍA FUTURA

Registrar:

* usuario
* fecha
* operación
* tabla
* valor anterior
* valor nuevo

Operaciones:

```text id="tup55g"
INSERT
UPDATE
DELETE
```

---

# 76. ARQUITECTURA DE CRECIMIENTO

Módulos previstos:

```text id="0dn73i"
Ventas LYL

Servicios

Profesionales

Agenda

Clientes

Caja

Comisiones

Reportes

Analytics Avanzado

IA
```

Todos deben respetar:

```text id="4yg65l"
Pantalla

↓

JS

↓

api.js

↓

Route

↓

Service

↓

DB
```

---

# 77. DOCUMENTO VIVO

El presente documento constituye la memoria oficial del proyecto.

Toda modificación estructural deberá reflejarse aquí.

Este documento tiene prioridad sobre documentación dispersa.

Su objetivo es:

* preservar conocimiento;
* reducir deuda técnica;
* facilitar crecimiento;
* mantener consistencia.

---

# 78. REGLA FUNDAMENTAL DE KIVOR

Nunca desarrollar primero.

Siempre:

```text id="w7gtg9"
Analizar

↓

Diseñar

↓

Arquitectura

↓

Modelo de datos

↓

Desarrollo

↓

QA

↓

Producción
```

La consistencia tiene prioridad sobre la velocidad.

---

FIN PARTE 7

---

> **Nota de mantenimiento del documento (2026-08-08):** la sección 92 aparece duplicada íntegramente en dos ubicaciones de este archivo (esta y otra más adelante). Ambas copias se actualizaron de forma idéntica al incorporar el modo "año completo". Se recomienda que el dueño del documento evalúe consolidarlas en una sola instancia para evitar futuras desincronizaciones.

# 92. ACTUALIZACIÓN VERSIÓN 1.1 – MÓDULO CARGA DE VENTAS LYL DESDE EXCEL

Fecha de actualización:

```text
2026-06-27
```

Estado:

```text
Proyecto en curso
Módulo Ventas LYL incorporado y pendiente de validación integral QA
```

---

## 92.1 REQUERIMIENTO INCORPORADO

KIVOR incorpora una nueva función operativa para cargar ventas desde un archivo Excel y dejarlas persistidas en una tabla staging de PostgreSQL.

Objetivo funcional:

```text
Archivo Excel de ventas
↓
Validación de estructura
↓
Selección de año y mes
↓
Filtro del período
↓
Eliminación de registros previos del período en staging
↓
Inserción de registros nuevos
↓
Respuesta de carga
```

---

## 92.2 REGLAS DE NEGOCIO

* La carga requiere seleccionar año.
* La carga requiere seleccionar mes específico o la opción "Todos los meses (año completo)".
* La carga requiere adjuntar archivo Excel.
* El archivo debe contener la hoja `VENTAS`.
* Las columnas obligatorias del Excel son `AÑO`, `AÑO-MES`, `RUT / CELULAR` y `FECHA ENTREGA`.
* `VENTAS_KEY` ya no se lee desde el Excel: se calcula en tiempo de carga para cada fila insertada en `core.stg_ventas_lyl`.
* Fórmula de `VENTAS_KEY`: `rut_celular & "-" & MM & DD & "-" & correlativo`, donde `MM` y `DD` corresponden al mes y día de `fecha_entrega` (con cero a la izquierda si son de un dígito, concatenados sin separador entre ambos) y `correlativo` es un número secuencial sin formato (sin ceros a la izquierda), de 1 a N, asignado según la posición de la fila dentro del conjunto ya filtrado por período. El correlativo se reinicia en cada carga.
* Si `RUT / CELULAR` viene vacío o inválido en una fila, no se rechaza la carga: se asigna el valor por defecto `"999"`, tanto para la columna `rut_celular` en `core.stg_ventas_lyl` como para el segmento correspondiente de `VENTAS_KEY`.
* Si `FECHA ENTREGA` viene vacía o con formato de fecha inválido en cualquier fila, la carga completa se rechaza informando el detalle de las filas con error; no se elimina ni se inserta ningún registro.
* El período operativo se representa como `YYYY-MM` en modo mes específico, o como `YYYY` en modo año completo.
* En modo mes específico, sólo se cargan filas cuyo `AÑO` y `AÑO-MES` coincidan con el período seleccionado.
* En modo año completo, se cargan todas las filas cuyo `AÑO` coincida con el año seleccionado, aunque el Excel no traiga los 12 meses (datos parciales permitidos).
* Si no existen registros para el período seleccionado (ningún registro del mes, o ningún registro del año en modo año completo), la carga se rechaza.
* Antes de insertar, KIVOR elimina los registros existentes del mismo período en `core.stg_ventas_lyl`: en modo mes específico elimina sólo ese mes; en modo año completo elimina todo el año, independientemente de qué meses trae el Excel.
* La operación confirma al usuario que reemplazará los datos del período seleccionado.
* La carga requiere usuario autenticado mediante JWT.

---

## 92.3 UX/UI

Nueva pantalla:

```text
ventas-lyl-carga.html
```

CSS asociado:

```text
ventas-lyl.css
```

Javascript asociado:

```text
ventas-lyl-carga.js
```

Elementos principales:

* Selector de año.
* Selector de mes.
* Input de archivo Excel.
* Caja de archivo seleccionado.
* Botón `Cargar ventas`.
* Modal de confirmación.
* Mensaje de resultado exitoso o error.

Flujo de usuario:

```text
Usuario abre Carga de Ventas
↓
Selecciona año
↓
Selecciona mes
↓
Adjunta Excel
↓
KIVOR muestra confirmación
↓
Usuario confirma
↓
Frontend envía FormData al backend
↓
KIVOR muestra registros eliminados e insertados
```

Observación UX:

La pantalla advierte explícitamente que la carga elimina primero los registros existentes del período seleccionado en staging.

Modo de carga:

```text
El selector de mes incluye la opción "Todos los meses (año completo)" (valor sentinel TODOS).
Si se elige, el mes se omite del envío y el backend interpreta la carga como año completo.
El modal de confirmación y el mensaje de resultado muestran textos distintos según el modo:
- Mes específico: referencia el período YYYY-MM.
- Año completo: referencia el año completo y advierte que elimina los 12 meses.
```

---

## 92.4 ARQUITECTURA

La función respeta el patrón oficial de KIVOR:

```text
HTML
↓
ventas-lyl-carga.js
↓
api.js
↓
POST /ventas-lyl/upload
↓
routes/ventas_lyl.py
↓
services/ventas_lyl_service.py
↓
core/db.py
↓
PostgreSQL
```

Router registrado en:

```text
main.py
```

Router:

```python
app.include_router(ventas_lyl.router)
```

Prefijo:

```text
/ventas-lyl
```

Endpoint:

```text
POST /ventas-lyl/upload
```

Autenticación:

```text
core.security.verify_token
```

---

## 92.5 MODELO DE DATOS POSTGRESQL

Tabla destino:

```text
core.stg_ventas_lyl
```

Rol de la tabla:

```text
Tabla staging para carga inicial de ventas LYL desde Excel.
```

DDL oficial (confirmado 2026-08-17):

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

Campos calculados en el proceso de carga (no se leen del Excel):

```text
ventas_key (varchar(50)) — ver fórmula en 92.2 REGLAS DE NEGOCIO
```

Campos mapeados desde Excel:

```text
sp
fecha_entrega
profesional
fecha_recau
rut_celular
nombre
origen
nro_formulario
familia
nivel_2
nivel_3
nivel_4
precio_profesional
precio_web
porcentaje_profesional
abono
pagados
total
nro_getnet
total_pw
valida_form
abono_perdido
descuento
descuentos
ganancia_prof
total_ganancia_prof
ganancia_salon
descprof_a_clientas
anio
anio_mes
obs
```

Campos de trazabilidad incorporados:

```text
archivo_origen
hoja_origen
fila_excel
```

Criterio de reemplazo del período:

```sql
-- Modo mes específico
DELETE FROM core.stg_ventas_lyl
WHERE anio = %s
AND anio_mes = %s;

-- Modo año completo
DELETE FROM core.stg_ventas_lyl
WHERE anio = %s;
```

---

## 92.6 BACKEND / API

Archivos incorporados:

```text
routes/ventas_lyl.py
services/ventas_lyl_service.py
schemas/ventas_lyl_schema.py
```

Route:

```text
POST /ventas-lyl/upload
```

Parámetros recibidos:

```text
anio: int
mes: Optional[int]  (ausente u omitido = modo año completo)
file: UploadFile
```

Dependencia de seguridad:

```text
verify_token
```

Schema de respuesta:

```text
UploadVentasResponse
```

Campos de respuesta:

```text
success
anio
mes        (Optional[int], null en modo año completo)
anio_mes   (Optional[str], null en modo año completo)
rows_deleted
rows_inserted
message
```

Servicio principal:

```text
upload_ventas_service()
```

Funciones internas:

```text
clean_value()
normalize_columns()
validate_columns()
filter_period()
build_insert_rows()
delete_period()
insert_dataframe_ventas()
```

Dependencias funcionales:

```text
pandas
openpyxl
python-multipart
```

---

## 92.7 COMPONENTES REUTILIZABLES

Componentes detectados en esta función:

```text
UPLOAD_KIVOR
MODAL_KIVOR
ALERT_KIVOR
SELECT_KIVOR
BUTTON_KIVOR
```

Estado:

```text
Implementados localmente en ventas-lyl-carga.html / ventas-lyl.css / ventas-lyl-carga.js.
Pendiente extraer a componentes reutilizables globales.
```

Regla futura:

Todo nuevo módulo que cargue archivos deberá reutilizar el patrón `UPLOAD_KIVOR`.

---

## 92.8 ESTADO ACTUAL DEL PROYECTO

Clasificación del módulo:

```text
Módulo: Ventas LYL - Carga Excel
Estado: Incorporado
Clasificación: Incompleto hasta completar QA, script SQL formal y validación de integración con api.js
```

Elementos completos:

* Pantalla HTML de carga.
* Estilos CSS específicos.
* Javascript de selección, confirmación y envío.
* Route FastAPI protegida por token.
* Schema Pydantic de respuesta.
* Servicio de lectura Excel, validación, filtrado, delete e insert.

Elementos confirmados (actualización 2026-08-17):

* Existencia formal de la tabla `core.stg_ventas_lyl` en el esquema productivo.
* Tipos de datos definitivos de cada columna (ver DDL oficial en 92.5).

Elementos pendientes de confirmación:

* Índices recomendados para `anio` y `anio_mes`.
* Restricciones de unicidad, si aplica.
* Integración exacta de función `cargarVentasLYL()` dentro de `api.js`.
* Permisos de menú para exponer la pantalla sólo a roles autorizados.
* Pruebas con archivos Excel reales.

---

## 92.9 DEUDA TÉCNICA DETECTADA

### Media – Tabla staging sin DDL documentado (RESUELTO)

Situación:

El código utiliza `core.stg_ventas_lyl`, pero el Documento Vivo aún no contenía el script `CREATE TABLE`.

Acción tomada:

DDL oficial documentado en 92.5 y en la sección 59 (actualización 2026-08-17). Pendiente aún: índices recomendados y restricciones de unicidad adicionales, si aplican.

---

### Media – Componentes de carga aún locales

Situación:

El módulo define su propio layout de carga, modal y alertas.

Acción recomendada:

Extraer `UPLOAD_KIVOR`, `MODAL_KIVOR`, `ALERT_KIVOR`, `SELECT_KIVOR` y `BUTTON_KIVOR`.

---

### Media – Manejo de errores genérico

Situación:

El route captura excepciones generales y responde HTTP 400.

Acción recomendada:

Migrar errores esperados a `AppException` / `InvalidDataError` para mantener estándar KIVOR.

---

### Baja – Código inalcanzable en servicio (RESUELTO)

Situación:

`upload_ventas_service()` contenía un segundo `return` después del bloque `try/except`, por lo que no se ejecutaba.

Acción tomada:

Eliminado al extender la función para soportar el modo "año completo" (actualización 2026-08-08).

---

### Baja – Validación de extensión y tamaño de archivo

Situación:

El frontend acepta `.xlsx,.xls`, pero el backend aún no valida explícitamente extensión, tamaño máximo o tipo MIME.

Acción recomendada:

Agregar validación backend antes de procesar el Excel.

---

## 92.10 QA REQUERIDO

Casos mínimos:

* Cargar archivo válido con registros del período.
* Cargar archivo válido sin registros del período.
* Cargar archivo sin hoja `VENTAS`.
* Cargar archivo sin columnas obligatorias.
* Cargar mes inválido.
* Cargar sin token.
* Repetir carga del mismo período y validar reemplazo.
* Validar conteo de `rows_deleted`.
* Validar conteo de `rows_inserted`.
* Validar trazabilidad `archivo_origen`, `hoja_origen`, `fila_excel`.
* Validar archivo con celdas vacías.
* Validar columnas opcionales ausentes.
* Cargar año completo con Excel que trae los 12 meses.
* Cargar año completo con Excel que trae solo algunos meses (datos parciales).
* Cargar año completo con Excel sin ningún registro de ese año (debe rechazar).
* Repetir carga de año completo y validar que elimina todo el año previo, sin importar qué meses traía la carga anterior.
* Confirmar que el modo mes específico sigue funcionando igual tras la incorporación del modo año completo.

---

## 92.11 ROADMAP DEL MÓDULO VENTAS LYL

### Paso 1 (COMPLETADO 2026-08-17)

Formalizar DDL de `core.stg_ventas_lyl`. Ver DDL oficial en 92.5 y en la sección 59.

### Paso 2

Validar carga en ambiente de desarrollo con Excel real.

### Paso 3

Conectar pantalla al menú dinámico por rol.

### Paso 4

Extraer componentes reutilizables de carga y modal.

### Paso 5

Crear proceso ETL desde staging hacia modelo definitivo de ventas.

### Paso 6

Conectar Analytics a la fuente validada.

### Paso 7

Agregar auditoría de carga por usuario, fecha y archivo.

---

FIN DOCUMENTO_VIVO_KIVOR.md VERSION 1.0


# DOCUMENTO_VIVO_KIVOR.md

# PARTE 8 – GOBIERNO DEL PROYECTO, METODOLOGÍA Y PLAN DE EVOLUCIÓN

---

# 79. PROPÓSITO DEL DOCUMENTO VIVO

El DOCUMENTO_VIVO_KIVOR constituye la memoria técnica oficial del sistema.

Objetivos:

* Preservar conocimiento.
* Mantener consistencia.
* Reducir deuda técnica.
* Facilitar la evolución del producto.
* Incorporar nuevos desarrolladores sin pérdida de contexto.

El documento debe evolucionar junto con el sistema.

---

# 80. METODOLOGÍA DE DESARROLLO KIVOR

Todo desarrollo deberá seguir el siguiente flujo:

```text id="4m6y9w"
Análisis

↓

UX/UI

↓

Arquitectura

↓

Modelo de datos

↓

Backend

↓

Frontend

↓

QA

↓

Producción
```

No se debe invertir este orden.

---

# 81. AGENTES DE DESARROLLO

## ANALISTA

Responsabilidad:

Comprender el problema.

Definir:

* requerimientos;
* reglas de negocio;
* casos borde.

No define implementación.

---

## UX/UI

Responsabilidad:

Diseñar:

* pantallas;
* flujos;
* componentes reutilizables.

---

## ARQUITECTO

Responsabilidad:

Definir:

* estructura;
* tecnologías;
* patrones;
* dependencias.

Objetivo:

Evitar sobreingeniería.

---

## DBA

Responsabilidad:

Definir:

* tablas;
* relaciones;
* restricciones;
* índices.

---

## DESARROLLADOR

Responsabilidad:

Construir:

* backend;
* frontend;
* SQL;
* APIs.

---

## QA

Responsabilidad:

Validar:

* funcionamiento;
* errores;
* consistencia;
* casos borde.

---

# 82. CICLO DE VIDA DE LOS MÓDULOS

Cada módulo debe pasar por:

```text id="qgw2b9"
Idea

↓

Análisis

↓

Diseño

↓

Desarrollo

↓

Pruebas

↓

Producción

↓

Mantenimiento
```

---

# 83. CONTROL DE DEUDA TÉCNICA

Clasificación:

## Alta

Afecta arquitectura.

Debe corregirse inmediatamente.

---

## Media

No bloquea.

Debe planificarse.

---

## Baja

Mejoras futuras.

---

Toda deuda técnica detectada deberá registrarse.

---

# 84. CONTROL DE VERSIONES

Versiones mayores:

```text id="74z4e4"
1.x
2.x
3.x
```

Versiones menores:

```text id="3g31c0"
1.1
1.2
1.3
```

Cambios pequeños:

```text id="6r2ok8"
1.1.1
1.1.2
```

---

# 85. ROADMAP GENERAL

## FASE 1

Consolidación.

Módulos:

```text id="wnyrk8"
Ventas LYL

Mantenedor de Precios

Analytics
```

---

## FASE 2

Operación.

Módulos:

```text id="y5f49e"
Servicios

Profesionales

Locales

Agenda
```

---

## FASE 3

Administración.

Módulos:

```text id="fl1wii"
Clientes

Caja

Comisiones

Reportes
```

---

## FASE 4

Inteligencia.

Módulos:

```text id="3x8wx8"
Analytics Avanzado

Predicciones

IA

Automatización
```

---

# 86. MÓDULOS FUTUROS

## VENTAS LYL

Objetivo:

Carga masiva desde Excel.

Modelo:

```text id="myjhdv"
Excel

↓

core.stg_ventas_lyl

↓

ETL

↓

Modelo definitivo
```

---

## SERVICIOS

Administración de servicios.

---

## PROFESIONALES

Control de profesionales.

---

## AGENDA

Reservas.

Bloques horarios.

Estados.

---

## CLIENTES

CRM.

Segmentación.

Historial.

---

## CAJA

Ingresos.

Egresos.

Cierres.

---

## COMISIONES

Liquidaciones.

Cálculos.

Pagos.

---

## REPORTES

KPIs.

Exportación.

PDF.

Excel.

---

## ANALYTICS AVANZADO

Rentabilidad.

Comparativos.

Tendencias.

---

## IA

Asistentes.

Predicciones.

Automatización.

---

# 87. PRINCIPIOS DE EVOLUCIÓN

Toda nueva funcionalidad deberá:

* Reutilizar componentes existentes.
* Mantener separación por capas.
* Evitar duplicación.
* Mantener bajo acoplamiento.
* Respetar el modelo multi-tenant.
* Mantener compatibilidad hacia atrás.

---

# 88. ESTRATEGIA DE COMPONENTIZACIÓN

Biblioteca futura:

```text id="m0c8k7"
INPUT_KIVOR

BUTTON_KIVOR

GRID_KIVOR

SELECT_KIVOR

DATE_KIVOR

MODAL_KIVOR

UPLOAD_KIVOR

ALERT_KIVOR
```

Objetivo:

Unificar comportamiento visual y funcional.

---

# 89. ESTRATEGIA DE TESTING

Niveles:

## Unitario

Services.

---

## Integración

Routes + Services + DB.

---

## Funcional

Frontend + Backend.

---

## Usuario

QA.

---

# 90. REGLA DE ORO DEL PROYECTO

No desarrollar primero.

Siempre:

```text id="nmpm0m"
Analizar

↓

Diseñar

↓

Arquitectura

↓

Datos

↓

Desarrollar

↓

Probar

↓

Liberar
```

---

# 91. VISIÓN DE LARGO PLAZO

KIVOR debe convertirse en una plataforma modular, escalable y mantenible, capaz de crecer durante años sin perder consistencia.

La prioridad no es la velocidad.

La prioridad es la sostenibilidad del software.

---

FIN PARTE 8

---

# 92. ACTUALIZACIÓN VERSIÓN 1.1 – MÓDULO CARGA DE VENTAS LYL DESDE EXCEL

Fecha de actualización:

```text
2026-06-27
```

Estado:

```text
Proyecto en curso
Módulo Ventas LYL incorporado y pendiente de validación integral QA
```

---

## 92.1 REQUERIMIENTO INCORPORADO

KIVOR incorpora una nueva función operativa para cargar ventas desde un archivo Excel y dejarlas persistidas en una tabla staging de PostgreSQL.

Objetivo funcional:

```text
Archivo Excel de ventas
↓
Validación de estructura
↓
Selección de año y mes
↓
Filtro del período
↓
Eliminación de registros previos del período en staging
↓
Inserción de registros nuevos
↓
Respuesta de carga
```

---

## 92.2 REGLAS DE NEGOCIO

* La carga requiere seleccionar año.
* La carga requiere seleccionar mes específico o la opción "Todos los meses (año completo)".
* La carga requiere adjuntar archivo Excel.
* El archivo debe contener la hoja `VENTAS`.
* Las columnas obligatorias del Excel son `AÑO`, `AÑO-MES`, `RUT / CELULAR` y `FECHA ENTREGA`.
* `VENTAS_KEY` ya no se lee desde el Excel: se calcula en tiempo de carga para cada fila insertada en `core.stg_ventas_lyl`.
* Fórmula de `VENTAS_KEY`: `rut_celular & "-" & MM & DD & "-" & correlativo`, donde `MM` y `DD` corresponden al mes y día de `fecha_entrega` (con cero a la izquierda si son de un dígito, concatenados sin separador entre ambos) y `correlativo` es un número secuencial sin formato (sin ceros a la izquierda), de 1 a N, asignado según la posición de la fila dentro del conjunto ya filtrado por período. El correlativo se reinicia en cada carga.
* Si `RUT / CELULAR` viene vacío o inválido en una fila, no se rechaza la carga: se asigna el valor por defecto `"999"`, tanto para la columna `rut_celular` en `core.stg_ventas_lyl` como para el segmento correspondiente de `VENTAS_KEY`.
* Si `FECHA ENTREGA` viene vacía o con formato de fecha inválido en cualquier fila, la carga completa se rechaza informando el detalle de las filas con error; no se elimina ni se inserta ningún registro.
* El período operativo se representa como `YYYY-MM` en modo mes específico, o como `YYYY` en modo año completo.
* En modo mes específico, sólo se cargan filas cuyo `AÑO` y `AÑO-MES` coincidan con el período seleccionado.
* En modo año completo, se cargan todas las filas cuyo `AÑO` coincida con el año seleccionado, aunque el Excel no traiga los 12 meses (datos parciales permitidos).
* Si no existen registros para el período seleccionado (ningún registro del mes, o ningún registro del año en modo año completo), la carga se rechaza.
* Antes de insertar, KIVOR elimina los registros existentes del mismo período en `core.stg_ventas_lyl`: en modo mes específico elimina sólo ese mes; en modo año completo elimina todo el año, independientemente de qué meses trae el Excel.
* La operación confirma al usuario que reemplazará los datos del período seleccionado.
* La carga requiere usuario autenticado mediante JWT.

---

## 92.3 UX/UI

Nueva pantalla:

```text
ventas-lyl-carga.html
```

CSS asociado:

```text
ventas-lyl.css
```

Javascript asociado:

```text
ventas-lyl-carga.js
```

Elementos principales:

* Selector de año.
* Selector de mes.
* Input de archivo Excel.
* Caja de archivo seleccionado.
* Botón `Cargar ventas`.
* Modal de confirmación.
* Mensaje de resultado exitoso o error.

Flujo de usuario:

```text
Usuario abre Carga de Ventas
↓
Selecciona año
↓
Selecciona mes
↓
Adjunta Excel
↓
KIVOR muestra confirmación
↓
Usuario confirma
↓
Frontend envía FormData al backend
↓
KIVOR muestra registros eliminados e insertados
```

Observación UX:

La pantalla advierte explícitamente que la carga elimina primero los registros existentes del período seleccionado en staging.

Modo de carga:

```text
El selector de mes incluye la opción "Todos los meses (año completo)" (valor sentinel TODOS).
Si se elige, el mes se omite del envío y el backend interpreta la carga como año completo.
El modal de confirmación y el mensaje de resultado muestran textos distintos según el modo:
- Mes específico: referencia el período YYYY-MM.
- Año completo: referencia el año completo y advierte que elimina los 12 meses.
```

---

## 92.4 ARQUITECTURA

La función respeta el patrón oficial de KIVOR:

```text
HTML
↓
ventas-lyl-carga.js
↓
api.js
↓
POST /ventas-lyl/upload
↓
routes/ventas_lyl.py
↓
services/ventas_lyl_service.py
↓
core/db.py
↓
PostgreSQL
```

Router registrado en:

```text
main.py
```

Router:

```python
app.include_router(ventas_lyl.router)
```

Prefijo:

```text
/ventas-lyl
```

Endpoint:

```text
POST /ventas-lyl/upload
```

Autenticación:

```text
core.security.verify_token
```

---

## 92.5 MODELO DE DATOS POSTGRESQL

Tabla destino:

```text
core.stg_ventas_lyl
```

Rol de la tabla:

```text
Tabla staging para carga inicial de ventas LYL desde Excel.
```

DDL oficial (confirmado 2026-08-17):

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

Campos calculados en el proceso de carga (no se leen del Excel):

```text
ventas_key (varchar(50)) — ver fórmula en 92.2 REGLAS DE NEGOCIO
```

Campos mapeados desde Excel:

```text
sp
fecha_entrega
profesional
fecha_recau
rut_celular
nombre
origen
nro_formulario
familia
nivel_2
nivel_3
nivel_4
precio_profesional
precio_web
porcentaje_profesional
abono
pagados
total
nro_getnet
total_pw
valida_form
abono_perdido
descuento
descuentos
ganancia_prof
total_ganancia_prof
ganancia_salon
descprof_a_clientas
anio
anio_mes
obs
```

Campos de trazabilidad incorporados:

```text
archivo_origen
hoja_origen
fila_excel
```

Criterio de reemplazo del período:

```sql
-- Modo mes específico
DELETE FROM core.stg_ventas_lyl
WHERE anio = %s
AND anio_mes = %s;

-- Modo año completo
DELETE FROM core.stg_ventas_lyl
WHERE anio = %s;
```

---

## 92.6 BACKEND / API

Archivos incorporados:

```text
routes/ventas_lyl.py
services/ventas_lyl_service.py
schemas/ventas_lyl_schema.py
```

Route:

```text
POST /ventas-lyl/upload
```

Parámetros recibidos:

```text
anio: int
mes: Optional[int]  (ausente u omitido = modo año completo)
file: UploadFile
```

Dependencia de seguridad:

```text
verify_token
```

Schema de respuesta:

```text
UploadVentasResponse
```

Campos de respuesta:

```text
success
anio
mes        (Optional[int], null en modo año completo)
anio_mes   (Optional[str], null en modo año completo)
rows_deleted
rows_inserted
message
```

Servicio principal:

```text
upload_ventas_service()
```

Funciones internas:

```text
clean_value()
normalize_columns()
validate_columns()
filter_period()
build_insert_rows()
delete_period()
insert_dataframe_ventas()
```

Dependencias funcionales:

```text
pandas
openpyxl
python-multipart
```

---

## 92.7 COMPONENTES REUTILIZABLES

Componentes detectados en esta función:

```text
UPLOAD_KIVOR
MODAL_KIVOR
ALERT_KIVOR
SELECT_KIVOR
BUTTON_KIVOR
```

Estado:

```text
Implementados localmente en ventas-lyl-carga.html / ventas-lyl.css / ventas-lyl-carga.js.
Pendiente extraer a componentes reutilizables globales.
```

Regla futura:

Todo nuevo módulo que cargue archivos deberá reutilizar el patrón `UPLOAD_KIVOR`.

---

## 92.8 ESTADO ACTUAL DEL PROYECTO

Clasificación del módulo:

```text
Módulo: Ventas LYL - Carga Excel
Estado: Incorporado
Clasificación: Incompleto hasta completar QA, script SQL formal y validación de integración con api.js
```

Elementos completos:

* Pantalla HTML de carga.
* Estilos CSS específicos.
* Javascript de selección, confirmación y envío.
* Route FastAPI protegida por token.
* Schema Pydantic de respuesta.
* Servicio de lectura Excel, validación, filtrado, delete e insert.

Elementos confirmados (actualización 2026-08-17):

* Existencia formal de la tabla `core.stg_ventas_lyl` en el esquema productivo.
* Tipos de datos definitivos de cada columna (ver DDL oficial en 92.5).

Elementos pendientes de confirmación:

* Índices recomendados para `anio` y `anio_mes`.
* Restricciones de unicidad, si aplica.
* Integración exacta de función `cargarVentasLYL()` dentro de `api.js`.
* Permisos de menú para exponer la pantalla sólo a roles autorizados.
* Pruebas con archivos Excel reales.

---

## 92.9 DEUDA TÉCNICA DETECTADA

### Media – Tabla staging sin DDL documentado (RESUELTO)

Situación:

El código utiliza `core.stg_ventas_lyl`, pero el Documento Vivo aún no contenía el script `CREATE TABLE`.

Acción tomada:

DDL oficial documentado en 92.5 y en la sección 59 (actualización 2026-08-17). Pendiente aún: índices recomendados y restricciones de unicidad adicionales, si aplican.

---

### Media – Componentes de carga aún locales

Situación:

El módulo define su propio layout de carga, modal y alertas.

Acción recomendada:

Extraer `UPLOAD_KIVOR`, `MODAL_KIVOR`, `ALERT_KIVOR`, `SELECT_KIVOR` y `BUTTON_KIVOR`.

---

### Media – Manejo de errores genérico

Situación:

El route captura excepciones generales y responde HTTP 400.

Acción recomendada:

Migrar errores esperados a `AppException` / `InvalidDataError` para mantener estándar KIVOR.

---

### Baja – Código inalcanzable en servicio (RESUELTO)

Situación:

`upload_ventas_service()` contenía un segundo `return` después del bloque `try/except`, por lo que no se ejecutaba.

Acción tomada:

Eliminado al extender la función para soportar el modo "año completo" (actualización 2026-08-08).

---

### Baja – Validación de extensión y tamaño de archivo

Situación:

El frontend acepta `.xlsx,.xls`, pero el backend aún no valida explícitamente extensión, tamaño máximo o tipo MIME.

Acción recomendada:

Agregar validación backend antes de procesar el Excel.

---

## 92.10 QA REQUERIDO

Casos mínimos:

* Cargar archivo válido con registros del período.
* Cargar archivo válido sin registros del período.
* Cargar archivo sin hoja `VENTAS`.
* Cargar archivo sin columnas obligatorias.
* Cargar mes inválido.
* Cargar sin token.
* Repetir carga del mismo período y validar reemplazo.
* Validar conteo de `rows_deleted`.
* Validar conteo de `rows_inserted`.
* Validar trazabilidad `archivo_origen`, `hoja_origen`, `fila_excel`.
* Validar archivo con celdas vacías.
* Validar columnas opcionales ausentes.
* Cargar año completo con Excel que trae los 12 meses.
* Cargar año completo con Excel que trae solo algunos meses (datos parciales).
* Cargar año completo con Excel sin ningún registro de ese año (debe rechazar).
* Repetir carga de año completo y validar que elimina todo el año previo, sin importar qué meses traía la carga anterior.
* Confirmar que el modo mes específico sigue funcionando igual tras la incorporación del modo año completo.

---

## 92.11 ROADMAP DEL MÓDULO VENTAS LYL

### Paso 1 (COMPLETADO 2026-08-17)

Formalizar DDL de `core.stg_ventas_lyl`. Ver DDL oficial en 92.5 y en la sección 59.

### Paso 2

Validar carga en ambiente de desarrollo con Excel real.

### Paso 3

Conectar pantalla al menú dinámico por rol.

### Paso 4

Extraer componentes reutilizables de carga y modal.

### Paso 5

Crear proceso ETL desde staging hacia modelo definitivo de ventas.

### Paso 6

Conectar Analytics a la fuente validada.

### Paso 7

Agregar auditoría de carga por usuario, fecha y archivo.

---

FIN DOCUMENTO_VIVO_KIVOR.md

VERSIÓN 1.1


