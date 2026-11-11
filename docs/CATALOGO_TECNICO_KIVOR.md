# CATALOGO_TECNICO_KIVOR.md

Versión: 1.0\
Estado: En construcción

------------------------------------------------------------------------

# Propósito

Este documento constituye la **memoria técnica del código fuente** de
KIVOR.

Complementa al `DOCUMENTO_VIVO_KIVOR.md`.

Mientras el Documento Vivo describe la arquitectura, reglas de negocio y
evolución del proyecto, este catálogo documenta el conocimiento extraído
del código fuente.

No pretende reemplazar al código, sino facilitar su comprensión y
mantenimiento.

------------------------------------------------------------------------

# Objetivos

-   Documentar el comportamiento funcional de cada archivo.
-   Registrar reglas de negocio implementadas.
-   Identificar dependencias y patrones de implementación.
-   Preservar el estilo de desarrollo de KIVOR.
-   Detectar deuda técnica y oportunidades de mejora.
-   Servir como referencia para futuros desarrolladores.

------------------------------------------------------------------------

# Estructura del catálogo

Cada archivo del proyecto será documentado utilizando la siguiente
plantilla:

## Archivo

Ruta completa.

## Responsabilidad

Rol del archivo dentro de la arquitectura.

## Funciones

Para cada función:

-   Nombre
-   Propósito
-   Flujo lógico
-   Entradas
-   Salidas
-   Dependencias
-   Tablas utilizadas
-   Reglas de negocio detectadas
-   Validaciones importantes
-   Pistas de implementación
-   Funciones auxiliares
-   Riesgos
-   Deuda técnica
-   Resumen ejecutivo

------------------------------------------------------------------------

# Patrones de implementación KIVOR

Durante la documentación se identificarán patrones repetidos para
convertirlos en estándares oficiales del proyecto.

Ejemplos:

-   Route → Service → DB
-   HTML → JS → api.js → Route → Service → PostgreSQL
-   SQL parametrizado
-   Componentes reutilizables
-   Separación entre presentación y lógica
-   Arquitectura Multi-Tenant

------------------------------------------------------------------------

# Índice de documentación (pendiente)

## Backend

-   main.py
-   core/db.py
-   core/security.py
-   core/exceptions.py

### Routes

-   auth.py
-   users.py
-   analytics.py
-   customers_express.py
-   menu.py
-   ventas_lyl.py

### Services

-   auth_service.py
-   analytics_service.py
-   customer_express_service.py
-   user_service.py
-   ventas_lyl_service.py

### Schemas

-   auth_schema.py
-   user_schema.py
-   ventas_lyl_schema.py

## Frontend

### HTML

-   ventas-lyl-carga.html

### JavaScript

-   ventas-lyl-carga.js
-   api.js

### CSS

-   ventas-lyl.css

------------------------------------------------------------------------

# Estado

Este documento será completado progresivamente aplicando la metodología
definida en el documento "Extracción de Conocimiento del Código",
incorporando únicamente conocimiento técnico relevante y evitando
duplicar el código fuente.
