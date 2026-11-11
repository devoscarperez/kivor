# Especificación Funcional
# Extracción y Normalización de Clientas desde Export de WhatsApp

**Proyecto:** Lindas y Lunáticas  
**Tabla destino:** `lindasylunaticas.customers`  
**Formato origen:** Export de conversación de WhatsApp (.txt)  
**Formato salida:** CSV UTF-8 compatible con DBeaver

---

# Objetivo

Procesar automáticamente un archivo exportado desde WhatsApp, identificar las clientas registradas en la conversación y generar un archivo CSV listo para ser importado a la tabla:

```sql
lindasylunaticas.customers
```

El proceso debe maximizar la recuperación de información, normalizar los datos y eliminar inconsistencias antes de la carga.

---

# Formato de salida

El archivo generado deberá cumplir exactamente con:

- Delimitador: `,`
- Encoding: `UTF-8`
- Header: Primera fila
- Quote Character: `"`
- Compatible con DBeaver Import Wizard

---

# Estructura de destino

| Columna | Valor |
|----------|-------|
| customers_type | person |
| customers_first_name | Extraído |
| customers_middle_name | Extraído |
| customers_last_name | Extraído |
| customers_second_name | Extraído |
| customers_full_name | Generado |
| customers_nickname | Extraído |
| customers_mobile | Extraído |
| customers_idtype | RUT |
| customers_personalid | RUT normalizado |
| customers_company_name | NULL |
| customers_trade_name | NULL |
| customers_birth_date | Normalizado |
| customers_gender | NULL |
| customers_taxpayer_type | NULL |
| customers_tax_document_preference | NULL |
| customers_status | active |
| customers_category | NULL |
| customers_notes | Observaciones relevantes |
| customers_created_at | Omitir |
| customers_updated_at | Omitir |
| customers_deleted_at | Omitir |
| customers_id | Omitir (lo genera PostgreSQL) |

---

# Regla 1
## Solo procesar clientas con RUT

Una clienta será considerada válida únicamente cuando se logre identificar un RUT.

Si no existe RUT:

- No generar registro.
- Enviar al archivo de descartados.

---

# Regla 2
## Llave de consolidación

La llave única de la clienta será:

```
RUT
```

Todos los registros correspondientes al mismo RUT deberán consolidarse.

---

# Regla 3
## Merge de registros duplicados

Cuando un mismo RUT aparezca varias veces:

- agrupar todos los registros
- combinar la información
- conservar el dato más completo

Ejemplo

Registro 1

```
Nombre
Correo
```

Registro 2

```
Nombre
Fecha nacimiento
Celular
```

Resultado

```
Nombre
Correo
Fecha nacimiento
Celular
```

Nunca eliminar información válida.

---

# Regla 4
## Prioridad de datos

Cuando existan dos valores distintos para un mismo campo:

1. Valor no vacío.
2. Valor más completo.
3. Valor más reciente dentro del chat.

---

# Regla 5
## Identificación de nombres

Aplicar la especificación de nombres latinoamericanos entregada.

Debe reconocer:

- nombres compuestos
- apellidos compuestos
- conectores

Ejemplos

```
María José
Juan Pablo
María del Pilar
José Luis
```

Apellidos

```
San Martín
Santa Cruz
Del Río
De la Fuente
```

---

# Regla 6
## Capitalización

Todos los nombres deberán quedar en formato Proper Case.

Ejemplo

```
MARIA DEL PILAR TOLEDO
```

↓

```
Maria del Pilar Toledo
```

Conservar

- tildes
- ñ
- caracteres UTF-8

---

# Regla 7
## Alias

Cuando exista alias:

```
Javi
Vale
Pili
Ale
```

Guardar en

```
customers_nickname
```

---

# Regla 8
## Nombre completo

Generar automáticamente

```
customers_full_name
```

Concatenando

```
Nombre
Segundo nombre
Apellido
Segundo apellido
```

Eliminando dobles espacios.

---

# Regla 9
## Correos electrónicos

Normalizar

- minúsculas
- eliminar espacios

Ejemplo

```
 Oscar@MAIL.COM
```

↓

```
oscar@mail.com
```

---

# Regla 10
## Celulares

Rescatar cualquier número telefónico encontrado.

Eliminar:

- espacios
- guiones
- paréntesis

Conservar formato internacional cuando exista.

---

# Regla 11
## RUT

Normalizar

Ejemplo

```
12.345.678-9
```

↓

```
12345678-9
```

Eliminar puntos.

Conservar guión.

---

# Regla 12
## Tipo documento

Siempre

```
customers_idtype = RUT
```

---

# Regla 13
## Tipo cliente

Siempre

```
customers_type = person
```

---

# Regla 14
## Estado

Siempre

```
customers_status = active
```

---

# Regla 15
## Fecha de nacimiento

Normalizar utilizando las siguientes reglas.

## Caso 1

Existe día, mes y año.

Ejemplo

```
20/09/1985
```

↓

```
1985-09-20
```

---

## Caso 2

Existe mes y año.

Ejemplo

```
Julio 1977
```

↓

```
1977-07-01
```

---

## Caso 3

Existe únicamente el año.

Ejemplo

```
1977
```

↓

```
1977-01-01
```

---

## Caso 4

Existe día y mes.

Ejemplo

```
03 de Junio
```

↓

```
2025-06-03
```

---

## Caso 5

Existe únicamente el mes.

Ejemplo

```
Junio
```

↓

```
2025-06-01
```

---

## Caso 6

Fecha imposible de interpretar.

Resultado

```
NULL
```

---

# Regla 16
## Meses

Reconocer tanto formato numérico como textual.

Ejemplo

```
ene
enero
01
```

↓

```
01
```

---

# Regla 17
## UUID

No generar.

La columna

```
customers_id
```

será omitida para que PostgreSQL genere automáticamente

```
gen_random_uuid()
```

---

# Regla 18
## Campos sin información

Guardar NULL.

Nunca generar valores ficticios salvo las reglas definidas para fechas.

---

# Regla 19
## Archivo de descartados

Generar

```
clientes_descartados.csv
```

Incluyendo:

- motivo descarte
- registro original
- observaciones

Ejemplos

- Sin RUT
- RUT inválido
- Información insuficiente
- Fecha imposible de interpretar

---

# Regla 20
## Reporte de procesamiento

Generar

```
reporte_procesamiento.csv
```

Debe incluir como mínimo:

- Total de registros encontrados
- Total de clientas procesadas
- Total descartadas
- Total de RUT únicos
- Total de duplicados consolidados
- Correos recuperados
- Celulares recuperados
- Fechas recuperadas
- Registros con observaciones

---

# Calidad esperada

El objetivo principal del proceso es privilegiar la **calidad del dato** por sobre la cantidad de registros importados.

Cada clienta deberá representar la mejor versión posible de la información disponible en el chat mediante consolidación, normalización y validación antes de la generación del archivo CSV.