# Complemento Funcional por Columna – Workspace de Ventas LYL

## Propósito
Complementa el diseño funcional del Workspace de Ventas LYL, detallando el comportamiento esperado de cada columna durante ingreso, edición, validación, cálculo y persistencia.

---

## 1. Principio general

La grilla debe funcionar como Excel:

- edición directa en celda;
- navegación con teclado;
- copiar/pegar;
- selección múltiple;
- duplicación de líneas;
- autocompletado;
- listas desplegables;
- búsqueda asistida.

KIVOR ayuda al usuario, pero no bloquea modificaciones manuales cuando el negocio lo requiera.

---

## 2. tipo_linea

Valores:

- `S` = Servicio
- `T` = Cierre de Ticket

### Línea S

Representa un servicio vendido.

Habilita principalmente:

- fecha_recau
- fecha_entrega
- profesional
- clienta
- servicio_codigo
- precios
- comentario

### Línea T

Representa el cierre financiero del ticket.

Solicita:

- abono
- pagado
- medio_pago
- voucher / nro_getnet
- comentario

Calcula:

- total precio web
- total cobrado
- descuento aplicado
- porcentaje de descuento

Cada grupo de líneas `S` debe terminar con una línea `T`.

---

## 3. nro_formulario

Número del formulario físico foliado.

Reglas:

- existe antes del ingreso de ventas;
- identifica el ticket operativo;
- en la web se ingresa normalizado, sin letras agregadas;
- participa en el cierre junto con `fecha_recau` y `profesional`.

Cierre:

```text
fecha_recau + nro_formulario + profesional
```

Comportamiento:

- se hereda desde el contexto activo;
- KIVOR propone el último formulario usado;
- editable por el usuario.

---

## 4. nro_formulario_original

Campo de trazabilidad para ventas históricas cargadas desde Excel.

Ejemplos:

- `2393K`
- `2393N`
- `2393J`

Reglas:

- conserva exactamente el valor proveniente de Excel;
- la lógica de negocio usa `nro_formulario` normalizado;
- en ventas web puede quedar vacío.

---

## 5. fecha_recau

### Descripción

Fecha principal utilizada para la lógica financiera del ticket.

### Tipo de dato

Fecha.

### Componente

`DATE_KIVOR`

### Reglas de negocio

* Representa la fecha en que se realizó la recaudación.
* Es la fecha principal utilizada para cierres financieros y análisis.
* Forma parte de la clave lógica de cierre del ticket junto con `nro_formulario` y `profesional`.

### Valor por defecto

En nuevos registros propone la fecha actual, permitiendo su modificación por el usuario.

---

## 6. fecha_entrega

### Descripción

Fecha en que se realizó o se realizará la prestación del servicio.

### Tipo de dato

Fecha.

### Componente

`DATE_KIVOR`

### Reglas de negocio

* Representa la fecha efectiva de realización del servicio.
* Puede ser igual, anterior o posterior a `fecha_recau`, según la operación registrada.
* No participa en la lógica de cierre financiero del ticket.
* Se utiliza para análisis operativos, planificación y control de servicios ejecutados o pendientes.

### Valor por defecto

En nuevos registros propone la fecha actual, permitiendo su modificación por el usuario.


## 7. profesional

Profesional asociada a la línea.

Reglas:

- proviene de tabla de profesionales;
- participa en el cierre del ticket.

Comportamiento:

- lista desplegable con búsqueda;
- selección rápida por teclado;
- se hereda desde contexto activo;
- editable.

---

## 8. sp

Define si la clienta se identifica por:

- RUT
- Celular

Comportamiento:

- determina cómo interpretar `rut_celular`.

---

## 9. rut_celular

Dato de identificación ingresado por el usuario.

Comportamiento:

- se interpreta según `sp`;
- gatilla búsqueda de clienta;
- puede consultar futura tabla de clientas.

### Componente

`RUT`

---

## 10. nombre

Nombre de la clienta.

Comportamiento:

- se recupera automáticamente desde `rut_celular`;
- si no existe, permite ingreso manual;
- se hereda desde contexto activo;
- editable.

---

## 11. origen

Medio por el cual la clienta llegó al salón.

Reglas:

- dato de marketing;
- proviene de catálogo por definir.

Comportamiento:

- lista desplegable;
- editable.

---

## 12. servicio_codigo

Código corto del servicio.

Reglas:

- dato principal de ingreso web;
- debe persistirse en `core.stg_ventas_lyl`;
- reemplaza visualmente el ingreso manual de familia/niveles.

Comportamiento:

El usuario puede:

- escribir código directo;
- abrir popup de búsqueda;
- buscar por descripción;
- seleccionar código + descripción.

Al seleccionar servicio, KIVOR recupera:

- familia
- nivel_2
- nivel_3
- nivel_4
- precio_web
- precio_profesional
- porcentaje_profesional

---

## 13. familia, nivel_2, nivel_3, nivel_4

Columnas internas/derivadas.

Reglas:

- no son columnas principales de ingreso;
- se completan desde `servicio_codigo`;
- se persisten por compatibilidad y análisis.

Carga histórica:

```text
familia + nivel_2 + nivel_3 + nivel_4 → servicio_codigo
```

Ingreso web:

```text
servicio_codigo → familia + nivel_2 + nivel_3 + nivel_4
```

---

## 14. precio_profesional

Valor sugerido desde el servicio.

Comportamiento:

- se completa automáticamente;
- editable por usuario.

---

## 15. precio_web

Precio lista/web del servicio.

Comportamiento:

- se completa automáticamente;
- editable;
- base para calcular descuento en línea T.

---

## 16. porcentaje_profesional

Porcentaje de comisión.

Comportamiento:

- se completa automáticamente;
- editable;
- se usa en cálculos de ganancia/comisión.

---

## 17. abono

Monto abonado por la clienta.

Reglas:

- se ingresa en línea T;
- puede representar anticipo o pago parcial.

---

## 18. pagado

Monto pagado al cierre.

Reglas:

- se ingresa en línea T;
- junto con abono determina total cobrado.

---

## 19. medio_pago

Medio de pago utilizado.

Comportamiento:

- lista desplegable;
- catálogo por definir.

---

## 20. nro_getnet / voucher

Número de comprobante.

Comportamiento:

- ingreso manual;
- disponible en línea T.

---

## 21. total

Campo calculado.

Pendiente fórmula final.

---

## 22. total_pw

Suma de precios web de líneas S asociadas al cierre.

---

## 23. descuento

Campo calculado en línea T.

```text
descuento = total_precio_web_servicios - total_cobrado
total_cobrado = abono + pagado
```

---

## 24. porcentaje_descuento

Campo calculado.

```text
porcentaje_descuento = descuento / total_precio_web_servicios
```

---

## 25. comentario

Comentario opcional.

Reglas:

- disponible en línea S;
- disponible en línea T;
- registra observaciones operativas.

---

## 26. valida_form

Campo pendiente de definición.

Debe evaluarse si sigue siendo necesario en el nuevo modelo.

---

## 27. abono_perdido

Corresponde al abono de clientas que no asistieron.

Pendiente definir cómo se representará en el flujo con línea T.

---

## 28. descuentos, descprof_a_clientas

Pendientes de definición detallada.

Podrían derivarse del cierre del ticket.

---

## 29. ganancia_prof, total_ganancia_prof, ganancia_salon

Campos calculados.

Deben derivarse de:

- servicio_codigo
- precio_profesional
- precio_web
- porcentaje_profesional
- abono
- pagado
- descuento

---

## 30. Carga histórica desde Excel

La carga mensual debe:

1. Leer ventas antiguas.
2. Normalizar `nro_formulario`.
3. Guardar `nro_formulario_original`.
4. Detectar `tipo_linea = S` o `T`.
5. Identificar cierre por:
   - fecha_recau
   - profesional
   - nro_formulario normalizado
6. Buscar `servicio_codigo` usando:
   - familia
   - nivel_2
   - nivel_3
   - nivel_4
7. Marcar para revisión si no encuentra servicio.

---

## 31. Persistencia Workspace → STG

Al grabar desde Workspace, KIVOR debe completar en `core.stg_ventas_lyl`:

- tipo_linea
- servicio_codigo
- familia
- nivel_2
- nivel_3
- nivel_4
- precios
- porcentajes
- totales
- descuentos
- anio
- anio_mes
- comentario
- campos técnicos

---

## 32. Principio final

El usuario trabaja en una grilla simple y rápida.

KIVOR se encarga de:

- completar datos;
- sugerir valores;
- calcular totales;
- interpretar tickets;
- normalizar formularios;
- reconstruir ventas históricas;
- explotar datos para análisis y persistencia.


# COMPONENTE: DATE_KIVOR

## Descripción

Componente estándar de KIVOR para la captura y selección de fechas.

Su objetivo es proporcionar una experiencia de usuario consistente en todos los módulos del sistema mediante un selector jerárquico que facilita la navegación cronológica y optimiza el uso del espacio en pantalla.

---

## Tipo de dato

Fecha.

---

## Modo de selección

Selector jerárquico (Tree ComboBox).

---

## Objetivos

* Estandarizar la captura de fechas en todo KIVOR.
* Facilitar la navegación entre períodos extensos.
* Reducir el espacio ocupado respecto a un calendario tradicional.
* Mantener una experiencia uniforme para todos los módulos del sistema.

---

## Comportamiento

Las fechas se presentan jerárquicamente utilizando la siguiente estructura:

```text
Año
 └── Mes
      └── Día
```

Ancho recomendado:
8 caracteres.

Formato visible:
DD-MM-YY

Alineación:
Centro.
Características:

* El período correspondiente a la fecha actual se presenta expandido por defecto.
* Los demás períodos permanecen contraídos hasta que el usuario los expanda.
* La selección corresponde siempre a una única fecha.
* La expansión o contracción de los nodos no modifica la fecha seleccionada.
* Permite navegación mediante teclado.
* Permite búsqueda incremental de fechas.
* El componente admite la escritura directa de una fecha válida.

---

## Persistencia

El valor seleccionado se almacena utilizando el formato ISO:

```text
YYYY-MM-DD
```

La representación jerárquica corresponde únicamente a la interfaz de usuario y no modifica el formato persistido.

---

## Valor por defecto

Cada módulo podrá definir el valor inicial del componente.

Ejemplos:

* Fecha actual.
* Fecha del contexto activo.
* Sin valor.

---

## Aplicabilidad

DATE_KIVOR debe utilizarse como componente estándar para todas las columnas de tipo Fecha del sistema KIVOR.

Ningún módulo debe implementar un selector de fechas diferente sin una justificación funcional explícita.



### Componente

`RUTCELPAS`


## Descripción

Componente estándar de KIVOR para la captura y selección de RUT (Rol Unico Tributario) o el numero de celular o Pasaporte.

Su objetivo es proporcionar una experiencia de usuario consistente en todos los módulos del sistema mediante el ingreso de un valor alfanumerico que identifique el RUT o Celular o pasaporte de la clienta, 
esto dependera de que valor tenga la columna sp (CEL o RUT o PAS).

---

## Tipo de dato

RUT/CELULAR/PAS.

---

## Modo de ingreso

ingreso libre de un identificador que de acuerdo al valor de la columna sp, establecerá si el dato lo trata como celular o un rut o un pasaporte.

---

## Objetivos

* Estandarizar la captura de RUTCELPAS en todo KIVOR.
* Facilitar la validación dinamica de acuerdo al valor que posea la columna sp, si es RUT, entonces lo validará como RUT aplicando el modulo 11, en caso de ser CEL lo validará como numero celular o si es pasaporte es un ingreso libre.
* Evita manejar tres columnas.
* Mantener una experiencia uniforme para todos los módulos del sistema.

---

## Comportamiento

Si el valor de la sp es RUT deberá aplicar la valiación y formato de RUT (10.065.203-K) y si la columna sp es CEL, entonces se aplicara la validación de numero de celular que inicio con +<codigo país><numero de celular>, 
la persona en el ingresod e este dato como celular debe poder elegir el codigo del país o si es PAS el valor de la columna sp entonces sera gtratado como pasaporte y el ingreso es libre.
Ejemplos de RUT: 10.065.203-2
Ejemplo de CEL: +56985951985
Ejemplo de PAS: AB565243bTyZ765


Ancho recomendado:
15 caracteres.

Formato visible:
Si es RUT: XX.XXX.XXX-X
Si es CEL: +56985642025
Si es PAS: ABCfvR653429
Alineación:
Centro.
Características:

* Depesde del valpor que tenga la columna sp el cómo trate esta columna, puede ser RUT (RUT o CELULAR (CEL) o PASAPORTE(PAS).

---

## Persistencia

El valor seleccionado se almacena utilizando el formato ISO:

```text
XXXXXXXXXXXXXXXX
```

---

## Valor por defecto

Cada módulo podrá definir el valor inicial del componente.

Ejemplos:

* Clienta

---

## Aplicabilidad

RUT/CELULAR/PAS debe utilizarse como componente estándar para todas las columnas de tipo rut_celular del sistema KIVOR.
