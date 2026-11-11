# Especificación funcional — Mapa Estratégico de Familias y Oportunidades

**Negocio:** Salón de belleza Lindas y Lunáticas  
**Versión:** 1.0  
**Horizonte histórico inicial:** últimos 36 meses cerrados  
**Destino:** base funcional y analítica para construir el reporte HTML definitivo

---

## 1. Propósito

Construir un reporte compacto, analíticamente profundo y orientado a decisiones que permita responder cuatro preguntas:

1. ¿Qué familias actuales son rentables para el salón?
2. ¿Qué familias muestran futuro, aunque hoy todavía tengan pocos ingresos?
3. ¿Qué familias o servicios adicionales pueden venderse a las clientas actuales para elevar su gasto por visita y por año?
4. ¿Qué familias nuevas conviene evaluar para atraer públicos nuevos sin perder coherencia con el negocio?

El reporte no debe limitarse a describir ventas históricas. Debe transformar la información en **señales de inversión, desarrollo, protección, corrección o salida**.

---

## 2. Decisiones que debe soportar

El reporte debe ayudar a decidir:

- En qué familias actuales invertir dinero, espacio, equipamiento, formación o promoción.
- Qué familias proteger por su aporte actual, aunque su crecimiento sea moderado.
- Qué familias pequeñas desarrollar porque muestran crecimiento, recurrencia o rentabilidad atractiva.
- Qué familias revisar porque ocupan capacidad y generan poco retorno.
- Qué cruces entre familias promover entre las clientas actuales.
- Qué campañas deben orientarse a clientas específicas y no a toda la base.
- Qué familias nuevas merecen un estudio de mercado y un caso de negocio.
- Qué información falta antes de comprometer una inversión.

El reporte **no debe decidir automáticamente**. Debe aportar evidencia, alertas y preguntas correctas.

---

## 3. Motores de crecimiento

### Motor A — Expansión

Busca crecimiento mediante:

- familias nuevas;
- públicos nuevos;
- nuevas necesidades;
- mayor capacidad de captación;
- nuevos ingresos que no dependan exclusivamente de las clientas actuales.

### Motor B — Profundización

Busca crecimiento mediante:

- mayor penetración de familias entre clientas actuales;
- más servicios por visita;
- mayor frecuencia;
- mayor gasto anual por clienta;
- combinaciones de servicios coherentes y rentables.

Ambos motores deben analizarse por separado. Una familia apta para captar un público nuevo no necesariamente es la mejor para aumentar el gasto de las clientas existentes.

---

## 4. Alcance funcional

El producto final será un reporte HTML compuesto por una portada ejecutiva y tres módulos analíticos:

1. **Portafolio actual:** rentabilidad, escala, tendencia y calidad de la demanda de cada familia.
2. **Share of Wallet:** oportunidades de venta cruzada sobre las clientas existentes.
3. **White Space / Nuevas familias:** evaluación de familias que hoy no existen o tienen presencia mínima.

La navegación debe permitir pasar de:

**Salón → Familia → Servicio → Profesional → Clienta → Ticket**

La primera versión debe priorizar familia y servicio. El detalle por clienta debe utilizarse para explicar resultados, no para llenar la pantalla principal.

---

## 5. Definiciones de negocio

### 5.1 Familia

Agrupación estable de servicios que responden a una necesidad común y utilizan capacidades similares. Ejemplos: Cabello, Manos, Pies, Cejas.

La clasificación histórica debe homologarse. Si un servicio cambió de nombre, debe permanecer asociado a una única familia y servicio analítico.

### 5.2 Venta bruta

Valor total cobrado a la clienta antes de distribuir la parte correspondiente al salón y a la profesional.

### 5.3 Ingreso salón

Parte económica que corresponde al salón después de aplicar la distribución con la profesional y los descuentos que afecten al salón.

Debe documentarse una fórmula única a partir de los campos disponibles. Esta métrica será la base económica principal del reporte.

### 5.4 Contribución salón

Ingreso salón menos costos variables directamente atribuibles, si estos existen y son confiables.

Mientras no estén disponibles los costos de insumos, **Ingreso salón no debe denominarse margen ni utilidad**. Será una contribución antes de costos operacionales y gastos fijos.

### 5.5 Horas estándar

Tiempo teórico consumido según la duración estándar del servicio:

```text
Horas estándar = suma de duración estándar en minutos / 60
```

No representa ocupación real de agenda ni tiempos muertos.

### 5.6 Clienta activa

Clienta con al menos un servicio entregado y válido dentro del período seleccionado.

### 5.7 Clienta nueva

Clienta cuya primera compra histórica conocida ocurre dentro del período analizado. Debe indicarse que la clasificación puede estar sesgada si no existe historia anterior al inicio de la base.

### 5.8 Ticket válido

Ticket con servicios efectivamente entregados. Deben excluirse anulaciones, pruebas, duplicados y servicios sin valor comercial según las reglas definidas.

### 5.9 Penetración de familia

```text
Clientas que compraron la familia / clientas activas
```

### 5.10 ARPC — Ingreso anual por clienta

```text
Venta bruta anual de las clientas / número de clientas únicas del grupo
```

También debe existir la variante **Ingreso salón anual por clienta**.

---

## 6. Requisitos mínimos de datos

### 6.1 Campos obligatorios

- Identificador único de ticket.
- Identificador estable de clienta.
- Fecha de entrega.
- Familia.
- Servicio.
- Profesional.
- Venta bruta.
- Ingreso o porcentaje correspondiente al salón.
- Descuento del salón, cuando corresponda.
- Duración estándar del servicio.
- Estado que permita identificar servicios entregados, anulados y registros no comerciales.

### 6.2 Campos recomendados

- Costo de insumos por servicio.
- Canal de captación.
- Fecha de alta de la clienta.
- Sucursal o local.
- Puesto o recurso utilizado.
- Indicador de promoción.
- Tipo de descuento.
- Hora de inicio y término para períodos futuros.

### 6.3 Controles de calidad

Antes de calcular indicadores debe verificarse:

- clientas duplicadas por diferencias de nombre o celular;
- servicios sin familia;
- nombres históricos no homologados;
- duraciones nulas o improbables;
- tickets duplicados;
- montos negativos o nulos;
- cambios de porcentaje profesional;
- fechas fuera del período operativo;
- tickets con varios servicios y correcta distribución de descuentos;
- existencia de servicios realizados simultáneamente.

El HTML debe mostrar una alerta si la calidad de los datos puede cambiar una conclusión.

---

## 7. Filtros globales

Todos los módulos deben responder a los mismos filtros:

- Período: mes, trimestre, semestre, año móvil y rango personalizado.
- Comparación: período anterior, mismo período año anterior y promedio de los tres años.
- Familia.
- Servicio.
- Profesional.
- Clienta nueva/existente.
- Día de semana, si aporta valor.
- Estado del servicio/ticket.

**Período predeterminado:** últimos 12 meses cerrados.  
**Comparación predeterminada:** 12 meses anteriores y tendencia mensual de 36 meses.

Los filtros activos siempre deben quedar visibles.

---

## 8. Portada ejecutiva

### 8.1 Objetivo

Entregar una lectura rápida del portafolio y dirigir al usuario hacia las excepciones que requieren análisis.

### 8.2 Indicadores de cabecera

Mostrar, para el período actual, comparación anual y tendencia:

- Venta bruta.
- Ingreso salón.
- Clientas únicas.
- Familias por clienta.
- Servicios por ticket.
- ARPC.
- Ingreso salón por hora estándar.
- Porcentaje de clientas monofamilia.
- Porcentaje de ingresos provenientes de clientas multifamilia.

### 8.3 Mensajes automáticos

La portada debe generar un máximo de cinco hallazgos redactados en lenguaje simple, por ejemplo:

- “La familia X aporta poco ingreso actual, pero sus clientas y su ingreso por hora crecen durante cuatro trimestres consecutivos.”
- “El 58% de las clientas de Cabello no compra ninguna otra familia.”
- “La familia Y crece en ventas, pero el crecimiento proviene solo de precio; disminuyen clientas y frecuencia.”

Cada mensaje debe enlazar con el módulo que contiene la evidencia.

---

## 9. Módulo A — Portafolio actual

### 9.1 Pregunta central

**¿Qué familias actuales generan valor y cuáles muestran futuro suficiente para justificar inversión?**

### 9.2 Matriz principal

Gráfico de dispersión:

- **Eje X:** Ingreso salón por hora estándar.
- **Eje Y:** crecimiento compuesto o tendencia del ingreso salón y clientas.
- **Tamaño del punto:** ingreso salón actual.
- **Color:** clasificación estratégica.

Los valores deben poder verse al posicionarse sobre cada punto. No se debe ocultar la fórmula detrás de un único índice.

### 9.3 Tabla analítica obligatoria

| Dimensión | Indicador |
|---|---|
| Escala | Venta bruta |
| Escala | Ingreso salón |
| Escala | Participación en ingreso salón |
| Demanda | Clientas únicas |
| Demanda | Tickets |
| Demanda | Servicios entregados |
| Demanda | Penetración de la familia |
| Conducta | Frecuencia por clienta |
| Conducta | Servicios por ticket |
| Conducta | ARPC de las clientas de la familia |
| Economía | Horas estándar |
| Economía | Ingreso salón por hora estándar |
| Economía | Contribución por hora, si hay costos |
| Tendencia | Variación de ingreso salón interanual |
| Tendencia | Variación de clientas interanual |
| Tendencia | Variación de frecuencia interanual |
| Tendencia | Pendiente de tendencia de 12 y 36 meses |
| Calidad | Retención de clientas a 90, 180 y 365 días |
| Calidad | Porcentaje de clientas nuevas captadas |
| Calidad | Porcentaje de clientas que luego compran otra familia |
| Riesgo | Concentración por profesional |
| Riesgo | Concentración por pocas clientas |

La tabla debe permitir ordenar cualquier columna, congelar Familia y abrir el detalle de servicios.

### 9.4 Fórmulas principales

```text
Participación ingreso = ingreso salón familia / ingreso salón total

Frecuencia = tickets de la familia / clientas únicas de la familia

Ingreso salón por hora estándar = ingreso salón / horas estándar

Retención 180 días = clientas elegibles que vuelven dentro de 180 días /
                     clientas elegibles para observar 180 días

Captación = clientas cuya primera compra fue en la familia /
            clientas nuevas del período

Conversión a otra familia = clientas que después compran otra familia /
                            clientas elegibles de la familia origen

Concentración profesional = participación del profesional principal
                            en el ingreso de la familia
```

La retención debe calcularse solo sobre cohortes con ventana completa de observación.

### 9.5 Separación de crecimiento real

Para cada familia debe explicarse el cambio de ingresos mediante:

```text
Ingreso = clientas × frecuencia × servicios por ticket × valor medio por servicio
```

El reporte debe distinguir si el crecimiento proviene de:

- más clientas;
- mayor frecuencia;
- más servicios por ticket;
- mayor precio o cambio de mezcla;
- una combinación de los anteriores.

### 9.6 Clasificación estratégica orientativa

- **Invertir:** rentabilidad relativa alta, tendencia positiva y demanda saludable.
- **Desarrollar:** tamaño pequeño, pero crecimiento y señales económicas atractivas.
- **Proteger:** aporte actual alto, rentabilidad saludable y crecimiento bajo o estable.
- **Optimizar:** demanda creciente, pero bajo ingreso por hora o contribución.
- **Reactivar:** economía atractiva, pero pérdida de clientas o frecuencia.
- **Revisar:** baja rentabilidad, bajo crecimiento y débil efecto estratégico.
- **Investigar:** datos insuficientes o señales contradictorias.

Estas etiquetas deben generarse mediante reglas visibles y configurables, no mediante una caja negra.

### 9.7 Reglas mínimas de interpretación

- No declarar “rentable” una familia usando solo venta bruta.
- No interpretar crecimiento monetario sin separar precio, volumen y mezcla.
- No comparar ingreso por hora sin revisar duración estándar y costos de insumos.
- No invertir por una tasa alta si la base es demasiado pequeña.
- No eliminar una familia de baja venta sin estudiar captación, retención y venta cruzada.
- No confundir correlación entre familias con causalidad.

---

## 10. Módulo B — Share of Wallet y venta cruzada

### 10.1 Pregunta central

**¿Qué más puede comprar cada grupo de clientas actuales y cuánto valor económico representa esa oportunidad?**

### 10.2 Selector de familia origen

El usuario selecciona una familia base, por ejemplo Cabello. El módulo debe separar:

- clientas que compran solo la familia origen;
- clientas que compran origen más una familia adicional;
- clientas que compran tres o más familias.

### 10.3 Matriz de penetración cruzada

Filas: familia origen.  
Columnas: familia destino.

Cada celda debe mostrar:

- clientas que compran ambas familias;
- penetración origen → destino;
- diferencia de ARPC entre monofamilia y multifamilia;
- orden habitual de compra;
- mediana de días entre familias;
- ingreso salón potencial estimado.

```text
Penetración A → B = clientas de A que también compran B / clientas de A

Brecha de penetración = referencia elegida - penetración observada

Clientas potenciales = clientas elegibles de A × brecha de penetración

Ingreso potencial prudente = clientas potenciales × tasa de conversión esperada
                            × ingreso salón medio de B
```

La referencia puede ser el mejor segmento comparable, la mejor profesional comparable o una meta definida. Nunca debe inventarse silenciosamente.

### 10.4 Ranking de oportunidades

| Origen | Destino | Clientas elegibles | Penetración actual | ARPC incremental observado | Afinidad | Ingreso potencial prudente | Confianza |
|---|---|---:|---:|---:|---:|---:|---|

La confianza debe considerar tamaño de muestra, estabilidad temporal y consistencia entre profesionales.

### 10.5 Afinidad

Para evitar recomendar una familia solo porque es popular, usar una medida de afinidad:

```text
Afinidad A-B = P(B dado A) / P(B)
```

- Mayor que 1: B aparece más entre clientas de A que en la base general.
- Igual a 1: no existe afinidad especial.
- Menor que 1: la combinación es menos habitual que el promedio.

Debe mostrarse junto con cantidad de clientas. Una afinidad alta con tres casos no es evidencia suficiente.

### 10.6 Secuencia y ventana temporal

El reporte debe distinguir:

- servicios/familias del mismo ticket;
- compras dentro de 30 días;
- compras dentro de 90 días;
- compras dentro de 180 días;
- cualquier compra posterior dentro de la historia observable.

Esto permite diferenciar un complemento inmediato de una extensión de relación de largo plazo.

### 10.7 Simulador de oportunidad

El usuario podrá modificar:

- número de clientas objetivo;
- tasa de conversión esperada;
- frecuencia anual esperada;
- ingreso salón medio por servicio;
- horas estándar requeridas.

Resultados:

- ingreso salón incremental;
- horas estándar adicionales;
- ingreso incremental por hora;
- impacto sobre ARPC;
- capacidad profesional requerida.

El simulador debe etiquetar el resultado como **escenario**, no como pronóstico.

---

## 11. Módulo C — White Space / Nuevas familias

### 11.1 Pregunta central

**¿Qué familias nuevas podrían atraer un público nuevo o ampliar el gasto de las clientas actuales, y cuáles justifican un estudio de inversión?**

### 11.2 Limitación fundamental

Las ventas históricas no pueden demostrar por sí solas la demanda de una familia inexistente. Este módulo debe combinar:

- evidencia interna;
- investigación externa;
- supuestos explícitos;
- prueba piloto.

### 11.3 Dos tipos de candidatas

#### Tipo 1 — Adyacencia para clientas actuales

Familias relacionadas con necesidades ya visibles y capaces de aumentar el gasto de la base actual.

#### Tipo 2 — Expansión hacia público nuevo

Familias coherentes con la propuesta del salón, pero destinadas parcial o principalmente a otro segmento.

Cada candidata debe marcarse como Tipo 1, Tipo 2 o Mixta.

### 11.4 Ficha de evaluación

| Área | Variable |
|---|---|
| Mercado | necesidad que resuelve |
| Mercado | segmento objetivo |
| Mercado | demanda local estimada |
| Mercado | competidores cercanos y precios |
| Encaje | relación con familias actuales |
| Encaje | porcentaje estimado de clientas actuales elegibles |
| Captación | potencial para atraer público nuevo |
| Economía | precio esperado |
| Economía | participación o costo profesional |
| Economía | costo variable e insumos |
| Economía | ingreso/contribución por hora esperada |
| Operación | duración |
| Operación | espacio y equipamiento |
| Operación | permisos y exigencias sanitarias |
| Operación | disponibilidad de profesionales |
| Riesgo | inversión inicial |
| Riesgo | punto de equilibrio |
| Riesgo | dependencia de una profesional |
| Validación | forma y duración del piloto |

### 11.5 Caso de negocio mínimo

```text
Contribución unitaria esperada = precio - pago profesional - insumos variables

Punto de equilibrio en servicios = costos fijos incrementales /
                                   contribución unitaria esperada

Horas necesarias = servicios esperados × duración / 60

Retorno simple = contribución anual esperada / inversión inicial
```

Debe incluir tres escenarios: conservador, base y exigente.

### 11.6 Proceso de validación

1. Formular hipótesis concreta.
2. Identificar segmento y necesidad.
3. Investigar competencia y precios.
4. Encuestar o entrevistar clientas elegibles.
5. Ejecutar piloto limitado.
6. Medir conversión, repetición, contribución/hora y canibalización.
7. Decidir escalar, ajustar o abandonar.

El reporte debe mostrar el estado de cada candidata: Idea, En investigación, Piloto, Validada, Descartada o Escalada.

---

## 12. Tendencias y alertas

### 12.1 Series temporales

Para evitar conclusiones por estacionalidad, mostrar:

- valores mensuales de 36 meses;
- media móvil de 3 meses;
- comparación contra el mismo mes del año anterior;
- acumulado móvil de 12 meses.

### 12.2 Señales sugeridas

- Crecimiento sostenido: al menos tres trimestres consecutivos positivos y base suficiente.
- Deterioro: caída interanual en clientas y frecuencia.
- Crecimiento por precio: ingresos suben, pero servicios/clientas no.
- Oportunidad emergente: baja participación actual, crecimiento consistente y buena economía por hora.
- Riesgo de concentración: una profesional genera una proporción excesiva de la familia.
- Venta cruzada desaprovechada: afinidad alta, penetración baja y base elegible grande.

Los umbrales deben ser configurables y quedar visibles en una sección metodológica.

---

## 13. Diseño del HTML

### 13.1 Principios

- Poca pantalla y alta densidad informativa.
- Una conclusión debe poder rastrearse hasta el dato que la explica.
- Priorizar tablas ordenables, pequeños gráficos de tendencia y excepciones.
- Evitar velocímetros, gráficos decorativos y exceso de tarjetas KPI.
- Usar color para significado, no para adorno.
- Mantener legibilidad en computador y tablet; vista móvil solo para consulta ejecutiva.

### 13.2 Estructura de navegación

Barra superior fija:

- período y comparación;
- filtros activos;
- fecha de actualización;
- acceso a metodología y calidad de datos.

Pestañas:

1. Resumen.
2. Portafolio actual.
3. Venta cruzada.
4. Nuevas familias.
5. Metodología.

### 13.3 Componentes obligatorios

- KPI con valor, variación y mini tendencia.
- Matriz Atractivo × Rentabilidad.
- Tabla analítica con columnas configurables.
- Heatmap de penetración cruzada.
- Panel de hallazgos automáticos.
- Simulador de escenarios.
- Ficha de nueva familia.
- Exportación de tabla filtrada a CSV.
- Definición visible al pasar sobre cada métrica.

### 13.4 Semántica de color

- Verde: señal positiva respaldada por datos suficientes.
- Ámbar: oportunidad o situación que requiere investigación.
- Rojo: deterioro o riesgo confirmado.
- Gris: datos insuficientes/no comparable.
- Azul: información neutral o selección activa.

No utilizar color como único medio de comunicación; acompañar con texto o íconos.

---

## 14. Hallazgos automáticos

Cada hallazgo debe contener:

1. **Qué ocurrió.**
2. **Qué variable lo explica.**
3. **Por qué importa.**
4. **Qué revisar o probar.**
5. **Nivel de confianza.**

Ejemplo:

> **Desarrollar Tratamientos:** su ingreso salón aumentó 24% interanual y las clientas crecieron 18%, mientras el ingreso por hora estándar supera en 31% la mediana del salón. La base aún es pequeña (74 clientas), por lo que se recomienda validar una campaña dirigida a clientas de Color antes de ampliar capacidad. Confianza: media.

El sistema no debe generar afirmaciones causales ni recomendaciones tajantes a partir de muestras pequeñas.

---

## 15. Niveles de confianza

- **Alta:** muestra suficiente, tendencia estable y resultado consistente en varios períodos/profesionales.
- **Media:** señal relevante, pero con base limitada o alguna inconsistencia.
- **Baja:** pocos casos, historia corta o fuerte dependencia de supuestos.

Los criterios numéricos se definirán después de perfilar el volumen real de datos.

---

## 16. Reglas de privacidad

- La portada y tablas agregadas no deben mostrar nombres ni celulares.
- El detalle de clientas debe requerir una acción explícita.
- Los archivos exportados deben respetar el nivel de acceso del usuario.
- Los identificadores técnicos deben reemplazar datos personales cuando no sean necesarios.

---

## 17. Pruebas de aceptación

El reporte se considerará funcionalmente correcto cuando permita:

1. Seleccionar cualquier período y comparar contra año anterior.
2. Identificar por familia ingresos, clientas, frecuencia, horas e ingreso por hora.
3. Separar crecimiento por clientas, frecuencia, servicios y precio/mezcla.
4. Reconocer familias pequeñas con tendencia atractiva sin confundirlas con familias grandes.
5. Seleccionar una familia origen y medir penetración hacia cada familia destino.
6. Identificar clientas monofamilia elegibles para una campaña.
7. Estimar un escenario prudente de ingreso incremental y capacidad requerida.
8. Registrar y comparar candidatas a nuevas familias.
9. Explicar toda etiqueta estratégica mediante métricas visibles.
10. Advertir cuando faltan datos para sostener una conclusión.

### Validaciones numéricas

- La suma por familias debe reconciliar con el total del período.
- Una clienta debe contarse una sola vez en clientas únicas del total.
- Los tickets y servicios deben diferenciarse correctamente.
- La matriz cruzada debe excluir la diagonal cuando se analicen familias distintas.
- Las cohortes incompletas deben excluirse de las tasas de retención.
- Las horas estándar deben reconciliar con la duración de los servicios válidos.
- Los escenarios deben mostrar y conservar todos sus supuestos.

---

## 18. Fases de construcción

### Fase 1 — Base analítica

- Homologar familias y servicios.
- Definir fórmula definitiva de ingreso salón.
- Limpiar clientas y tickets.
- Validar duraciones.
- Crear métricas del Portafolio actual.

### Fase 2 — Portafolio actual

- Tabla completa.
- Tendencias de 36 meses.
- Matriz Atractivo × Rentabilidad.
- Reglas iniciales de clasificación.

### Fase 3 — Share of Wallet

- Matriz cruzada.
- Afinidad y secuencias.
- Ranking de oportunidades.
- Simulador.

### Fase 4 — Nuevas familias

- Fichas de evaluación.
- Escenarios económicos.
- Seguimiento de pilotos.

### Fase 5 — Resumen ejecutivo

- Hallazgos automáticos.
- Alertas.
- Navegación final y exportaciones.

---

## 19. Preguntas pendientes antes del desarrollo

1. ¿Cuál es la fórmula exacta y auditada del ingreso salón por servicio?
2. ¿Existen costos de insumos históricos confiables?
3. ¿La duración estándar está disponible por servicio y período?
4. ¿Cómo se distribuyen descuentos en tickets con varios servicios?
5. ¿Qué estados identifican un servicio realmente entregado?
6. ¿El identificador de clienta es estable durante los tres años?
7. ¿Cuáles son las familias y servicios analíticos definitivos?
8. ¿Hay servicios simultáneos que distorsionen la suma de horas?
9. ¿Qué capacidad máxima tiene cada profesional/familia?
10. ¿Qué umbrales mínimos de muestra se utilizarán?

Estas preguntas no impiden construir la maqueta HTML, pero sí impiden validar completamente ciertas conclusiones económicas.

---

## 20. Resultado esperado

El reporte debe permitir llegar a conclusiones como:

- “Esta familia es grande y rentable, pero madura: conviene protegerla, no necesariamente expandirla.”
- “Esta familia aún es pequeña, pero crece en clientas, tiene buen ingreso por hora y alta recurrencia: merece una prueba de inversión.”
- “Esta familia vende mucho, pero consume demasiadas horas y pierde frecuencia: requiere optimización.”
- “Las clientas de Cabello muestran afinidad con Manos, pero la penetración es baja: existe una campaña concreta sobre una base identificable.”
- “La nueva familia propuesta podría atraer otro público, pero antes necesita un piloto porque la información interna no demuestra su demanda.”

La medida de éxito del proyecto no será la cantidad de gráficos, sino la capacidad de convertir los datos en **decisiones comprobables, acciones medibles y aprendizaje acumulativo**.
