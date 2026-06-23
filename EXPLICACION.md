# Explicación del Proyecto Hidrandina

Este documento detalla de manera sencilla y sin tecnicismos innecesarios qué sucede en cada fase del proyecto.

---

## Fase 1 - Carga y Limpieza (Loader)

### 1. ¿Qué datos entran? (El Input)
Ingresan los archivos originales de Hidrandina: 31 archivos mensuales que contienen el registro bruto de todos los recibos de luz de los clientes a lo largo de varios años.

### 2. ¿Por qué ingresan estos datos?
Porque los datos crudos del mundo real vienen con errores: valores nulos, textos desordenados, y formatos inconsistentes. Necesitamos una base sólida y limpia antes de hacer cualquier cálculo.

### 3. ¿Qué se hizo en esta fase?
Actuamos como un filtro gigante. Leímos los archivos y aplicamos reglas de limpieza vitales:
- Eliminamos recibos sin monto o sin consumo.
- Borramos valores ilógicos (consumos negativos o cero).
- Arreglamos las fechas y unificamos textos en mayúsculas.
- Separamos toda la información en dos tablas más ordenadas: una de "Consumo" y otra de "Ubicación del Cliente".

### 4. ¿Cómo lo medimos?
Usamos el indicador **OE1**, que nos exige que al menos el **85%** de los datos originales sobrevivan a la limpieza (es decir, que la calidad de origen haya sido aceptable).

### 5. ¿Cómo se interpreta?
Si de cada 100 recibos, nos quedamos con 85 o más válidos, significa que nuestra materia prima es confiable.

### 6. ¿Cómo sabemos si el resultado es bueno?
Si el indicador **OE1** se cumple, sabemos que tenemos una base de datos robusta y libre de "basura".

### 7. ¿Cuál es el resultado final? (La Salida)
Dos tablas impecables en formato CSV listas para usar: **Datos de Consumo** y **Datos del Cliente**.

### 8. ¿Cómo se conecta con lo que sigue?
Estos datos impecables son el combustible de la Fase 2. Sin ellos, las estadísticas que calculemos estarían contaminadas.

---

## Fase 1.5 - Validación Exploratoria

### 1. ¿Qué datos entran? (El Input)
Las dos tablas limpias generadas en la Fase 1.

### 2. ¿Por qué ingresan estos datos?
Porque nunca debemos confiar a ciegas. Necesitamos ver en gráficos que la limpieza funcionó y que los datos tienen sentido.

### 3. ¿Qué se hizo en esta fase?
Generamos visualizaciones iniciales para observar cómo se distribuye la clientela, qué zonas consumen más y detectar si quedaron valores extremos extraños.

### 4. ¿Cómo lo medimos?
Se mide visualmente. Revisamos que las gráficas no muestren anomalías lógicas.

### 5. ¿Cómo se interpreta?
Nos da una perspectiva gerencial rápida de cómo está conformada la base de clientes de Hidrandina antes de aplicar algoritmos complejos.

### 6. ¿Cómo sabemos si el resultado es bueno?
Si las gráficas reflejan un comportamiento normal de consumo eléctrico, damos el visto bueno.

### 7. ¿Cuál es el resultado final? (La Salida)
Gráficos e imágenes exploratorias.

### 8. ¿Cómo se conecta con lo que sigue?
Con la certeza visual de que la información cuadra, avanzamos a la Fase 2 para el cálculo pesado.

---

## Fase 2 - Procesamiento Histórico (Batch Layer)

### 1. ¿Qué datos entran? (El Input)
Recibimos las dos tablas limpias de la Fase 1 (Consumo y Cliente).

### 2. ¿Por qué ingresan estos datos?
Para saber si un cliente gasta "demasiado" hoy, primero debemos calcular qué es lo "normal" en el pasado.

### 3. ¿Qué se hizo en esta fase?
Metemos todos los historiales en una súper calculadora para procesos masivos (PySpark). Aquí hicimos:
- **Estadísticas Históricas:** Calculamos el consumo promedio, máximo y mínimo para cada combinación de distrito y tipo de tarifa.
- **Segmentación RFM:** Agrupamos a los clientes en "Estrella", "Activos", "En Riesgo" o "Perdidos" según sus hábitos de pago.
- **Tendencias:** Sumamos los consumos totales mensuales para ver la evolución en el tiempo.

### 4. ¿Cómo lo medimos?
Con el indicador **OE2**, que revisa que la tabla principal ("Estadísticas Históricas") se haya creado perfectamente con sus **10 columnas de información**, y sin consumos promedios nulos.

### 5. ¿Cómo se interpreta?
Significa que hemos logrado resumir todo el historial gigante de una zona en una sola línea que nos dicta el "límite normal" de consumo.

### 6. ¿Cómo sabemos si el resultado es bueno?
Sabemos que es exitoso si nuestro programa no colapsa por la cantidad de datos, y si el **OE2** da luz verde generando resultados matemáticamente coherentes.

**Nota sobre OE2 con el dataset completo (27.3M registros):** al agrupar por DISTRITO+TARIFA+CARTERA se generan 1,839 grupos, y 268 de ellos quedaban con `consumo_std = 0`. Al investigar, 224 eran grupos con un solo registro (el std muestral es matemáticamente indefinido con n=1, Spark devuelve NULL y se rellena con 0 — esto se corrigió excluyendo n=1 de la validación). Los 44 restantes son grupos con varios clientes que consumen exactamente la misma cantidad (consumo realmente constante, no un error de cálculo). Por eso OE2 queda en "NO" con datos reales: no es un fallo del pipeline, sino una característica esperada en distritos rurales con pocos clientes homogéneos.

### 7. ¿Cuál es el resultado final? (La Salida)
Generamos **6 archivos compactos CSV** con los resúmenes. El más importante es la tabla de "Estadísticas Históricas", que funcionará como nuestro gran "libro de reglas". Adicionalmente, graficamos los resultados.

### 8. ¿Cómo se conecta con lo que sigue?
Este "libro de reglas" es el cerebro de la **Fase 3 (Tiempo Real)**. Cuando un cliente empiece a consumir energía en el futuro, tomaremos ese nuevo dato y lo compararemos con este libro. Si choca con el promedio, ¡disparará una alerta automática!

---

## Fase 3 - Transmisión en Vivo (Kafka Producer / Simulador)

### 1. ¿Qué datos entran? (El Input)
La tabla limpia de "Datos de Consumo" de la Fase 1.

### 2. ¿Por qué ingresan estos datos?
Porque necesitamos simular cómo llegarían los recibos de luz en la vida real: uno por uno, en orden cronológico, como si los medidores estuvieran enviando la información en este preciso instante.

### 3. ¿Qué se hizo en esta fase?
Ordenamos todos los recibos por fecha y los preparamos para ser enviados como "mensajes" individuales a través de un sistema de mensajería rápida (Kafka). En nuestro caso local, al no tener un servidor Kafka encendido, el sistema automáticamente lo detectó y guardó estos eventos en un archivo simulado para replicar la experiencia de tiempo real.

### 4. ¿Cómo lo medimos?
Nos aseguramos de que la cantidad de eventos simulados generados coincida exactamente con la cantidad de recibos limpios que teníamos en la base.

### 5. ¿Cómo se interpreta?
En un entorno corporativo, esto sería el equivalente a tener millones de medidores eléctricos en todo el norte del Perú disparando lecturas de consumo a un servidor central continuamente.

### 6. ¿Cómo sabemos si el resultado es bueno?
Si se generaron los archivos JSON (eventos simulados) sin errores y en pocos segundos, sabemos que el simulador de tiempo real está funcionando a la perfección.

### 7. ¿Cuál es el resultado final? (La Salida)
Archivos en formato JSON (`eventos_simples.json`) que contienen cientos de miles de eventos individuales listos para ser procesados.

### 8. ¿Cómo se conecta con lo que sigue?
Estos eventos "volando" a toda velocidad son atrapados al vuelo por la **Fase 4 (Streaming)**, la cual los recibe uno a uno y los cruza con nuestro "libro de reglas" (Fase 2) para clasificar instantáneamente si el consumo es normal o una anomalía grave.

---

## Fase 4 - Interceptor en Tiempo Real (Streaming Layer)

### 1. ¿Qué datos entran? (El Input)
Recibe dos insumos al mismo tiempo:
1. El "libro de reglas históricas" que calculamos minuciosamente en la Fase 2.
2. El flujo continuo de nuevos recibos que van llegando a gran velocidad (simulados en la Fase 3).

### 2. ¿Por qué ingresan estos datos?
Porque necesitamos interceptar cada recibo nuevo "al vuelo" antes de que se almacene, para evaluar inmediatamente si el consumo eléctrico reportado tiene sentido o es una locura imposible.

### 3. ¿Qué se hizo en esta fase?
Activamos un escáner de alta velocidad (PySpark Streaming) que toma cada recibo entrante y busca su distrito y tarifa en el "libro de reglas". En milisegundos, aplica una fórmula estadística llamada "Z-Score" para medir qué tan desviado está este consumo frente a la normalidad histórica.
Según el resultado, se le asigna una etiqueta automática:
- **Consumo extremadamente alto** (Riesgo Alto)
- **Consumo alto** (Riesgo Medio)
- **Incremento brusco** (Riesgo Alto)
- **Variación moderada** (Riesgo Bajo)

### 4. ¿Cómo lo medimos?
Con el indicador **OE3**, que exige que nuestro interceptor trabaje a una altísima velocidad (latencia mínima) para garantizar que las clasificaciones se hagan en tiempo real (micro-batches de 5 segundos).

### 5. ¿Cómo se interpreta?
Es el equivalente a tener un guardia de seguridad infalible en la puerta. A cada recibo que entra, le revisa sus antecedentes en una fracción de segundo y le pega una etiqueta de color ("Alerta Roja", "Verde", etc.) sin detener nunca el tráfico.

### 6. ¿Cómo sabemos si el resultado es bueno?
Si el programa se queda escuchando los datos, procesa todos los lotes simulados de 107 mil registros sin que la memoria colapse, y genera carpetas de salida sin detenerse.

### 7. ¿Cuál es el resultado final? (La Salida)
Nuevos archivos particionados que contienen el recibo original, pero ahora "enriquecido" con **17 columnas de información de gran valor**, incluyendo su Nivel de Riesgo, el Tipo de Anomalía detectada y el timestamp exacto de la detección.

### 8. ¿Cómo se conecta con lo que sigue?
Toda esta avalancha de recibos clasificados (ya etiquetados como anomalías o normales) caerá suavemente en la última **Fase 5 (Serving Layer)**, donde consolidaremos los datos y generaremos el Dashboard final para los gerentes de Hidrandina.

---

## Fase 5 - Consolidación y Reporte Final (Serving Layer)

### 1. ¿Qué datos entran? (El Input)
Todo lo procesado anteriormente. Específicamente, las "anomalías detectadas en tiempo real" (de la Fase 4) y los "cálculos históricos" (de la Fase 2).

### 2. ¿Por qué ingresan estos datos?
Porque los datos sueltos no sirven para tomar decisiones de negocio. La alta gerencia no leerá miles de archivos; necesita resúmenes listos, gráficas entendibles y KPI (Indicadores Clave de Rendimiento).

### 3. ¿Qué se hizo en esta fase?
Actuamos como el gran "empaquetador". Tomamos todos los recibos y generamos tres grandes resúmenes para el directorio de la empresa:
- **Resumen por Distrito:** ¿Qué zonas tienen más riesgo eléctrico?
- **Resumen por Tarifa:** ¿Qué tipo de contrato es más problemático?
- **Resumen por Cartera:** ¿A qué grupo de clientes hay que prestarle más atención?
Además, generamos de forma automática los **gráficos finales** del proyecto (Dashboard).

### 4. ¿Cómo lo medimos?
Usamos dos indicadores finales:
- **OE4:** Verifica matemáticamente que la gran tabla final no tenga errores, ni campos nulos, y contenga sus 17 columnas perfectas.
- **OE5:** Verifica que el sistema haya exportado correctamente los 4 "entregables finales" que se le prometió a gerencia (La tabla global y los tres resúmenes).

### 5. ¿Cómo se interpreta?
Esta fase consolida que nuestro producto Big Data pasó de ser solo código técnico a convertirse en verdadero "Business Intelligence" (Inteligencia de Negocios).

### 6. ¿Cómo sabemos si el resultado es bueno?
Si el reporte de estado automático indica "EXITOSO" y vemos el archivo gráfico (`dashboard_anomalias.png`) generado correctamente en nuestra carpeta, significa que hemos triunfado en procesar millones de registros y volverlos gráficas.

### 7. ¿Cuál es el resultado final? (La Salida)
- La Gran Tabla de Anomalías (17 columnas maestras).
- Tres tablas resumen para la gerencia.
- El Dashboard visual interactivo.
- Un reporte que certifica el cumplimiento de los indicadores (KPIs).

### 8. ¿Cómo se conecta con lo que sigue?
¡No sigue nada técnico! Estos archivos finales están listos para ser presentados a los directores de Hidrandina S.A. para que tomen decisiones sobre cortes de servicio, inspecciones de campo o ajustes tarifarios.
