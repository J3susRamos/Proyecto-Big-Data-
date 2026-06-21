# Plan de Acción - Arquitectura Lambda para Detección de Anomalías

El objetivo de este plan es estructurar el desarrollo del pipeline de datos de Hidrandina en fases secuenciales. Cada fase cuenta con un propósito claro, métricas de éxito (KPIs) y una forma de validar manualmente los resultados antes de pasar a la siguiente etapa.

## ¿Por qué es necesario presentar un Dashboard al final?
En la Arquitectura Lambda, las capas **Batch** y **Speed** procesan millones de registros para generar resultados consolidados. Sin embargo, estos datos terminan almacenados en formatos técnicos (como archivos Parquet o JSON) que no son amigables para la lectura directa. 

El **Dashboard** representa la capa de presentación dentro de la **Serving Layer**. Es el producto final que traduce los datos crudos y procesados en información visual. Permite a los usuarios de negocio (que no saben programar ni usar SQL) visualizar las alertas y tomar decisiones estratégicas de forma inmediata sobre:
- Tendencias mensuales de consumo.
- Distribución de los niveles de riesgo de las anomalías (Riesgo Alto, Medio, Bajo).
- Identificación de los Distritos con mayores incidentes (Top 10).

---

## Fases del Proyecto

### Fase 1: Input Layer (Carga y Limpieza de Datos - Loader)
**Consiste en:** 
Leer los 31 archivos CSV mensuales de consumos crudos proporcionados por la empresa, estandarizar su formato, limpiar los datos (eliminar nulos, filtrar montos $\le0$, identificar outliers extremos usando z-score) y dividirlos en dos tablas normalizadas: `FACT_CONSUMO` (hechos) y `DIM_CLIENTE_UBICACION` (dimensiones), unidas por la llave `NRO_DOC_FAC`.

**Validación Manual y Resultados Esperados:**
- **Cómo validar:** Ejecutaremos el script correspondiente al loader (`utils/loader.py`) y revisaremos el log de salida. Validaremos que los archivos se hayan guardado en la carpeta designada.
- **Métrica Clave (KPI OE1):** **Tasa de validez $\ge$ 85%**. Esto significa que al menos el 85% de las filas originales deben sobrevivir al proceso de limpieza y guardarse correctamente.

---

### Fase 2: Batch Layer (Procesamiento Histórico con PySpark)
**Consiste en:** 
Procesar de forma distribuida (usando PySpark) todo el historial completo de datos limpios. En esta etapa se realizan múltiples cálculos analíticos de fondo: estadísticas históricas, tendencias mensuales, KPIs globales y la Segmentación RFM de los clientes.

**Validación Manual y Resultados Esperados:**
- **Cómo validar:** Se ejecutará `batch_layer/spark_batch.py`. Entraremos a la carpeta `serving_layer/batch_results/` para comprobar visualmente que existen los archivos Parquet procesados.
- **Métrica Clave (KPI OE2):** La tabla base más importante que se genere (`TMP_ESTADISTICAS_HISTORICAS`) debe contar con exactamente **10 columnas**. Además, comprobaremos en los resultados que para todos los registros, el valor de `consumo_promedio` sea estrictamente mayor a 0.

---

### Fase 3: Speed Layer (Productor Kafka y Streaming)
**Consiste en:** 
Procesar datos "en caliente" (tiempo real). Primero, mediante un **Productor** publicaremos eventos de consumo como mensajes en Apache Kafka. Luego, en la fase de **Streaming**, escucharemos estos eventos, cruzaremos la información con las estadísticas históricas calculadas en la Fase 2 y calcularemos un *z-score* en vivo para clasificar cada nuevo registro (riesgo alto, riesgo medio o riesgo bajo).

**Validación Manual y Resultados Esperados:**
- **Cómo validar:** Levantaremos el broker Kafka (o ejecutaremos en modo "simulado" generando JSON) y observaremos en consola cómo fluyen los mensajes a través de los scripts `speed_layer/kafka_producer.py` y `speed_layer/spark_streaming.py`.
- **Métrica Clave (KPI OE3):** El tiempo de latencia del streaming debe ser **$\le$ 5 segundos**. La precisión de la clasificación de las anomalías debe ser **$\ge$ 90%**.

---

### Fase 4: Serving Layer (Consolidación de Datos)
**Consiste en:** 
Unificar la información que nos arrojó la Batch Layer con la que nos arroja la Speed Layer. En esta etapa preparamos los datos definitivos que van a alimentar al dashboard: se crea la tabla unificada de hechos `FACT_ANOMALIAS_CONSUMO` y tres tablas resumidas (por distrito, tarifa y cartera).

**Validación Manual y Resultados Esperados:**
- **Cómo validar:** Ejecutaremos `serving_layer/serving.py` y abriremos con herramientas de análisis de datos las tablas resultantes para inspeccionar la completitud.
- **Métricas Clave:**
  - **KPI OE4:** `FACT_ANOMALIAS_CONSUMO` debe tener íntegramente **17 columnas**. No debe haber ningún valor nulo (0 nulos) en el campo `zscore_consumo` y todos los registros con el flag de anomalía deben tener `flag_anomalia = TRUE`.
  - **KPI OE5:** Se validará que el script genere **4 outputs finales** (1 FACT y 3 resúmenes).

---

### Fase 5: Visualización y Dashboard
**Consiste en:** 
Consumir todos los resultados finales (los outputs de la Fase 4) para renderizarlos en un dashboard interactivo web y reportes en PDF/JSON. 

**Validación Manual y Resultados Esperados:**
- **Cómo validar:** Levantaremos el servidor local del dashboard (usando Docker o `serve_dashboard.py`). Abriremos el navegador (`http://localhost:8050`) y comprobaremos la interactividad visual de la página.
- **Métrica Clave:** Deberán renderizar exitosamente los **4 gráficos de negocio requeridos** (Tendencia mensual, Top 10 distritos, Distribución de riesgo, Top de anomalías por distrito) y generarse el reporte `reporte_kpis.json`.

---

## Open Questions
> [!IMPORTANT]
> ¿Estás de acuerdo con el enfoque y las métricas planteadas? Si te parece correcto, procederé a crear el archivo de Tareas (`task.md`) y empezaremos con la **Fase 1 (Loader)**.
