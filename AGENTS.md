# Hidrandina - Arquitectura Lambda para Detección de Anomalías en Consumo Eléctrico

## Descripción General

Pipeline de **Arquitectura Lambda** que procesa 30 millones de registros reales de consumo eléctrico de **Hidrandina S.A.** (empresa de distribución eléctrica del norte del Perú: La Libertad, Ancash, Cajamarca).

El pipeline Lee 31 CSV mensuales crudos, los limpia, calcula estadísticas históricas con PySpark, transmite eventos por Kafka (o simulado), clasifica anomalías en tiempo real mediante z-score, y genera un dashboard final con KPIs.

## Stack Tecnológico

- **Python 3.11.9** — lenguaje principal
- **PySpark 4.1.2** — procesamiento batch y streaming
- **Apache Kafka 7.4.0** (Confluent) — broker de mensajes (speed layer)
- **Pandas 3.0.3** — manipulación de datos en loader y serving
- **Matplotlib 3.10.9** — dashboard de anomalías
- **OpenJDK 17** — requerido por PySpark
- **Docker** — contenedorización del pipeline completo

## Estructura del Proyecto

```
hidrandina_project/
├── main.py                    # Orquestador: ejecuta las 5 etapas en orden
├── utils/
│   └── loader.py              # Carga CSV, limpia, separa en FACT y DIM
├── batch_layer/
│   └── spark_batch.py         # PySpark batch: estadísticas, KPIs, ranking, RFM
├── speed_layer/
│   ├── kafka_producer.py      # Publica eventos a Kafka (o modo simulado)
│   └── spark_streaming.py     # Consume eventos, calcula z-score, clasifica anomalías
├── serving_layer/
│   └── serving.py             # Une batch+stream, genera CSV, dashboard.png, reporte_kpis.json
├── data/                      # CSV de entrada/salida (ignorados por git)
│   └── originales/            # CSV mensuales crudos de Hidrandina
├── serving_layer/batch_results/   # Parquet generados por batch
├── Dockerfile                 # Imagen Docker (Python + Java 17 + Spark)
├── docker-compose.yml         # Orquestación: Zookeeper + Kafka + Pipeline
├── requirements.txt
├── .env / .env.example
├── .gitignore
├── .dockerignore
└── AGENTS.md                  # ← este archivo
```

## Las 5 Etapas del Pipeline

### 1. LOADER (`utils/loader.py`)
- Lee 31 CSV mensuales desde `data/originales/`
- Detecta separador automáticamente (`;`, `,`, `\t`)
- Limpia: convierte tipos, elimina nulos en CONSUMO/IMPORTE, filtra ≤0, estandariza texto, marca outliers (z-score > 3)
- Separa en dos tablas: `FACT_CONSUMO` (hechos) y `DIM_CLIENTE_UBICACION` (dimensional)
- KPI OE1: tasa de validez >= 85%

### 2. BATCH LAYER (`batch_layer/spark_batch.py`)
- PySpark: JOIN entre FACT y DIM por `NRO_DOC_FAC`
- 6 cálculos en paralelo:
  - **TMP_ESTADISTICAS_HISTORICAS**: agrupado por DISTRITO, TARIFA, CARTERA → promedio/std/min/max de CONSUMO e IMPORTE (10 columnas) — ¡es la tabla más importante!
  - **KPIs globales**: facturación total, consumo total, ticket promedio, tasa de outliers
  - **Ranking por departamento**: suma de importe/consumo por DEPARTAMENTO
  - **Tendencia mensual**: suma por PERIODO ordenado ascendente
  - **Análisis tarifa/cartera**: suma por TARIFA + CARTERA
  - **Segmentación RFM**: Recency (días desde última emisión), Frequency (periodos distintos), Monetary (suma importe) → scores 1-3 → segmentos: Champion / Cliente activo / En riesgo / Perdido
- Salidas: archivos Parquet en `serving_layer/batch_results/`
- KPI OE2: 10 columnas en TMP_ESTADISTICAS_HISTORICAS, consumo_promedio > 0

### 3. PRODUCER (`speed_layer/kafka_producer.py`)
- Lee `hidrandina_limpio.csv`, ordena por FECHA_EMISION
- Publica cada fila como evento JSON al topic `hidrandina-consumo` de Kafka
- Modo simulado: guarda eventos en `data/eventos_simples.json` (cuando Kafka no está disponible)

### 4. STREAMING (`speed_layer/spark_streaming.py`)
- Consume eventos desde Kafka (o JSON simulado)
- JOIN con TMP_ESTADISTICAS_HISTORICAS por DISTRITO
- Calcula z-score: `(consumo_actual - consumo_promedio) / desviacion_consumo`
- Clasifica anomalías:
  - z-score > 3 → "Consumo extremadamente alto" / Riesgo Alto
  - 2 ≤ z-score ≤ 3 → "Consumo alto" / Riesgo Medio
  - z-score < -2 → "Consumo sospechosamente bajo" / Riesgo Bajo
  - Variación > 100% → "Incremento brusco"
- Salida: Parquet con 17 columnas finales
- KPI OE3: latencia < 5 segundos (streaming), precisión >= 90%

### 5. SERVING (`serving_layer/serving.py`)
- Carga datos batch + streaming (con fallback: Parquet → CSV → Spark)
- Genera **FACT_ANOMALIAS_CONSUMO** (17 columnas estandarizadas)
- 3 tablas de resumen (6 columnas cada una): por DISTRITO, TARIFA, CARTERA
- Dashboard PNG (4 gráficos): tendencia mensual, top 10 distritos, distribución riesgo, top z-score por distrito
- Reporte JSON con KPIs finales
- KPI OE4: 17 columnas, 0 nulos en z-score, flag_anomalia = TRUE 100%
- KPI OE5: 4 outputs generados (FACT + 3 resúmenes)

## Tablas y Columnas Clave

### FACT_CONSUMO (8 columnas)
`NRO_DOC_FAC` (PK, String), `PERIODO` (Int), `CONSUMO` (Float), `IMPORTE` (Float), `FECHA_EMISION` (Date), `FECHA_VENCIMIENTO` (Date), `FECHA_CONSUMO_DESDE` (Date), `FECHA_CONSUMO_HASTA` (Date)

### DIM_CLIENTE_UBICACION (8 columnas)
`NRO_DOC_FAC` (FK, String), `DEPARTAMENTO`, `PROVINCIA`, `DISTRITO`, `UBIGEO`, `TARIFA`, `CARTERA`, `UNIDAD_NEGOCIO`

### TMP_ESTADISTICAS_HISTORICAS (10 columnas)
`DISTRITO`, `TARIFA`, `CARTERA`, `consumo_promedio`, `consumo_std`, `importe_promedio`, `importe_std`, `consumo_minimo`, `consumo_maximo`, `total_registros`

### FACT_ANOMALIAS_CONSUMO (17 columnas)
`id_anomalia`, `nro_servicio`, `periodo`, `consumo_actual`, `importe_actual`, `distrito`, `tarifa`, `cartera`, `consumo_promedio_historico`, `importe_promedio_historico`, `desviacion_consumo`, `zscore_consumo`, `porcentaje_variacion`, `tipo_anomalia`, `nivel_riesgo`, `fecha_deteccion`, `flag_anomalia`

## Convenciones Importantes

### NRO_SERVICIO está anonimizado
El dataset real tiene `NRO_SERVICIO = 0` en todos los registros. Se usa **`NRO_DOC_FAC`** como identificador único en todas las tablas (formato: `S501-60798028`).

### Rutas y Variables de Entorno
El código usa `os.environ.get("VAR", "fallback_local")` para compatibilidad dual:
- **Local (Windows)**: usa el valor por defecto (ruta Windows relativa)
- **Docker (Linux)**: usa la variable de entorno del `.env`

Variables disponibles: `RUTA_CSV_ORIGINALES`, `RUTA_DATA`, `RUTA_SERVING`, `RUTA_SPEED`, `KAFKA_BOOTSTRAP_SERVERS`, `SPARK_MASTER`

### Spark
- Configurado con `spark.sql.shuffle.partitions=8` y Adaptive Query Engine
- Modo `local[*]` para desarrollo, se puede cambiar vía `SPARK_MASTER`
- Compresión Parquet: snappy
- Writer mode: `"overwrite"` para todos los resultados

### KPIs del Proyecto
| KPI | Descripción | Meta |
|-----|-------------|------|
| OE1 | Calidad de datos (loader) | ≥ 85% tasa de validez |
| OE2 | Estadísticas históricas completas | 10 columnas, consumo > 0 |
| OE3 | Latencia speed layer | < 5 segundos |
| OE4 | FACT_ANOMALIAS_CONSUMO íntegro | 17 cols, 0 nulos z-score, 100% flag_anomalia |
| OE5 | 4 outputs de serving | FACT + 3 resúmenes generados |

### Cómo Ejecutar (para usuarios)

```bash
# Solo ver el dashboard (datos pre-computados incluidos) — RECOMENDADO
docker-compose up --build

# Ejecutar pipeline completo y luego ver dashboard
MODO=full docker-compose up --build

# Sin Docker (solo dashboard, requiere Python + JSONs pre-computados)
pip install -r requirements.txt
python serve_dashboard.py
# Abrir http://localhost:8050/dashboard.html

# Sin Docker (pipeline completo, requiere Java 17 + Spark 4.1.2)
python main.py --simulado
```

### Reglas para modificar código
1. No usar rutas Windows hardcodeadas — siempre usar `os.environ.get()` con fallback
2. Los comentarios deben estar en español
3. No usar `RUTA_PROYECTO` — usar `RUTA_DATA` y `RUTA_SERVING` directamente
4. Para nuevas funciones Spark, usar `F.col()`, `F.when()`, `F.lit()` de `pyspark.sql.functions`
5. `NRO_DOC_FAC` es la llave primaria — NO usar `NRO_SERVICIO`
