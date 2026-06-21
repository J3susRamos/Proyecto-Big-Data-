# Hidrandina — Arquitectura Lambda para Detección de Anomalías en Consumo Eléctrico

> **Proyecto académico de Big Data** — Procesamiento de 30 millones de registros reales de consumo eléctrico de Hidrandina (norte del Perú) usando una arquitectura Lambda con Python, PySpark y Kafka.

---

## Tabla de Contenidos

- [Descripción del Proyecto](#descripcion-del-proyecto)
- [Stack Tecnológico](#stack-tecnologico)
- [Estructura del Repositorio](#estructura-del-repositorio)
- [Requisitos](#requisitos)
- [Instalación y Ejecución](#instalacion-y-ejecucion)
- [Descarga de Datos](#descarga-de-datos)
- [Arquitectura](#arquitectura)
- [KPIs del Proyecto](#kpis-del-proyecto)
- [Resultados Esperados](#resultados-esperados)
- [Solución de Problemas](#solucion-de-problemas)

---

## Descripción del Proyecto

Pipeline de **Arquitectura Lambda** que procesa el historial de consumo eléctrico de **Hidrandina S.A.** (empresa de distribución eléctrica del norte del Perú: La Libertad, Ancash, Cajamarca).

### Flujo del Pipeline

```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│  LOADER   │───▶│  BATCH   │───▶│ PRODUCER │───▶│STREAMING │───▶│ SERVING  │
│ (Pandas)  │    │ (PySpark)│    │  (Kafka) │    │ (PySpark)│    │ (Pandas) │
└──────────┘    └──────────┘    └──────────┘    └──────────┘    └──────────┘
```

1. **LOADER** — Carga y limpia los CSV de Hidrandina, separa en `FACT_CONSUMO` y `DIM_CLIENTE_UBICACION`
2. **BATCH** — Procesamiento histórico con PySpark: estadísticas por distrito/tarifa/cartera, KPIs globales, ranking, tendencia mensual y segmentación RFM
3. **PRODUCER** — Publica eventos de consumo a Kafka (o modo simulado sin Kafka)
4. **STREAMING** — Consume eventos, enriquece con estadísticas históricas, calcula z-score y clasifica anomalías
5. **SERVING** — Une capas batch y speed, genera dashboard y reporte final de KPIs

---

## Stack Tecnológico

| Componente | Versión | Propósito |
|------------|---------|-----------|
| Python | 3.11 | Lenguaje principal |
| PySpark | 4.1.2 | Procesamiento distribuido batch y streaming |
| Apache Kafka | 7.4.0 (Confluent) | Broker de mensajes para el speed layer |
| Pandas | 3.0.3 | Manipulación de datos en serving layer |
| Matplotlib | 3.10.9 | Dashboard de anomalías |
| OpenJDK | 11 | Requerido por PySpark |
| Docker | 24+ | Contenedorización del pipeline completo |

---

## Estructura del Repositorio

```
hidrandina_project/
│
├── .dockerignore          # Archivos excluidos del contexto Docker
├── .env                   # Variables de entorno para docker-compose
├── .gitignore             # Archivos ignorados por Git (CSV grandes, resultados)
├── docker-compose.yml     # Orquestación: Zookeeper + Kafka + Pipeline
├── Dockerfile             # Imagen del pipeline (Python + Java + Spark)
├── requirements.txt       # Dependencias Python
├── README.md              # Este archivo
│
├── main.py                # Orquestador principal del pipeline
│
├── utils/
│   └── loader.py          # Carga, limpieza y separación de tablas
│
├── batch_layer/
│   └── spark_batch.py     # Procesamiento batch con PySpark
│
├── speed_layer/
│   ├── kafka_producer.py  # Publicación de eventos a Kafka
│   └── spark_streaming.py # Consumo y clasificación de anomalías
│
├── serving_layer/
│   └── serving.py         # Unión batch+stream, dashboard y KPIs
│
└── data/                  # ⬅️ Aquí se colocan los CSV de Hidrandina
    ├── FACT_CONSUMO.csv          (generado por loader)
    ├── DIM_CLIENTE_UBICACION.csv (generado por loader)
    └── originales/               (CSV originales de Hidrandina aquí)
```

---

## Requisitos

Solo necesitas **Docker Desktop** instalado en tu máquina:

- **Windows**: [Docker Desktop for Windows](https://docs.docker.com/desktop/setup/install/windows-install/)
- **macOS**: [Docker Desktop for Mac](https://docs.docker.com/desktop/setup/install/mac-install/)
- **Linux**: [Docker Engine](https://docs.docker.com/engine/install/) + [Docker Compose](https://docs.docker.com/compose/install/)

> **No necesitas instalar Python, Java, Spark ni Kafka en tu máquina.** Todo corre dentro de contenedores Docker.

### Recursos Recomendados

| Recurso | Mínimo | Recomendado |
|---------|--------|-------------|
| RAM | 8 GB | 16 GB |
| Disco | 10 GB libres | 20 GB libres |
| CPUs | 4 núcleos | 8 núcleos |

> Los 30 millones de registros requieren al menos **4 GB de RAM** para PySpark. Docker Desktop debe tener al menos 6 GB asignados.

---

## Instalación y Ejecución

### 1. Clonar el repositorio

```bash
git clone https://github.com/tu-usuario/hidrandina_project.git
cd hidrandina_project
```

### 2. Colocar los datos

Crea la carpeta de datos originales y coloca allí los CSV de Hidrandina:

```bash
mkdir -p data/originales
```

Copia todos los archivos CSV mensuales (Ene 2023 - Jun 2025) a la carpeta `data/originales/`.

> **Si los CSV no están disponibles**, el pipeline se ejecutará en modo limitado usando solo el loader (que requiere los archivos originales).

### 3. Ejecutar el pipeline completo

```bash
docker-compose up --build
```

Este comando:

1. Construye la imagen Docker con Python 3.11 + Java 11 + PySpark 4.1.2
2. Inicia Zookeeper (coordinación de Kafka)
3. Inicia Kafka (broker de mensajes)
4. Ejecuta `main.py` que orquesta las 5 etapas del pipeline

### 4. Ejecutar etapas específicas

Para ejecutar solo una etapa (útil para desarrollo o depuración):

```bash
# Solo el loader
docker-compose run --rm pipeline --etapa loader

# Solo batch
docker-compose run --rm pipeline --etapa batch

# Producer + Streaming + Serving (modo simulado)
docker-compose run --rm pipeline --etapa producer --etapa streaming --etapa serving
```

### 5. Ver los resultados

Los resultados se generan en la carpeta `serving_layer/` de tu máquina:

| Archivo | Descripción |
|---------|-------------|
| `FACT_ANOMALIAS_CONSUMO.csv` | 17 columnas con anomalías detectadas |
| `RESUMEN_ANOMALIAS_DISTRITO.csv` | Resumen por distrito (6 columnas) |
| `RESUMEN_ANOMALIAS_TARIFA.csv` | Resumen por tarifa (6 columnas) |
| `RESUMEN_ANOMALIAS_CARTERA.csv` | Resumen por cartera (6 columnas) |
| `dashboard.png` | Dashboard con 4 gráficos |
| `reporte_kpis.json` | KPIs del proyecto en JSON |
| `reporte_calidad.json` | Reporte de calidad del loader |
| `batch_results/` | Resultados intermedios del batch layer (Parquet) |

---

## Descarga de Datos

Los archivos CSV de Hidrandina **NO están incluidos** en este repositorio porque:

- Son archivos grandes (varios GB en total)
- Contienen datos sensibles de clientes (anonimizados)
- Son propiedad de Hidrandina S.A.

### Para obtener los datos

1. **Opción 1** — Contacta al equipo del proyecto para acceder a los archivos originales
2. **Opción 2** — Usa datos de prueba generados con `Faker` (ejecutar modo simulado)
3. **Opción 3** — Solicita acceso al dataset en el repositorio institucional de la universidad

### Estructura esperada de los datos

```
data/originales/
├── ENE_2023.csv
├── FEB_2023.csv
├── MAR_2023.csv
├── ...
└── JUN_2025.csv
```

Cada archivo CSV debe tener las siguientes columnas (separadas por `;`):

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `NRO_SERVICIO` | Entero | Identificador del suministro (0 = anonimizado) |
| `NRO_DOC_FAC` | String | Identificador único de factura (PK real) |
| `PERIODO` | Entero | Período de facturación (aaaamm) |
| `CONSUMO` | Decimal | Consumo en kWh |
| `IMPORTE` | Decimal | Monto facturado en soles |
| `FECHA_EMISION` | Entero | Fecha de emisión (yyyymmdd) |
| `FECHA_VENCIMIENTO` | Entero | Fecha de vencimiento (yyyymmdd) |
| `FECHA_COSNUMO_DESDE` | Entero | Inicio consumo (yyyymmdd) |
| `FECHA_CONSUMO_HASTA` | Entero | Fin consumo (yyyymmdd) |
| `DEPARTAMENTO` | String | Departamento del cliente |
| `PROVINCIA` | String | Provincia del cliente |
| `DISTRITO` | String | Distrito del cliente |
| `UBIGEO` | String | Código geográfico INEI |
| `TARIFA` | String | Tipo de tarifa (BT5B, BT2, MT2, etc.) |
| `CARTERA` | String | COMÚN o MAYOR |
| `UNIDAD_NEGOCIO` | String | Zona operacional |

---

## Arquitectura

### Diagrama de la Arquitectura Lambda

```
                          ┌─────────────────────┐
                          │   Datos Históricos   │
                          │  (CSV mensuales)     │
                          └──────────┬──────────┘
                                     │
                          ┌──────────▼──────────┐
                          │      LOADER          │
                          │   (Pandas, limpieza) │
                          └──────┬─────────┬─────┘
                                 │         │
                    ┌────────────▼──┐  ┌───▼──────────────┐
                    │  BATCH LAYER  │  │  SPEED LAYER      │
                    │  (PySpark)    │  │  (Kafka + Spark)  │
                    │               │  │                   │
                    │ Estadísticas  │  │ Eventos en tiempo │
                    │ históricas    │  │ real / simulados  │
                    │ KPIs globales │  │                   │
                    │ Ranking, RFM  │  │ Anomalías         │
                    └───────┬───────┘  └────────┬──────────┘
                            │                   │
                            └───────┬───────────┘
                                    │
                          ┌─────────▼──────────┐
                          │   SERVING LAYER    │
                          │  (Pandas + Matplotlib)
                          │                    │
                          │ FACT_ANOMALIAS     │
                          │ Dashboard + KPIs   │
                          └────────────────────┘
```

### Flujo de Datos

1. **Loader** → Lee 31 CSV mensuales, limpia, estandariza y separa en `FACT_CONSUMO` (hechos) y `DIM_CLIENTE_UBICACION` (dimensional)
2. **Batch** → JOIN ambas tablas, calcula estadísticas agrupadas por distrito/tarifa/cartera, tendencias mensuales, ranking departamentos, segmentación RFM
3. **Producer** → Lee el CSV limpio, ordena cronológicamente y publica cada registro como evento JSON a Kafka
4. **Streaming** → Consume eventos, hace JOIN con las estadísticas históricas, calcula z-score y clasifica anomalías
5. **Serving** → Unifica resultados, genera tabla de 17 columnas, dashboard y reporte de KPIs

---

## KPIs del Proyecto

| KPI | Descripción | Métrica | Estado |
| **OE1** | Calidad de datos en el loader | Tasa de validez >= 85% | ✅ / ❌ |
| **OE2** | Estadísticas históricas completas | 10 columnas, consumo_promedio > 0 | ✅ / ❌ |
| **OE3** | Latencia del speed layer | < 5 segundos | ✅ / ❌ |
| **OE4** | FACT_ANOMALIAS_CONSUMO completo | 17 columnas, 0 nulos z-score, flag_anomalia = TRUE 100% | ✅ / ❌ |
| **OE5** | 4 outputs generados por serving | FACT + 3 resúmenes (distrito, tarifa, cartera) | ✅ / ❌ |

---

## Resultados Esperados

Al ejecutar el pipeline completo, deberías ver una salida similar a esta:

```
======================================================================
  PIPELINE LAMBDA — Deteccion de Anomalias en Consumo Electrico
  Hidrandina S.A. | Diciembre 2022 - Junio 2025
  Arquitectura Lambda: Batch + Speed + Serving
======================================================================

ETAPA 1/5: LOADER — Carga y limpieza de datos
----------------------------------------------------------------------
  FACT_CONSUMO: 27,304,677 filas x 8 cols
  DIM_CLIENTE_UBICACION: 27,304,677 filas x 8 cols
  Tasa de validez: 88.49%
  OE1 cumplido: SI

ETAPA 2/5: BATCH — Procesamiento historico con PySpark
----------------------------------------------------------------------
  TMP_ESTADISTICAS_HISTORICAS: X filas x 10 cols
  KPIs globales: 8 indicadores
  OE2 cumplido: SI

...

======================================================================
  PIPELINE FINALIZADO
  Duracion total: ~600 segundos
  Estado: EXITOSO
======================================================================
```

---

## Solución de Problemas

### Error: `Java not found`

```bash
Error: Java not found. Please install Java 11+.
```

**Solución**: El Dockerfile instala Java automáticamente. Si usas el pipeline fuera de Docker (instalación manual), instala OpenJDK 11:

```bash
# En Windows: Descargar e instalar desde https://adoptium.net/
# En Linux:
sudo apt-get install openjdk-11-jdk-headless
```

### Error: `OutOfMemoryError: Java heap space`

**Solución**: Aumentar la memoria asignada a Docker Desktop:

1. Abrir Docker Desktop → Settings → Resources → Advanced
2. Asignar al menos 6 GB de RAM
3. Aplicar y reiniciar Docker

### Error: `Kafka not available`

**Solución**: El pipeline detecta automáticamente si Kafka está disponible. Si no, cambia a modo simulado. Para forzar modo simulado:

```bash
docker-compose run --rm pipeline --etapa producer --etapa streaming --simulado
```

### Error: `No such file or directory: data/hidrandina_limpio.csv`

**Solución**: El loader debe ejecutarse primero para generar los CSV de salida:

```bash
docker-compose run --rm pipeline --etapa loader
```

### Error de permisos en Linux

Si los archivos generados pertenecen a `root`, ejecuta:

```bash
sudo chown -R $USER:$USER serving_layer/ data/
```

---

## Adaptación del Código para Docker

El código original del proyecto usa rutas Windows (`r"C:\Users\Roxwell\..."`) y `localhost` para Kafka. Para funcionar dentro de contenedores Docker, el código lee las variables de entorno definidas en `docker-compose.yml`:

| Variable | Valor en Docker | Propósito |
| `RUTA_PROYECTO` | `/app` | Directorio raíz dentro del contenedor |
| `RUTA_DATA` | `/app/data` | Datos de entrada/salida |
| `RUTA_SERVING` | `/app/output` | Resultados finales |
| `RUTA_CSV_ORIGINALES` | `/app/data/originales` | CSV originales de Hidrandina |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Hostname del servicio Kafka |

> Si deseas ejecutar el proyecto **sin Docker** (instalación manual), modifica las rutas en cada archivo para que apunten a tu directorio local.

---

## Licencia

Proyecto académico — Universidad Privada del Norte (UPN)
Curso: Big Data | Arquitectura Lambda

---

## Créditos

- **Dataset**: Hidrandina S.A. — Consumo eléctrico del norte del Perú
- **Stack**: Apache Spark + Kafka + Python + Docker
- **Arquitectura**: Lambda Architecture (Batch + Speed + Serving)
