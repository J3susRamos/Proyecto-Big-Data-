# Manual para IA — Estado del Pipeline Hidrandina (post-batch)

> Este archivo está pensado para que un asistente de IA (Claude, etc.) lea el estado actual
> del proyecto antes de ayudar a un usuario a continuar el pipeline. Complementa a
> [AGENTS.md](AGENTS.md), que tiene la documentación general de arquitectura.

## Contexto

El **Batch Layer** ya se ejecutó una vez sobre el dataset completo (27,304,677 registros,
tras limpieza del loader) y sus resultados están commiteados en el repositorio. Esto significa
que **nadie del equipo necesita volver a correr el batch** (tarda ~22 minutos: JOIN 139s,
KPIs 52s, RFM 95s+57s) salvo que cambien los datos de entrada o la lógica de `spark_batch.py`.

## Qué ya está disponible (no recalcular)

Carpeta `serving_layer/batch_results/` — **subida a git, no está en `.gitignore`**:

| Archivo | Contenido | Usado por |
|---|---|---|
| `tmp_estadisticas_historicas.csv` | 1,839 grupos (DISTRITO+TARIFA+CARTERA): promedio/std/min/max de CONSUMO e IMPORTE | **Crítico** — lo necesitan `spark_streaming.py` y `serving.py` para calcular z-score |
| `tendencia_mensual.csv` | Consumo/importe agregado por PERIODO (31 periodos) | `serving.py` (gráfico de tendencia) |
| `kpis_globales.csv` | 8 indicadores (facturación total, consumo total, ticket promedio, etc.) | `graficos_fase2.py` |
| `ranking_departamentos.csv` | Suma de importe/consumo por los 13 departamentos | `graficos_fase2.py` |
| `analisis_tarifa_cartera.csv` | 26 combinaciones TARIFA+CARTERA | `graficos_fase2.py` |
| `rfm_clientes.csv` | Segmentación RFM de 27.3M clientes (Champion/Activo/En riesgo/Perdido) | `graficos_fase2.py` |

## Qué SIGUE faltando por correr

El **dataset crudo** (`data/originales/*.csv`) y los **CSV intermedios del loader**
(`data/FACT_CONSUMO.csv`, `data/DIM_CLIENTE_UBICACION.csv`, `data/hidrandina_limpio.csv`)
**NO están en git** (son varios GB, ver `.gitignore`). Cada compañero necesita conseguirlos
por separado (contactar al equipo o repositorio institucional) si quiere volver a correr el
loader o el batch desde cero.

Las etapas que cada compañero sí debe ejecutar localmente:

1. **PRODUCER** (`speed_layer/kafka_producer.py`) — publica eventos a Kafka o simulado.
2. **STREAMING** (`speed_layer/spark_streaming.py`) — consume eventos, hace join con
   `tmp_estadisticas_historicas.csv` (ya disponible), calcula z-score, clasifica anomalías.
3. **SERVING** (`serving_layer/serving.py`) — une resultados batch + streaming, genera
   `FACT_ANOMALIAS_CONSUMO`, dashboard y reporte de KPIs.

## Cómo saltarse loader + batch

Si un compañero ya tiene `serving_layer/batch_results/` (viene con `git pull`), puede ir
directo a producer/streaming/serving:

```bash
# Sin Docker
python main.py --etapa producer --etapa streaming --etapa serving

# Con Docker (perfil producer + streaming, sin tocar el pipeline principal)
docker-compose --profile producer --profile streaming up --build
```

**No usar `--etapa loader` ni `--etapa batch`** a menos que se quiera regenerar los
resultados (requiere los CSV originales en `data/originales/`, ver arriba).

## Nota sobre el KPI OE2 (importante para no repetir el diagnóstico)

`tmp_estadisticas_historicas.csv` tiene 1,839 grupos. De ellos, **44 grupos** quedan con
`consumo_std = 0` de forma **legítima** (varios clientes con consumo idéntico, ej. distritos
rurales con pocos contratos homogéneos). Esto es un dato real, no un bug — `validar_oe2()`
en [batch_layer/spark_batch.py](batch_layer/spark_batch.py) ya excluye los grupos con
`total_registros == 1` (donde el std es matemáticamente indefinido), pero **no fuerza estos
44 a pasar**, porque hacerlo ocultaría información real sobre baja variabilidad. Detalle
completo en [EXPLICACION.md](EXPLICACION.md).

Si una IA detecta `OE2 cumplido: NO` en una corrida futura, **no es necesariamente un error**
— revisar primero si los grupos en cero corresponden a varianza real antes de "arreglarlo".

## Reglas para una IA que edite código en este repo

1. Comentarios en español, sin docstrings largos.
2. No hardcodear rutas Windows — usar `os.environ.get("VAR", fallback)`.
3. `NRO_DOC_FAC` es la PK real (no `NRO_SERVICIO`, que viene anonimizado en 0).
4. Antes de "arreglar" un KPI que falla, verificar si el fallo refleja una limitación real
   de los datos (como OE2 arriba) en vez de relajar el umbral sin justificación.
5. Los archivos en `serving_layer/batch_results/` son pequeños (~5MB) y SÍ se versionan en
   git. Los CSV en `data/` (varios GB) NUNCA se versionan.
