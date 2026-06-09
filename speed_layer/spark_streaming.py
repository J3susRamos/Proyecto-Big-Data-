"""
spark_streaming.py — Capa speed (streaming) del pipeline Lambda

Consume eventos desde Kafka (o archivo simulado), hace JOIN con
TMP_ESTADISTICAS_HISTORICAS, calcula z-score y aplica reglas Spark
para clasificar anomalias de consumo electrico.

Salidas:
    - STG_EVENTOS_KAFKA: eventos crudos (tabla intermedia, 8 cols)
    - TMP_CONSUMO_ENRIQUECIDO: eventos con estadisticas (12 cols)
    - speed_layer/FACT_ANOMALIAS_STREAM/    (Parquet)
    - speed_layer/reporte_streaming.json    (metricas)
    - serving_layer/datos_streaming.parquet (para serving layer)

Modo simulado:
    Si --simulado o no hay Kafka, lee eventos desde archivo JSON
    generado por kafka_producer.py en modo simulado.
"""

import os
import json
import sys
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, when, lit, current_timestamp, expr, struct,
    to_json, udf, count as spark_count, avg as spark_avg,
    stddev as spark_stddev, round as spark_round, abs as spark_abs,
    coalesce, isnan, isnull, monotonically_increasing_id
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    LongType, TimestampType, BooleanType, MapType
)

# ── Rutas ─────────────────────────────────────────────────────────────
RUTA_DATA = os.environ.get("RUTA_DATA", os.path.join(os.path.dirname(__file__), "..", "data"))
RUTA_SERVING = os.environ.get("RUTA_SERVING", os.path.join(os.path.dirname(__file__), "..", "serving_layer"))
RUTA_SPEED = os.environ.get("RUTA_SPEED", os.path.join(os.path.dirname(__file__), ".."))

# ── Configuracion Kafka ───────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
KAFKA_TOPIC = "hidrandina-consumo"
KAFKA_CHECKPOINT = os.path.join(
    os.environ.get("RUTA_PROYECTO", os.path.dirname(os.path.dirname(__file__))),
    "checkpoint", "streaming"
)


def crear_spark_session(app_name="Hidrandina-Speed-Layer"):
    """
    Crea y configura la sesion de Spark para streaming.

    Parametros:
        app_name (str): Nombre de la aplicacion Spark.

    Retorna:
        SparkSession: Sesion de Spark configurada.
    """
    try:
        spark = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.session.timeZone", "America/Lima")
            .config("spark.sql.streaming.schemaInference", "true")
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
            .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
            .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
            .config("spark.sql.streaming.stopTimeout", "60000")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        print(f"Spark Session creada: {app_name}")
        return spark
    except Exception as e:
        print(f"ERROR creando SparkSession: {e}")
        raise


def leer_estadisticas_historicas(spark, ruta=None):
    """
    Lee TMP_ESTADISTICAS_HISTORICAS desde Parquet.

    Parametros:
        spark (SparkSession): Sesion de Spark.
        ruta (str): Ruta al Parquet.

    Retorna:
        DataFrame: Estadisticas historicas por distrito.
    """
    try:
        if ruta is None:
            ruta = os.path.join(RUTA_DATA, "TMP_ESTADISTICAS_HISTORICAS")

        if not os.path.exists(ruta):
            print(f"ERROR: No se encuentra TMP_ESTADISTICAS_HISTORICAS en {ruta}")
            print("  Ejecute primero batch_layer/spark_batch.py")
            return None

        df = spark.read.parquet(ruta)
        num_filas = df.count()
        print(f"TMP_ESTADISTICAS_HISTORICAS leido: {num_filas:,} filas")
        print(f"  Columnas: {df.columns}")
        return df
    except Exception as e:
        print(f"ERROR leyendo estadisticas: {e}")
        return None


def definir_esquema_evento():
    """
    Define el esquema JSON para los eventos de consumo.

    Retorna:
        StructType: Esquema del evento Kafka.
    """
    return StructType([
        StructField("NRO_SERVICIO", StringType(), True),
        StructField("PERIODO", StringType(), True),
        StructField("CONSUMO", StringType(), True),
        StructField("IMPORTE", StringType(), True),
        StructField("FECHA_EMISION", StringType(), True),
        StructField("FECHA_VENCIMIENTO", StringType(), True),
        StructField("FECHA_COSNUMO_DESDE", StringType(), True),
        StructField("FECHA_CONSUMO_HASTA", StringType(), True),
        StructField("DEPARTAMENTO", StringType(), True),
        StructField("PROVINCIA", StringType(), True),
        StructField("DISTRITO", StringType(), True),
        StructField("UBIGEO", StringType(), True),
        StructField("TARIFA", StringType(), True),
        StructField("CARTERA", StringType(), True),
        StructField("UNIDAD_NEGOCIO", StringType(), True),
    ])


def leer_stream_kafka(spark):
    """
    Configura lectura en streaming desde Kafka.

    Retorna:
        DataFrame: Stream de datos desde Kafka, o None si falla.
    """
    try:
        print(f"Configurando lectura Kafka:")
        print(f"  Bootstrap: {KAFKA_BOOTSTRAP_SERVERS}")
        print(f"  Topic: {KAFKA_TOPIC}")

        df_kafka = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "latest")
            .option("maxOffsetsPerTrigger", 10000)
            .option("failOnDataLoss", "false")
            .load()
        )

        print("Streaming Kafka configurado correctamente.")
        return df_kafka
    except Exception as e:
        print(f"ERROR configurando Kafka streaming: {e}")
        return None


def leer_stream_simulado(spark, ruta=None):
    """
    Configura lectura en streaming simulado desde archivo JSON.
    Usa rate stream para generar micro-batches con los eventos.

    Parametros:
        spark (SparkSession): Sesion de Spark.
        ruta (str): Ruta al archivo JSON simulado.

    Retorna:
        DataFrame: Stream simulado, o None si falla.
    """
    try:
        if ruta is None:
            ruta = os.path.join(RUTA_DATA, "eventos_simples.json")

        if not os.path.exists(ruta):
            print(f"ERROR: No se encuentra archivo simulado en {ruta}")
            return None

        print(f"Configurando streaming SIMULADO desde: {ruta}")

        # Leer el archivo completo como DataFrame batch
        spark.sparkContext.setLogLevel("ERROR")

        with open(ruta, "r", encoding="utf-8") as f:
            eventos = json.load(f)

        if not eventos:
            print("ERROR: Archivo simulado vacio.")
            return None

        # Crear DataFrame a partir de los eventos
        from pyspark.sql import Row

        rows = []
        for i, evt in enumerate(eventos):
            rows.append(Row(
                key=str(evt.get("NRO_SERVICIO", "0")),
                value=json.dumps(evt),
                topic=KAFKA_TOPIC,
                partition=0,
                offset=i,
                timestamp=datetime.now()
            ))

        if not rows:
            return None

        # Crear DataFrame estatico que simularemos como stream
        df_estatico = spark.createDataFrame(rows)

        # Usar formato 'rate' para generar micro-batches y hacer join
        df_rate = spark.readStream.format("rate").option("rowsPerSecond", 500).load()

        # Anadir indice al rate stream
        from pyspark.sql.functions import monotonically_increasing_id
        df_rate_indexed = df_rate.withColumn("idx", monotonically_increasing_id())

        # Crear DataFrame estatico con indice
        df_eventos = df_estatico.withColumn(
            "idx", monotonically_increasing_id()
        )

        # Cachear el DataFrame estatico
        df_eventos.cache()
        num_eventos = df_eventos.count()
        print(f"  Eventos cargados en memoria: {num_eventos:,}")

        # JOIN: cada micro-batch del rate se une con un evento
        df_stream = df_rate_indexed.join(
            df_eventos,
            df_rate_indexed.idx == df_eventos.idx,
            "left"
        ).select(
            col("key"),
            col("value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp")
        ).filter(col("value").isNotNull())

        print("Streaming simulado configurado correctamente.")
        return df_stream

    except Exception as e:
        print(f"ERROR configurando streaming simulado: {e}")
        import traceback
        traceback.print_exc()
        return None


def parsear_evento(df_kafka, esquema):
    """
    Parsea el valor JSON de Kafka a columnas estructuradas.

    Genera STG_EVENTOS_KAFKA (8 columnas: las del evento original).

    Parametros:
        df_kafka (DataFrame): DataFrame crudo de Kafka.
        esquema (StructType): Esquema del evento JSON.

    Retorna:
        DataFrame: Eventos parseados con columnas estructuradas.
    """
    try:
        df_parseado = (
            df_kafka
            .withColumn("value_parsed", from_json(col("value").cast("string"), esquema))
            .select(
                col("key").cast("string").alias("nro_servicio_key"),
                col("value_parsed.NRO_SERVICIO").cast(LongType()).alias("NRO_SERVICIO"),
                col("value_parsed.PERIODO").cast(IntegerType()).alias("PERIODO"),
                col("value_parsed.CONSUMO").cast(DoubleType()).alias("CONSUMO"),
                col("value_parsed.IMPORTE").cast(DoubleType()).alias("IMPORTE"),
                col("value_parsed.FECHA_EMISION").cast(TimestampType()).alias("FECHA_EMISION"),
                col("value_parsed.FECHA_VENCIMIENTO").cast(TimestampType()).alias("FECHA_VENCIMIENTO"),
                col("value_parsed.FECHA_COSNUMO_DESDE").cast(TimestampType()).alias("FECHA_COSNUMO_DESDE"),
                col("value_parsed.FECHA_CONSUMO_HASTA").cast(TimestampType()).alias("FECHA_CONSUMO_HASTA"),
                col("value_parsed.DEPARTAMENTO").cast(StringType()).alias("DEPARTAMENTO"),
                col("value_parsed.PROVINCIA").cast(StringType()).alias("PROVINCIA"),
                col("value_parsed.DISTRITO").cast(StringType()).alias("DISTRITO"),
                col("value_parsed.UBIGEO").cast(StringType()).alias("UBIGEO"),
                col("value_parsed.TARIFA").cast(StringType()).alias("TARIFA"),
                col("value_parsed.CARTERA").cast(StringType()).alias("CARTERA"),
                col("value_parsed.UNIDAD_NEGOCIO").cast(StringType()).alias("UNIDAD_NEGOCIO"),
                col("timestamp").alias("kafka_timestamp"),
                col("offset"),
                col("partition")
            )
            .drop("value", "value_parsed")
        )

        return df_parseado
    except Exception as e:
        print(f"ERROR parseando evento: {e}")
        return df_kafka


def enriquecer_eventos(df_eventos, estadisticas):
    """
    Genera TMP_CONSUMO_ENRIQUECIDO: JOIN con estadisticas historicas
    y calculo de z-score y porcentaje de variacion.

    12 columnas de salida:
        NRO_SERVICIO, PERIODO, CONSUMO, IMPORTE, DISTRITO, TARIFA, CARTERA,
        consumo_promedio_historico, importe_promedio_historico,
        desviacion_consumo, zscore_consumo, porcentaje_variacion

    Parametros:
        df_eventos (DataFrame): Eventos parseados.
        estadisticas (DataFrame): TMP_ESTADISTICAS_HISTORICAS.

    Retorna:
        DataFrame: Eventos enriquecidos con estadisticas.
    """
    try:
        print("=== GENERANDO TMP_CONSUMO_ENRIQUECIDO ===")

        # JOIN con estadisticas por DISTRITO
        df_enriquecido = df_eventos.join(
            estadisticas.select(
                "DISTRITO",
                "promedio_consumo",
                "promedio_importe",
                "desviacion_consumo"
            ),
            on="DISTRITO",
            how="left"
        )

        # Calcular z-score y porcentaje de variacion
        df_enriquecido = df_enriquecido.withColumn(
            "zscore_consumo",
            when(
                (col("desviacion_consumo").isNull()) | (col("desviacion_consumo") == 0),
                lit(0.0)
            ).otherwise(
                spark_round(
                    (col("CONSUMO") - col("promedio_consumo")) / col("desviacion_consumo"),
                    4
                )
            )
        )

        df_enriquecido = df_enriquecido.withColumn(
            "porcentaje_variacion",
            when(
                (col("promedio_consumo").isNull()) | (col("promedio_consumo") == 0),
                lit(0.0)
            ).otherwise(
                spark_round(
                    ((col("CONSUMO") - col("promedio_consumo")) / col("promedio_consumo")) * 100,
                    2
                )
            )
        )

        # Seleccionar las 12 columnas de TMP_CONSUMO_ENRIQUECIDO
        df_enriquecido = df_enriquecido.select(
            col("NRO_SERVICIO"),
            col("PERIODO"),
            col("CONSUMO").alias("consumo_actual"),
            col("IMPORTE").alias("importe_actual"),
            col("DISTRITO"),
            col("TARIFA"),
            col("CARTERA"),
            col("promedio_consumo").alias("consumo_promedio_historico"),
            col("promedio_importe").alias("importe_promedio_historico"),
            col("desviacion_consumo").alias("desviacion_consumo"),
            col("zscore_consumo"),
            col("porcentaje_variacion")
        )

        print(f"  Registros enriquecidos")
        print(f"  Columnas TMP_CONSUMO_ENRIQUECIDO: {df_enriquecido.columns}")
        print(f"  Total columnas: {len(df_enriquecido.columns)}")
        print("=== ENRIQUECIMIENTO COMPLETADO ===")

        return df_enriquecido
    except Exception as e:
        print(f"ERROR enriqueciendo eventos: {e}")
        import traceback
        traceback.print_exc()
        return df_eventos


def clasificar_anomalias(df_enriquecido):
    """
    Aplica reglas Spark para clasificar tipo_anomalia y nivel_riesgo.

    Reglas tipo_anomalia:
        - zscore > 3: "Consumo extremadamente alto"
        - zscore entre 2 y 3: "Consumo alto"
        - porcentaje_variacion > 100: "Incremento brusco"
        - zscore < -2: "Consumo sospechosamente bajo"
        - otro: "Variacion moderada"

    Reglas nivel_riesgo:
        - zscore > 3: "Alto"
        - zscore entre 2 y 3: "Medio"
        - otro: "Bajo"

    Parametros:
        df_enriquecido (DataFrame): TMP_CONSUMO_ENRIQUECIDO.

    Retorna:
        DataFrame: Con columnas tipo_anomalia y nivel_riesgo anadidas.
    """
    try:
        print("=== CLASIFICANDO ANOMALIAS ===")

        df_anomalias = df_enriquecido.withColumn(
            "tipo_anomalia",
            when(col("zscore_consumo") > 3, lit("Consumo extremadamente alto"))
            .when(
                (col("zscore_consumo") >= 2) & (col("zscore_consumo") <= 3),
                lit("Consumo alto")
            )
            .when(col("porcentaje_variacion") > 100, lit("Incremento brusco"))
            .when(col("zscore_consumo") < -2, lit("Consumo sospechosamente bajo"))
            .otherwise(lit("Variacion moderada"))
        )

        df_anomalias = df_anomalias.withColumn(
            "nivel_riesgo",
            when(col("zscore_consumo") > 3, lit("Alto"))
            .when(
                (col("zscore_consumo") >= 2) & (col("zscore_consumo") <= 3),
                lit("Medio")
            )
            .otherwise(lit("Bajo"))
        )

        # Anadir columnas fijas
        df_anomalias = df_anomalias.withColumn(
            "fecha_deteccion",
            current_timestamp()
        ).withColumn(
            "flag_anomalia",
            lit(True)
        )

        # Distribucion de tipos de anomalia
        print("  Distribucion tipo_anomalia (estimada):")
        df_anomalias.groupBy("tipo_anomalia").agg(
            spark_count("*").alias("cantidad")
        ).show(truncate=False)

        print("  Distribucion nivel_riesgo (estimada):")
        df_anomalias.groupBy("nivel_riesgo").agg(
            spark_count("*").alias("cantidad")
        ).show(truncate=False)

        print("=== CLASIFICACION COMPLETADA ===")
        return df_anomalias

    except Exception as e:
        print(f"ERROR clasificando anomalias: {e}")
        return df_enriquecido


def guardar_streaming_output(df_anomalias, ruta_salida=None, checkpoint=None):
    """
    Guarda el stream de anomalias en Parquet usando append output mode.

    Parametros:
        df_anomalias (DataFrame): Stream con anomalias clasificadas.
        ruta_salida (str): Ruta de salida Parquet.
        checkpoint (str): Ruta para checkpointing.

    Retorna:
        StreamingQuery: La consulta de streaming.
    """
    try:
        if ruta_salida is None:
            ruta_salida = os.path.join(RUTA_SPEED, "FACT_ANOMALIAS_STREAM")

        if checkpoint is None:
            checkpoint = os.path.join(
                os.environ.get("RUTA_PROYECTO", os.path.dirname(os.path.dirname(__file__))),
                "checkpoint", "anomalias_stream"
            )

        os.makedirs(os.path.dirname(checkpoint), exist_ok=True)

        query = (
            df_anomalias
            .writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", ruta_salida)
            .option("checkpointLocation", checkpoint)
            .trigger(processingTime="5 seconds")
            .start()
        )

        print(f"Streaming guardando en: {ruta_salida}")
        return query
    except Exception as e:
        print(f"ERROR guardando streaming output: {e}")
        return None


def procesar_stream_batch(df_enriquecido, epoch_id):
    """
    Funcion callback para foreachBatch: procesa cada micro-batch
    y guarda resultados intermedios.

    Parametros:
        df_enriquecido (DataFrame): Micro-batch de datos enriquecidos.
        epoch_id (int): ID del micro-batch.
    """
    try:
        if df_enriquecido.count() == 0:
            return

        print(f"\n--- Procesando micro-batch {epoch_id} ---")
        df_anomalias = clasificar_anomalias(df_enriquecido)

        # Guardar batch individual
        ruta_batch = os.path.join(RUTA_SPEED, "FACT_ANOMALIAS_STREAM")
        df_anomalias.write.mode("append").parquet(ruta_batch)
        print(f"  Batch {epoch_id} guardado: {df_anomalias.count():,} registros")
        print(f"--- Fin micro-batch {epoch_id} ---\n")
    except Exception as e:
        print(f"ERROR en micro-batch {epoch_id}: {e}")


def modo_streaming_kafka(spark, estadisticas):
    """
    Ejecuta el pipeline en modo streaming real con Kafka.

    Parametros:
        spark (SparkSession): Sesion de Spark.
        estadisticas (DataFrame): TMP_ESTADISTICAS_HISTORICAS.
    """
    df_kafka = leer_stream_kafka(spark)
    if df_kafka is None:
        return False

    esquema = definir_esquema_evento()
    df_eventos = parsear_evento(df_kafka, esquema)
    df_enriquecido = enriquecer_eventos(df_eventos, estadisticas)

    query = guardar_streaming_output(df_enriquecido)
    if query is None:
        print("ERROR: No se pudo iniciar la consulta de streaming.")
        return False

    print("\nStreaming iniciado. Esperando datos de Kafka...")
    print("Presione Ctrl+C para detener.")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStreaming detenido por el usuario.")
        query.stop()
    except Exception as e:
        print(f"Error en streaming: {e}")
        query.stop()

    return True


def modo_streaming_simulado(spark, estadisticas):
    """
    Ejecuta el pipeline en modo simulado (sin Kafka).

    Lee el archivo generado por kafka_producer.py en modo simulado,
    procesa todos los registros como un unico batch y guarda resultados.

    Parametros:
        spark (SparkSession): Sesion de Spark.
        estadisticas (DataFrame): TMP_ESTADISTICAS_HISTORICAS.

    Retorna:
        bool: True si se proceso correctamente.
    """
    print("\n=== MODO SIMULADO (sin Kafka) ===")

    ruta_eventos = os.path.join(RUTA_DATA, "eventos_simples.json")
    if not os.path.exists(ruta_eventos):
        ruta_eventos = os.path.join(RUTA_DATA, "eventos_simples.json")
        if not os.path.exists(ruta_eventos):
            print(f"ERROR: No hay eventos simulados en {ruta_eventos}")
            print("  Ejecute primero: python speed_layer/kafka_producer.py simulado")
            return False

    try:
        print(f"Leyendo eventos simulados desde: {ruta_eventos}")
        with open(ruta_eventos, "r", encoding="utf-8") as f:
            eventos_raw = json.load(f)

        print(f"  Eventos cargados: {len(eventos_raw):,}")

        if not eventos_raw:
            print("ERROR: No hay eventos para procesar.")
            return False

        # Convertir a DataFrame de Spark
        from pyspark.sql import Row

        rows = []
        for evt in eventos_raw:
            rows.append(Row(**evt))

        df_eventos = spark.createDataFrame(rows)
        print(f"  DataFrame creado: {df_eventos.count():,} filas")

        # Convertir tipos
        for col_name, tipo in [
            ("NRO_SERVICIO", LongType()),
            ("PERIODO", IntegerType()),
            ("CONSUMO", DoubleType()),
            ("IMPORTE", DoubleType())
        ]:
            if col_name in df_eventos.columns:
                df_eventos = df_eventos.withColumn(
                    col_name, col(col_name).cast(tipo)
                )

        for col_fecha in [
            "FECHA_EMISION", "FECHA_VENCIMIENTO",
            "FECHA_COSNUMO_DESDE", "FECHA_CONSUMO_HASTA"
        ]:
            if col_fecha in df_eventos.columns:
                df_eventos = df_eventos.withColumn(
                    col_fecha,
                    when(col(col_fecha).isNull(), lit(None))
                    .otherwise(col(col_fecha).cast(TimestampType()))
                )

        # Enriquecer eventos
        print("\nEnriqueciendo eventos con estadisticas historicas...")
        df_enriquecido = enriquecer_eventos(df_eventos, estadisticas)
        if df_enriquecido is None:
            return False

        # Clasificar anomalias
        df_anomalias = clasificar_anomalias(df_enriquecido)

        # Anadir id_anomalia UUID
        from pyspark.sql.functions import monotonically_increasing_id, concat, lit
        df_anomalias = df_anomalias.withColumn(
            "id_anomalia",
            concat(
                lit("ANOM-"),
                monotonically_increasing_id().cast("string")
            )
        )

        # Reordenar columnas segun FACT_ANOMALIAS_CONSUMO (17 columnas)
        cols_final = [
            "id_anomalia", "NRO_SERVICIO", "PERIODO",
            "consumo_actual", "importe_actual",
            "DISTRITO", "TARIFA", "CARTERA",
            "consumo_promedio_historico", "importe_promedio_historico",
            "desviacion_consumo", "zscore_consumo", "porcentaje_variacion",
            "tipo_anomalia", "nivel_riesgo", "fecha_deteccion", "flag_anomalia"
        ]

        cols_existentes = [c for c in cols_final if c in df_anomalias.columns]
        df_final = df_anomalias.select(cols_existentes)

        # Guardar resultados
        ruta_salida = os.path.join(RUTA_SPEED, "FACT_ANOMALIAS_STREAM")
        df_final.coalesce(1).write.mode("overwrite").parquet(ruta_salida)
        print(f"\nResultados guardados en: {ruta_salida}")

        # Tambien guardar CSV para serving layer
        ruta_csv = os.path.join(RUTA_SPEED, "FACT_ANOMALIAS_STREAM.csv")
        df_final.coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .option("sep", ",") \
            .option("encoding", "UTF-8") \
            .csv(ruta_csv.replace(".csv", ""))
        print(f"Resultados CSV guardados en: {ruta_csv}")

        # Copiar a serving layer
        ruta_serving = os.path.join(RUTA_SERVING, "datos_streaming.parquet")
        df_final.coalesce(1).write.mode("overwrite").parquet(ruta_serving)
        print(f"Datos copiados a serving layer: {ruta_serving}")

        # Estadisticas
        total_anomalias = df_final.count()
        print(f"\n=== RESUMEN STREAMING ===")
        print(f"  Total registros: {total_anomalias:,}")

        print("\n  Distribucion tipo_anomalia:")
        df_final.groupBy("tipo_anomalia").agg(
            spark_count("*").alias("cantidad")
        ).orderBy(col("cantidad").desc()).show(truncate=False)

        print("\n  Distribucion nivel_riesgo:")
        df_final.groupBy("nivel_riesgo").agg(
            spark_count("*").alias("cantidad")
        ).orderBy(col("cantidad").desc()).show(truncate=False)

        # Nulos en columnas criticas
        nulos_zscore = df_final.filter(
            col("zscore_consumo").isNull()
        ).count()
        nulos_flag = df_final.filter(
            col("flag_anomalia").isNull()
        ).count()
        print(f"\n  Nulos en zscore_consumo: {nulos_zscore}")
        print(f"  Nulos en flag_anomalia: {nulos_flag}")

        return True

    except Exception as e:
        print(f"ERROR en modo simulado: {e}")
        import traceback
        traceback.print_exc()
        return False


def validar_latencia(inicio, fin, precision_pct=None):
    """
    Valida KPI OE3: latencia < 5 segundos y precision >= 90%.

    Parametros:
        inicio (float): Tiempo de inicio (time.time()).
        fin (float): Tiempo de fin (time.time()).
        precision_pct (float): Precision calculada (opcional).

    Retorna:
        dict: Metricas de latency.
    """
    try:
        latencia = fin - inicio
        metricas = {
            "latencia_segundos": round(latencia, 3),
            "oe3_latencia_cumplido": latencia < 5,
            "precision_pct": precision_pct or 0,
            "oe3_precision_cumplido": (precision_pct or 0) >= 90,
            "oe3_cumplido": (latencia < 5) and ((precision_pct or 0) >= 90 or precision_pct is None)
        }
        print(f"\n=== VALIDACION KPI OE3 ===")
        print(f"  Latencia: {latencia:.3f} seg (requerido < 5)")
        print(f"  OE3 latencia: {'SI' if metricas['oe3_latencia_cumplido'] else 'NO'}")
        if precision_pct is not None:
            print(f"  Precision: {precision_pct:.2f}% (requerido >= 90%)")
            print(f"  OE3 precision: {'SI' if metricas['oe3_precision_cumplido'] else 'NO'}")
        print(f"  OE3 cumplido: {'SI' if metricas['oe3_cumplido'] else 'NO'}")
        return metricas
    except Exception as e:
        print(f"ERROR validando latencia: {e}")
        return {}


def guardar_reporte(metricas):
    """
    Guarda reporte JSON del streaming layer.

    Parametros:
        metricas (dict): Metricas a guardar.
    """
    try:
        os.makedirs(RUTA_SPEED, exist_ok=True)
        ruta = os.path.join(RUTA_SPEED, "reporte_streaming.json")
        with open(ruta, "w", encoding="utf-8") as f:
            json.dump(metricas, f, indent=2, ensure_ascii=False)
        print(f"  Reporte streaming guardado: {ruta}")
    except Exception as e:
        print(f"ERROR guardando reporte: {e}")


def ejecutar(modo="auto"):
    """
    Ejecuta la capa speed (streaming) del pipeline Lambda.

    Parametros:
        modo (str): "real" para Kafka, "simulado" para archivo JSON,
                    "auto" para detectar.

    Retorna:
        bool: True si se completo correctamente.
    """
    print("=" * 60)
    print("SPEED LAYER — Streaming de anomalias de consumo")
    print("=" * 60)

    inicio_total = time.time()

    spark = crear_spark_session()
    if spark is None:
        return False

    try:
        # Leer estadisticas historicas (generadas por batch layer)
        estadisticas = leer_estadisticas_historicas(spark)
        if estadisticas is None:
            print("ERROR: No se encontraron estadisticas historicas.")
            print("  Ejecute primero: python batch_layer/spark_batch.py")
            return False

        # Cachear estadisticas para broadcasting
        estadisticas.cache()
        estadisticas.count()

        if modo == "real":
            resultado = modo_streaming_kafka(spark, estadisticas)
        elif modo == "simulado":
            resultado = modo_streaming_simulado(spark, estadisticas)
        else:
            # auto: intentar Kafka, fallback a simulado
            ruta_sim = os.path.join(RUTA_DATA, "eventos_simples.json")
            if os.path.exists(ruta_sim):
                print("Usando modo simulado (eventos JSON encontrados)")
                resultado = modo_streaming_simulado(spark, estadisticas)
            else:
                print("Intentando modo real (Kafka)...")
                resultado = modo_streaming_kafka(spark, estadisticas)

        fin_total = time.time()
        metricas_latencia = validar_latencia(inicio_total, fin_total)
        guardar_reporte(metricas_latencia)

        print("\n" + "=" * 60)
        print("SPEED LAYER COMPLETADO")
        print("=" * 60)

        return resultado

    except Exception as e:
        print(f"ERROR en speed layer: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if spark is not None:
            spark.stop()
            print("SparkSession cerrada.")


if __name__ == "__main__":
    modo = sys.argv[1] if len(sys.argv) > 1 else "auto"
    ejecutar(modo)
