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
    coalesce, isnan, isnull, monotonically_increasing_id, concat,
    hour, sum as spark_sum
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


def create_spark_session(app_name="Hidrandina-Speed-Layer"):
    """
    Crea y configura la sesion de Spark para streaming.

    Parametros:
        app_name (str): Nombre de la aplicacion Spark.

    Retorna:
        SparkSession: Sesion de Spark configurada.
    """
    try:
        # Configurar HADOOP_HOME para Windows (winutils.exe necesario para Parquet)
        hadoop_home = "C:\\hadoop"
        os.environ.setdefault("HADOOP_HOME", hadoop_home)
        os.environ.setdefault("hadoop.home.dir", hadoop_home)
        # PYSPARK_PYTHON explicito para evitar el stub de Microsoft Store
        python_path = "C:\\Users\\Roxwell\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
        os.environ.setdefault("PYSPARK_PYTHON", python_path)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_path)
        hadoop_bin = f"{hadoop_home}\\bin"
        if hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = f"{hadoop_bin};{os.environ.get('PATH', '')}"

        spark = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.hadoop.hadoop.home.dir", hadoop_home)
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


def read_historical_statistics(spark, path=None):
    """
    Lee TMP_ESTADISTICAS_HISTORICAS desde Parquet.

    Parametros:
        spark (SparkSession): Sesion de Spark.
        path (str): Ruta al Parquet.

    Retorna:
        DataFrame: Estadisticas historicas por distrito.
    """
    try:
        if path is None:
            path = os.path.join(RUTA_SERVING, "batch_results", "tmp_estadisticas_historicas")

        if not os.path.exists(path):
            # Fallback: buscar en RUTA_DATA directamente
            path = os.path.join(RUTA_DATA, "TMP_ESTADISTICAS_HISTORICAS")

        if not os.path.exists(path):
            print(f"ERROR: No se encuentra TMP_ESTADISTICAS_HISTORICAS")
            print("  Buscado en: batch_results/tmp_estadisticas_historicas")
            print("  Buscado en: data/TMP_ESTADISTICAS_HISTORICAS")
            print("  Ejecute primero batch_layer/spark_batch.py")
            return None

        df = spark.read.parquet(path)
        num_rows = df.count()
        print(f"TMP_ESTADISTICAS_HISTORICAS leido: {num_rows:,} filas")
        print(f"  Columnas: {df.columns}")
        return df
    except Exception as e:
        print(f"ERROR leyendo estadisticas: {e}")
        return None


def define_event_schema():
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


def read_kafka_stream(spark):
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
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 50000)
            .option("failOnDataLoss", "false")
            .load()
        )

        print("Streaming Kafka configurado correctamente.")
        return df_kafka
    except Exception as e:
        print(f"ERROR configurando Kafka streaming: {e}")
        return None


def read_simulated_stream(spark, path=None):
    """
    Configura lectura en streaming simulado desde archivo JSON.
    Usa rate stream para generar micro-batches con los eventos.

    Parametros:
        spark (SparkSession): Sesion de Spark.
        path (str): Ruta al archivo JSON simulado.

    Retorna:
        DataFrame: Stream simulado, o None si falla.
    """
    try:
        if path is None:
            path = os.path.join(RUTA_DATA, "eventos_simples.json")

        if not os.path.exists(path):
            print(f"ERROR: No se encuentra archivo simulado en {path}")
            return None

        print(f"Configurando streaming SIMULADO desde: {path}")

        # Leer el archivo completo como DataFrame batch
        spark.sparkContext.setLogLevel("ERROR")

        with open(path, "r", encoding="utf-8") as f:
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
        events_df = df_estatico.withColumn(
            "idx", monotonically_increasing_id()
        )

        # Cachear el DataFrame estatico
        events_df.cache()
        num_eventos = events_df.count()
        print(f"  Eventos cargados en memoria: {num_eventos:,}")

        # JOIN: cada micro-batch del rate se une con un evento
        df_stream = df_rate_indexed.join(
            events_df,
            df_rate_indexed.idx == events_df.idx,
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


def parse_event(df_kafka, schema):
    """
    Parsea el valor JSON de Kafka a columnas estructuradas.

    Genera STG_EVENTOS_KAFKA (8 columnas: las del evento original).

    Parametros:
        df_kafka (DataFrame): DataFrame crudo de Kafka.
        schema (StructType): Esquema del evento JSON.

    Retorna:
        DataFrame: Eventos parseados con columnas estructuradas.
    """
    try:
        df_parseado = (
            df_kafka
            .withColumn("value_parsed", from_json(col("value").cast("string"), schema))
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


def enrich_events(df_events, statistics_df):
    """
    Genera TMP_CONSUMO_ENRIQUECIDO: JOIN con estadisticas historicas
    y calculo de z-score y porcentaje de variacion.

    12 columnas de salida:
        NRO_SERVICIO, PERIODO, CONSUMO, IMPORTE, DISTRITO, TARIFA, CARTERA,
        consumo_promedio_historico, importe_promedio_historico,
        desviacion_consumo, zscore_consumo, porcentaje_variacion

    Parametros:
        df_events (DataFrame): Eventos parseados.
        statistics_df (DataFrame): TMP_ESTADISTICAS_HISTORICAS.

    Retorna:
        DataFrame: Eventos enriquecidos con estadisticas.
    """
    try:
        print("=== GENERANDO TMP_CONSUMO_ENRIQUECIDO ===")

        # JOIN con estadisticas por DISTRITO, TARIFA y CARTERA (FIX 4 APLICADO)
        enriched_df = df_events.join(
            statistics_df.select(
                "DISTRITO",
                "TARIFA",
                "CARTERA",
                "consumo_promedio",
                "importe_promedio",
                "consumo_std"
            ),
            on=["DISTRITO", "TARIFA", "CARTERA"],
            how="left"
        )

        # Calcular z-score y porcentaje de variacion
        enriched_df = enriched_df.withColumn(
            "zscore_consumo",
            when(
                (col("consumo_std").isNull()) | (col("consumo_std") == 0),
                lit(0.0)
            ).otherwise(
                spark_round(
                    (col("CONSUMO") - col("consumo_promedio")) / col("consumo_std"),
                    4
                )
            )
        )

        enriched_df = enriched_df.withColumn(
            "porcentaje_variacion",
            when(
                (col("consumo_promedio").isNull()) | (col("consumo_promedio") == 0),
                lit(0.0)
            ).otherwise(
                spark_round(
                    ((col("CONSUMO") - col("consumo_promedio")) / col("consumo_promedio")) * 100,
                    2
                )
            )
        )

        # Seleccionar las 12 columnas de TMP_CONSUMO_ENRIQUECIDO
        enriched_df = enriched_df.select(
            col("NRO_SERVICIO"),
            col("PERIODO"),
            col("CONSUMO").alias("consumo_actual"),
            col("IMPORTE").alias("importe_actual"),
            col("DISTRITO"),
            col("TARIFA"),
            col("CARTERA"),
            col("consumo_promedio").alias("consumo_promedio_historico"),
            col("importe_promedio").alias("importe_promedio_historico"),
            col("consumo_std").alias("desviacion_consumo"),
            col("zscore_consumo"),
            col("porcentaje_variacion")
        )

        print(f"  Registros enriquecidos")
        print(f"  Columnas TMP_CONSUMO_ENRIQUECIDO: {enriched_df.columns}")
        print(f"  Total columnas: {len(enriched_df.columns)}")
        print("=== ENRIQUECIMIENTO COMPLETADO ===")

        return enriched_df
    except Exception as e:
        print(f"ERROR enriqueciendo eventos: {e}")
        import traceback
        traceback.print_exc()
        return events_df


def classify_anomalies(enriched_df):
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
        enriched_df (DataFrame): TMP_CONSUMO_ENRIQUECIDO.

    Retorna:
        DataFrame: Con columnas tipo_anomalia y nivel_riesgo anadidas.
    """
    try:
        print("=== CLASIFICANDO ANOMALIAS ===")

        anomalies_df = enriched_df.withColumn(
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

        anomalies_df = anomalies_df.withColumn(
            "nivel_riesgo",
            when(col("zscore_consumo") > 3, lit("Alto"))
            .when(
                (col("zscore_consumo") >= 2) & (col("zscore_consumo") <= 3),
                lit("Medio")
            )
            .otherwise(lit("Bajo"))
        )

        # Anadir columnas fijas
        anomalies_df = anomalies_df.withColumn(
            "fecha_deteccion",
            current_timestamp()
        ).withColumn(
            "flag_anomalia",
            lit(True)
        )

        # ── Regla adicional independiente del z-score ────────────────
        # Si CONSUMO > 500, riesgo es Crítico siempre
        anomalies_df = anomalies_df.withColumn(
            "nivel_riesgo",
            when(col("consumo_actual") > 500, lit("Crítico"))
            .otherwise(col("nivel_riesgo"))
        )
        # Si CONSUMO > 500 y no había anomalía por z-score, alerta específica
        anomalies_df = anomalies_df.withColumn(
            "tipo_anomalia",
            when(
                (col("consumo_actual") > 500) & (col("tipo_anomalia") == "Variacion moderada"),
                lit("Alerta consumo crítico > 500 kWh")
            ).otherwise(col("tipo_anomalia"))
        )

        # Distribucion de tipos de anomalia
        print("  Distribucion tipo_anomalia (estimada):")
        anomalies_df.groupBy("tipo_anomalia").agg(
            spark_count("*").alias("cantidad")
        ).show(truncate=False)

        print("  Distribucion nivel_riesgo (estimada):")
        anomalies_df.groupBy("nivel_riesgo").agg(
            spark_count("*").alias("cantidad")
        ).show(truncate=False)

        print("=== CLASIFICACION COMPLETADA ===")
        return anomalies_df

    except Exception as e:
        print(f"ERROR clasificando anomalias: {e}")
        return enriched_df


def save_streaming_output(anomalies_df, output_path=None, checkpoint_path=None):
    """
    Guarda el stream de anomalias en Parquet usando append output mode.

    Parametros:
        anomalies_df (DataFrame): Stream con anomalias clasificadas.
        output_path (str): Ruta de salida Parquet.
        checkpoint_path (str): Ruta para checkpointing.

    Retorna:
        StreamingQuery: La consulta de streaming.
    """
    try:
        if output_path is None:
            output_path = os.path.join(RUTA_SPEED, "FACT_ANOMALIAS_STREAM")

        if checkpoint_path is None:
            checkpoint_path = os.path.join(
                os.environ.get("RUTA_PROYECTO", os.path.dirname(os.path.dirname(__file__))),
                "checkpoint", "anomalias_stream"
            )

        os.makedirs(os.path.dirname(checkpoint_path), exist_ok=True)

        query = (
            anomalies_df
            .writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", output_path)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime="5 seconds")
            .start()
        )

        print(f"Streaming guardando en: {output_path}")
        return query
    except Exception as e:
        print(f"ERROR guardando streaming output: {e}")
        return None


def calculate_region_hourly_accumulation(enriched_df):
    """
    Calcula acumulacion regional por hora a partir de eventos enriquecidos.

    Agrupa por DISTRITO + hora de fecha_deteccion:
        - total_kwh: suma de consumo_actual
        - total_records: count de registros
        - avg_consumption: promedio de consumo_actual
        - anomaly_count: count donde flag_anomalia = True

    Guarda JSON en data/region_hourly_accumulation.json.

    Parametros:
        enriched_df (DataFrame): TMP_CONSUMO_ENRIQUECIDO.

    Retorna:
        DataFrame: region_hourly_df ordenado por DISTRITO, hour.
    """
    try:
        print("=== CALCULANDO ACUMULACION REGIONAL POR HORA ===")

        region_hourly_df = (
            enriched_df
            .withColumn("hour", hour(col("fecha_deteccion")))
            .groupBy("DISTRITO", "hour")
            .agg(
                spark_sum("consumo_actual").alias("total_kwh"),
                spark_count("*").alias("total_records"),
                spark_avg("consumo_actual").alias("avg_consumption"),
                spark_sum(
                    when(col("flag_anomalia") == True, lit(1)).otherwise(lit(0))
                ).alias("anomaly_count"),
            )
            .select(
                "DISTRITO",
                "hour",
                spark_round("total_kwh", 2).alias("total_kwh"),
                "total_records",
                spark_round("avg_consumption", 2).alias("avg_consumption"),
                "anomaly_count",
            )
            .orderBy("DISTRITO", "hour")
        )

        rows = region_hourly_df.count()
        print(f"  Total grupos DISTRITO×hora: {rows:,}")

        # Guardar como JSON
        output_path = os.path.join(RUTA_DATA, "region_hourly_accumulation.json")
        local_rows = region_hourly_df.collect()
        records = [
            {
                "distrito": r["DISTRITO"],
                "hour": r["hour"],
                "total_kwh": r["total_kwh"],
                "total_records": r["total_records"],
                "avg_consumption": r["avg_consumption"],
                "anomaly_count": r["anomaly_count"],
            }
            for r in local_rows
        ]
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, ensure_ascii=False)
        print(f"  Acumulacion regional por hora guardada en: {output_path}")

        print("=== ACUMULACION REGIONAL COMPLETADA ===")
        return region_hourly_df

    except Exception as e:
        print(f"ERROR calculando acumulacion regional: {e}")
        import traceback
        traceback.print_exc()
        return enriched_df


def process_stream_batch(enriched_df, epoch_id):
    """
    Funcion callback para foreachBatch: procesa cada micro-batch
    y guarda resultados intermedios.

    Parametros:
        enriched_df (DataFrame): Micro-batch de datos enriquecidos.
        epoch_id (int): ID del micro-batch.
    """
    try:
        if enriched_df.count() == 0:
            return

        print(f"\n--- Procesando micro-batch {epoch_id} ---")
        anomalies_df = classify_anomalies(enriched_df)

        # Guardar batch individual
        output_path = os.path.join(RUTA_SPEED, "FACT_ANOMALIAS_STREAM")
        anomalies_df.write.mode("append").parquet(output_path)
        print(f"  Batch {epoch_id} guardado: {anomalies_df.count():,} registros")
        print(f"--- Fin micro-batch {epoch_id} ---\n")
    except Exception as e:
        print(f"ERROR en micro-batch {epoch_id}: {e}")


def kafka_streaming_mode(spark, statistics_df):
    """
    Ejecuta el pipeline en modo streaming real con Kafka.

    Parametros:
        spark (SparkSession): Sesion de Spark.
        statistics_df (DataFrame): TMP_ESTADISTICAS_HISTORICAS.
    """
    df_kafka = read_kafka_stream(spark)
    if df_kafka is None:
        return False

    schema = define_event_schema()
    df_events = parse_event(df_kafka, schema)
    enriched_df = enrich_events(df_events, statistics_df)

    query = save_streaming_output(enriched_df)
    if query is None:
        print("ERROR: No se pudo iniciar la consulta de streaming.")
        return False

        print("\nStreaming iniciado. Procesando datos de Kafka...")
        print("  Timeout: 300s (5 minutos)")
        print("  Presione Ctrl+C para detener antes.")

        try:
            query.awaitTermination(timeout=300)
    except KeyboardInterrupt:
        print("\nStreaming detenido por el usuario.")
        query.stop()
    except Exception as e:
        print(f"Error en streaming: {e}")
        query.stop()

    return True


def simulated_streaming_mode(spark, statistics_df):
    """
    Ejecuta el pipeline en modo simulado (sin Kafka).

    Lee el archivo generado por kafka_producer.py en modo simulado,
    procesa todos los registros como un unico batch y guarda resultados.

    Parametros:
        spark (SparkSession): Sesion de Spark.
        statistics_df (DataFrame): TMP_ESTADISTICAS_HISTORICAS.

    Retorna:
        bool: True si se proceso correctamente.
    """
    print("\n=== MODO SIMULADO (sin Kafka) ===")

    events_path = os.path.join(RUTA_DATA, "eventos_simples.json")
    if not os.path.exists(events_path):
        events_path = os.path.join(RUTA_DATA, "eventos_simples.json")
        if not os.path.exists(events_path):
            print(f"ERROR: No hay eventos simulados en {events_path}")
            print("  Ejecute primero: python speed_layer/kafka_producer.py simulado")
            return False

    try:
        print(f"Leyendo eventos simulados desde: {events_path}")
        df_events = spark.read.json(events_path)
        print(f"  Eventos cargados: {df_events.count():,}")

        if df_events.count() == 0:
            print("ERROR: No hay eventos para procesar.")
            return False

        # Convertir tipos
        for col_name, tipo in [
            ("NRO_SERVICIO", LongType()),
            ("PERIODO", IntegerType()),
            ("CONSUMO", DoubleType()),
            ("IMPORTE", DoubleType())
        ]:
            if col_name in df_events.columns:
                df_events = df_events.withColumn(
                    col_name, col(col_name).cast(tipo)
                )

        for col_fecha in [
            "FECHA_EMISION", "FECHA_VENCIMIENTO",
            "FECHA_COSNUMO_DESDE", "FECHA_CONSUMO_HASTA"
        ]:
            if col_fecha in df_events.columns:
                df_events = df_events.withColumn(
                    col_fecha,
                    when(col(col_fecha).isNull(), lit(None))
                    .otherwise(col(col_fecha).cast(TimestampType()))
                )

        # Enriquecer eventos
        print("\nEnriqueciendo eventos con estadisticas historicas...")
        enriched_df = enrich_events(df_events, statistics_df)
        if enriched_df is None:
            return False

        # Clasificar anomalias
        anomalies_df = classify_anomalies(enriched_df)

        # Acumulacion regional por hora
        calculate_region_hourly_accumulation(anomalies_df)

        # Anadir id_anomalia UUID
        anomalies_df = anomalies_df.withColumn(
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

        cols_existentes = [c for c in cols_final if c in anomalies_df.columns]
        final_df = anomalies_df.select(cols_existentes)

        # Guardar resultados
        output_path = os.path.join(RUTA_SPEED, "FACT_ANOMALIAS_STREAM")
        final_df.coalesce(1).write.mode("overwrite").parquet(output_path)
        print(f"\nResultados guardados en: {output_path}")

        # Tambien guardar CSV para serving layer
        csv_path = os.path.join(RUTA_SPEED, "FACT_ANOMALIAS_STREAM.csv")
        final_df.coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .option("sep", ",") \
            .option("encoding", "UTF-8") \
            .csv(csv_path.replace(".csv", ""))
        print(f"Resultados CSV guardados en: {csv_path}")

        # Copiar a serving layer
        serving_path = os.path.join(RUTA_SERVING, "datos_streaming.parquet")
        final_df.coalesce(1).write.mode("overwrite").parquet(serving_path)
        print(f"Datos copiados a serving layer: {serving_path}")

        # Estadisticas
        total_anomalies = final_df.count()
        print(f"\n=== RESUMEN STREAMING ===")
        print(f"  Total registros: {total_anomalies:,}")

        print("\n  Distribucion tipo_anomalia:")
        final_df.groupBy("tipo_anomalia").agg(
            spark_count("*").alias("cantidad")
        ).orderBy(col("cantidad").desc()).show(truncate=False)

        print("\n  Distribucion nivel_riesgo:")
        final_df.groupBy("nivel_riesgo").agg(
            spark_count("*").alias("cantidad")
        ).orderBy(col("cantidad").desc()).show(truncate=False)

        # Nulos en columnas criticas
        nulos_zscore = final_df.filter(
            col("zscore_consumo").isNull()
        ).count()
        nulos_flag = final_df.filter(
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


def validate_latency(start_time, end_time, precision_pct=None):
    """
    Valida KPI OE3: latencia < 5 segundos y precision >= 90%.

    Parametros:
        start_time (float): Tiempo de inicio (time.time()).
        end_time (float): Tiempo de fin (time.time()).
        precision_pct (float): Precision calculada (opcional).

    Retorna:
        dict: Metricas de latency.
    """
    try:
        latency = end_time - start_time
        metrics = {
            "latencia_segundos": round(latency, 3),
            "oe3_latencia_cumplido": latency < 5,
            "precision_pct": precision_pct or 0,
            "oe3_precision_cumplido": (precision_pct or 0) >= 90,
            "oe3_cumplido": (latency < 5) and ((precision_pct or 0) >= 90 or precision_pct is None)
        }
        print(f"\n=== VALIDACION KPI OE3 ===")
        print(f"  Latencia: {latency:.3f} seg (requerido < 5)")
        print(f"  OE3 latencia: {'SI' if metrics['oe3_latencia_cumplido'] else 'NO'}")
        if precision_pct is not None:
            print(f"  Precision: {precision_pct:.2f}% (requerido >= 90%)")
            print(f"  OE3 precision: {'SI' if metrics['oe3_precision_cumplido'] else 'NO'}")
        print(f"  OE3 cumplido: {'SI' if metrics['oe3_cumplido'] else 'NO'}")
        return metrics
    except Exception as e:
        print(f"ERROR validando latencia: {e}")
        return {}


def save_report(metrics):
    """
    Guarda reporte JSON del streaming layer.

    Parametros:
        metrics (dict): Metricas a guardar.
    """
    try:
        os.makedirs(RUTA_SPEED, exist_ok=True)
        path = os.path.join(RUTA_SPEED, "reporte_streaming.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2, ensure_ascii=False)
        print(f"  Reporte streaming guardado: {path}")
    except Exception as e:
        print(f"ERROR guardando reporte: {e}")


def execute(mode="auto"):
    """
    Ejecuta la capa speed (streaming) del pipeline Lambda.

    Parametros:
        mode (str): "real" para Kafka, "simulado" para archivo JSON,
                    "auto" para detectar.

    Retorna:
        bool: True si se completo correctamente.
    """
    print("=" * 60)
    print("SPEED LAYER — Streaming de anomalias de consumo")
    print("=" * 60)

    start_time_total = time.time()

    spark = create_spark_session()
    if spark is None:
        return False

    try:
        # Leer estadisticas historicas (generadas por batch layer)
        statistics_df = read_historical_statistics(spark)
        if statistics_df is None:
            print("ERROR: No se encontraron estadisticas historicas.")
            print("  Ejecute primero: python batch_layer/spark_batch.py")
            return False

        # Cachear estadisticas para broadcasting
        statistics_df.cache()
        statistics_df.count()

        if mode == "real":
            result = kafka_streaming_mode(spark, statistics_df)
        elif mode == "simulado":
            result = simulated_streaming_mode(spark, statistics_df)
        else:
            # auto: intentar Kafka, fallback a simulado
            sim_path = os.path.join(RUTA_DATA, "eventos_simples.json")
            if os.path.exists(sim_path):
                print("Usando modo simulado (eventos JSON encontrados)")
                result = simulated_streaming_mode(spark, statistics_df)
            else:
                print("Intentando modo real (Kafka)...")
                result = kafka_streaming_mode(spark, statistics_df)

        end_time_total = time.time()
        latency_metrics = validate_latency(start_time_total, end_time_total)
        save_report(latency_metrics)

        print("\n" + "=" * 60)
        print("SPEED LAYER COMPLETADO")
        print("=" * 60)

        return result

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
    mode = sys.argv[1] if len(sys.argv) > 1 else "auto"
    execute(mode)
