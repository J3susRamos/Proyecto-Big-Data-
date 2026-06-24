"""
spark_batch.py - Capa Batch de la Arquitectura Lambda
Procesa FACT_CONSUMO y DIM_CLIENTE_UBICACION para generar
estadisticas historicas, KPIs, rankings y segmentacion RFM.

Dataset: Consumo electrico Hidrandina (norte del Peru)
Salida: tmp_estadisticas_historicas y rfm_clientes en Parquet
        (se reutilizan/pesan mas), el resto en CSV (legibles)
        en serving_layer/batch_results
"""

import time
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import sys

# ensure pyspark uses the exact same python executable to spawn workers
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

load_dotenv()
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType
)
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

# nombres de tablas que se guardan como parquet (pesadas / reutilizadas)
# el resto se guarda en csv para que se puedan abrir directo en excel
parquet_output_names = {"tmp_estadisticas_historicas", "rfm_clientes"}

# ──────────────────────────────────────────────────────────────────────
# rutas
# ──────────────────────────────────────────────────────────────────────
fact_path = os.path.join(
    os.environ.get("RUTA_DATA", os.path.join(os.path.dirname(__file__), "..", "data")),
    "FACT_CONSUMO.csv"
)
dim_path = os.path.join(
    os.environ.get("RUTA_DATA", os.path.join(os.path.dirname(__file__), "..", "data")),
    "DIM_CLIENTE_UBICACION.csv"
)
results_path = os.path.join(
    os.environ.get("RUTA_SERVING", os.path.join(os.path.dirname(__file__), "..", "serving_layer")),
    "batch_results"
)


# ──────────────────────────────────────────────────────────────────────
# 1. crear spark session
# ──────────────────────────────────────────────────────────────────────
def create_spark_session(app_name="Hidrandina_Batch"):
    """
    Crea y configura la sesion Spark optimizada para Windows.
    """
    try:
        print("\n  [INICIO] Creando Spark Session...")
        start_time = time.time()

        spark = (
            SparkSession.builder
            .master(os.environ.get("SPARK_MASTER", "local[*]"))
            .appName(app_name)
            .master("local[2]")
            .config("spark.driver.memory", "3g")
            .config("spark.executor.memory", "3g")
            .config("spark.driver.maxResultSize", "1g")
            .config("spark.sql.files.maxPartitionBytes", "67108864")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.sql.parquet.mergeSchema", "false")
            .config("spark.sql.session.timeZone", "America/Lima")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .config("spark.hadoop.io.native.lib.available", "false")
            .config("spark.hadoop.parquet.enable.summary-metadata", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        elapsed_seconds = time.time() - start_time
        print(f"  [OK] Spark Session creada en {elapsed_seconds:.2f}s")
        print(f"       Particiones shuffle: 8")
        print(f"       Compresion: snappy")
        print(f"       Memoria driver: 3g | Memoria executor: 3g")
        return spark
    except Exception as e:
        print(f"ERROR creando Spark Session: {e}")
        raise


# ──────────────────────────────────────────────────────────────────────
# 2. cargar tablas
# ──────────────────────────────────────────────────────────────────────
def load_tables(spark):
    """
    Lee FACT_CONSUMO y DIM_CLIENTE_UBICACION desde CSV con schema explicito.
    """
    try:
        print("\n  [INICIO] Cargando FACT_CONSUMO desde:", fact_path)
        fact_start_time = time.time()

        fact_schema = StructType([
            StructField("NRO_DOC_FAC", StringType(), True),
            StructField("PERIODO", IntegerType(), True),
            StructField("CONSUMO", DoubleType(), True),
            StructField("IMPORTE", DoubleType(), True),
            StructField("FECHA_EMISION", StringType(), True),
            StructField("FECHA_VENCIMIENTO", StringType(), True),
            StructField("FECHA_CONSUMO_HASTA", StringType(), True),
        ])

        fact_df = (
            spark.read
            .option("header", "true")
            .schema(fact_schema)
            .option("encoding", "UTF-8")
            .csv(fact_path)
        )

        # convertir fechas a datetype para calculos posteriores
        for date_column in ["FECHA_EMISION", "FECHA_VENCIMIENTO", "FECHA_CONSUMO_HASTA"]:
            if date_column in fact_df.columns:
                fact_df = fact_df.withColumn(date_column, F.to_date(F.col(date_column), "yyyy-MM-dd"))

        fact_elapsed_seconds = time.time() - fact_start_time
        fact_column_count = len(fact_df.columns)
        print(f"  [OK] FACT_CONSUMO: {fact_column_count} columnas en {fact_elapsed_seconds:.2f}s")
        print(f"       Columnas: {', '.join(fact_df.columns)}")

        print("\n  [INICIO] Cargando DIM_CLIENTE_UBICACION desde:", dim_path)
        dim_start_time = time.time()

        dim_schema = StructType([
            StructField("NRO_DOC_FAC", StringType(), True),
            StructField("DEPARTAMENTO", StringType(), True),
            StructField("PROVINCIA", StringType(), True),
            StructField("DISTRITO", StringType(), True),
            StructField("UBIGEO", IntegerType(), True),
            StructField("TARIFA", StringType(), True),
            StructField("CARTERA", StringType(), True),
            StructField("UNIDAD_NEGOCIO", StringType(), True),
        ])

        dim_df = (
            spark.read
            .option("header", "true")
            .schema(dim_schema)
            .option("encoding", "UTF-8")
            .csv(dim_path)
        )

        dim_elapsed_seconds = time.time() - dim_start_time
        dim_column_count = len(dim_df.columns)
        print(f"  [OK] DIM_CLIENTE_UBICACION: {dim_column_count} columnas en {dim_elapsed_seconds:.2f}s")
        print(f"       Columnas: {', '.join(dim_df.columns)}")

        total_elapsed_seconds = fact_elapsed_seconds + dim_elapsed_seconds
        print(f"\n  Total carga: {total_elapsed_seconds:.2f}s")

        return fact_df, dim_df
    except Exception as e:
        print(f"ERROR cargando tablas: {e}")
        raise


# ──────────────────────────────────────────────────────────────────────
# 3. join
# ──────────────────────────────────────────────────────────────────────
def join_fact_dim(fact_df, dim_df):
    """
    Une FACT_CONSUMO y DIM_CLIENTE_UBICACION por NRO_DOC_FAC.
    """
    try:
        print("\n  [INICIO] JOIN: FACT_CONSUMO -- NRO_DOC_FAC --> DIM_CLIENTE_UBICACION")
        start_time = time.time()
        joined_df = fact_df.join(dim_df, on="NRO_DOC_FAC", how="inner")
        joined_df = joined_df.repartition(128, "DISTRITO", "TARIFA", "CARTERA")
        joined_df = joined_df.persist(StorageLevel.MEMORY_AND_DISK)
        row_count = joined_df.count()
        elapsed_seconds = time.time() - start_time
        column_count = len(joined_df.columns)
        print(f"  [OK] JOIN completado en {elapsed_seconds:.2f}s")
        print(f"       Filas resultado: {row_count:,} x {column_count} columnas")
        return joined_df
    except Exception as e:
        print(f"ERROR en JOIN: {e}")
        raise


# ──────────────────────────────────────────────────────────────────────
# 4. estadisticas historicas
# ──────────────────────────────────────────────────────────────────────
def calculate_historical_statistics(joined_df):
    """
    Agrupa por DISTRITO, TARIFA, CARTERA y calcula estadisticas
    de CONSUMO e IMPORTE. Resultado: tmp_estadisticas_historicas.
    """
    try:
        print("\n  [INICIO] Calculando TMP_ESTADISTICAS_HISTORICAS...")
        print("           (Agrupar por: DISTRITO + TARIFA + CARTERA)")
        start_time = time.time()

        statistics_df = (
            joined_df.groupBy("DISTRITO", "TARIFA", "CARTERA")
            .agg(
                F.avg("CONSUMO").alias("consumo_promedio"),
                F.stddev("CONSUMO").alias("consumo_std"),
                F.avg("IMPORTE").alias("importe_promedio"),
                F.stddev("IMPORTE").alias("importe_std"),
                F.min("CONSUMO").alias("consumo_minimo"),
                F.max("CONSUMO").alias("consumo_maximo"),
                F.count("*").alias("total_registros")
            )
            .na.fill(0)
            .repartition(32, "DISTRITO", "TARIFA", "CARTERA")
            .orderBy("DISTRITO", "TARIFA", "CARTERA")
        )

        row_count = statistics_df.count()
        elapsed_seconds = time.time() - start_time
        column_count = len(statistics_df.columns)
        print(f"  [OK] Estadisticas historicas en {elapsed_seconds:.2f}s")
        print(f"       Grupos (DISTRITO+TARIFA+CARTERA): {row_count:,}")
        print(f"       Columnas: {column_count} (esperadas: 10)")
        return statistics_df
    except Exception as e:
        print(f"ERROR calculando estadisticas historicas: {e}")
        raise


# ──────────────────────────────────────────────────────────────────────
# 5. kpis globales
# ──────────────────────────────────────────────────────────────────────
def calculate_global_kpis(joined_df, spark):
    """
    Calcula KPIs del negocio a nivel global.

    Parametros:
        spark: SparkSession
        joined_df: DataFrame con JOIN entre FACT y DIM

    Incluye tasa de outliers usando z-score.
    """
    try:
        print("\n  [INICIO] Calculando KPIs globales...")
        start_time = time.time()

        # estadisticas globales
        global_stats_row = joined_df.agg(
            F.sum("IMPORTE").alias("facturacion_total_soles"),
            F.sum("CONSUMO").alias("consumo_total_kwh"),
            F.countDistinct("NRO_DOC_FAC").alias("total_facturas"),
            F.avg("IMPORTE").alias("ticket_promedio"),
            F.avg("CONSUMO").alias("consumo_promedio_global"),
            F.stddev("CONSUMO").alias("std_consumo_global"),
            F.stddev("IMPORTE").alias("std_importe_global")
        ).collect()[0]

        total_billing        = global_stats_row["facturacion_total_soles"] or 0
        total_consumption     = global_stats_row["consumo_total_kwh"] or 0
        total_invoices         = global_stats_row["total_facturas"] or 0
        average_ticket          = global_stats_row["ticket_promedio"] or 0
        average_consumption       = global_stats_row["consumo_promedio_global"] or 0
        consumption_std             = global_stats_row["std_consumo_global"] or 0
        billing_std                   = global_stats_row["std_importe_global"] or 0

        # tasa de outliers (zscore > 3 en consumo o importe)
        total_rows = joined_df.count()
        if consumption_std > 0 and billing_std > 0 and total_rows > 0:
            outlier_count = (
                joined_df.filter(
                    (F.abs((F.col("CONSUMO") - average_consumption) / consumption_std) > 3) |
                    (F.abs((F.col("IMPORTE") - average_ticket) / billing_std) > 3)
                )
                .count()
            )
            outlier_rate_pct = round(outlier_count / total_rows * 100, 2)
        else:
            outlier_count = 0
            outlier_rate_pct = 0.0

        kpi_rows = [
            {
                "indicador": "facturacion_total_soles",
                "valor": round(float(total_billing), 2)
            },
            {
                "indicador": "consumo_total_kwh",
                "valor": round(float(total_consumption), 2)
            },
            {
                "indicador": "total_facturas",
                "valor": float(total_invoices)
            },
            {
                "indicador": "ticket_promedio",
                "valor": round(float(average_ticket), 2)
            },
            {
                "indicador": "consumo_promedio_global",
                "valor": round(float(average_consumption), 2)
            },
            {
                "indicador": "total_registros",
                "valor": float(total_rows)
            },
            {
                "indicador": "registros_outliers",
                "valor": float(outlier_count)
            },
            {
                "indicador": "tasa_outlier_pct",
                "valor": float(outlier_rate_pct)
            }
        ]

        kpis_df = spark.createDataFrame(kpi_rows)
        elapsed_seconds = time.time() - start_time
        print(f"  [OK] KPIs calculados en {elapsed_seconds:.2f}s")
        print(f"       Total registros procesados: {total_rows:,}")
        print(f"       Facturacion total: S/. {total_billing:,.2f}")
        print(f"       Consumo total: {total_consumption:,.2f} kWh")
        print(f"       Ticket promedio: S/. {average_ticket:,.2f}")
        print(f"       Registros outliers: {outlier_count:,} ({outlier_rate_pct}%)")
        return kpis_df
    except Exception as e:
        print(f"ERROR calculando KPIs globales: {e}")
        raise


# ──────────────────────────────────────────────────────────────────────
# 6. ranking por departamento
# ──────────────────────────────────────────────────────────────────────
def calculate_department_ranking(joined_df):
    """
    Agrupa por DEPARTAMENTO: suma de importe, consumo y conteo.
    """
    try:
        print("\n  [INICIO] Calculando ranking por departamento...")
        start_time = time.time()

        ranking_df = (
            joined_df.groupBy("DEPARTAMENTO")
            .agg(
                F.sum("IMPORTE").alias("total_importe"),
                F.sum("CONSUMO").alias("total_consumo"),
                F.count("*").alias("cantidad_registros")
            )
            .orderBy(F.col("total_importe").desc())
        )

        row_count = ranking_df.count()
        elapsed_seconds = time.time() - start_time
        print(f"  [OK] Ranking departamentos en {elapsed_seconds:.2f}s")
        print(f"       Departamentos unicos: {row_count:,}")
        return ranking_df
    except Exception as e:
        print(f"ERROR calculando ranking departamentos: {e}")
        raise


# ──────────────────────────────────────────────────────────────────────
# 7. tendencia mensual
# ──────────────────────────────────────────────────────────────────────
def calculate_monthly_trend(joined_df):
    """
    Agrupa por PERIODO (aaaamm): suma de importe, consumo y conteo.
    """
    try:
        print("\n  [INICIO] Calculando tendencia mensual...")
        start_time = time.time()

        trend_df = (
            joined_df.groupBy("PERIODO")
            .agg(
                F.sum("IMPORTE").alias("total_importe"),
                F.sum("CONSUMO").alias("total_consumo"),
                F.count("*").alias("cantidad_registros")
            )
            .orderBy("PERIODO")
        )

        row_count = trend_df.count()
        elapsed_seconds = time.time() - start_time
        print(f"  [OK] Tendencia mensual en {elapsed_seconds:.2f}s")
        print(f"       Periodos unicos: {row_count:,}")
        return trend_df
    except Exception as e:
        print(f"ERROR calculando tendencia mensual: {e}")
        raise


# ──────────────────────────────────────────────────────────────────────
# 8. analisis por tarifa y cartera
# ──────────────────────────────────────────────────────────────────────
def calculate_rate_portfolio_analysis(joined_df):
    """
    Agrupa por TARIFA y CARTERA: suma de importe, promedio consumo, conteo.
    """
    try:
        print("\n  [INICIO] Calculando analisis por TARIFA y CARTERA...")
        start_time = time.time()

        analysis_df = (
            joined_df.groupBy("TARIFA", "CARTERA")
            .agg(
                F.sum("IMPORTE").alias("total_importe"),
                F.avg("CONSUMO").alias("consumo_promedio"),
                F.count("*").alias("cantidad_registros")
            )
            .orderBy(F.col("total_importe").desc())
        )

        row_count = analysis_df.count()
        elapsed_seconds = time.time() - start_time
        print(f"  [OK] Analisis tarifa/cartera en {elapsed_seconds:.2f}s")
        print(f"       Combinaciones TARIFA+CARTERA: {row_count:,}")
        return analysis_df
    except Exception as e:
        print(f"ERROR calculando analisis tarifa/cartera: {e}")
        raise


# ──────────────────────────────────────────────────────────────────────
# 9. segmentacion rfm de clientes
# ──────────────────────────────────────────────────────────────────────
def calculate_rfm_segmentation(joined_df):
    """
    Calcula Recency, Frequency, Monetary por NRO_DOC_FAC
    y asigna segmento de cliente.
    """
    try:
        print("\n  [INICIO] Calculando segmentacion RFM por cliente...")
        start_time = time.time()
        print("           R=Recency(dias), F=Frequency(periodos), M=Monetary(importe)")

        # asegurar que fecha_emision sea datetype antes de operar
        if "FECHA_EMISION" in joined_df.columns:
            joined_df = joined_df.withColumn("FECHA_EMISION", F.to_date(F.col("FECHA_EMISION")))

        # fecha maxima del dataset
        max_date = joined_df.agg(F.max("FECHA_EMISION")).collect()[0][0]
        if max_date is None:
            max_date = datetime.now()

        # calcular rfm por cliente
        rfm_df = (
            joined_df.groupBy("NRO_DOC_FAC")
            .agg(
                F.datediff(F.lit(max_date), F.max("FECHA_EMISION")).alias("recency"),
                F.countDistinct("PERIODO").alias("frequency"),
                F.sum("IMPORTE").alias("monetary")
            )
            .na.fill(0)
        )

        # asignar scores 1-3 usando percentiles
        # recency: menor es mejor (mas reciente)
        recency_terciles = rfm_df.approxQuantile("recency", [1/3, 2/3], 0.01)
        # frequency: mayor es mejor
        frequency_terciles = rfm_df.approxQuantile("frequency", [1/3, 2/3], 0.01)
        # monetary: mayor es mejor
        monetary_terciles = rfm_df.approxQuantile("monetary", [1/3, 2/3], 0.01)

        recency_t1, recency_t2 = recency_terciles if len(recency_terciles) == 2 else (0, 0)
        frequency_t1, frequency_t2 = frequency_terciles if len(frequency_terciles) == 2 else (0, 0)
        monetary_t1, monetary_t2 = monetary_terciles if len(monetary_terciles) == 2 else (0, 0)

        rfm_scored_df = (
            rfm_df
            .withColumn(
                "r_score",
                F.when(F.col("recency") <= recency_t1, 3)
                 .when(F.col("recency") <= recency_t2, 2)
                 .otherwise(1)
            )
            .withColumn(
                "f_score",
                F.when(F.col("frequency") <= frequency_t1, 1)
                 .when(F.col("frequency") <= frequency_t2, 2)
                 .otherwise(3)
            )
            .withColumn(
                "m_score",
                F.when(F.col("monetary") <= monetary_t1, 1)
                 .when(F.col("monetary") <= monetary_t2, 2)
                 .otherwise(3)
            )
            .withColumn("rfm_total", F.col("r_score") + F.col("f_score") + F.col("m_score"))
            .withColumn(
                "segmento",
                F.when(F.col("rfm_total") >= 8, "Champion")
                 .when(F.col("rfm_total") >= 6, "Cliente activo")
                 .when(F.col("rfm_total") >= 4, "En riesgo")
                 .otherwise("Perdido")
            )
            .orderBy(F.col("rfm_total").desc())
        )

        row_count = rfm_scored_df.count()
        elapsed_seconds = time.time() - start_time

        # contar clientes por segmento
        segment_rows = rfm_scored_df.groupBy("segmento").count().collect()
        segment_counts = {row["segmento"]: row["count"] for row in segment_rows}

        print(f"  [OK] RFM en {elapsed_seconds:.2f}s")
        print(f"       Total clientes analizados: {row_count:,}")
        print(f"       Segmentos:")
        for segment_name in ["Champion", "Cliente activo", "En riesgo", "Perdido"]:
            segment_count = segment_counts.get(segment_name, 0)
            segment_pct = (segment_count / row_count * 100) if row_count > 0 else 0
            print(f"         - {segment_name:20s}: {segment_count:7,d} ({segment_pct:5.1f}%)")
        return rfm_scored_df
    except Exception as e:
        print(f"ERROR calculando RFM: {e}")
        raise


# ──────────────────────────────────────────────────────────────────────
# 10. guardar resultados
# ──────────────────────────────────────────────────────────────────────
def save_results(result_dfs, output_dir):
    """
    Guarda cada DataFrame en el formato adecuado:
    - tmp_estadisticas_historicas y rfm_clientes -> Parquet (snappy)
      porque son los que mas se reutilizan/pesan.
    - el resto -> CSV via pandas, para que se abran directo en Excel
      (y porque evita el NativeIO de Hadoop, que falla en Windows).
    """
    try:
        import pandas as pd
        os.makedirs(output_dir, exist_ok=True)
        print(f"\n  [INICIO] Guardando {len(result_dfs)} tablas en:")
        print(f"           {output_dir}")
        start_time = time.time()

        for table_index, (table_name, table_df) in enumerate(result_dfs.items(), 1):
            table_start_time = time.time()
            row_count = table_df.count()
            column_count = len(table_df.columns)

            if table_name in parquet_output_names:
                output_path = os.path.join(output_dir, table_name)
                print(f"    [{table_index}] {table_name:40s} -> {row_count:8,d} filas x {column_count:2d} cols (Parquet)...")
                table_df.write.mode("overwrite").parquet(output_path)
                table_elapsed_seconds = time.time() - table_start_time
                print(f"         -> Completado en {table_elapsed_seconds:.2f}s")
                continue

            output_path = os.path.join(output_dir, f"{table_name}.csv")
            if row_count > 500000:
                # tablas grandes: guardar en lotes para evitar memoryerror
                print(f"    [{table_index}] {table_name:40s} -> {row_count:8,d} filas x {column_count:2d} cols (guardando en lotes)...")
                chunk_size = 100000
                ordered_df = table_df.orderBy("NRO_DOC_FAC") if "NRO_DOC_FAC" in table_df.columns else table_df
                header_written = False
                for chunk_offset in range(0, row_count, chunk_size):
                    chunk_pdf = ordered_df.limit(chunk_size).offset(chunk_offset).toPandas()
                    chunk_pdf.to_csv(output_path, index=False, encoding="utf-8-sig", mode="a", header=not header_written)
                    header_written = True
                table_elapsed_seconds = time.time() - table_start_time
                print(f"         -> Completado en {table_elapsed_seconds:.2f}s")
            else:
                table_pdf = table_df.toPandas()
                table_pdf.to_csv(output_path, index=False, encoding="utf-8-sig")
                table_elapsed_seconds = time.time() - table_start_time
                print(f"    [{table_index}] {table_name:40s} -> {row_count:8,d} filas x {column_count:2d} cols ({table_elapsed_seconds:.2f}s)")

        total_elapsed_seconds = time.time() - start_time
        print(f"\n  [OK] Todos los archivos guardados en {total_elapsed_seconds:.2f}s")
    except Exception as e:
        print(f"ERROR guardando resultados: {e}")
        raise


# ──────────────────────────────────────────────────────────────────────
# 11. validar oe2
# ──────────────────────────────────────────────────────────────────────
def validate_oe2(statistics_df):
    """
    Verifica el KPI OE2:
    - TMP_ESTADISTICAS_HISTORICAS con 10 columnas
    - consumo_promedio y consumo_std distintos de cero
    """
    try:
        print("\n  Validando KPI OE2...")
        statistics_columns = statistics_df.columns
        column_count = len(statistics_columns)
        print(f"    Columnas: {column_count} (esperado: 10)")

        # verificar que existan las columnas clave
        expected_columns = [
            "DISTRITO", "TARIFA", "CARTERA",
            "consumo_promedio", "consumo_std",
            "importe_promedio", "importe_std",
            "consumo_minimo", "consumo_maximo", "total_registros"
        ]
        expected_columns_present = all(c in statistics_columns for c in expected_columns)
        print(f"    Columnas esperadas presentes: {expected_columns_present}")

        # verificar que consumo_promedio y consumo_std > 0
        # (con total_registros == 1 el std es matematicamente indefinido, se excluye)
        zero_value_group_count = statistics_df.filter(
            (F.col("total_registros") > 1)
            & ((F.col("consumo_promedio") == 0) | (F.col("consumo_std") == 0))
        ).count()
        print(f"    Grupos con consumo_promedio=0 o consumo_std=0 (excluyendo n=1): {zero_value_group_count}")

        # oe2 cumplido si 10 columnas y sin grupos con valores 0 (entre los con n>1)
        oe2_passed = (column_count == 10) and expected_columns_present and (zero_value_group_count == 0)
        print(f"    OE2 cumplido: {'SI' if oe2_passed else 'NO'}")

        return oe2_passed
    except Exception as e:
        print(f"ERROR validando OE2: {e}")
        return False


# ──────────────────────────────────────────────────────────────────────
# 12. ejecutar
# ──────────────────────────────────────────────────────────────────────
def execute():
    """
    Orquesta todo el pipeline batch y muestra resumen final.
    """
    start_time = time.time()
    spark = None
    try:
        print("=" * 60)
        print("BATCH LAYER - Procesamiento Historico Hidrandina")
        print("=" * 60)

        # 1. spark session
        spark = create_spark_session()

        # 2. cargar tablas
        fact_df, dim_df = load_tables(spark)

        # 3. join
        joined_df = join_fact_dim(fact_df, dim_df)

        # 4-9. calculos
        statistics_df = calculate_historical_statistics(joined_df)
        kpis_df = calculate_global_kpis(joined_df, spark)
        ranking_df = calculate_department_ranking(joined_df)
        trend_df = calculate_monthly_trend(joined_df)
        analysis_df = calculate_rate_portfolio_analysis(joined_df)
        rfm_df = calculate_rfm_segmentation(joined_df)

        # 10. guardar resultados
        result_dfs = {
            "tmp_estadisticas_historicas": statistics_df,
            "kpis_globales":               kpis_df,
            "ranking_departamentos":       ranking_df,
            "tendencia_mensual":           trend_df,
            "analisis_tarifa_cartera":     analysis_df,
            "rfm_clientes":                rfm_df
        }
        save_results(result_dfs, results_path)

        # 11. validar oe2
        oe2_passed = validate_oe2(statistics_df)

        # 12. resumen final
        elapsed_seconds = time.time() - start_time
        print("\n" + "=" * 60)
        print("RESUMEN BATCH LAYER")
        print("=" * 60)
        print(f"  Tiempo de ejecucion: {elapsed_seconds:.2f} segundos")
        print(f"  TMP_ESTADISTICAS_HISTORICAS: {statistics_df.count():,} filas x {len(statistics_df.columns)} cols")
        print(f"  KPIs globales: {kpis_df.count():,} indicadores")
        print(f"  Ranking departamentos: {ranking_df.count():,} filas")
        print(f"  Tendencia mensual: {trend_df.count():,} periodos")
        print(f"  Analisis tarifa/cartera: {analysis_df.count():,} filas")
        print(f"  RFM clientes: {rfm_df.count():,} clientes segmentados")
        print(f"  OE2 cumplido: {'SI' if oe2_passed else 'NO'}")
        print("=" * 60)

        return statistics_df, kpis_df, ranking_df, trend_df, analysis_df, rfm_df, oe2_passed

    except Exception as e:
        print(f"\nERROR en execute: {e}")
        elapsed_seconds = time.time() - start_time
        print(f"\n  [!]  PIPELINE INTERRUMPIDO tras {elapsed_seconds:.2f} segundos")
        raise
    finally:
        if spark is not None:
            print("\n  [LIMPIEZA] Deteniendo Spark Session...")
            spark.stop()
            print("  [OK] Spark Session detenida")
            print("  Spark Session cerrada.")


# ──────────────────────────────────────────────────────────────────────
# main
# ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    execute()
