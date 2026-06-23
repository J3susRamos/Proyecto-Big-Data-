"""
spark_batch.py - Capa Batch de la Arquitectura Lambda
Procesa FACT_CONSUMO y DIM_CLIENTE_UBICACION para generar
estadisticas historicas, KPIs, rankings y segmentacion RFM.

Dataset: Consumo electrico Hidrandina (norte del Peru)
Salida: Archivos Parquet en serving_layer/batch_results
"""

import time
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import sys

# Ensure PySpark uses the exact same python executable to spawn workers
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

load_dotenv()
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType
)
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

# ──────────────────────────────────────────────────────────────────────
# RUTAS
# ──────────────────────────────────────────────────────────────────────
RUTA_FACT = os.path.join(
    os.environ.get("RUTA_DATA", os.path.join(os.path.dirname(__file__), "..", "data")),
    "FACT_CONSUMO.csv"
)
RUTA_DIM = os.path.join(
    os.environ.get("RUTA_DATA", os.path.join(os.path.dirname(__file__), "..", "data")),
    "DIM_CLIENTE_UBICACION.csv"
)
RUTA_RESULTADOS = os.path.join(
    os.environ.get("RUTA_SERVING", os.path.join(os.path.dirname(__file__), "..", "serving_layer")),
    "batch_results"
)


# ──────────────────────────────────────────────────────────────────────
# 1. CREAR SPARK SESSION
# ──────────────────────────────────────────────────────────────────────
def crear_spark_session(app_nombre="Hidrandina_Batch"):
    """
    Crea y configura la sesion Spark optimizada para Windows.
    """
    try:
        print("\n  [INICIO] Creando Spark Session...")
        inicio = time.time()

        spark = (
            SparkSession.builder
            .master(os.environ.get("SPARK_MASTER", "local[*]"))
            .appName(app_nombre)
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
        elapsed = time.time() - inicio
        print(f"  [OK] Spark Session creada en {elapsed:.2f}s")
        print(f"       Particiones shuffle: 8")
        print(f"       Compresion: snappy")
        print(f"       Memoria driver: 3g | Memoria executor: 3g")
        return spark
    except Exception as e:
        print(f"ERROR creando Spark Session: {e}")
        raise


# ──────────────────────────────────────────────────────────────────────
# 2. CARGAR TABLAS
# ──────────────────────────────────────────────────────────────────────
def cargar_tablas(spark):
    """
    Lee FACT_CONSUMO y DIM_CLIENTE_UBICACION desde CSV con schema explícito.
    """
    try:
        print("\n  [INICIO] Cargando FACT_CONSUMO desde:", RUTA_FACT)
        inicio_fact = time.time()

        schema_fact = StructType([
            StructField("NRO_DOC_FAC", StringType(), True),
            StructField("PERIODO", IntegerType(), True),
            StructField("CONSUMO", DoubleType(), True),
            StructField("IMPORTE", DoubleType(), True),
            StructField("FECHA_EMISION", StringType(), True),
            StructField("FECHA_VENCIMIENTO", StringType(), True),
            StructField("FECHA_CONSUMO_HASTA", StringType(), True),
        ])

        fact = (
            spark.read
            .option("header", "true")
            .schema(schema_fact)
            .option("encoding", "UTF-8")
            .csv(RUTA_FACT)
        )

        # Convertir fechas a DateType para cálculos posteriores
        for col in ["FECHA_EMISION", "FECHA_VENCIMIENTO", "FECHA_CONSUMO_HASTA"]:
            if col in fact.columns:
                fact = fact.withColumn(col, F.to_date(F.col(col), "yyyy-MM-dd"))

        elapsed_fact = time.time() - inicio_fact
        cols_fact = len(fact.columns)
        print(f"  [OK] FACT_CONSUMO: {cols_fact} columnas en {elapsed_fact:.2f}s")
        print(f"       Columnas: {', '.join(fact.columns)}")

        print("\n  [INICIO] Cargando DIM_CLIENTE_UBICACION desde:", RUTA_DIM)
        inicio_dim = time.time()

        schema_dim = StructType([
            StructField("NRO_DOC_FAC", StringType(), True),
            StructField("DEPARTAMENTO", StringType(), True),
            StructField("PROVINCIA", StringType(), True),
            StructField("DISTRITO", StringType(), True),
            StructField("UBIGEO", IntegerType(), True),
            StructField("TARIFA", StringType(), True),
            StructField("CARTERA", StringType(), True),
            StructField("UNIDAD_NEGOCIO", StringType(), True),
        ])

        dim = (
            spark.read
            .option("header", "true")
            .schema(schema_dim)
            .option("encoding", "UTF-8")
            .csv(RUTA_DIM)
        )

        elapsed_dim = time.time() - inicio_dim
        cols_dim = len(dim.columns)
        print(f"  [OK] DIM_CLIENTE_UBICACION: {cols_dim} columnas en {elapsed_dim:.2f}s")
        print(f"       Columnas: {', '.join(dim.columns)}")

        total_elapsed = elapsed_fact + elapsed_dim
        print(f"\n  Total carga: {total_elapsed:.2f}s")

        return fact, dim
    except Exception as e:
        print(f"ERROR cargando tablas: {e}")
        raise


# ──────────────────────────────────────────────────────────────────────
# 3. JOIN
# ──────────────────────────────────────────────────────────────────────
def hacer_join(fact, dim):
    """
    Une FACT_CONSUMO y DIM_CLIENTE_UBICACION por NRO_DOC_FAC.
    """
    try:
        print("\n  [INICIO] JOIN: FACT_CONSUMO -- NRO_DOC_FAC --> DIM_CLIENTE_UBICACION")
        inicio = time.time()
        df_join = fact.join(dim, on="NRO_DOC_FAC", how="inner")
        df_join = df_join.repartition(128, "DISTRITO", "TARIFA", "CARTERA")
        df_join = df_join.persist(StorageLevel.MEMORY_AND_DISK)
        filas = df_join.count()
        elapsed = time.time() - inicio
        cols = len(df_join.columns)
        print(f"  [OK] JOIN completado en {elapsed:.2f}s")
        print(f"       Filas resultado: {filas:,} x {cols} columnas")
        return df_join
    except Exception as e:
        print(f"ERROR en JOIN: {e}")
        raise


# ──────────────────────────────────────────────────────────────────────
# 4. ESTADISTICAS HISTORICAS
# ──────────────────────────────────────────────────────────────────────
def calcular_estadisticas_historicas(df_join):
    """
    Agrupa por DISTRITO, TARIFA, CARTERA y calcula estadisticas
    de CONSUMO e IMPORTE. Guarda en tmp_estadisticas_historicas.
    """
    try:
        print("\n  [INICIO] Calculando TMP_ESTADISTICAS_HISTORICAS...")
        print("           (Agrupar por: DISTRITO + TARIFA + CARTERA)")
        inicio = time.time()

        stats = (
            df_join.groupBy("DISTRITO", "TARIFA", "CARTERA")
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

        filas = stats.count()
        elapsed = time.time() - inicio
        cols = len(stats.columns)
        print(f"  [OK] Estadisticas históricas en {elapsed:.2f}s")
        print(f"       Grupos (DISTRITO+TARIFA+CARTERA): {filas:,}")
        print(f"       Columnas: {cols} (esperadas: 10)")
        return stats
    except Exception as e:
        print(f"ERROR calculando estadisticas historicas: {e}")
        raise


# ──────────────────────────────────────────────────────────────────────
# 5. KPIS GLOBALES
# ──────────────────────────────────────────────────────────────────────
def calcular_kpis_globales(df_join, spark):
    """
    Calcula KPIS del negocio a nivel global.
    
    Parametros:
        spark: SparkSession
        df_join: DataFrame con JOIN entre FACT y DIM
    
    Incluye tasa de outliers usando z-score.
    """
    try:
        print("\n  [INICIO] Calculando KPIs globales...")
        inicio = time.time()

        # Estadisticas globales
        stats_global = df_join.agg(
            F.sum("IMPORTE").alias("facturacion_total_soles"),
            F.sum("CONSUMO").alias("consumo_total_kwh"),
            F.countDistinct("NRO_DOC_FAC").alias("total_facturas"),
            F.avg("IMPORTE").alias("ticket_promedio"),
            F.avg("CONSUMO").alias("consumo_promedio_global"),
            F.stddev("CONSUMO").alias("std_consumo_global"),
            F.stddev("IMPORTE").alias("std_importe_global")
        ).collect()[0]

        facturacion     = stats_global["facturacion_total_soles"] or 0
        consumo_total   = stats_global["consumo_total_kwh"] or 0
        total_facturas  = stats_global["total_facturas"] or 0
        ticket_promedio = stats_global["ticket_promedio"] or 0
        consumo_prom    = stats_global["consumo_promedio_global"] or 0
        std_consumo     = stats_global["std_consumo_global"] or 0
        std_importe     = stats_global["std_importe_global"] or 0

        # Tasa de outliers (zscore > 3 en CONSUMO o IMPORTE)
        total_registros = df_join.count()
        if std_consumo > 0 and std_importe > 0 and total_registros > 0:
            outliers = (
                df_join.filter(
                    (F.abs((F.col("CONSUMO") - consumo_prom) / std_consumo) > 3) |
                    (F.abs((F.col("IMPORTE") - ticket_promedio) / std_importe) > 3)
                )
                .count()
            )
            tasa_outlier = round(outliers / total_registros * 100, 2)
        else:
            outliers = 0
            tasa_outlier = 0.0

        kpis = [
            {
                "indicador": "facturacion_total_soles",
                "valor": round(float(facturacion), 2)
            },
            {
                "indicador": "consumo_total_kwh",
                "valor": round(float(consumo_total), 2)
            },
            {
                "indicador": "total_facturas",
                "valor": float(total_facturas)
            },
            {
                "indicador": "ticket_promedio",
                "valor": round(float(ticket_promedio), 2)
            },
            {
                "indicador": "consumo_promedio_global",
                "valor": round(float(consumo_prom), 2)
            },
            {
                "indicador": "total_registros",
                "valor": float(total_registros)
            },
            {
                "indicador": "registros_outliers",
                "valor": float(outliers)
            },
            {
                "indicador": "tasa_outlier_pct",
                "valor": float(tasa_outlier)
            }
        ]

        df_kpis = spark.createDataFrame(kpis)
        elapsed = time.time() - inicio
        print(f"  [OK] KPIs calculados en {elapsed:.2f}s")
        print(f"       Total registros procesados: {total_registros:,}")
        print(f"       Facturacion total: S/. {facturacion:,.2f}")
        print(f"       Consumo total: {consumo_total:,.2f} kWh")
        print(f"       Ticket promedio: S/. {ticket_promedio:,.2f}")
        print(f"       Registros outliers: {outliers:,} ({tasa_outlier}%)")
        return df_kpis
    except Exception as e:
        print(f"ERROR calculando KPIs globales: {e}")
        raise


# ──────────────────────────────────────────────────────────────────────
# 6. RANKING POR DEPARTAMENTO
# ──────────────────────────────────────────────────────────────────────
def calcular_ranking_departamentos(df_join):
    """
    Agrupa por DEPARTAMENTO: suma de importe, consumo y conteo.
    """
    try:
        print("\n  [INICIO] Calculando ranking por departamento...")
        inicio = time.time()

        ranking = (
            df_join.groupBy("DEPARTAMENTO")
            .agg(
                F.sum("IMPORTE").alias("total_importe"),
                F.sum("CONSUMO").alias("total_consumo"),
                F.count("*").alias("cantidad_registros")
            )
            .orderBy(F.col("total_importe").desc())
        )

        filas = ranking.count()
        elapsed = time.time() - inicio
        print(f"  [OK] Ranking departamentos en {elapsed:.2f}s")
        print(f"       Departamentos únicos: {filas:,}")
        return ranking
    except Exception as e:
        print(f"ERROR calculando ranking departamentos: {e}")
        raise


# ──────────────────────────────────────────────────────────────────────
# 7. TENDENCIA MENSUAL
# ──────────────────────────────────────────────────────────────────────
def calcular_tendencia_mensual(df_join):
    """
    Agrupa por PERIODO (aaaamm): suma de importe, consumo y conteo.
    """
    try:
        print("\n  [INICIO] Calculando tendencia mensual...")
        inicio = time.time()

        tendencia = (
            df_join.groupBy("PERIODO")
            .agg(
                F.sum("IMPORTE").alias("total_importe"),
                F.sum("CONSUMO").alias("total_consumo"),
                F.count("*").alias("cantidad_registros")
            )
            .orderBy("PERIODO")
        )

        filas = tendencia.count()
        elapsed = time.time() - inicio
        print(f"  [OK] Tendencia mensual en {elapsed:.2f}s")
        print(f"       Periodos únicos: {filas:,}")
        return tendencia
    except Exception as e:
        print(f"ERROR calculando tendencia mensual: {e}")
        raise


# ──────────────────────────────────────────────────────────────────────
# 8. ANALISIS POR TARIFA Y CARTERA
# ──────────────────────────────────────────────────────────────────────
def calcular_analisis_tarifa_cartera(df_join):
    """
    Agrupa por TARIFA y CARTERA: suma de importe, promedio consumo, conteo.
    """
    try:
        print("\n  [INICIO] Calculando análisis por TARIFA y CARTERA...")
        inicio = time.time()

        analisis = (
            df_join.groupBy("TARIFA", "CARTERA")
            .agg(
                F.sum("IMPORTE").alias("total_importe"),
                F.avg("CONSUMO").alias("consumo_promedio"),
                F.count("*").alias("cantidad_registros")
            )
            .orderBy(F.col("total_importe").desc())
        )

        filas = analisis.count()
        elapsed = time.time() - inicio
        print(f"  [OK] Análisis tarifa/cartera en {elapsed:.2f}s")
        print(f"       Combinaciones TARIFA+CARTERA: {filas:,}")
        return analisis
    except Exception as e:
        print(f"ERROR calculando analisis tarifa/cartera: {e}")
        raise


# ──────────────────────────────────────────────────────────────────────
# 9. SEGMENTACION RFM DE CLIENTES
# ──────────────────────────────────────────────────────────────────────
def calcular_rfm(df_join):
    """
    Calcula Recency, Frequency, Monetary por NRO_DOC_FAC
    y asigna segmento de cliente.
    """
    try:
        print("\n  [INICIO] Calculando segmentación RFM por cliente...")
        inicio = time.time()
        print("           R=Recency(dias), F=Frequency(periodos), M=Monetary(importe)")

        # Asegurar que FECHA_EMISION sea DateType antes de operar
        if "FECHA_EMISION" in df_join.columns:
            df_join = df_join.withColumn("FECHA_EMISION", F.to_date(F.col("FECHA_EMISION")))

        # Fecha maxima del dataset
        fecha_max = df_join.agg(F.max("FECHA_EMISION")).collect()[0][0]
        if fecha_max is None:
            fecha_max = datetime.now()

        # Calcular RFM por cliente
        rfm = (
            df_join.groupBy("NRO_DOC_FAC")
            .agg(
                F.datediff(F.lit(fecha_max), F.max("FECHA_EMISION")).alias("recency"),
                F.countDistinct("PERIODO").alias("frequency"),
                F.sum("IMPORTE").alias("monetary")
            )
            .na.fill(0)
        )

        # Asignar scores 1-3 usando percentiles
        # Recency: menor es mejor (mas reciente)
        recency_terciles = rfm.approxQuantile("recency", [1/3, 2/3], 0.01)
        # Frequency: mayor es mejor
        freq_terciles = rfm.approxQuantile("frequency", [1/3, 2/3], 0.01)
        # Monetary: mayor es mejor
        mon_terciles = rfm.approxQuantile("monetary", [1/3, 2/3], 0.01)

        r1, r2 = recency_terciles if len(recency_terciles) == 2 else (0, 0)
        f1, f2 = freq_terciles if len(freq_terciles) == 2 else (0, 0)
        m1, m2 = mon_terciles if len(mon_terciles) == 2 else (0, 0)

        rfm_score = (
            rfm
            .withColumn(
                "r_score",
                F.when(F.col("recency") <= r1, 3)
                 .when(F.col("recency") <= r2, 2)
                 .otherwise(1)
            )
            .withColumn(
                "f_score",
                F.when(F.col("frequency") <= f1, 1)
                 .when(F.col("frequency") <= f2, 2)
                 .otherwise(3)
            )
            .withColumn(
                "m_score",
                F.when(F.col("monetary") <= m1, 1)
                 .when(F.col("monetary") <= m2, 2)
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

        filas = rfm_score.count()
        elapsed = time.time() - inicio
        
        # Contar clientes por segmento
        segmentos = rfm_score.groupBy("segmento").count().collect()
        seg_dict = {row["segmento"]: row["count"] for row in segmentos}
        
        print(f"  [OK] RFM en {elapsed:.2f}s")
        print(f"       Total clientes analizados: {filas:,}")
        print(f"       Segmentos:")
        for seg in ["Champion", "Cliente activo", "En riesgo", "Perdido"]:
            count = seg_dict.get(seg, 0)
            pct = (count / filas * 100) if filas > 0 else 0
            print(f"         - {seg:20s}: {count:7,d} ({pct:5.1f}%)")
        return rfm_score
    except Exception as e:
        print(f"ERROR calculando RFM: {e}")
        raise


# ──────────────────────────────────────────────────────────────────────
# 10. GUARDAR RESULTADOS
# ──────────────────────────────────────────────────────────────────────
def guardar_resultados(dfs, output_dir):
    """
    Guarda todos los DataFrames como CSV usando pandas
    (evita Hadoop NativeIO que falla en Windows).
    """
    try:
        import pandas as pd
        os.makedirs(output_dir, exist_ok=True)
        print(f"\n  [INICIO] Guardando {len(dfs)} tablas en:")
        print(f"           {output_dir}")
        inicio = time.time()

        for i, (nombre, df) in enumerate(dfs.items(), 1):
            ruta = os.path.join(output_dir, f"{nombre}.csv")
            inicio_tabla = time.time()
            filas = df.count()
            cols = len(df.columns)

            if filas > 500000:
                # Tablas grandes: guardar en lotes para evitar MemoryError
                print(f"    [{i}] {nombre:40s} -> {filas:8,d} filas x {cols:2d} cols (guardando en lotes)...")
                chunk_size = 100000
                df_order = df.orderBy("NRO_DOC_FAC") if "NRO_DOC_FAC" in df.columns else df
                total = filas
                header_written = False
                for offset in range(0, total, chunk_size):
                    chunk = df_order.limit(chunk_size).offset(offset).toPandas()
                    chunk.to_csv(ruta, index=False, encoding="utf-8-sig", mode="a", header=not header_written)
                    header_written = True
                elapsed_tabla = time.time() - inicio_tabla
                print(f"         -> Completado en {elapsed_tabla:.2f}s")
            else:
                pdf = df.toPandas()
                pdf.to_csv(ruta, index=False, encoding="utf-8-sig")
                elapsed_tabla = time.time() - inicio_tabla
                print(f"    [{i}] {nombre:40s} -> {filas:8,d} filas x {cols:2d} cols ({elapsed_tabla:.2f}s)")

        elapsed_total = time.time() - inicio
        print(f"\n  [OK] Todos los archivos guardados en {elapsed_total:.2f}s")
    except Exception as e:
        print(f"ERROR guardando resultados: {e}")
        raise


# ──────────────────────────────────────────────────────────────────────
# 11. VALIDAR OE2
# ──────────────────────────────────────────────────────────────────────
def validar_oe2(stats):
    """
    Verifica el KPI OE2:
    - TMP_ESTADISTICAS_HISTORICAS con 10 columnas
    - consumo_promedio y consumo_std distintos de cero
    """
    try:
        print("\n  Validando KPI OE2...")
        columnas = stats.columns
        num_cols = len(columnas)
        print(f"    Columnas: {num_cols} (esperado: 10)")

        # Verificar que existan las columnas clave
        cols_esperadas = [
            "DISTRITO", "TARIFA", "CARTERA",
            "consumo_promedio", "consumo_std",
            "importe_promedio", "importe_std",
            "consumo_minimo", "consumo_maximo", "total_registros"
        ]
        cols_ok = all(c in columnas for c in cols_esperadas)
        print(f"    Columnas esperadas presentes: {cols_ok}")

        # Verificar que consumo_promedio y consumo_std > 0
        # (con total_registros == 1 el std es matematicamente indefinido, se excluye)
        filas_con_cero = stats.filter(
            (F.col("total_registros") > 1)
            & ((F.col("consumo_promedio") == 0) | (F.col("consumo_std") == 0))
        ).count()
        print(f"    Grupos con consumo_promedio=0 o consumo_std=0 (excluyendo n=1): {filas_con_cero}")

        # OE2 cumplido si 10 columnas y sin grupos con valores 0 (entre los con n>1)
        oe2_cumplido = (num_cols == 10) and cols_ok and (filas_con_cero == 0)
        print(f"    OE2 cumplido: {'SI' if oe2_cumplido else 'NO'}")

        return oe2_cumplido
    except Exception as e:
        print(f"ERROR validando OE2: {e}")
        return False


# ──────────────────────────────────────────────────────────────────────
# 12. EJECUTAR
# ──────────────────────────────────────────────────────────────────────
def execute():
    """
    Orquesta todo el pipeline batch y muestra resumen final.
    """
    inicio = time.time()
    spark = None
    try:
        print("=" * 60)
        print("BATCH LAYER - Procesamiento Historico Hidrandina")
        print("=" * 60)

        # 1. Spark Session
        spark = crear_spark_session()

        # 2. Cargar tablas
        fact, dim = cargar_tablas(spark)

        # 3. JOIN
        df_join = hacer_join(fact, dim)

        # 4-9. Calculos
        stats   = calcular_estadisticas_historicas(df_join)
        kpis    = calcular_kpis_globales(df_join, spark)
        ranking = calcular_ranking_departamentos(df_join)
        tendencia = calcular_tendencia_mensual(df_join)
        analisis  = calcular_analisis_tarifa_cartera(df_join)
        rfm       = calcular_rfm(df_join)

        # 10. Guardar resultados
        dfs = {
            "tmp_estadisticas_historicas": stats,
            "kpis_globales":               kpis,
            "ranking_departamentos":       ranking,
            "tendencia_mensual":           tendencia,
            "analisis_tarifa_cartera":     analisis,
            "rfm_clientes":                rfm
        }
        guardar_resultados(dfs, RUTA_RESULTADOS)

        # 11. Validar OE2
        oe2 = validar_oe2(stats)

        # 12. Resumen final
        elapsed = time.time() - inicio
        print("\n" + "=" * 60)
        print("RESUMEN BATCH LAYER")
        print("=" * 60)
        print(f"  Tiempo de ejecucion: {elapsed:.2f} segundos")
        print(f"  TMP_ESTADISTICAS_HISTORICAS: {stats.count():,} filas x {len(stats.columns)} cols")
        print(f"  KPIs globales: {kpis.count():,} indicadores")
        print(f"  Ranking departamentos: {ranking.count():,} filas")
        print(f"  Tendencia mensual: {tendencia.count():,} periodos")
        print(f"  Analisis tarifa/cartera: {analisis.count():,} filas")
        print(f"  RFM clientes: {rfm.count():,} clientes segmentados")
        print(f"  OE2 cumplido: {'SI' if oe2 else 'NO'}")
        print("=" * 60)

        return stats, kpis, ranking, tendencia, analisis, rfm, oe2

    except Exception as e:
        print(f"\nERROR en execute: {e}")
        elapsed = time.time() - inicio
        print(f"\n  [!]  PIPELINE INTERRUMPIDO tras {elapsed:.2f} segundos")
        raise
    finally:
        if spark is not None:
            print("\n  [LIMPIEZA] Deteniendo Spark Session...")
            spark.stop()
            print("  [OK] Spark Session detenida")
            print("  Spark Session cerrada.")


# ──────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    execute()
