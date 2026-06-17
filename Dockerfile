# =============================================================================
# Dockerfile — Pipeline Lambda Hidrandina (PySpark + Kafka)
# =============================================================================
# Base: Python 3.11 slim + OpenJDK 17 (requerido por Spark)
# =============================================================================

FROM python:3.11-slim

LABEL maintainer="Hidrandina Big Data Team"
LABEL description="Arquitectura Lambda para deteccion de anomalias en consumo electrico"

# ── Evitar prompts interactivos ──────────────────────────────────────────
ENV DEBIAN_FRONTEND=noninteractive

# ── Instalar Java 17 (requerido por PySpark) ─────────────────────────────
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jdk-headless \
        wget \
        ca-certificates \
        procps \
    && rm -rf /var/lib/apt/lists/*

# ── Variables de entorno Java y Spark ────────────────────────────────────
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# ── Descargar Spark 4.1.2 (sin Hadoop incluido) ──────────────────────────
RUN wget -q https://archive.apache.org/dist/spark/spark-4.1.2/spark-4.1.2-bin-without-hadoop.tgz -O /tmp/spark.tgz && \
    mkdir -p $SPARK_HOME && \
    tar -xzf /tmp/spark.tgz -C $SPARK_HOME --strip-components=1 && \
    rm /tmp/spark.tgz

# ── Variables adicionales de Spark ───────────────────────────────────────
ENV SPARK_DIST_CLASSPATH=$SPARK_HOME/jars/*
ENV PYTHONPATH="$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*-src.zip:$PYTHONPATH"

# ── Establecer directorio de trabajo ─────────────────────────────────────
WORKDIR /app

# ── Copiar requirements primero (cache de capas Docker) ──────────────────
COPY requirements.txt .

# ── Instalar dependencias Python ─────────────────────────────────────────
RUN pip install --no-cache-dir -r requirements.txt

# ── Copiar codigo fuente del proyecto ────────────────────────────────────
COPY main.py .
COPY utils/ ./utils/
COPY batch_layer/ ./batch_layer/
COPY speed_layer/ ./speed_layer/
COPY serving_layer/ ./serving_layer/
COPY serve_dashboard.py .
COPY entrypoint.sh .

# ── Copiar dashboard.html y JSONs pre-computados a la carpeta de salida ─
RUN mkdir -p /app/output/dashboard_data && \
    cp /app/serving_layer/dashboard.html /app/output/dashboard.html && \
    cp /app/serving_layer/dashboard_data/*.json /app/output/dashboard_data/

# ── Crear directorios de datos y resultados ─────────────────────────────
RUN mkdir -p /app/data /app/checkpoint

# ── Variables de entorno por defecto ─────────────────────────────────────
ENV RUTA_PROYECTO=/app
ENV RUTA_DATA=/app/data
ENV RUTA_SERVING=/app/output
ENV RUTA_CSV_ORIGINALES=/app/data/originales
ENV SPARK_MASTER=local[*]
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV KAFKA_TOPIC=hidrandina-consumo
ENV PYTHONUNBUFFERED=1
ENV MODO=simulado

# ── Entry point: entrypoint.sh ──────────────────────────────────────────
RUN chmod +x /app/entrypoint.sh
ENTRYPOINT ["/app/entrypoint.sh"]
