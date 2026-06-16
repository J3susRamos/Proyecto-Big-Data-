"""
alert_api.py — API REST de alertas Hidrandina

Consume el topic 'hidrandina-alertas' de Kafka y expone endpoints
para consultar alertas de consumo eléctrico.

Endpoints:
    GET /health          — Healthcheck
    GET /alertas         — Todas las alertas (paginado)
    GET /alertas/stats   — Estadísticas de alertas
    GET /alertas/{distrito} — Alertas por distrito
"""

import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from threading import Thread
from typing import Optional

import uvicorn
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_ALERTAS = "hidrandina-alertas"
API_PORT = int(os.environ.get("API_PORT", "8060"))

# Almacen en memoria de alertas
alertas_store = []
alertas_lock = asyncio.Lock()
MAX_ALERTAS = 10000


def wait_for_kafka(bootstrap_servers=None, timeout=30):
    """Espera a que Kafka esté disponible."""
    if bootstrap_servers is None:
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
    print(f"Esperando Kafka en {bootstrap_servers}...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            from kafka import KafkaConsumer
            consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                api_version_auto_timeout_ms=3000,
                request_timeout_ms=3000,
                consumer_timeout_ms=3000,
            )
            consumer.close()
            print("  Kafka disponible")
            return True
        except Exception as e:
            elapsed = time.time() - start
            print(f"  Esperando... ({elapsed:.0f}s) {e}")
            time.sleep(3)
    print(f"ERROR: Kafka no disponible tras {timeout}s")
    return False


def consume_alertas_background():
    """Consume alertas de Kafka en segundo plano."""
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            KAFKA_TOPIC_ALERTAS,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="hidrandina-api-alertas",
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            request_timeout_ms=30000,
            session_timeout_ms=15000,
        )
        print(f"Consumiendo alertas desde '{KAFKA_TOPIC_ALERTAS}'...")
        for msg in consumer:
            alerta = msg.value
            alerta["_partition"] = msg.partition
            alerta["_offset"] = msg.offset
            alerta["_timestamp"] = msg.timestamp
            alertas_store.append(alerta)
            # Limitar tamaño del store
            if len(alertas_store) > MAX_ALERTAS:
                alertas_store[:1000] = []
    except Exception as e:
        print(f"Error en consumer de alertas: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Inicia el consumer de Kafka al arrancar."""
    if wait_for_kafka():
        thread = Thread(target=consume_alertas_background, daemon=True)
        thread.start()
        print("Consumer de alertas iniciado en segundo plano")
    else:
        print("ADVERTENCIA: Kafka no disponible, API sin datos en vivo")
    yield


app = FastAPI(
    title="Hidrandina Alertas API",
    description="API REST de alertas de consumo eléctrico",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    """Healthcheck del servicio."""
    return {
        "status": "ok",
        "alertas_count": len(alertas_store),
        "kafka": KAFKA_BOOTSTRAP_SERVERS,
        "topic": KAFKA_TOPIC_ALERTAS,
    }


@app.get("/alertas")
def get_alertas(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    nivel: Optional[str] = None,
    departamento: Optional[str] = None,
    distrito: Optional[str] = None,
):
    """Lista todas las alertas con paginación y filtros."""
    resultados = list(alertas_store)

    if nivel:
        resultados = [a for a in resultados if a.get("nivel", "").upper() == nivel.upper()]
    if departamento:
        resultados = [a for a in resultados if departamento.upper() in a.get("DEPARTAMENTO", "").upper()]
    if distrito:
        resultados = [a for a in resultados if distrito.upper() in a.get("DISTRITO", "").upper()]

    total = len(resultados)
    page = resultados[skip:skip + limit]

    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "data": page,
    }


@app.get("/alertas/stats")
def get_alertas_stats():
    """Estadísticas agregadas de alertas."""
    if not alertas_store:
        return {"total": 0, "por_departamento": {}, "por_nivel": {}}

    por_nivel = {}
    por_departamento = {}
    consumo_total = 0.0

    for a in alertas_store:
        nivel = a.get("nivel", "DESCONOCIDO")
        por_nivel[nivel] = por_nivel.get(nivel, 0) + 1

        dept = a.get("DEPARTAMENTO", "DESCONOCIDO")
        if dept not in por_departamento:
            por_departamento[dept] = {"total": 0, "consumo_total": 0.0, "distritos": set()}
        por_departamento[dept]["total"] += 1
        por_departamento[dept]["consumo_total"] += a.get("consumo_total_hora", 0)
        por_departamento[dept]["distritos"].add(a.get("DISTRITO", ""))
        consumo_total += a.get("consumo_total_hora", 0)

    dept_stats = {}
    for d, s in por_departamento.items():
        dept_stats[d] = {
            "total_alertas": s["total"],
            "consumo_total_hora": round(s["consumo_total"], 2),
            "distritos_afectados": len(s["distritos"]),
        }

    consumo_promedio = round(consumo_total / len(alertas_store), 2) if alertas_store else 0

    return {
        "total": len(alertas_store),
        "consumo_total_hora": round(consumo_total, 2),
        "consumo_promedio_hora": consumo_promedio,
        "por_nivel": por_nivel,
        "por_departamento": dept_stats,
    }


@app.get("/alertas/{distrito}")
def get_alertas_por_distrito(distrito: str):
    """Alertas filtradas por distrito."""
    resultados = [
        a for a in alertas_store
        if distrito.upper() in a.get("DISTRITO", "").upper()
    ]
    return {
        "distrito": distrito,
        "total": len(resultados),
        "data": resultados,
    }


def main():
    print(f"Iniciando API de alertas en puerto {API_PORT}...")
    uvicorn.run(app, host="0.0.0.0", port=API_PORT)


if __name__ == "__main__":
    main()
