"""
kafka_producer.py — Productor de eventos para el pipeline Lambda de Hidrandina

Lee el CSV limpio (hidrandina_limpio.csv) ordenado por FECHA_EMISION
y publica cada fila como evento JSON al topic 'hidrandina-consumo'.

Modo simulado (sin Kafka):
    Si Kafka no esta disponible, escribe los eventos en un archivo JSON
    para que spark_streaming.py pueda leerlos posteriormente.

Uso:
    python kafka_producer.py            # auto: detecta Kafka o simula
    python kafka_producer.py real       # fuerza modo Kafka
    python kafka_producer.py simulado   # fuerza modo JSON
"""

import os
import json
import time
import csv
import sys
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
# ── Constantes de conexion Kafka ─────────────────────────────────────
KAFKA_TOPIC = "hidrandina-consumo"
KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
KAFKA_CLIENT_ID = "hidrandina-producer-01"

# ── Rutas del proyecto ───────────────────────────────────────────────
DATA_PATH = os.environ.get("RUTA_DATA", os.path.join(os.path.dirname(__file__), "..", "data"))
SIMULATION_PATH = os.path.join(DATA_PATH, "eventos_kafka_simulados.json")
SIMPLE_EVENTS_PATH = os.path.join(DATA_PATH, "eventos_simples.json")


# =====================================================================
# 1. VERIFICACION / CONEXION KAFKA
# =====================================================================

def check_kafka_available():
    """
    Verifica si el broker Kafka responde en bootstrap_servers.

    Retorna:
        bool: True si Kafka esta disponible, False en caso contrario.
    """
    try:
        from confluent_kafka import Producer
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                             'socket.timeout.ms': 3000})
        meta = admin.list_topics(timeout=3)
        print(f"  Kafka disponible en {KAFKA_BOOTSTRAP_SERVERS}")
        return True
    except ImportError:
        print("  confluent-kafka no instalado. Use: pip install confluent-kafka")
        return False
    except Exception as e:
        print(f"  Kafka NO disponible: {e}")
        return False


def get_kafka_producer():
    """
    Crea un productor Kafka con serializacion JSON.

    Retorna:
        Producer: Productor listo para enviar, o None si falla.
    """
    try:
        from confluent_kafka import Producer
        producer = Producer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': KAFKA_CLIENT_ID,
            'acks': '1',
            'retries': 5,
            'batch.size': 131072,
            'linger.ms': 50,
            'compression.type': 'gzip',
            'message.timeout.ms': 30000,
            'socket.timeout.ms': 10000,
            'queue.buffering.max.messages': 1000000,
            'queue.buffering.max.kbytes': 524288,
        })
        return producer
    except Exception as e:
        print(f"  ERROR creando productor Kafka: {e}")
        return None


# =====================================================================
# 2. LECTURA DEL CSV LIMPIO
# =====================================================================

def read_ordered_csv(path=None, max_events=500000):
    """
    Lee hidrandina_limpio.csv y lo ordena por FECHA_EMISION ascendente.

    Lee hasta max_events registros para mantener el proceso rapido en Windows.
    Las columnas incluyen las 15 necesarias para el evento (factura + ubicacion).

    Parametros:
        path (str): Ruta al CSV. Por defecto data/hidrandina_limpio.csv.
        max_events (int): Maximo de eventos a leer (0 = todos).

    Retorna:
        list: Lista de diccionarios ordenados cronologicamente.
              Vacia si no se encuentra el archivo o hay error.
    """
    try:
        if path is None:
            path = os.path.join(DATA_PATH, "FACT_CONSUMO.csv")

        if not os.path.exists(path):
            print(f"ERROR: No se encuentra el CSV en {path}")
            fallback_path = os.path.join(DATA_PATH, "FACT_CONSUMO.csv")
            if os.path.exists(fallback_path):
                path = fallback_path
                print(f"  Usando ruta alternativa: {path}")
            else:
                print("  Ejecute primero loader.py para generar el CSV limpio.")
                return []

        print(f"Leyendo CSV: {path}")
        import pandas as pd
        chunks = []
        for chunk in pd.read_csv(path, encoding="utf-8-sig", chunksize=50000):
            chunks.append(chunk)
            total_leido = sum(len(c) for c in chunks)
            print(f"  Registros leidos: {total_leido:,}")
            if max_events > 0 and total_leido >= max_events:
                break

        df = pd.concat(chunks)
        if max_events > 0:
            df = df.head(max_events)
        rows = df.to_dict(orient="records")

        print(f"  Total registros leidos: {len(rows):,}")

        # Ordenar por FECHA_EMISION y NRO_DOC_FAC como criterio de desempate
        ordered_rows = sorted(
            rows,
            key=lambda x: (
                str(x.get("FECHA_EMISION", "") or "").strip() or "00000000",
                str(x.get("NRO_DOC_FAC", "0")).strip().zfill(20)
            )
        )
        print(f"  Registros ordenados por FECHA_EMISION + NRO_DOC_FAC")

        return ordered_rows

    except Exception as e:
        print(f"ERROR en read_ordered_csv: {e}")
        return []


# =====================================================================
# 3. PUBLICACION KAFKA
# =====================================================================

def publish_kafka_event(producer, event, key):
    """
    Publica un evento individual al topic de Kafka.

    Parametros:
        producer (Producer): Productor kafka activo (confluent_kafka).
        event (dict): Diccionario con los datos del evento.
        key (str): Clave del mensaje (NRO_DOC_FAC).

    Retorna:
        bool: True si se confirmo la publicacion.
    """
    while True:
        try:
            producer.produce(
                KAFKA_TOPIC,
                key=str(key).encode("utf-8"),
                value=json.dumps(event).encode("utf-8")
            )
            return True
        except BufferError:
            producer.poll(0.1)
        except Exception as e:
            print(f"  Error publicando {key}: {e}")
            return False


def publish_kafka_batch(producer, events, batch_size=5000, stream_delay_ms=0, start_time=None):
    """
    Publica una lista de eventos a Kafka en lotes con reporte de progreso.

    Parametros:
        producer (KafkaProducer): Productor kafka activo.
        events (list): Lista de diccionarios a publicar.
        batch_size (int): Cada cuantos eventos hacer flush y reportar.
        stream_delay_ms (int): Pausa entre flushes para simular streaming real.

    Retorna:
        int: Numero de eventos publicados exitosamente.
    """
    total = len(events)
    published = 0
    if start_time is None:
        start_time = time.time()

    for i, event in enumerate(events):
        key = str(event.get("NRO_DOC_FAC", "0"))
        if publish_kafka_event(producer, event, key):
            published += 1

        if (i + 1) % batch_size == 0:
            producer.poll(0)
            pct = (i + 1) / total * 100
            rate = published / (time.time() - start_time) if i > 0 else 0
            remaining = (total - i - 1) / rate if rate > 0 else 0
            print(f"  Progreso: {i+1:,}/{total:,} ({pct:.1f}%) "
                  f"| {rate:,.0f} evt/s | ~{remaining:.0f}s restantes")
            if stream_delay_ms > 0:
                time.sleep(stream_delay_ms / 1000.0)

    remaining = producer.flush(timeout=30)
    if remaining > 0:
        print(f"  ADVERTENCIA: {remaining} mensajes no confirmados")
    return published


def real_kafka_mode(rows):
    """
    Ejecuta el productor en modo real con Kafka.

    Parametros:
        rows (list): Lista de diccionarios a publicar.
    """
    if not rows:
        print("  No hay datos para publicar.")
        return

    producer = get_kafka_producer()
    if producer is None:
        print("ERROR: No se pudo crear el productor Kafka.")
        return

    total = len(rows)
    print(f"\nPublicando {total:,} eventos a Kafka...")
    print(f"  Topic: '{KAFKA_TOPIC}'")
    print(f"  Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")

    start_time = time.time()
    published = publish_kafka_batch(producer, rows, start_time=start_time)
    producer.flush(timeout=30)
    elapsed_time = time.time() - start_time

    rate = published / elapsed_time if elapsed_time > 0 else 0
    print(f"\nResumen Kafka:")
    print(f"  Publicados: {published:,} / {total:,}")
    print(f"  Duracion:   {elapsed_time:.2f} s")
    print(f"  Tasa:       {rate:,.0f} eventos/s")

    # Guardar copia JSON para compatibilidad con serving layer
    # (comentado para acelerar modo real — descomentar si serving layer lo necesita)
    # print("\nGuardando copia local para serving layer...")
    # simulated_mode(rows)


# =====================================================================
# 4. MODO SIMULADO (SIN KAFKA)
# =====================================================================

def simulated_mode(rows):
    """
    Guarda los eventos en archivos JSON para consumo offline.

    Genera dos archivos:
        - eventos_kafka_simulados.json: con metadatos de simulacion
        - eventos_simples.json: solo los datos planos (para spark_streaming)

    Parametros:
        rows (list): Lista de diccionarios a guardar.
    """
    if not rows:
        print("  No hay datos para simular.")
        return

    total = len(rows)
    print(f"\nGuardando {total:,} eventos simulados... (por lotes)")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(os.path.dirname(SIMULATION_PATH), exist_ok=True)

    # ── Archivo con metadatos (escritura por lotes) ─────────────────
    with open(SIMULATION_PATH, "w", encoding="utf-8") as f:
        f.write("[\n")
        for i, row in enumerate(rows):
            event = {
                "_id_evento": f"SIM-{timestamp}-{i+1:08d}",
                "_timestamp_simulacion": datetime.now().isoformat(),
                "datos": row
            }
            if i > 0:
                f.write(",\n")
            json.dump(event, f, ensure_ascii=False)
            if (i + 1) % 50000 == 0:
                print(f"  Metadatos: {i+1:,}/{total:,}")
        f.write("\n]\n")
    print(f"  Con metadatos: {SIMULATION_PATH}")

    # ── Archivo simple (solo datos) ────────────────────────────────
    with open(SIMPLE_EVENTS_PATH, "w", encoding="utf-8") as f:
        f.write("[\n")
        for i, row in enumerate(rows):
            if i > 0:
                f.write(",\n")
            json.dump(row, f, ensure_ascii=False)
            if (i + 1) % 50000 == 0:
                print(f"  Simple: {i+1:,}/{total:,}")
        f.write("\n]\n")
    print(f"  Solo datos:    {SIMPLE_EVENTS_PATH}")
    print(f"  Total eventos guardados: {total:,}")


# =====================================================================
# 5. ORQUESTACION
# =====================================================================

def execute(mode="auto"):
    """
    Ejecuta el productor de eventos segun el mode indicado.

    Parametros:
        mode (str): "auto"     → detecta Kafka, si no, simula
                    "real"     → fuerza Kafka
                    "simulado" → fuerza archivo JSON

    Retorna:
        bool: True si se completo sin errores graves.
    """
    print("=" * 60)
    print("  KAFKA PRODUCER — Publicacion de eventos de consumo")
    print("=" * 60)
    print(f"  Mode: {mode}")
    print()

    rows = read_ordered_csv()
    if not rows:
        print("ERROR: No se pudieron leer los datos del CSV limpio.")
        return False

    if mode == "auto":
        if check_kafka_available():
            real_kafka_mode(rows)
        else:
            print("  -> Cambiando a modo simulado")
            simulated_mode(rows)
    elif mode == "real":
        real_kafka_mode(rows)
    elif mode == "simulado":
        simulated_mode(rows)
    else:
        print(f"Modo desconocido: '{mode}'. Use: auto, real o simulado")
        return False

    print("\n" + "=" * 60)
    print("  KAFKA PRODUCER FINALIZADO")
    print("=" * 60)
    return True


# =====================================================================
# ENTRY POINT
# =====================================================================

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "auto"
    execute(mode)
