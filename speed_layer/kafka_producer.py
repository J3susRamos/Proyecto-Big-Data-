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
# ── constantes de conexion kafka ─────────────────────────────────────
kafka_topic = "hidrandina-consumo"
kafka_bootstrap_servers = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
kafka_client_id = "hidrandina-producer-01"

# ── rutas del proyecto ───────────────────────────────────────────────
data_path = os.environ.get("RUTA_DATA", os.path.join(os.path.dirname(__file__), "..", "data"))
simulation_events_path = os.path.join(data_path, "eventos_kafka_simulados.json")
simple_events_path = os.path.join(data_path, "eventos_simples.json")


# =====================================================================
# 1. verificacion / conexion kafka
# =====================================================================

def check_kafka_available():
    """
    Verifica si el broker Kafka responde en kafka_bootstrap_servers.

    Retorna:
        bool: True si Kafka esta disponible, False en caso contrario.
    """
    try:
        from confluent_kafka import Producer
        from confluent_kafka.admin import AdminClient
        admin_client = AdminClient({'bootstrap.servers': kafka_bootstrap_servers,
                                     'socket.timeout.ms': 3000})
        topic_metadata = admin_client.list_topics(timeout=3)
        print(f"  Kafka disponible en {kafka_bootstrap_servers}")
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
        kafka_producer = Producer({
            'bootstrap.servers': kafka_bootstrap_servers,
            'client.id': kafka_client_id,
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
        return kafka_producer
    except Exception as e:
        print(f"  ERROR creando productor Kafka: {e}")
        return None


# =====================================================================
# 2. lectura del csv limpio
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
            path = os.path.join(data_path, "FACT_CONSUMO.csv")

        if not os.path.exists(path):
            print(f"ERROR: No se encuentra el CSV en {path}")
            fallback_path = os.path.join(data_path, "FACT_CONSUMO.csv")
            if os.path.exists(fallback_path):
                path = fallback_path
                print(f"  Usando ruta alternativa: {path}")
            else:
                print("  Ejecute primero loader.py para generar el CSV limpio.")
                return []

        print(f"Leyendo CSV: {path}")
        import pandas as pd
        csv_chunks = []
        for csv_chunk in pd.read_csv(path, encoding="utf-8-sig", chunksize=50000):
            csv_chunks.append(csv_chunk)
            total_rows_read = sum(len(chunk) for chunk in csv_chunks)
            print(f"  Registros leidos: {total_rows_read:,}")
            if max_events > 0 and total_rows_read >= max_events:
                break

        events_df = pd.concat(csv_chunks)
        if max_events > 0:
            events_df = events_df.head(max_events)
        event_rows = events_df.to_dict(orient="records")

        print(f"  Total registros leidos: {len(event_rows):,}")

        # ordenar por fecha_emision y nro_doc_fac como criterio de desempate
        ordered_event_rows = sorted(
            event_rows,
            key=lambda row: (
                str(row.get("FECHA_EMISION", "") or "").strip() or "00000000",
                str(row.get("NRO_DOC_FAC", "0")).strip().zfill(20)
            )
        )
        print(f"  Registros ordenados por FECHA_EMISION + NRO_DOC_FAC")

        return ordered_event_rows

    except Exception as e:
        print(f"ERROR en read_ordered_csv: {e}")
        return []


# =====================================================================
# 3. publicacion kafka
# =====================================================================

def publish_kafka_event(kafka_producer, event, event_key):
    """
    Publica un evento individual al topic de Kafka.

    Parametros:
        kafka_producer (Producer): Productor kafka activo (confluent_kafka).
        event (dict): Diccionario con los datos del evento.
        event_key (str): Clave del mensaje (NRO_DOC_FAC).

    Retorna:
        bool: True si se confirmo la publicacion.
    """
    while True:
        try:
            kafka_producer.produce(
                kafka_topic,
                key=str(event_key).encode("utf-8"),
                value=json.dumps(event).encode("utf-8")
            )
            return True
        except BufferError:
            kafka_producer.poll(0.1)
        except Exception as e:
            print(f"  Error publicando {event_key}: {e}")
            return False


def publish_kafka_batch(kafka_producer, event_rows, batch_size=5000, stream_delay_ms=0, start_time=None):
    """
    Publica una lista de eventos a Kafka en lotes con reporte de progreso.

    Parametros:
        kafka_producer (KafkaProducer): Productor kafka activo.
        event_rows (list): Lista de diccionarios a publicar.
        batch_size (int): Cada cuantos eventos hacer flush y reportar.
        stream_delay_ms (int): Pausa entre flushes para simular streaming real.

    Retorna:
        int: Numero de eventos publicados exitosamente.
    """
    total_events = len(event_rows)
    published_count = 0
    if start_time is None:
        start_time = time.time()

    for event_index, event in enumerate(event_rows):
        event_key = str(event.get("NRO_DOC_FAC", "0"))
        if publish_kafka_event(kafka_producer, event, event_key):
            published_count += 1

        if (event_index + 1) % batch_size == 0:
            kafka_producer.poll(0)
            progress_pct = (event_index + 1) / total_events * 100
            events_per_second = published_count / (time.time() - start_time) if event_index > 0 else 0
            remaining_seconds = (total_events - event_index - 1) / events_per_second if events_per_second > 0 else 0
            print(f"  Progreso: {event_index+1:,}/{total_events:,} ({progress_pct:.1f}%) "
                  f"| {events_per_second:,.0f} evt/s | ~{remaining_seconds:.0f}s restantes")
            if stream_delay_ms > 0:
                time.sleep(stream_delay_ms / 1000.0)

    unconfirmed_count = kafka_producer.flush(timeout=30)
    if unconfirmed_count > 0:
        print(f"  ADVERTENCIA: {unconfirmed_count} mensajes no confirmados")
    return published_count


def real_kafka_mode(event_rows):
    """
    Ejecuta el productor en modo real con Kafka.

    Parametros:
        event_rows (list): Lista de diccionarios a publicar.
    """
    if not event_rows:
        print("  No hay datos para publicar.")
        return

    kafka_producer = get_kafka_producer()
    if kafka_producer is None:
        print("ERROR: No se pudo crear el productor Kafka.")
        return

    total_events = len(event_rows)
    print(f"\nPublicando {total_events:,} eventos a Kafka...")
    print(f"  Topic: '{kafka_topic}'")
    print(f"  Bootstrap servers: {kafka_bootstrap_servers}")

    start_time = time.time()
    published_count = publish_kafka_batch(kafka_producer, event_rows, start_time=start_time)
    kafka_producer.flush(timeout=30)
    elapsed_seconds = time.time() - start_time

    events_per_second = published_count / elapsed_seconds if elapsed_seconds > 0 else 0
    print(f"\nResumen Kafka:")
    print(f"  Publicados: {published_count:,} / {total_events:,}")
    print(f"  Duracion:   {elapsed_seconds:.2f} s")
    print(f"  Tasa:       {events_per_second:,.0f} eventos/s")

    # guardar copia json para compatibilidad con serving layer
    # (comentado para acelerar modo real — descomentar si serving layer lo necesita)
    # print("\nGuardando copia local para serving layer...")
    # simulated_mode(event_rows)


# =====================================================================
# 4. modo simulado (sin kafka)
# =====================================================================

def simulated_mode(event_rows):
    """
    Guarda los eventos en archivos JSON para consumo offline.

    Genera dos archivos:
        - eventos_kafka_simulados.json: con metadatos de simulacion
        - eventos_simples.json: solo los datos planos (para spark_streaming)

    Parametros:
        event_rows (list): Lista de diccionarios a guardar.
    """
    if not event_rows:
        print("  No hay datos para simular.")
        return

    total_events = len(event_rows)
    print(f"\nGuardando {total_events:,} eventos simulados... (por lotes)")

    simulation_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(os.path.dirname(simulation_events_path), exist_ok=True)

    # ── archivo con metadatos (escritura por lotes) ─────────────────
    with open(simulation_events_path, "w", encoding="utf-8") as simulation_file:
        simulation_file.write("[\n")
        for event_index, event_row in enumerate(event_rows):
            simulated_event = {
                "_id_evento": f"SIM-{simulation_timestamp}-{event_index+1:08d}",
                "_timestamp_simulacion": datetime.now().isoformat(),
                "datos": event_row
            }
            if event_index > 0:
                simulation_file.write(",\n")
            json.dump(simulated_event, simulation_file, ensure_ascii=False)
            if (event_index + 1) % 50000 == 0:
                print(f"  Metadatos: {event_index+1:,}/{total_events:,}")
        simulation_file.write("\n]\n")
    print(f"  Con metadatos: {simulation_events_path}")

    # ── archivo simple (solo datos) ────────────────────────────────
    with open(simple_events_path, "w", encoding="utf-8") as simple_events_file:
        simple_events_file.write("[\n")
        for event_index, event_row in enumerate(event_rows):
            if event_index > 0:
                simple_events_file.write(",\n")
            json.dump(event_row, simple_events_file, ensure_ascii=False)
            if (event_index + 1) % 50000 == 0:
                print(f"  Simple: {event_index+1:,}/{total_events:,}")
        simple_events_file.write("\n]\n")
    print(f"  Solo datos:    {simple_events_path}")
    print(f"  Total eventos guardados: {total_events:,}")


# =====================================================================
# 5. orquestacion
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

    event_rows = read_ordered_csv()
    if not event_rows:
        print("ERROR: No se pudieron leer los datos del CSV limpio.")
        return False

    if mode == "auto":
        if check_kafka_available():
            real_kafka_mode(event_rows)
        else:
            print("  -> Cambiando a modo simulado")
            simulated_mode(event_rows)
    elif mode == "real":
        real_kafka_mode(event_rows)
    elif mode == "simulado":
        simulated_mode(event_rows)
    else:
        print(f"Modo desconocido: '{mode}'. Use: auto, real o simulado")
        return False

    print("\n" + "=" * 60)
    print("  KAFKA PRODUCER FINALIZADO")
    print("=" * 60)
    return True


# =====================================================================
# entry point
# =====================================================================

if __name__ == "__main__":
    selected_mode = sys.argv[1] if len(sys.argv) > 1 else "auto"
    execute(selected_mode)
