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
RUTA_DATA = os.environ.get("RUTA_DATA", os.path.join(os.path.dirname(__file__), "..", "data"))
RUTA_SIMULACION = os.path.join(RUTA_DATA, "eventos_kafka_simulados.json")
RUTA_EVENTOS_SIMPLES = os.path.join(RUTA_DATA, "eventos_simples.json")


# =====================================================================
# 1. VERIFICACION / CONEXION KAFKA
# =====================================================================

def verificar_kafka_disponible():
    """
    Verifica si el broker Kafka responde en bootstrap_servers.

    Retorna:
        bool: True si Kafka esta disponible, False en caso contrario.
    """
    try:
        from kafka import KafkaProducer
        productor = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            api_version_auto_timeout_ms=3000,
            max_block_ms=3000,
            request_timeout_ms=3000
        )
        # Si llega hasta aqui sin excepcion, Kafka responde
        productor.close(timeout=1)
        print(f"  Kafka disponible en {KAFKA_BOOTSTRAP_SERVERS}")
        return True
    except ImportError:
        print("  kafka-python no instalado. Use: pip install kafka-python")
        return False
    except Exception as e:
        print(f"  Kafka NO disponible: {e}")
        return False


def obtener_productor_kafka():
    """
    Crea un productor Kafka con serializacion JSON.

    Retorna:
        KafkaProducer: Productor listo para enviar, o None si falla.
    """
    try:
        from kafka import KafkaProducer
        productor = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id=KAFKA_CLIENT_ID,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
            acks="all",
            retries=5,
            batch_size=65536,
            linger_ms=50,
            max_block_ms=15000,
            compression_type="gzip"
        )
        return productor
    except Exception as e:
        print(f"  ERROR creando productor Kafka: {e}")
        return None


# =====================================================================
# 2. LECTURA DEL CSV LIMPIO
# =====================================================================

def leer_csv_ordenado(ruta=None):
    """
    Lee hidrandina_limpio.csv y lo ordena por FECHA_EMISION ascendente.

    Lee todas las columnas del CSV completo (incluye las 15 columnas
    necesarias para el evento: factura + ubicacion).

    Parametros:
        ruta (str): Ruta al CSV. Por defecto data/hidrandina_limpio.csv.

    Retorna:
        list: Lista de diccionarios ordenados cronologicamente.
              Vacia si no se encuentra el archivo o hay error.
    """
    try:
        if ruta is None:
            ruta = os.path.join(RUTA_DATA, "hidrandina_limpio.csv")

        if not os.path.exists(ruta):
            print(f"ERROR: No se encuentra el CSV en {ruta}")
            ruta_fallback = os.path.join(RUTA_DATA, "hidrandina_limpio.csv")
            if os.path.exists(ruta_fallback):
                ruta = ruta_fallback
                print(f"  Usando ruta alternativa: {ruta}")
            else:
                print("  Ejecute primero loader.py para generar el CSV limpio.")
                return []

        print(f"Leyendo CSV: {ruta}")
        with open(ruta, "r", encoding="utf-8-sig") as f:
            lector = csv.DictReader(f)
            filas = list(lector)

        total = len(filas)
        print(f"  Total registros leidos: {total:,}")

        # Ordenar por FECHA_EMISION y NRO_SERVICIO como criterio de desempate
        filas_ordenadas = sorted(
            filas,
            key=lambda x: (
                x.get("FECHA_EMISION", "").strip() or "00000000",
                str(x.get("NRO_SERVICIO", "0")).strip().zfill(10)
            )
        )
        print(f"  Registros ordenados por FECHA_EMISION + NRO_SERVICIO")

        return filas_ordenadas

    except Exception as e:
        print(f"ERROR en leer_csv_ordenado: {e}")
        return []


# =====================================================================
# 3. PUBLICACION KAFKA
# =====================================================================

def publicar_evento_kafka(productor, evento, clave):
    """
    Publica un evento individual al topic de Kafka.

    Parametros:
        productor (KafkaProducer): Productor kafka activo.
        evento (dict): Diccionario con los datos del evento.
        clave (str): Clave del mensaje (NRO_SERVICIO).

    Retorna:
        bool: True si se confirmo la publicacion.
    """
    try:
        futuro = productor.send(
            KAFKA_TOPIC,
            key=clave,
            value=evento
        )
        futuro.get(timeout=10)
        return True
    except Exception as e:
        print(f"  Error publicando {clave}: {e}")
        return False


def publicar_lote_kafka(productor, eventos, tamano_lote=500):
    """
    Publica una lista de eventos a Kafka en lotes con reporte de progreso.

    Parametros:
        productor (KafkaProducer): Productor kafka activo.
        eventos (list): Lista de diccionarios a publicar.
        tamano_lote (int): Cada cuantos eventos hacer flush y reportar.

    Retorna:
        int: Numero de eventos publicados exitosamente.
    """
    total = len(eventos)
    exitosos = 0

    for i, evento in enumerate(eventos):
        clave = str(evento.get("NRO_SERVICIO", "0"))
        if publicar_evento_kafka(productor, evento, clave):
            exitosos += 1

        if (i + 1) % tamano_lote == 0:
            productor.flush()
            pct = (i + 1) / total * 100
            print(f"  Progreso: {i+1:,}/{total:,} ({pct:.1f}%)")

    productor.flush()
    return exitosos


def modo_real_kafka(filas):
    """
    Ejecuta el productor en modo real con Kafka.

    Parametros:
        filas (list): Lista de diccionarios a publicar.
    """
    if not filas:
        print("  No hay datos para publicar.")
        return

    productor = obtener_productor_kafka()
    if productor is None:
        print("ERROR: No se pudo crear el productor Kafka.")
        return

    total = len(filas)
    print(f"\nPublicando {total:,} eventos a Kafka...")
    print(f"  Topic: '{KAFKA_TOPIC}'")
    print(f"  Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")

    inicio = time.time()
    exitosos = publicar_lote_kafka(productor, filas)
    productor.close()
    duracion = time.time() - inicio

    tasa = exitosos / duracion if duracion > 0 else 0
    print(f"\nResumen Kafka:")
    print(f"  Publicados: {exitosos:,} / {total:,}")
    print(f"  Duracion:   {duracion:.2f} s")
    print(f"  Tasa:       {tasa:,.0f} eventos/s")


# =====================================================================
# 4. MODO SIMULADO (SIN KAFKA)
# =====================================================================

def modo_simulado(filas):
    """
    Guarda los eventos en archivos JSON para consumo offline.

    Genera dos archivos:
        - eventos_kafka_simulados.json: con metadatos de simulacion
        - eventos_simples.json: solo los datos planos (para spark_streaming)

    Parametros:
        filas (list): Lista de diccionarios a guardar.
    """
    if not filas:
        print("  No hay datos para simular.")
        return

    total = len(filas)
    print(f"\nGuardando {total:,} eventos simulados...")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # ── Archivo con metadatos ─────────────────────────────────────
    eventos_md = [
        {
            "_id_evento": f"SIM-{ts}-{i+1:08d}",
            "_timestamp_simulacion": datetime.now().isoformat(),
            "datos": fila
        }
        for i, fila in enumerate(filas)
    ]

    os.makedirs(os.path.dirname(RUTA_SIMULACION), exist_ok=True)
    with open(RUTA_SIMULACION, "w", encoding="utf-8") as f:
        json.dump(eventos_md, f, indent=1, ensure_ascii=False)
    print(f"  Con metadatos: {RUTA_SIMULACION}")

    # ── Archivo simple (solo datos, usado por spark_streaming) ────
    with open(RUTA_EVENTOS_SIMPLES, "w", encoding="utf-8") as f:
        json.dump(filas, f, indent=1, ensure_ascii=False)
    print(f"  Solo datos:    {RUTA_EVENTOS_SIMPLES}")

    print(f"  Total eventos guardados: {total:,}")


# =====================================================================
# 5. ORQUESTACION
# =====================================================================

def ejecutar(modo="auto"):
    """
    Ejecuta el productor de eventos segun el modo indicado.

    Parametros:
        modo (str): "auto"     → detecta Kafka, si no, simula
                    "real"     → fuerza Kafka
                    "simulado" → fuerza archivo JSON

    Retorna:
        bool: True si se completo sin errores graves.
    """
    print("=" * 60)
    print("  KAFKA PRODUCER — Publicacion de eventos de consumo")
    print("=" * 60)
    print(f"  Modo: {modo}")
    print()

    filas = leer_csv_ordenado()
    if not filas:
        print("ERROR: No se pudieron leer los datos del CSV limpio.")
        return False

    if modo == "auto":
        if verificar_kafka_disponible():
            modo_real_kafka(filas)
        else:
            print("  -> Cambiando a modo simulado")
            modo_simulado(filas)
    elif modo == "real":
        modo_real_kafka(filas)
    elif modo == "simulado":
        modo_simulado(filas)
    else:
        print(f"Modo desconocido: '{modo}'. Use: auto, real o simulado")
        return False

    print("\n" + "=" * 60)
    print("  KAFKA PRODUCER FINALIZADO")
    print("=" * 60)
    return True


# =====================================================================
# 6. ENTRY POINT
# =====================================================================

if __name__ == "__main__":
    modo = sys.argv[1] if len(sys.argv) > 1 else "auto"
    ejecutar(modo)
