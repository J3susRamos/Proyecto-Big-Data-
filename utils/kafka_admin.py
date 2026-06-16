"""
kafka_admin.py — Administracion de topics Kafka para el pipeline Hidrandina

Funciones:
    - check_kafka(bootstrap_servers, timeout=30): Verifica si Kafka responde
    - create_topic(bootstrap_servers, topic, partitions, replication, timeout=30):
      Crea un topic si no existe
    - ensure_topics(bootstrap_servers): Crea los topics necesarios del pipeline
"""

import os
import time
import sys

KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)

TOPICS = {
    "hidrandina-consumo": {"partitions": 3, "replication": 1, "retention_hours": 48},
    "hidrandina-alertas": {"partitions": 1, "replication": 1, "retention_hours": 168},
}


def check_kafka(bootstrap_servers=None, timeout=30):
    """
    Espera a que Kafka responda en bootstrap_servers.

    Parametros:
        bootstrap_servers (str): Servidor Kafka.
        timeout (int): Timeout maximo en segundos.

    Retorna:
        bool: True si Kafka esta disponible.
    """
    if bootstrap_servers is None:
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS

    print(f"Esperando conexion a Kafka en {bootstrap_servers}...")
    start = time.time()

    while time.time() - start < timeout:
        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                api_version_auto_timeout_ms=3000,
                max_block_ms=3000,
                request_timeout_ms=3000,
            )
            producer.close(timeout=1)
            print(f"  Kafka disponible en {bootstrap_servers}")
            return True
        except Exception as e:
            elapsed = time.time() - start
            print(f"  Esperando Kafka... ({elapsed:.0f}s) {e}")
            time.sleep(3)

    print(f"ERROR: Kafka no disponible tras {timeout}s")
    return False


def create_topic(bootstrap_servers=None, topic=None, partitions=3,
                 replication=1, retention_hours=48, timeout=30):
    """
    Crea un topic Kafka si no existe.

    Parametros:
        bootstrap_servers (str): Servidor Kafka.
        topic (str): Nombre del topic.
        partitions (int): Numero de particiones.
        replication (int): Factor de replicacion.
        retention_hours (int): Horas de retencion.
        timeout (int): Timeout en segundos.

    Retorna:
        bool: True si el topic existe (creado o ya existia).
    """
    if bootstrap_servers is None:
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS

    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError

        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            request_timeout_ms=timeout * 1000,
        )

        existing = admin.list_topics()
        if topic in existing:
            print(f"  Topic '{topic}' ya existe ({existing[topic]})")
            admin.close()
            return True

        topic_config = {
            "retention.ms": str(retention_hours * 3600 * 1000),
            "retention.bytes": "-1",
        }

        new_topic = NewTopic(
            name=topic,
            num_partitions=partitions,
            replication_factor=replication,
            topic_configs=topic_config,
        )

        admin.create_topics([new_topic])
        print(f"  Topic '{topic}' creado: {partitions} particiones, "
              f"replicacion {replication}, retencion {retention_hours}h")
        admin.close()
        return True

    except TopicAlreadyExistsError:
        print(f"  Topic '{topic}' ya existe")
        return True
    except ImportError:
        print("  kafka-python no instalado (pip install kafka-python)")
        return False
    except Exception as e:
        print(f"  Error creando topic '{topic}': {e}")
        return False


def ensure_topics(bootstrap_servers=None):
    """
    Verifica Kafka y crea todos los topics del pipeline.

    Parametros:
        bootstrap_servers (str): Servidor Kafka.

    Retorna:
        bool: True si todos los topics estan listos.
    """
    if bootstrap_servers is None:
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS

    if not check_kafka(bootstrap_servers):
        return False

    print("\nCreando topics...")
    all_ok = True
    for name, config in TOPICS.items():
        ok = create_topic(
            bootstrap_servers=bootstrap_servers,
            topic=name,
            partitions=config["partitions"],
            replication=config["replication"],
            retention_hours=config["retention_hours"],
        )
        if not ok:
            all_ok = False

    return all_ok


if __name__ == "__main__":
    success = ensure_topics()
    sys.exit(0 if success else 1)
