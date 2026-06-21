"""
main.py — Orquestador principal del pipeline Lambda de Hidrandina

Ejecuta las 6 etapas del pipeline en orden:
    1. LOADER: carga y limpia los 31 CSV, separa en FACT_CONSUMO y DIM_CLIENTE_UBICACION
    2. BATCH: procesa el historico con PySpark, genera TMP_ESTADISTICAS_HISTORICAS
    3. PRODUCER: publica eventos a Kafka (o modo simulado)
    4. STREAMING: consume eventos, enriquece con estadisticas, clasifica anomalias
    5. SERVING: une batch+stream, genera 17 columnas, resumenes y dashboard
    6. REPORT: genera reporte final con todos los KPIs

Uso:
    python main.py                     # Ejecuta todo el pipeline
    python main.py --etapa loader      # Solo una etapa especifica
    python main.py --simulado          # Modo simulado (sin Kafka)
    python main.py --etapa loader --etapa batch  # Etapas especificas
"""

import os
import sys
import time
import json
import importlib.util
import traceback
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ── Rutas del proyecto ────────────────────────────────────────────────
RUTA_SERVING = os.environ.get("RUTA_SERVING", os.path.join(os.path.dirname(__file__), "serving_layer"))

# ── Estado global del pipeline ────────────────────────────────────────
PIPELINE_STATE = {
    "inicio": None,
    "fin": None,
    "etapas": {},
    "errores": [],
    "exitoso": False
}


def print_banner():
    """
    Imprime el banner de inicio del pipeline.
    """
    print()
    print("=" * 70)
    print("  PIPELINE LAMBDA — Deteccion de Anomalias en Consumo Electrico")
    print("  Hidrandina S.A. | Diciembre 2022 - Junio 2025")
    print("  Arquitectura Lambda: Batch + Speed + Serving")
    print("=" * 70)
    print(f"  Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print()


def print_footer():
    """
    Imprime el footer de finalizacion del pipeline.
    """
    duration = PIPELINE_STATE.get("duracion_total", 0)
    print()
    print("=" * 70)
    print(f"  PIPELINE FINALIZADO")
    print(f"  Duracion total: {duration:.2f} segundos")
    print(f"  Estado: {'EXITOSO' if PIPELINE_STATE['exitoso'] else 'FALLIDO'}")
    if PIPELINE_STATE["errores"]:
        print(f"  Errores: {len(PIPELINE_STATE['errores'])}")
        for err in PIPELINE_STATE["errores"][-3:]:
            print(f"    - {err}")
    print("=" * 70)
    print()


def register_stage(name, success, duration, details=None):
    """
    Registra el resultado de una etapa del pipeline.

    Parametros:
        name (str): Nombre de la etapa.
        success (bool): Si la etapa se completo exitosamente.
        duration (float): Duracion en segundos.
        details (dict): Detalles adicionales de la etapa.
    """
    PIPELINE_STATE["etapas"][name] = {
        "exitosa": success,
        "duracion_seg": round(duration, 2),
        "timestamp": datetime.now().isoformat(),
        "detalles": details or {}
    }
    if not success:
        PIPELINE_STATE["errores"].append(
            f"Etapa '{name}' fallo tras {duration:.2f}s"
        )


def execute_loader(max_records_per_file=None):
    """
    Ejecuta la etapa LOADER: carga y limpieza de datos.

    Parametros:
        max_records_per_file (int): Máximo de filas por archivo CSV.
                                    Si es None, usa MAX_RECORDS_PER_FILE de entorno.

    Retorna:
        bool: True si fue exitoso.
    """
    print("\n" + "-" * 60)
    print("ETAPA 1/5: LOADER — Carga y limpieza de datos")
    print("-" * 60)

    from utils import loader

    start_time = time.time()
    try:
        fact, dim, metrics = loader.execute(max_records_per_file=max_records_per_file)
        success = fact is not None and not fact.empty
        duration = time.time() - start_time

        register_stage("loader", success, duration, metrics)

        if success:
            print(f"  LOADER exitoso: {len(fact):,} registros en FACT_CONSUMO")
            if max_records_per_file:
                print(f"  (Limitado a {max_records_per_file:,} filas por archivo CSV)")

        return success
    except Exception as e:
        duration = time.time() - start_time
        register_stage("loader", False, duration, {"error": str(e)})
        print(f"  LOADER FALLIDO: {e}")
        traceback.print_exc()
        return False


def execute_batch():
    """
    Ejecuta la etapa BATCH: procesamiento historico con PySpark.

    Retorna:
        bool: True si fue exitoso.
    """
    print("\n" + "-" * 60)
    print("ETAPA 2/5: BATCH LAYER — Procesamiento historico con PySpark")
    print("-" * 60)

    from batch_layer import spark_batch

    start_time = time.time()
    try:
        statistics, kpis, ranking, trend, analysis, rfm, oe2 = spark_batch.execute()
        success = statistics is not None
        duration = time.time() - start_time

        metrics = {}
        if success:
            try:
                metrics["num_distritos"] = statistics.count()
                metrics["columnas"] = statistics.columns
            except Exception:
                pass

        register_stage("batch", success, duration, metrics)
        return success
    except Exception as e:
        duration = time.time() - start_time
        register_stage("batch", False, duration, {"error": str(e)})
        print(f"  BATCH FALLIDO: {e}")
        traceback.print_exc()
        return False


def execute_producer(mode="auto"):
    """
    Ejecuta la etapa PRODUCER: publicacion de eventos a Kafka.

    Parametros:
        mode (str): "real", "simulado" o "auto".

    Retorna:
        bool: True si fue exitoso.
    """
    print("\n" + "-" * 60)
    print(f"ETAPA 3/5: KAFKA PRODUCER — Publicacion de eventos (modo={mode})")
    print("-" * 60)

    from speed_layer import kafka_producer

    start_time = time.time()
    try:
        success = kafka_producer.execute(mode=mode)
        duration = time.time() - start_time
        register_stage("producer", success, duration, {"modo": mode})
        return success
    except Exception as e:
        duration = time.time() - start_time
        register_stage("producer", False, duration, {"error": str(e)})
        print(f"  PRODUCER FALLIDO: {e}")
        traceback.print_exc()
        return False


def execute_streaming(mode="auto"):
    """
    Ejecuta la etapa STREAMING: consumo y clasificacion de anomalias.

    Parametros:
        mode (str): "real", "simulado" o "auto".

    Retorna:
        bool: True si fue exitoso.
    """
    print("\n" + "-" * 60)
    print(f"ETAPA 4/5: SPARK STREAMING — Consumo y clasificacion (modo={mode})")
    print("-" * 60)

    from speed_layer import spark_streaming

    start_time = time.time()
    try:
        success = spark_streaming.execute(mode=mode)
        duration = time.time() - start_time
        register_stage("streaming", success, duration, {"modo": mode})
        return success
    except Exception as e:
        duration = time.time() - start_time
        register_stage("streaming", False, duration, {"error": str(e)})
        print(f"  STREAMING FALLIDO: {e}")
        traceback.print_exc()
        return False


def execute_serving():
    """
    Ejecuta la etapa SERVING: union batch+stream, dashboard y KPIs.

    Retorna:
        bool: True si fue exitoso.
    """
    print("\n" + "-" * 60)
    print("ETAPA 5/5: SERVING LAYER — Union batch+stream y outputs finales")
    print("-" * 60)

    from serving_layer import serving

    start_time = time.time()
    try:
        success = serving.ejecutar()
        duration = time.time() - start_time
        register_stage("serving", success, duration)
        return success
    except Exception as e:
        duration = time.time() - start_time
        register_stage("serving", False, duration, {"error": str(e)})
        print(f"  SERVING FALLIDO: {e}")
        traceback.print_exc()
        return False


def generate_final_report():
    """
    Genera el reporte final del pipeline con todas las metricas.

    Retorna:
        dict: Reporte completo del pipeline.
    """
    try:
        total_duration = time.time() - PIPELINE_STATE["inicio"]
        PIPELINE_STATE["duracion_total"] = total_duration
        PIPELINE_STATE["fin"] = datetime.now().isoformat()

        successful_stages = sum(
            1 for e in PIPELINE_STATE["etapas"].values() if e["exitosa"]
        )
        total_stages = len(PIPELINE_STATE["etapas"])
        PIPELINE_STATE["exitoso"] = (
            successful_stages == total_stages and total_stages > 0
        )

        report = {
            "pipeline": "Hidrandina Lambda - Deteccion de Anomalias",
            "inicio": datetime.fromtimestamp(PIPELINE_STATE["inicio"]).isoformat(),
            "fin": PIPELINE_STATE["fin"],
            "duracion_total_seg": round(total_duration, 2),
            "etapas_ejecutadas": total_stages,
            "etapas_exitosas": successful_stages,
            "etapas_fallidas": total_stages - successful_stages,
            "exitoso": PIPELINE_STATE["exitoso"],
            "etapas": PIPELINE_STATE["etapas"],
            "errores": PIPELINE_STATE["errores"]
        }

        # Cargar reportes individuales de cada capa si existen
        for report_name in [
            "reporte_calidad.json",
            "reporte_batch.json",
            "reporte_streaming.json",
            "reporte_kpis.json"
        ]:
            path = os.path.join(RUTA_SERVING, report_name)
            if os.path.exists(path):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        report[report_name.replace(".json", "")] = json.load(f)
                except Exception:
                    pass

        # Guardar reporte final
        report_path = os.path.join(RUTA_SERVING, "reporte_pipeline_completo.json")
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        print(f"\n  Reporte final guardado: {report_path}")
        return report
    except Exception as e:
        print(f"ERROR generando reporte final: {e}")
        return {}


def check_dependencies():
    """
    Verifica que todas las dependencias del proyecto esten instaladas.

    Retorna:
        bool: True si todas las dependencias estan disponibles.
    """
    print("Verificando dependencias...")
    dependencies = [
        ("pyspark", "pyspark.sql"),
        ("pandas", "pandas"),
        ("matplotlib", "matplotlib"),
        ("numpy", "numpy"),
        ("python-dotenv", "dotenv"),
    ]

    all_ok = True
    for name, module in dependencies:
        try:
            importlib.import_module(module)
            print(f"  [OK] {name}")
        except ImportError:
            print(f"  [FALTA] {name}")
            all_ok = False

    # kafka-python es opcional
    try:
        importlib.import_module("kafka")
        print(f"  [OK] kafka-python")
    except ImportError:
        print(f"  [OPCIONAL] kafka-python no instalado (modo simulado)")

    return all_ok


def main():
    """
    Funcion principal del orquestador.

    Procesa argumentos de linea de comandos y ejecuta las etapas
    solicitadas en el orden correcto.
    """
    print_banner()

    # Procesar argumentos
    args = sys.argv[1:]
    requested_stages = []
    simulated_mode = False
    kafka_mode = False
    max_records_per_file = None

    i = 0
    while i < len(args):
        if args[i] == "--etapa" and i + 1 < len(args):
            requested_stages.append(args[i + 1].lower())
            i += 2
        elif args[i] == "--simulado":
            simulated_mode = True
            i += 1
        elif args[i] == "--kafka" or args[i] == "--real":
            kafka_mode = True
            i += 1
        elif args[i] == "--max-records-per-file" and i + 1 < len(args):
            try:
                max_records_per_file = int(args[i + 1])
                i += 2
            except ValueError:
                print(f"ERROR: --max-records-per-file requiere un numero entero, got {args[i + 1]}")
                return
        elif args[i] == "--help" or args[i] == "-h":
            print("Uso: python main.py [opciones]")
            print("  --etapa NOMBRE   Ejecuta solo una etapa (loader, batch, producer, streaming, serving)")
            print("  --simulado       Usa modo simulado (sin Kafka)")
            print("  --kafka / --real Usa Kafka real (requiere broker corriendo)")
            print("  --max-records-per-file N   Limita a N filas por archivo CSV (ej: 100000)")
            print("  --help           Muestra esta ayuda")
            print("  Sin argumentos: ejecuta el pipeline completo")
            print("\nModos:")
            print("  Sin flag:         Auto (detecta Kafka, fallback simulado)")
            print("  --simulado:       Fuerza modo simulado")
            print("  --kafka / --real: Fuerza modo Kafka real")
            print("\nEtapas disponibles:")
            print("  loader     - Carga y limpia los CSV")
            print("  batch      - Procesa historico con PySpark")
            print("  producer   - Publica eventos a Kafka")
            print("  streaming  - Consume y clasifica anomalias")
            print("  serving    - Genera outputs finales")
            print("\nEjemplos:")
            print("  python main.py --etapa loader --max-rows 100000")
            print("  python main.py --simulado")
            print("  python main.py --max-rows 1000000 --etapa loader --etapa batch")
            return
        else:
            print(f"Argumento desconocido: {args[i]}")
            i += 1

    if not requested_stages:
        requested_stages = ["loader", "batch", "producer", "streaming", "serving"]

    # Verificar dependencias
    if not check_dependencies():
        print("\nADVERTENCIA: Faltan dependencias. El pipeline podria fallar.")
        respuesta = input("  Continuar de todas formas? (s/N): ")
        if respuesta.lower() != "s":
            print("Pipeline cancelado.")
            return

    # Registrar inicio
    PIPELINE_STATE["inicio"] = time.time()

    # Mapa de etapas
    def get_producer_mode():
        if simulated_mode:
            return "simulado"
        if kafka_mode:
            return "real"
        return "auto"

    def get_streaming_mode():
        if simulated_mode:
            return "simulado"
        if kafka_mode:
            return "real"
        return "auto"

    stages_map = {
        "loader": lambda: execute_loader(max_records_per_file),
        "batch": execute_batch,
        "producer": lambda: execute_producer(get_producer_mode()),
        "streaming": lambda: execute_streaming(get_streaming_mode()),
        "serving": execute_serving,
    }

    # Ejecutar etapas en orden
    print(f"\nEtapas a ejecutar: {', '.join(requested_stages)}")
    if simulated_mode:
        print("Modo: SIMULADO (sin Kafka)")
    if kafka_mode:
        print("Modo: KAFKA REAL")
    if max_records_per_file:
        print(f"Límite de datos por archivo: {max_records_per_file:,} filas")

    pipeline_successful = True
    for stage in requested_stages:
        if stage in stages_map:
            try:
                result = stages_map[stage]()
                if not result:
                    pipeline_successful = False
                    print(f"\n  [ADVERTENCIA] Etapa '{stage}' reporto fallo.")
                    continue_prompt = input("  Continuar con la siguiente etapa? (S/n): ")
                    if continue_prompt.lower() == "n":
                        print("Pipeline detenido por el usuario.")
                        break
            except Exception as e:
                pipeline_successful = False
                print(f"\n  [ERROR] Etapa '{stage}' lanzo excepcion: {e}")
                traceback.print_exc()
                continue_prompt = input("  Continuar con la siguiente etapa? (S/n): ")
                if continue_prompt.lower() == "n":
                    print("Pipeline detenido por el usuario.")
                    break
        else:
            print(f"  Etapa desconocida: '{stage}'. Opciones: {list(stages_map.keys())}")

    # Generar reporte final
    generate_final_report()
    print_footer()


if __name__ == "__main__":
    main()
