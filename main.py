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
estado_pipeline = {
    "inicio": None,
    "fin": None,
    "etapas": {},
    "errores": [],
    "exitoso": False
}


def imprimir_banner():
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


def imprimir_footer():
    """
    Imprime el footer de finalizacion del pipeline.
    """
    duracion = estado_pipeline.get("duracion_total", 0)
    print()
    print("=" * 70)
    print(f"  PIPELINE FINALIZADO")
    print(f"  Duracion total: {duracion:.2f} segundos")
    print(f"  Estado: {'EXITOSO' if estado_pipeline['exitoso'] else 'FALLIDO'}")
    if estado_pipeline["errores"]:
        print(f"  Errores: {len(estado_pipeline['errores'])}")
        for err in estado_pipeline["errores"][-3:]:
            print(f"    - {err}")
    print("=" * 70)
    print()


def registrar_etapa(nombre, exitosa, duracion, detalles=None):
    """
    Registra el resultado de una etapa del pipeline.

    Parametros:
        nombre (str): Nombre de la etapa.
        exitosa (bool): Si la etapa se completo exitosamente.
        duracion (float): Duracion en segundos.
        detalles (dict): Detalles adicionales de la etapa.
    """
    estado_pipeline["etapas"][nombre] = {
        "exitosa": exitosa,
        "duracion_seg": round(duracion, 2),
        "timestamp": datetime.now().isoformat(),
        "detalles": detalles or {}
    }
    if not exitosa:
        estado_pipeline["errores"].append(
            f"Etapa '{nombre}' fallo tras {duracion:.2f}s"
        )


def ejecutar_loader():
    """
    Ejecuta la etapa LOADER: carga y limpieza de datos.

    Retorna:
        bool: True si fue exitoso.
    """
    print("\n" + "-" * 60)
    print("ETAPA 1/5: LOADER — Carga y limpieza de datos")
    print("-" * 60)

    from utils import loader

    inicio = time.time()
    try:
        fact, dim, metricas = loader.ejecutar()
        exitoso = fact is not None and not fact.empty
        duracion = time.time() - inicio

        registrar_etapa("loader", exitoso, duracion, metricas)

        if exitoso:
            print(f"  LOADER exitoso: {len(fact):,} registros en FACT_CONSUMO")

        return exitoso
    except Exception as e:
        duracion = time.time() - inicio
        registrar_etapa("loader", False, duracion, {"error": str(e)})
        print(f"  LOADER FALLIDO: {e}")
        traceback.print_exc()
        return False


def ejecutar_batch():
    """
    Ejecuta la etapa BATCH: procesamiento historico con PySpark.

    Retorna:
        bool: True si fue exitoso.
    """
    print("\n" + "-" * 60)
    print("ETAPA 2/5: BATCH LAYER — Procesamiento historico con PySpark")
    print("-" * 60)

    from batch_layer import spark_batch

    inicio = time.time()
    try:
        spark, estadisticas = spark_batch.ejecutar()
        exitoso = estadisticas is not None
        duracion = time.time() - inicio

        metricas = {}
        if exitoso:
            try:
                metricas["num_distritos"] = estadisticas.count()
                metricas["columnas"] = estadisticas.columns
            except Exception:
                pass

        registrar_etapa("batch", exitoso, duracion, metricas)
        return exitoso
    except Exception as e:
        duracion = time.time() - inicio
        registrar_etapa("batch", False, duracion, {"error": str(e)})
        print(f"  BATCH FALLIDO: {e}")
        traceback.print_exc()
        return False


def ejecutar_producer(modo="auto"):
    """
    Ejecuta la etapa PRODUCER: publicacion de eventos a Kafka.

    Parametros:
        modo (str): "real", "simulado" o "auto".

    Retorna:
        bool: True si fue exitoso.
    """
    print("\n" + "-" * 60)
    print(f"ETAPA 3/5: KAFKA PRODUCER — Publicacion de eventos (modo={modo})")
    print("-" * 60)

    from speed_layer import kafka_producer

    inicio = time.time()
    try:
        exitoso = kafka_producer.ejecutar(modo=modo)
        duracion = time.time() - inicio
        registrar_etapa("producer", exitoso, duracion, {"modo": modo})
        return exitoso
    except Exception as e:
        duracion = time.time() - inicio
        registrar_etapa("producer", False, duracion, {"error": str(e)})
        print(f"  PRODUCER FALLIDO: {e}")
        traceback.print_exc()
        return False


def ejecutar_streaming(modo="auto"):
    """
    Ejecuta la etapa STREAMING: consumo y clasificacion de anomalias.

    Parametros:
        modo (str): "real", "simulado" o "auto".

    Retorna:
        bool: True si fue exitoso.
    """
    print("\n" + "-" * 60)
    print(f"ETAPA 4/5: SPARK STREAMING — Consumo y clasificacion (modo={modo})")
    print("-" * 60)

    from speed_layer import spark_streaming

    inicio = time.time()
    try:
        exitoso = spark_streaming.ejecutar(modo=modo)
        duracion = time.time() - inicio
        registrar_etapa("streaming", exitoso, duracion, {"modo": modo})
        return exitoso
    except Exception as e:
        duracion = time.time() - inicio
        registrar_etapa("streaming", False, duracion, {"error": str(e)})
        print(f"  STREAMING FALLIDO: {e}")
        traceback.print_exc()
        return False


def ejecutar_serving():
    """
    Ejecuta la etapa SERVING: union batch+stream, dashboard y KPIs.

    Retorna:
        bool: True si fue exitoso.
    """
    print("\n" + "-" * 60)
    print("ETAPA 5/5: SERVING LAYER — Union batch+stream y outputs finales")
    print("-" * 60)

    from serving_layer import serving

    inicio = time.time()
    try:
        exitoso = serving.ejecutar()
        duracion = time.time() - inicio
        registrar_etapa("serving", exitoso, duracion)
        return exitoso
    except Exception as e:
        duracion = time.time() - inicio
        registrar_etapa("serving", False, duracion, {"error": str(e)})
        print(f"  SERVING FALLIDO: {e}")
        traceback.print_exc()
        return False


def generar_reporte_final():
    """
    Genera el reporte final del pipeline con todas las metricas.

    Retorna:
        dict: Reporte completo del pipeline.
    """
    try:
        duracion_total = time.time() - estado_pipeline["inicio"]
        estado_pipeline["duracion_total"] = duracion_total
        estado_pipeline["fin"] = datetime.now().isoformat()

        etapas_exitosas = sum(
            1 for e in estado_pipeline["etapas"].values() if e["exitosa"]
        )
        etapas_totales = len(estado_pipeline["etapas"])
        estado_pipeline["exitoso"] = (
            etapas_exitosas == etapas_totales and etapas_totales > 0
        )

        reporte = {
            "pipeline": "Hidrandina Lambda - Deteccion de Anomalias",
            "inicio": estado_pipeline["inicio"],
            "fin": estado_pipeline["fin"],
            "duracion_total_seg": round(duracion_total, 2),
            "etapas_ejecutadas": etapas_totales,
            "etapas_exitosas": etapas_exitosas,
            "etapas_fallidas": etapas_totales - etapas_exitosas,
            "exitoso": estado_pipeline["exitoso"],
            "etapas": estado_pipeline["etapas"],
            "errores": estado_pipeline["errores"]
        }

        # Cargar reportes individuales de cada capa si existen
        for nombre_reporte in [
            "reporte_calidad.json",
            "reporte_batch.json",
            "reporte_streaming.json",
            "reporte_kpis.json"
        ]:
            ruta = os.path.join(RUTA_SERVING, nombre_reporte)
            if os.path.exists(ruta):
                try:
                    with open(ruta, "r", encoding="utf-8") as f:
                        reporte[nombre_reporte.replace(".json", "")] = json.load(f)
                except Exception:
                    pass

        # Guardar reporte final
        ruta_reporte = os.path.join(RUTA_SERVING, "reporte_pipeline_completo.json")
        os.makedirs(os.path.dirname(ruta_reporte), exist_ok=True)
        with open(ruta_reporte, "w", encoding="utf-8") as f:
            json.dump(reporte, f, indent=2, ensure_ascii=False)

        print(f"\n  Reporte final guardado: {ruta_reporte}")
        return reporte
    except Exception as e:
        print(f"ERROR generando reporte final: {e}")
        return {}


def verificar_dependencias():
    """
    Verifica que todas las dependencias del proyecto esten instaladas.

    Retorna:
        bool: True si todas las dependencias estan disponibles.
    """
    print("Verificando dependencias...")
    dependencias = [
        ("pyspark", "pyspark.sql"),
        ("pandas", "pandas"),
        ("matplotlib", "matplotlib"),
        ("numpy", "numpy"),
    ]

    todas_ok = True
    for nombre, modulo in dependencias:
        try:
            importlib.import_module(modulo)
            print(f"  [OK] {nombre}")
        except ImportError:
            print(f"  [FALTA] {nombre}")
            todas_ok = False

    # kafka-python es opcional
    try:
        importlib.import_module("kafka")
        print(f"  [OK] kafka-python")
    except ImportError:
        print(f"  [OPCIONAL] kafka-python no instalado (modo simulado)")

    return todas_ok


def main():
    """
    Funcion principal del orquestador.

    Procesa argumentos de linea de comandos y ejecuta las etapas
    solicitadas en el orden correcto.
    """
    imprimir_banner()

    # Procesar argumentos
    args = sys.argv[1:]
    etapas_solicitadas = []
    modo_simulado = False

    i = 0
    while i < len(args):
        if args[i] == "--etapa" and i + 1 < len(args):
            etapas_solicitadas.append(args[i + 1].lower())
            i += 2
        elif args[i] == "--simulado":
            modo_simulado = True
            i += 1
        elif args[i] == "--help" or args[i] == "-h":
            print("Uso: python main.py [opciones]")
            print("  --etapa NOMBRE   Ejecuta solo una etapa (loader, batch, producer, streaming, serving)")
            print("  --simulado       Usa modo simulado (sin Kafka)")
            print("  --help           Muestra esta ayuda")
            print("  Sin argumentos: ejecuta el pipeline completo")
            print("\nEtapas disponibles:")
            print("  loader     - Carga y limpia los CSV")
            print("  batch      - Procesa historico con PySpark")
            print("  producer   - Publica eventos a Kafka")
            print("  streaming  - Consume y clasifica anomalias")
            print("  serving    - Genera outputs finales")
            return
        else:
            print(f"Argumento desconocido: {args[i]}")
            i += 1

    if not etapas_solicitadas:
        etapas_solicitadas = ["loader", "batch", "producer", "streaming", "serving"]

    # Verificar dependencias
    if not verificar_dependencias():
        print("\nADVERTENCIA: Faltan dependencias. El pipeline podria fallar.")
        respuesta = input("  Continuar de todas formas? (s/N): ")
        if respuesta.lower() != "s":
            print("Pipeline cancelado.")
            return

    # Registrar inicio
    estado_pipeline["inicio"] = datetime.now().isoformat()

    # Mapa de etapas
    mapa_etapas = {
        "loader": ejecutar_loader,
        "batch": ejecutar_batch,
        "producer": lambda: ejecutar_producer("simulado" if modo_simulado else "auto"),
        "streaming": lambda: ejecutar_streaming("simulado" if modo_simulado else "auto"),
        "serving": ejecutar_serving,
    }

    # Ejecutar etapas en orden
    print(f"\nEtapas a ejecutar: {', '.join(etapas_solicitadas)}")
    if modo_simulado:
        print("Modo: SIMULADO (sin Kafka)")

    pipeline_exitoso = True
    for etapa in etapas_solicitadas:
        if etapa in mapa_etapas:
            try:
                resultado = mapa_etapas[etapa]()
                if not resultado:
                    pipeline_exitoso = False
                    print(f"\n  [ADVERTENCIA] Etapa '{etapa}' reporto fallo.")
                    continuar = input("  Continuar con la siguiente etapa? (S/n): ")
                    if continuar.lower() == "n":
                        print("Pipeline detenido por el usuario.")
                        break
            except Exception as e:
                pipeline_exitoso = False
                print(f"\n  [ERROR] Etapa '{etapa}' lanzo excepcion: {e}")
                traceback.print_exc()
                continuar = input("  Continuar con la siguiente etapa? (S/n): ")
                if continuar.lower() == "n":
                    print("Pipeline detenido por el usuario.")
                    break
        else:
            print(f"  Etapa desconocida: '{etapa}'. Opciones: {list(mapa_etapas.keys())}")

    # Generar reporte final
    generar_reporte_final()
    imprimir_footer()


if __name__ == "__main__":
    main()
