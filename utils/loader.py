"""
loader.py — Carga, limpieza y separación del dataset Hidrandina

Funciones principales:
    - detectar_separador: Detecta automáticamente el separador del CSV
    - cargar_todos_los_csv: Lee y combina todos los CSV mensuales
    - limpiar_dataframe: Aplica las 6 reglas de limpieza definidas
    - separar_tablas: Genera FACT_CONSUMO y DIM_CLIENTE_UBICACION
    - marcar_outliers: Detecta outliers con z-score > 3
    - estandarizar_texto: Convierte texto a mayúsculas y aplica strip
    - validar_calidad: Calcula tasa de registros válidos (KPI OE1)
    - guardar_resultados: Persiste CSV limpio y tablas separadas
"""

import os
import glob
import uuid
import warnings
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
warnings.filterwarnings("ignore")

RUTA_CSV = os.environ.get(
    "RUTA_CSV_ORIGINALES",
    os.path.join(os.path.dirname(__file__), "..", "data", "originales")
)
RUTA_DATA = os.environ.get("RUTA_DATA", os.path.join(os.path.dirname(__file__), "..", "data"))
RUTA_SERVING = os.environ.get("RUTA_SERVING", os.path.join(os.path.dirname(__file__), "..", "serving_layer"))


def detectar_separador(archivo, num_lineas=5):
    """
    Detecta el separador de un archivo CSV probando con ';', ',' y tabulador.

    Parametros:
        archivo (str): Ruta al archivo CSV.
        num_lineas (int): Lineas a leer para la deteccion.

    Retorna:
        str: El separador detectado (';', ',' o '\\t').
    """
    try:
        with open(archivo, "r", encoding="latin-1", errors="ignore") as f:
            lineas = [f.readline() for _ in range(num_lineas)]
        texto = "".join(lineas)
        puntajes = {}
        for sep in [";", ",", "\t"]:
            puntajes[sep] = texto.count(sep)
        separador = max(puntajes, key=puntajes.get)
        print(f"  Separador detectado: '{separador}' (puntaje={puntajes[separador]})")
        return separador
    except Exception as e:
        print(f"  Error detectando separador: {e}")
        return ";"


def cargar_todos_los_csv(ruta=None):
    """
    Lee todos los archivos CSV de la carpeta y los combina en un DataFrame.

    Parametros:
        ruta (str): Directorio con los CSV. Usa RUTA_CSV por defecto.

    Retorna:
        pd.DataFrame: DataFrame combinado, o None si no hay archivos.
    """
    if ruta is None:
        ruta = RUTA_CSV
    try:
        archivos = sorted(glob.glob(os.path.join(ruta, "*.csv")))
        if not archivos:
            print("ERROR: No se encontraron archivos CSV en:", ruta)
            return None

        print(f"Archivos encontrados: {len(archivos)}")
        dfs = []
        for archivo in archivos:
            try:
                sep = detectar_separador(archivo)
                # Probar encoding latin-1, fallback a utf-8
                try:
                    df = pd.read_csv(
                        archivo, sep=sep, encoding="latin-1", low_memory=False,
                        on_bad_lines="skip"
                    )
                except UnicodeDecodeError:
                    df = pd.read_csv(
                        archivo, sep=sep, encoding="utf-8", low_memory=False,
                        on_bad_lines="skip"
                    )
                dfs.append(df)
                print(f"  OK {os.path.basename(archivo)}: {len(df):,} filas x {len(df.columns)} cols")
            except Exception as e:
                print(f"  ERROR en {os.path.basename(archivo)}: {e}")

        if not dfs:
            print("ERROR: No se pudo leer ningun archivo.")
            return None

        df_total = pd.concat(dfs, ignore_index=True)
        print(f"\nTotal filas combinadas: {len(df_total):,}")
        print(f"Columnas detectadas: {df_total.columns.tolist()}")
        return df_total
    except Exception as e:
        print(f"ERROR en cargar_todos_los_csv: {e}")
        return None


def estandarizar_texto(df, columnas_texto=None):
    """
    Convierte columnas de texto a mayusculas y elimina espacios al inicio/final.

    Parametros:
        df (pd.DataFrame): DataFrame a procesar.
        columnas_texto (list): Lista de columnas de texto. Si es None, se
                                detectan automaticamente.

    Retorna:
        pd.DataFrame: DataFrame con texto estandarizado.
    """
    try:
        if columnas_texto is None:
            columnas_texto = df.select_dtypes(include=["object"]).columns.tolist()
        for col in columnas_texto:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.upper()
        return df
    except Exception as e:
        print(f"ERROR en estandarizar_texto: {e}")
        return df


def limpiar_dataframe(df):
    """
    Aplica las 6 reglas de limpieza definidas en el proyecto.

    Reglas:
        1. Deteccion automatica del separador (ya hecho en carga)
        2. Eliminacion de nulos en IMPORTE y CONSUMO
        3. Filtrado de consumo o importe <= 0
        4. Conversion de tipos (fechas yyyymmdd a datetime, numericos)
        5. Deteccion y marcado de outliers (>3 desv. estandar)
        6. Estandarizacion de texto (mayusculas y strip)

    Parametros:
        df (pd.DataFrame): DataFrame crudo.

    Retorna:
        pd.DataFrame: DataFrame limpio con columna 'outlier' bool.
    """
    try:
        print("\n=== INICIO LIMPIEZA ===")
        # Limpiar BOM en nombres de columnas
        df.columns = df.columns.str.replace('ï»¿', '', regex=False).str.strip()
        filas_inicial = len(df)

        # ── 4. Conversion de tipos ──────────────────────────────────
        # Columnas de fecha
        cols_fecha = [
            "FECHA_EMISION", "FECHA_VENCIMIENTO",
            "FECHA_COSNUMO_DESDE", "FECHA_CONSUMO_HASTA"
        ]
        for col in cols_fecha:
            if col in df.columns:
                df[col] = pd.to_datetime(
                    df[col], format="%Y%m%d", errors="coerce"
                )

        # Columnas numericas
        for col in ["IMPORTE", "CONSUMO"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "PERIODO" in df.columns:
            df["PERIODO"] = pd.to_numeric(df["PERIODO"], errors="coerce").astype("Int64")

        if "NRO_SERVICIO" in df.columns:
            df["NRO_SERVICIO"] = pd.to_numeric(df["NRO_SERVICIO"], errors="coerce").astype("Int64")

        print(f"  Conversion de tipos OK")

        # ── 2. Eliminacion de nulos en IMPORTE y CONSUMO ────────────
        filas_antes_nulos = len(df)
        df = df.dropna(subset=["IMPORTE", "CONSUMO"])
        nulos_eliminados = filas_antes_nulos - len(df)
        print(f"  Nulos eliminados (IMPORTE/CONSUMO): {nulos_eliminados:,}")

        # ── 3. Filtrado de valores invalidos (<= 0) ─────────────────
        filas_antes_invalidos = len(df)
        df = df[df["CONSUMO"] > 0]
        df = df[df["IMPORTE"] > 0]
        invalidos_eliminados = filas_antes_invalidos - len(df)
        print(f"  Valores <=0 eliminados: {invalidos_eliminados:,}")

        # ── 6. Estandarizacion de texto ─────────────────────────────
        df = estandarizar_texto(df)

        # ── 5. Deteccion de outliers (z-score > 3) ─────────────────
        df = marcar_outliers(df)

        # ── Reporte final ───────────────────────────────────────────
        filas_final = len(df)
        eliminadas = filas_inicial - filas_final
        tasa_validos = (filas_final / filas_inicial) * 100 if filas_inicial > 0 else 0
        print(f"\n  Filas iniciales: {filas_inicial:,}")
        print(f"  Filas finales:   {filas_final:,}")
        print(f"  Eliminadas:      {eliminadas:,}")
        print(f"  Tasa validez:    {tasa_validos:.2f}%")
        print(f"  Outliers marcados: {df['outlier'].sum():,}")
        print("=== FIN LIMPIEZA ===\n")

        return df
    except Exception as e:
        print(f"ERROR en limpiar_dataframe: {e}")
        return df


def marcar_outliers(df, umbral=3):
    """
    Marca outliers usando z-score > umbral en CONSUMO e IMPORTE.

    Parametros:
        df (pd.DataFrame): DataFrame con columnas CONSUMO e IMPORTE.
        umbral (int/float): Numero de desviaciones estandar para el corte.

    Retorna:
        pd.DataFrame: Con columna adicional 'outlier' (bool).
    """
    try:
        df["outlier"] = False
        for col in ["CONSUMO", "IMPORTE"]:
            if col in df.columns:
                media = df[col].mean()
                std = df[col].std()
                if std > 0:
                    z = (df[col] - media).abs() / std
                    df["outlier"] = df["outlier"] | (z > umbral)
        return df
    except Exception as e:
        print(f"ERROR en marcar_outliers: {e}")
        df["outlier"] = False
        return df


def separar_tablas(df):
    """
    Separa el DataFrame en FACT_CONSUMO y DIM_CLIENTE_UBICACION.
    Usa NRO_DOC_FAC como identificador único porque NRO_SERVICIO 
    está anonimizado con 0 en el dataset real de Hidrandina.
    """
    try:
        print("=== SEPARANDO TABLAS ===")

        # Limpiar BOM en nombres de columnas
        df.columns = df.columns.str.replace('ï»¿', '', regex=False).str.strip()

        # ── FACT_CONSUMO ─────────────────────────────────────────
        cols_fact = [
            "NRO_DOC_FAC", "PERIODO", "CONSUMO", "IMPORTE",
            "FECHA_EMISION", "FECHA_VENCIMIENTO",
            "FECHA_COSNUMO_DESDE", "FECHA_CONSUMO_HASTA"
        ]
        cols_fact_ok = [c for c in cols_fact if c in df.columns]
        fact = df[cols_fact_ok].copy()
        fact = fact.dropna(subset=["NRO_DOC_FAC"])
        fact = fact.drop_duplicates(subset=["NRO_DOC_FAC"])
        print(f"  FACT_CONSUMO: {len(fact):,} filas x {len(fact.columns)} cols")

        # ── DIM_CLIENTE_UBICACION ────────────────────────────────
        cols_dim = [
            "NRO_DOC_FAC", "DEPARTAMENTO", "PROVINCIA", "DISTRITO",
            "UBIGEO", "TARIFA", "CARTERA", "UNIDAD_NEGOCIO"
        ]
        cols_dim_ok = [c for c in cols_dim if c in df.columns]
        dim = df[cols_dim_ok].copy()
        dim = dim.dropna(subset=["NRO_DOC_FAC"])
        dim = dim.drop_duplicates(subset=["NRO_DOC_FAC"])
        print(f"  DIM_CLIENTE_UBICACION: {len(dim):,} filas x {len(dim.columns)} cols")
        print("=== TABLAS SEPARADAS ===\n")

        return fact, dim

    except Exception as e:
        print(f"ERROR en separar_tablas: {e}")
        return df, pd.DataFrame()


def validar_calidad(fact, dim, total_original=0):
    """
    Calcula indicadores de calidad (KPI OE1).

    Parametros:
        fact (pd.DataFrame): FACT_CONSUMO.
        dim (pd.DataFrame): DIM_CLIENTE_UBICACION.
        total_original (int): Filas originales antes de limpieza.

    Retorna:
        dict: Diccionario con metricas de calidad.
    """
    try:
        total_fact = len(fact)
        nulos_consumo = fact["CONSUMO"].isna().sum()
        nulos_importe = fact["IMPORTE"].isna().sum()
        consumo_cero = (fact["CONSUMO"] <= 0).sum()
        importe_cero = (fact["IMPORTE"] <= 0).sum()
        fact_sin_dim = total_fact - fact["NRO_DOC_FAC"].isin(dim["NRO_DOC_FAC"]).sum()

        tasa_validez_pct = round(
            total_fact / max(total_original, 1) * 100, 2
        )

        metricas = {
            "total_filas_originales":  int(total_original),
            "total_fact_consumo":      int(total_fact),
            "total_dim_cliente":       int(len(dim)),
            "nulos_consumo":           int(nulos_consumo),
            "nulos_importe":           int(nulos_importe),
            "consumo_cero_o_negativo": int(consumo_cero),
            "importe_cero_o_negativo": int(importe_cero),
            "fact_sin_dim_asociada":   int(fact_sin_dim),
            "tasa_validez_pct":        tasa_validez_pct,
            "oe1_cumplido":            bool(tasa_validez_pct >= 85.0)
        }
        return metricas
    except Exception as e:
        print(f"ERROR en validar_calidad: {e}")
        return {}


def guardar_resultados(fact, dim, df_limpio=None):
    """
    Guarda CSV limpio, FACT_CONSUMO y DIM_CLIENTE_UBICACION en disco.

    Parametros:
        fact (pd.DataFrame): Tabla de hechos.
        dim (pd.DataFrame): Tabla dimensional.
        df_limpio (pd.DataFrame): DataFrame completo limpio (opcional).
    """
    try:
        os.makedirs(RUTA_DATA, exist_ok=True)
        os.makedirs(RUTA_SERVING, exist_ok=True)

        if df_limpio is not None:
            ruta_limpio = os.path.join(RUTA_DATA, "hidrandina_limpio.csv")
            df_limpio.to_csv(ruta_limpio, index=False, encoding="utf-8-sig")
            print(f"  CSV limpio guardado: {ruta_limpio}")

        ruta_fact = os.path.join(RUTA_DATA, "FACT_CONSUMO.csv")
        fact.to_csv(ruta_fact, index=False, encoding="utf-8-sig")
        print(f"  FACT_CONSUMO guardado: {ruta_fact} ({len(fact):,} filas)")

        ruta_dim = os.path.join(RUTA_DATA, "DIM_CLIENTE_UBICACION.csv")
        dim.to_csv(ruta_dim, index=False, encoding="utf-8-sig")
        print(f"  DIM_CLIENTE_UBICACION guardado: {ruta_dim} ({len(dim):,} filas)")

        print("  Archivos guardados correctamente.")
    except Exception as e:
        print(f"ERROR en guardar_resultados: {e}")


def generar_reporte_calidad(metricas, ruta=None):
    """
    Genera un reporte JSON con las metricas de calidad.

    Parametros:
        metricas (dict): Diccionario con metricas de validar_calidad.
        ruta (str): Ruta de salida. Por defecto serving_layer/reporte_calidad.json.
    """
    try:
        if ruta is None:
            ruta = os.path.join(RUTA_SERVING, "reporte_calidad.json")
        os.makedirs(os.path.dirname(ruta), exist_ok=True)
        import json
        with open(ruta, "w", encoding="utf-8") as f:
            json.dump(
                {k: (bool(v) if isinstance(v, (bool, np.bool_)) else 
                     int(v) if isinstance(v, (np.integer,)) else
                     float(v) if isinstance(v, (np.floating,)) else v)
                 for k, v in metricas.items()},
                f, indent=2, ensure_ascii=False
            )
        print(f"  Reporte calidad guardado: {ruta}")
    except Exception as e:
        print(f"ERROR en generar_reporte_calidad: {e}")


def ejecutar():
    """
    Ejecuta el pipeline completo de loader:
    1. Carga todos los CSV
    2. Limpia el DataFrame
    3. Separa en FACT_CONSUMO y DIM_CLIENTE_UBICACION
    4. Valida calidad (OE1)
    5. Guarda resultados

    Retorna:
        tuple: (fact, dim, metricas) o (None, None, None) si falla.
    """
    print("=" * 60)
    print("LOADER — Carga y limpieza del dataset Hidrandina")
    print("=" * 60)

    df = cargar_todos_los_csv()
    if df is None or df.empty:
        print("ERROR: No hay datos para procesar.")
        return None, None, None

    total_original = len(df)

    df = limpiar_dataframe(df)
    if df.empty:
        print("ERROR: DataFrame vacio tras limpieza.")
        return None, None, None

    fact, dim = separar_tablas(df)
    if fact.empty:
        print("ERROR: FACT_CONSUMO vacia.")
        return None, None, None

    metricas = validar_calidad(fact, dim, total_original)
    guardar_resultados(fact, dim, df_limpio=df)
    generar_reporte_calidad(metricas)

    print("\nRESUMEN DE CALIDAD (OE1):")
    print(f"  Tasa de validez: {metricas.get('tasa_validez_pct', 0):.2f}%")
    print(f"  OE1 cumplido: {'SI' if metricas.get('oe1_cumplido') else 'NO'}")
    print("=" * 60)

    return fact, dim, metricas


if __name__ == "__main__":
    ejecutar()
