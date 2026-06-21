"""
loader.py — Carga, limpieza y separación del dataset Hidrandina

Funciones principales:
    - detectar_separador: Detecta automáticamente el separador del CSV
    - cargar_todos_los_csv: Lee y combina todos los CSV mensuales
    - clean_dataframe: Aplica las 6 reglas de limpieza definidas
    - split_tables: Genera FACT_CONSUMO y DIM_CLIENTE_UBICACION
    - mark_outliers: Detecta outliers con z-score > 3
    - standardize_text: Convierte texto a mayúsculas y aplica strip
    - validar_calidad: Calcula tasa de registros válidos (KPI OE1)
    - guardar_resultados: Persiste CSV limpio y tablas separadas

Variables de entorno:
    - MAX_RECORDS_PER_FILE: Máximo de filas por archivo CSV (ej: 100000)
    - MAX_RECORDS: Total máximo de filas (proporcional por archivo)
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


def detect_separator(file_path, num_lines=5):
    """
    Detecta el separador de un archivo CSV probando con ';', ',' y tabulador.

    Parametros:
        file_path (str): Ruta al archivo CSV.
        num_lines (int): Lineas a leer para la deteccion.

    Retorna:
        str: El separador detectado (';', ',' o '\\t').
    """
    try:
        with open(file_path, "r", encoding="latin-1", errors="ignore") as f:
            lines = [f.readline() for _ in range(num_lines)]
        text = "".join(lines)
        scores = {}
        for sep in [";", ",", "\t"]:
            scores[sep] = text.count(sep)
        separator = max(scores, key=scores.get)
        print(f"  Separador detectado: '{separator}' (puntaje={scores[separator]})")
        return separator
    except Exception as e:
        print(f"  Error detectando separador: {e}")
        return ";"


def load_all_csvs(path=None, max_records_per_file=None):
    """
    Lee todos los archivos CSV de la carpeta y los combina en un DataFrame.

    Parametros:
        path (str): Directorio con los CSV. Usa RUTA_CSV por defecto.
        max_records_per_file (int): Máximo de filas a leer por archivo CSV.
                                    Si es None, usa MAX_RECORDS_PER_FILE de entorno.

    Retorna:
        pd.DataFrame: DataFrame combinado, o None si no hay archivos.
    """
    if path is None:
        path = RUTA_CSV
    try:
        files = sorted(glob.glob(os.path.join(path, "*.csv")))
        if not files:
            print("ERROR: No se encontraron archivos CSV en:", path)
            return None

        # ── Muestreo por archivo ────────────────────────────────
        if max_records_per_file is None:
            max_records_per_file = os.environ.get("MAX_RECORDS_PER_FILE")
            if max_records_per_file is not None:
                max_records_per_file = int(max_records_per_file)
                print(f"MAX_RECORDS_PER_FILE={max_records_per_file:,} filas por archivo")
            else:
                max_records_per_file = os.environ.get("MAX_RECORDS")
                if max_records_per_file is not None:
                    max_records_per_file = max(1, int(max_records_per_file) // len(files))
                    print(f"MAX_RECORDS={max_records_per_file:,} filas por archivo (proporcional)")

        print(f"Archivos encontrados: {len(files)}")
        dataframes = []
        for file_path in files:
            try:
                sep = detect_separator(file_path)
                # Probar encoding latin-1, fallback a utf-8
                try:
                    df = pd.read_csv(
                        file_path, sep=sep, encoding="latin-1", low_memory=False,
                        on_bad_lines="skip", nrows=max_records_per_file
                    )
                except UnicodeDecodeError:
                    df = pd.read_csv(
                        file_path, sep=sep, encoding="utf-8", low_memory=False,
                        on_bad_lines="skip", nrows=max_records_per_file
                    )
                dataframes.append(df)
                print(f"  OK {os.path.basename(file_path)}: {len(df):,} filas x {len(df.columns)} cols")
            except Exception as e:
                print(f"  ERROR en {os.path.basename(file_path)}: {e}")

        if not dataframes:
            print("ERROR: No se pudo leer ningun archivo.")
            return None

        combined_df = pd.concat(dataframes, ignore_index=True)
        print(f"\nTotal filas combinadas: {len(combined_df):,}")
        print(f"Columnas detectadas: {combined_df.columns.tolist()}")
        return combined_df
    except Exception as e:
        print(f"ERROR en cargar_todos_los_csv: {e}")
        return None


def standardize_text(raw_df, text_columns=None):
    """
    Convierte columnas de texto a mayusculas y elimina espacios al inicio/final.

    Parametros:
        raw_df (pd.DataFrame): DataFrame a procesar.
        text_columns (list): Lista de columnas de texto. Si es None, se
                                detectan automaticamente.

    Retorna:
        pd.DataFrame: DataFrame con texto estandarizado.
    """
    try:
        if text_columns is None:
            text_columns = raw_df.select_dtypes(include=["object"]).columns.tolist()
        for col in text_columns:
            if col in raw_df.columns:
                raw_df[col] = raw_df[col].astype(str).str.strip().str.upper()
        return raw_df
    except Exception as e:
        print(f"ERROR en standardize_text: {e}")
        return raw_df


def clean_dataframe(df):
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
            "FECHA_CONSUMO_DESDE", "FECHA_CONSUMO_HASTA"
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
        df = standardize_text(df)

        # ── 5. Deteccion de outliers (z-score > 3) ─────────────────
        df = mark_outliers(df)

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
        print(f"ERROR en clean_dataframe: {e}")
        return df


def mark_outliers(raw_df, ZSCORE_THRESHOLD=3):
    """
    Marca outliers usando z-score > umbral en CONSUMO e IMPORTE.

    Parametros:
        raw_df (pd.DataFrame): DataFrame con columnas CONSUMO e IMPORTE.
        ZSCORE_THRESHOLD (int/float): Numero de desviaciones estandar para el corte.

    Retorna:
        pd.DataFrame: Con columna adicional 'outlier' (bool).
    """
    try:
        raw_df["outlier"] = False
        for col in ["CONSUMO", "IMPORTE"]:
            if col in raw_df.columns:
                mean = raw_df[col].mean()
                std_dev = raw_df[col].std()
                if std_dev > 0:
                    z_score = (raw_df[col] - mean).abs() / std_dev
                    raw_df["outlier"] = raw_df["outlier"] | (z_score > ZSCORE_THRESHOLD)
        return raw_df
    except Exception as e:
        print(f"ERROR en mark_outliers: {e}")
        raw_df["outlier"] = False
        return raw_df


def split_tables(df):
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
            "FECHA_CONSUMO_DESDE", "FECHA_CONSUMO_HASTA"
        ]
        cols_fact_ok = [c for c in cols_fact if c in df.columns]
        fact = df[cols_fact_ok].copy()
        fact = fact.dropna(subset=["NRO_DOC_FAC"])
        fact = fact.drop_duplicates(subset=["NRO_DOC_FAC", "PERIODO"])
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
        print(f"ERROR en split_tables: {e}")
        return df, pd.DataFrame()


def validate_quality(fact_df, dim_df, total_original=0):
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
        total_fact = len(fact_df)
        nulos_consumo = fact_df["CONSUMO"].isna().sum()
        nulos_importe = fact_df["IMPORTE"].isna().sum()
        consumo_cero = (fact_df["CONSUMO"] <= 0).sum()
        importe_cero = (fact_df["IMPORTE"] <= 0).sum()
        fact_sin_dim = total_fact - fact_df["NRO_DOC_FAC"].isin(dim_df["NRO_DOC_FAC"]).sum()

        tasa_validez_pct = round(
            total_fact / max(total_original, 1) * 100, 2
        )

        metrics = {
            "total_filas_originales":  int(total_original),
            "total_fact_consumo":      int(total_fact),
            "total_dim_cliente":       int(len(dim_df)),
            "nulos_consumo":           int(nulos_consumo),
            "nulos_importe":           int(nulos_importe),
            "consumo_cero_o_negativo": int(consumo_cero),
            "importe_cero_o_negativo": int(importe_cero),
            "fact_sin_dim_asociada":   int(fact_sin_dim),
            "tasa_validez_pct":        tasa_validez_pct,
            "oe1_cumplido":            bool(tasa_validez_pct >= 85.0)
        }
        return metrics
    except Exception as e:
        print(f"ERROR en validate_quality: {e}")
        return {}


def save_results(fact_df, dim_df, raw_cleaned_df=None):
    """
    Guarda CSV limpio, FACT_CONSUMO y DIM_CLIENTE_UBICACION en disco.

    Parametros:
        fact_df (pd.DataFrame): Tabla de hechos.
        dim_df (pd.DataFrame): Tabla dimensional.
        raw_cleaned_df (pd.DataFrame): DataFrame completo limpio (opcional).
    """
    try:
        os.makedirs(RUTA_DATA, exist_ok=True)
        os.makedirs(RUTA_SERVING, exist_ok=True)

        if raw_cleaned_df is not None:
            path_cleaned = os.path.join(RUTA_DATA, "hidrandina_limpio.csv")
            raw_cleaned_df.to_csv(path_cleaned, index=False, encoding="utf-8-sig")
            print(f"  CSV limpio guardado: {path_cleaned}")

        path_fact = os.path.join(RUTA_DATA, "FACT_CONSUMO.csv")
        fact_df.to_csv(path_fact, index=False, encoding="utf-8-sig")
        print(f"  FACT_CONSUMO guardado: {path_fact} ({len(fact_df):,} filas)")

        path_dim = os.path.join(RUTA_DATA, "DIM_CLIENTE_UBICACION.csv")
        dim_df.to_csv(path_dim, index=False, encoding="utf-8-sig")
        print(f"  DIM_CLIENTE_UBICACION guardado: {path_dim} ({len(dim_df):,} filas)")

        print("  Archivos guardados correctamente.")
    except Exception as e:
        print(f"ERROR en save_results: {e}")


def generate_quality_report(metrics, path=None):
    """
    Genera un reporte JSON con las metricas de calidad.

    Parametros:
        metrics (dict): Diccionario con metricas de validate_quality.
        path (str): Ruta de salida. Por defecto serving_layer/reporte_calidad.json.
    """
    try:
        if path is None:
            path = os.path.join(RUTA_SERVING, "reporte_calidad.json")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        import json
        with open(path, "w", encoding="utf-8") as f:
            json.dump(
                {k: (bool(v) if isinstance(v, (bool, np.bool_)) else 
                     int(v) if isinstance(v, (np.integer,)) else
                     float(v) if isinstance(v, (np.floating,)) else v)
                 for k, v in metrics.items()},
                f, indent=2, ensure_ascii=False
            )
        print(f"  Reporte calidad guardado: {path}")
    except Exception as e:
        print(f"ERROR en generate_quality_report: {e}")


def execute(max_records_per_file=None):
    """
    Ejecuta el pipeline completo de loader:
    1. Carga todos los CSV
    2. Limpia el DataFrame
    3. Separa en FACT_CONSUMO y DIM_CLIENTE_UBICACION
    4. Valida calidad (OE1)
    5. Guarda resultados

    Parametros:
        max_records_per_file (int): Máximo de filas a leer por archivo CSV.
                                    Si es None, usa MAX_RECORDS_PER_FILE de entorno.

    Retorna:
        tuple: (fact_df, dim_df, metrics) o (None, None, None) si falla.
    """
    print("=" * 60)
    print("LOADER — Carga y limpieza del dataset Hidrandina")
    print("=" * 60)

    raw_df = load_all_csvs(max_records_per_file=max_records_per_file)
    if raw_df is None or raw_df.empty:
        print("ERROR: No hay datos para procesar.")
        return None, None, None

    total_original = len(raw_df)

    raw_df = clean_dataframe(raw_df)
    if raw_df.empty:
        print("ERROR: DataFrame vacio tras limpieza.")
        return None, None, None

    fact_df, dim_df = split_tables(raw_df)
    if fact_df.empty:
        print("ERROR: FACT_CONSUMO vacia.")
        return None, None, None

    metrics = validate_quality(fact_df, dim_df, total_original)
    save_results(fact_df, dim_df, raw_cleaned_df=raw_df)
    generate_quality_report(metrics)

    print("\nRESUMEN DE CALIDAD (OE1):")
    print(f"  Tasa de validez: {metrics.get('tasa_validez_pct', 0):.2f}%")
    print(f"  OE1 cumplido: {'SI' if metrics.get('oe1_cumplido') else 'NO'}")
    print("=" * 60)

    return fact_df, dim_df, metrics


if __name__ == "__main__":
    # Limitado a 100k filas por archivo para evitar sobrecarga de memoria
    execute(max_records_per_file=100000)
