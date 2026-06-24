"""
loader.py — Carga, limpieza y separación del dataset Hidrandina

Funciones principales:
    - detect_separator: Detecta automáticamente el separador del CSV
    - load_all_csvs: Lee y combina todos los CSV mensuales
    - clean_dataframe: Aplica las 6 reglas de limpieza definidas
    - split_tables: Genera FACT_CONSUMO y DIM_CLIENTE_UBICACION
    - mark_outliers: Detecta outliers con z-score > 3
    - standardize_text: Convierte texto a mayúsculas y aplica strip
    - validate_quality: Calcula tasa de registros válidos (KPI OE1)
    - save_results: Persiste CSV limpio y tablas separadas

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

csv_originales_path = os.environ.get(
    "RUTA_CSV_ORIGINALES",
    os.path.join(os.path.dirname(__file__), "..", "data", "originales")
)
data_path = os.environ.get("RUTA_DATA", os.path.join(os.path.dirname(__file__), "..", "data"))
serving_path = os.environ.get("RUTA_SERVING", os.path.join(os.path.dirname(__file__), "..", "serving_layer"))


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
        with open(file_path, "r", encoding="latin-1", errors="ignore") as text_file:
            sample_lines = [text_file.readline() for _ in range(num_lines)]
        sample_text = "".join(sample_lines)
        separator_scores = {}
        for candidate_separator in [";", ",", "\t"]:
            separator_scores[candidate_separator] = sample_text.count(candidate_separator)
        best_separator = max(separator_scores, key=separator_scores.get)
        print(f"  Separador detectado: '{best_separator}' (puntaje={separator_scores[best_separator]})")
        return best_separator
    except Exception as e:
        print(f"  Error detectando separador: {e}")
        return ";"


def load_all_csvs(path=None, max_records_per_file=None):
    """
    Lee todos los archivos CSV de la carpeta y los combina en un DataFrame.

    Parametros:
        path (str): Directorio con los CSV. Usa csv_originales_path por defecto.
        max_records_per_file (int): Máximo de filas a leer por archivo CSV.
                                    Si es None, usa MAX_RECORDS_PER_FILE de entorno.

    Retorna:
        pd.DataFrame: DataFrame combinado, o None si no hay archivos.
    """
    if path is None:
        path = csv_originales_path
    try:
        csv_files = sorted(glob.glob(os.path.join(path, "*.csv")))
        if not csv_files:
            print("ERROR: No se encontraron archivos CSV en:", path)
            return None

        # ── muestreo por archivo ────────────────────────────────
        if max_records_per_file is None:
            max_records_per_file = os.environ.get("MAX_RECORDS_PER_FILE")
            if max_records_per_file is not None:
                max_records_per_file = int(max_records_per_file)
                print(f"MAX_RECORDS_PER_FILE={max_records_per_file:,} filas por archivo")
            else:
                max_records_per_file = os.environ.get("MAX_RECORDS")
                if max_records_per_file is not None:
                    max_records_per_file = max(1, int(max_records_per_file) // len(csv_files))
                    print(f"MAX_RECORDS={max_records_per_file:,} filas por archivo (proporcional)")

        print(f"Archivos encontrados: {len(csv_files)}")
        monthly_dfs = []
        for csv_file_path in csv_files:
            try:
                separator = detect_separator(csv_file_path)
                # probar encoding latin-1, fallback a utf-8
                try:
                    monthly_df = pd.read_csv(
                        csv_file_path, sep=separator, encoding="latin-1", low_memory=False,
                        on_bad_lines="skip", nrows=max_records_per_file
                    )
                except UnicodeDecodeError:
                    monthly_df = pd.read_csv(
                        csv_file_path, sep=separator, encoding="utf-8", low_memory=False,
                        on_bad_lines="skip", nrows=max_records_per_file
                    )
                monthly_dfs.append(monthly_df)
                print(f"  OK {os.path.basename(csv_file_path)}: {len(monthly_df):,} filas x {len(monthly_df.columns)} cols")
            except Exception as e:
                print(f"  ERROR en {os.path.basename(csv_file_path)}: {e}")

        if not monthly_dfs:
            print("ERROR: No se pudo leer ningun archivo.")
            return None

        combined_df = pd.concat(monthly_dfs, ignore_index=True)
        print(f"\nTotal filas combinadas: {len(combined_df):,}")
        print(f"Columnas detectadas: {combined_df.columns.tolist()}")
        return combined_df
    except Exception as e:
        print(f"ERROR en load_all_csvs: {e}")
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
        for column_name in text_columns:
            if column_name in raw_df.columns:
                raw_df[column_name] = raw_df[column_name].astype(str).str.strip().str.upper()
        return raw_df
    except Exception as e:
        print(f"ERROR en standardize_text: {e}")
        return raw_df


def clean_dataframe(raw_df):
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
        raw_df (pd.DataFrame): DataFrame crudo.

    Retorna:
        pd.DataFrame: DataFrame limpio con columna 'outlier' bool.
    """
    try:
        print("\n=== INICIO LIMPIEZA ===")
        # limpiar bom en nombres de columnas
        raw_df.columns = raw_df.columns.str.replace('ï»¿', '', regex=False).str.strip()
        initial_row_count = len(raw_df)

        # ── 4. conversion de tipos ──────────────────────────────────
        # columnas de fecha
        date_columns = [
            "FECHA_EMISION", "FECHA_VENCIMIENTO",
            "FECHA_CONSUMO_DESDE", "FECHA_CONSUMO_HASTA"
        ]
        for date_column in date_columns:
            if date_column in raw_df.columns:
                raw_df[date_column] = pd.to_datetime(
                    raw_df[date_column], format="%Y%m%d", errors="coerce"
                )

        # columnas numericas
        for numeric_column in ["IMPORTE", "CONSUMO"]:
            if numeric_column in raw_df.columns:
                raw_df[numeric_column] = pd.to_numeric(raw_df[numeric_column], errors="coerce")

        if "PERIODO" in raw_df.columns:
            raw_df["PERIODO"] = pd.to_numeric(raw_df["PERIODO"], errors="coerce").astype("Int64")

        if "NRO_SERVICIO" in raw_df.columns:
            raw_df["NRO_SERVICIO"] = pd.to_numeric(raw_df["NRO_SERVICIO"], errors="coerce").astype("Int64")

        print(f"  Conversion de tipos OK")

        # ── 2. eliminacion de nulos en importe y consumo ────────────
        rows_before_null_drop = len(raw_df)
        raw_df = raw_df.dropna(subset=["IMPORTE", "CONSUMO"])
        null_rows_removed = rows_before_null_drop - len(raw_df)
        print(f"  Nulos eliminados (IMPORTE/CONSUMO): {null_rows_removed:,}")

        # ── 3. filtrado de valores invalidos (<= 0) ─────────────────
        rows_before_invalid_drop = len(raw_df)
        raw_df = raw_df[raw_df["CONSUMO"] > 0]
        raw_df = raw_df[raw_df["IMPORTE"] > 0]
        invalid_rows_removed = rows_before_invalid_drop - len(raw_df)
        print(f"  Valores <=0 eliminados: {invalid_rows_removed:,}")

        # ── 6. estandarizacion de texto ─────────────────────────────
        raw_df = standardize_text(raw_df)

        # ── 5. deteccion de outliers (z-score > 3) ─────────────────
        raw_df = mark_outliers(raw_df)

        # ── reporte final ───────────────────────────────────────────
        final_row_count = len(raw_df)
        rows_removed = initial_row_count - final_row_count
        valid_rate_pct = (final_row_count / initial_row_count) * 100 if initial_row_count > 0 else 0
        print(f"\n  Filas iniciales: {initial_row_count:,}")
        print(f"  Filas finales:   {final_row_count:,}")
        print(f"  Eliminadas:      {rows_removed:,}")
        print(f"  Tasa validez:    {valid_rate_pct:.2f}%")
        print(f"  Outliers marcados: {raw_df['outlier'].sum():,}")
        print("=== FIN LIMPIEZA ===\n")

        return raw_df
    except Exception as e:
        print(f"ERROR en clean_dataframe: {e}")
        return raw_df


def mark_outliers(raw_df, zscore_threshold=3):
    """
    Marca outliers usando z-score > umbral en CONSUMO e IMPORTE.

    Parametros:
        raw_df (pd.DataFrame): DataFrame con columnas CONSUMO e IMPORTE.
        zscore_threshold (int/float): Numero de desviaciones estandar para el corte.

    Retorna:
        pd.DataFrame: Con columna adicional 'outlier' (bool).
    """
    try:
        raw_df["outlier"] = False
        for numeric_column in ["CONSUMO", "IMPORTE"]:
            if numeric_column in raw_df.columns:
                column_mean = raw_df[numeric_column].mean()
                column_std = raw_df[numeric_column].std()
                if column_std > 0:
                    zscore = (raw_df[numeric_column] - column_mean).abs() / column_std
                    raw_df["outlier"] = raw_df["outlier"] | (zscore > zscore_threshold)
        return raw_df
    except Exception as e:
        print(f"ERROR en mark_outliers: {e}")
        raw_df["outlier"] = False
        return raw_df


def split_tables(raw_df):
    """
    Separa el DataFrame en FACT_CONSUMO y DIM_CLIENTE_UBICACION.
    Usa NRO_DOC_FAC como identificador único porque NRO_SERVICIO
    está anonimizado con 0 en el dataset real de Hidrandina.
    """
    try:
        print("=== SEPARANDO TABLAS ===")

        # limpiar bom en nombres de columnas
        raw_df.columns = raw_df.columns.str.replace('ï»¿', '', regex=False).str.strip()

        # ── fact_consumo ─────────────────────────────────────────
        fact_columns = [
            "NRO_DOC_FAC", "PERIODO", "CONSUMO", "IMPORTE",
            "FECHA_EMISION", "FECHA_VENCIMIENTO",
            "FECHA_CONSUMO_DESDE", "FECHA_CONSUMO_HASTA"
        ]
        available_fact_columns = [c for c in fact_columns if c in raw_df.columns]
        fact_df = raw_df[available_fact_columns].copy()
        fact_df = fact_df.dropna(subset=["NRO_DOC_FAC"])
        fact_df = fact_df.drop_duplicates(subset=["NRO_DOC_FAC", "PERIODO"])
        print(f"  FACT_CONSUMO: {len(fact_df):,} filas x {len(fact_df.columns)} cols")

        # ── dim_cliente_ubicacion ────────────────────────────────
        dim_columns = [
            "NRO_DOC_FAC", "DEPARTAMENTO", "PROVINCIA", "DISTRITO",
            "UBIGEO", "TARIFA", "CARTERA", "UNIDAD_NEGOCIO"
        ]
        available_dim_columns = [c for c in dim_columns if c in raw_df.columns]
        dim_df = raw_df[available_dim_columns].copy()
        dim_df = dim_df.dropna(subset=["NRO_DOC_FAC"])
        dim_df = dim_df.drop_duplicates(subset=["NRO_DOC_FAC"])
        print(f"  DIM_CLIENTE_UBICACION: {len(dim_df):,} filas x {len(dim_df.columns)} cols")
        print("=== TABLAS SEPARADAS ===\n")

        return fact_df, dim_df

    except Exception as e:
        print(f"ERROR en split_tables: {e}")
        return raw_df, pd.DataFrame()


def validate_quality(fact_df, dim_df, total_original=0):
    """
    Calcula indicadores de calidad (KPI OE1).

    Parametros:
        fact_df (pd.DataFrame): FACT_CONSUMO.
        dim_df (pd.DataFrame): DIM_CLIENTE_UBICACION.
        total_original (int): Filas originales antes de limpieza.

    Retorna:
        dict: Diccionario con metricas de calidad.
    """
    try:
        total_fact_rows = len(fact_df)
        null_consumption_count = fact_df["CONSUMO"].isna().sum()
        null_billing_count = fact_df["IMPORTE"].isna().sum()
        zero_consumption_count = (fact_df["CONSUMO"] <= 0).sum()
        zero_billing_count = (fact_df["IMPORTE"] <= 0).sum()
        fact_without_dim_count = total_fact_rows - fact_df["NRO_DOC_FAC"].isin(dim_df["NRO_DOC_FAC"]).sum()

        valid_rate_pct = round(
            total_fact_rows / max(total_original, 1) * 100, 2
        )

        quality_metrics = {
            "total_filas_originales":  int(total_original),
            "total_fact_consumo":      int(total_fact_rows),
            "total_dim_cliente":       int(len(dim_df)),
            "nulos_consumo":           int(null_consumption_count),
            "nulos_importe":           int(null_billing_count),
            "consumo_cero_o_negativo": int(zero_consumption_count),
            "importe_cero_o_negativo": int(zero_billing_count),
            "fact_sin_dim_asociada":   int(fact_without_dim_count),
            "tasa_validez_pct":        valid_rate_pct,
            "oe1_cumplido":            bool(valid_rate_pct >= 85.0)
        }
        return quality_metrics
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
        os.makedirs(data_path, exist_ok=True)
        os.makedirs(serving_path, exist_ok=True)

        if raw_cleaned_df is not None:
            cleaned_csv_path = os.path.join(data_path, "hidrandina_limpio.csv")
            raw_cleaned_df.to_csv(cleaned_csv_path, index=False, encoding="utf-8-sig")
            print(f"  CSV limpio guardado: {cleaned_csv_path}")

        fact_csv_path = os.path.join(data_path, "FACT_CONSUMO.csv")
        fact_df.to_csv(fact_csv_path, index=False, encoding="utf-8-sig")
        print(f"  FACT_CONSUMO guardado: {fact_csv_path} ({len(fact_df):,} filas)")

        dim_csv_path = os.path.join(data_path, "DIM_CLIENTE_UBICACION.csv")
        dim_df.to_csv(dim_csv_path, index=False, encoding="utf-8-sig")
        print(f"  DIM_CLIENTE_UBICACION guardado: {dim_csv_path} ({len(dim_df):,} filas)")

        print("  Archivos guardados correctamente.")
    except Exception as e:
        print(f"ERROR en save_results: {e}")


def generate_quality_report(quality_metrics, path=None):
    """
    Genera un reporte JSON con las metricas de calidad.

    Parametros:
        quality_metrics (dict): Diccionario con metricas de validate_quality.
        path (str): Ruta de salida. Por defecto serving_layer/reporte_calidad.json.
    """
    try:
        if path is None:
            path = os.path.join(serving_path, "reporte_calidad.json")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        import json
        with open(path, "w", encoding="utf-8") as report_file:
            json.dump(
                {key: (bool(value) if isinstance(value, (bool, np.bool_)) else
                       int(value) if isinstance(value, (np.integer,)) else
                       float(value) if isinstance(value, (np.floating,)) else value)
                 for key, value in quality_metrics.items()},
                report_file, indent=2, ensure_ascii=False
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
        tuple: (fact_df, dim_df, quality_metrics) o (None, None, None) si falla.
    """
    print("=" * 60)
    print("LOADER — Carga y limpieza del dataset Hidrandina")
    print("=" * 60)

    raw_df = load_all_csvs(max_records_per_file=max_records_per_file)
    if raw_df is None or raw_df.empty:
        print("ERROR: No hay datos para procesar.")
        return None, None, None

    total_original_rows = len(raw_df)

    raw_df = clean_dataframe(raw_df)
    if raw_df.empty:
        print("ERROR: DataFrame vacio tras limpieza.")
        return None, None, None

    fact_df, dim_df = split_tables(raw_df)
    if fact_df.empty:
        print("ERROR: FACT_CONSUMO vacia.")
        return None, None, None

    quality_metrics = validate_quality(fact_df, dim_df, total_original_rows)
    save_results(fact_df, dim_df, raw_cleaned_df=raw_df)
    generate_quality_report(quality_metrics)

    print("\nRESUMEN DE CALIDAD (OE1):")
    print(f"  Tasa de validez: {quality_metrics.get('tasa_validez_pct', 0):.2f}%")
    print(f"  OE1 cumplido: {'SI' if quality_metrics.get('oe1_cumplido') else 'NO'}")
    print("=" * 60)

    return fact_df, dim_df, quality_metrics


if __name__ == "__main__":
    # limitado a 100k filas por archivo para evitar sobrecarga de memoria
    execute(max_records_per_file=100000)
