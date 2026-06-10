"""
analisis_exploratorio.py — Analisis estadistico y visualizacion
de los datos de consumo electrico de Hidrandina.

Genera graficos para identificar valores atipicos y calidad del dato.
"""

import os
import glob
import warnings
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

warnings.filterwarnings("ignore")

RUTA_CSV = os.path.join(os.path.dirname(__file__), "data", "originales")
RUTA_SALIDA = os.path.join(os.path.dirname(__file__), "analisis_graficos.png")


def cargar_muestra(ruta, n_max=200000):
    """
    Carga una muestra representativa de los CSV.
    Toma los primeros archivos para tener ~200k registros.
    """
    archivos = sorted(glob.glob(os.path.join(ruta, "*.csv")))[:3]  # 3 meses
    print(f"Archivos a leer: {len(archivos)}")
    dfs = []
    total = 0
    for archivo in archivos:
        sep = ";" if ";" in open(archivo, encoding="latin-1").readline() else ","
        df = pd.read_csv(archivo, sep=sep, encoding="latin-1", low_memory=False, nrows=n_max // len(archivos))
        dfs.append(df)
        total += len(df)
        print(f"  {os.path.basename(archivo)}: {len(df):,} filas")
    df = pd.concat(dfs, ignore_index=True)
    print(f"\nTotal muestra: {len(df):,} filas")
    return df


def analizar(df):
    """Calcula estadisticas y genera graficos."""
    for col in ["CONSUMO", "IMPORTE"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["CONSUMO", "IMPORTE"])
    df = df[(df["CONSUMO"] > 0) & (df["IMPORTE"] > 0)]

    print("\n=== ESTADISTICAS DESCRIPTIVAS ===")
    for col in ["CONSUMO", "IMPORTE"]:
        s = df[col]
        media = s.mean()
        std = s.std()
        print(f"\n{col}:")
        print(f"  Media:     {media:,.2f}")
        print(f"  Std Dev:   {std:,.2f}")
        print(f"  Min:       {s.min():,.2f}")
        print(f"  Max:       {s.max():,.2f}")
        print(f"  P1:        {s.quantile(0.01):,.2f}")
        print(f"  P25:       {s.quantile(0.25):,.2f}")
        print(f"  P50:       {s.median():,.2f}")
        print(f"  P75:       {s.quantile(0.75):,.2f}")
        print(f"  P99:       {s.quantile(0.99):,.2f}")

    # Outliers por z-score
    print("\n=== DETECCION DE OUTLIERS (|z| > 3) ===")
    for col in ["CONSUMO", "IMPORTE"]:
        s = df[col]
        z = (s - s.mean()).abs() / s.std()
        outliers = (z > 3).sum()
        print(f"  {col}: {outliers:,} outliers ({outliers/len(df)*100:.2f}%)")

    return df


def generar_graficos(df):
    """Genera 4 graficos estadisticos."""
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle(
        "Analisis Exploratorio - Consumo Electrico Hidrandina",
        fontsize=16, fontweight="bold"
    )

    # 1. Histograma CONSUMO
    ax1 = axes[0, 0]
    consumo = df["CONSUMO"]
    consumo_capped = consumo.clip(upper=consumo.quantile(0.99))
    ax1.hist(consumo_capped, bins=80, color="#2196F3", edgecolor="white", alpha=0.8)
    ax1.axvline(consumo.mean(), color="red", linestyle="--", label=f"Media: {consumo.mean():.0f}")
    ax1.axvline(consumo.median(), color="orange", linestyle="--", label=f"Mediana: {consumo.median():.0f}")
    ax1.set_xlabel("Consumo (kWh)")
    ax1.set_ylabel("Frecuencia")
    ax1.set_title("Distribucion del Consumo (kWh)")
    ax1.legend()
    ax1.grid(alpha=0.3)

    # 2. Histograma IMPORTE
    ax2 = axes[0, 1]
    importe = df["IMPORTE"]
    importe_capped = importe.clip(upper=importe.quantile(0.99))
    ax2.hist(importe_capped, bins=80, color="#FF5722", edgecolor="white", alpha=0.8)
    ax2.axvline(importe.mean(), color="red", linestyle="--", label=f"Media: {importe.mean():.0f}")
    ax2.axvline(importe.median(), color="orange", linestyle="--", label=f"Mediana: {importe.median():.0f}")
    ax2.set_xlabel("Importe (S/)")
    ax2.set_ylabel("Frecuencia")
    ax2.set_title("Distribucion del Importe (S/)")
    ax2.legend()
    ax2.grid(alpha=0.3)

    # 3. Boxplot comparativo
    ax3 = axes[1, 0]
    df_sample = df.sample(min(50000, len(df)))
    bp = ax3.boxplot(
        [df_sample["CONSUMO"], df_sample["IMPORTE"]],
        labels=["Consumo (kWh)", "Importe (S/)"],
        patch_artist=True,
        boxprops=dict(facecolor="#2196F3", alpha=0.6),
        medianprops=dict(color="red", linewidth=2),
        flierprops=dict(marker="o", markerfacecolor="red", markersize=3, alpha=0.5)
    )
    ax3.set_title("Boxplot - Consumo e Importe")
    ax3.grid(alpha=0.3, axis="y")

    # 4. Z-score vs Outliers
    ax4 = axes[1, 1]
    z_consumo = ((df["CONSUMO"] - df["CONSUMO"].mean()) / df["CONSUMO"].std()).abs()
    z_importe = ((df["IMPORTE"] - df["IMPORTE"].mean()) / df["IMPORTE"].std()).abs()
    ax4.scatter(
        df["CONSUMO"].sample(10000), df["IMPORTE"].sample(10000),
        c="blue", alpha=0.3, s=5, label="Normal"
    )
    mask_out = (z_consumo > 3) | (z_importe > 3)
    df_out = df[mask_out]
    ax4.scatter(
        df_out["CONSUMO"].sample(min(500, len(df_out))),
        df_out["IMPORTE"].sample(min(500, len(df_out))),
        c="red", alpha=0.7, s=15, label=f"Outliers (n={mask_out.sum():,})"
    )
    ax4.set_xlabel("Consumo (kWh)")
    ax4.set_ylabel("Importe (S/)")
    ax4.set_title("Consumo vs Importe - Outliers destacados")
    ax4.legend()
    ax4.grid(alpha=0.3)

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig(RUTA_SALIDA, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"\nGrafico guardado: {RUTA_SALIDA}")


def ejecutar():
    print("=" * 60)
    print("ANALISIS EXPLORATORIO - Hidrandina")
    print("=" * 60)
    df = cargar_muestra(RUTA_CSV)
    if df.empty:
        print("No hay datos.")
        return
    df = analizar(df)
    generar_graficos(df)
    print("\nAnalisis completado. Revisa el grafico generado.")


if __name__ == "__main__":
    ejecutar()
