"""
analisis_exploratorio.py — Analisis estadistico y visualizacion
de los datos de consumo electrico de Hidrandina.

Genera graficos para identificar valores atipicos y calidad del dato.
"""

import os
import glob
import warnings
import json
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap

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


def analizar_categoricas(df):
    """
    Analiza variables categoricas para detectar:
    - Valores nulos/vacios
    - Categorias raras (< 10 registros)
    - Inconsistencias (espacios, mayusculas)
    - Distribucion desbalanceada
    """
    print("\n=== ANALISIS DE VARIABLES CATEGORICAS ===")
    
    # Identificar columnas categoricas
    cols_cat = df.select_dtypes(include=['object']).columns.tolist()
    
    # Excluir campos de fecha/texto muy largos
    cols_cat = [col for col in cols_cat if col not in 
                ['FECHA_EMISION', 'FECHA_VENCIMIENTO', 'FECHA_CONSUMO_DESDE', 
                 'FECHA_CONSUMO_HASTA', 'NRO_DOC_FAC']]
    
    stats_categoricas = {}
    
    for col in cols_cat:
        print(f"\n{col}:")
        
        # Nulos
        nulos = df[col].isnull().sum()
        blancos = (df[col].fillna("").str.strip() == "").sum()
        print(f"  Nulos: {nulos:,} | Blancos: {blancos:,}")
        
        # Únicos
        unicos = df[col].nunique()
        print(f"  Valores únicos: {unicos}")
        
        # Top 10
        top10 = df[col].value_counts().head(10)
        for val, count in top10.items():
            pct = count / len(df) * 100
            print(f"    - {val}: {count:,} ({pct:.2f}%)")
        
        # Categorias raras (< 10 registros)
        raras = df[col].value_counts()
        raras_count = (raras < 10).sum()
        if raras_count > 0:
            print(f"  ⚠️  Categorias raras (n < 10): {raras_count}")
            for val, count in raras[raras < 10].items():
                print(f"      - {val}: {count}")
        
        # Detectar inconsistencias de espacios
        vals_unicos = df[col].unique()
        vals_sin_espacios = set(str(v).strip() for v in vals_unicos if pd.notna(v))
        if len(vals_sin_espacios) < unicos:
            print(f"  ⚠️  Posibles inconsistencias de espacios detectadas")
        
        stats_categoricas[col] = {
            "nulos": int(nulos),
            "blancos": int(blancos),
            "unicos": int(unicos),
            "raras": int(raras_count)
        }
    
    return stats_categoricas


def generar_graficos_categoricas(df):
    """
    Genera graficos para analizar distribuciones categoricas.
    """
    # Identificar columnas categoricas principales
    cols_cat = ['DEPARTAMENTO', 'PROVINCIA', 'DISTRITO', 'TARIFA', 'CARTERA', 'UNIDAD_NEGOCIO']
    cols_cat = [col for col in cols_cat if col in df.columns]
    
    if not cols_cat:
        print("No hay columnas categoricas para graficar")
        return
    
    # Crear figura con subplots para cada categorica
    n_cols = min(len(cols_cat), 3)
    n_rows = (len(cols_cat) + n_cols - 1) // n_cols
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(18, 5 * n_rows))
    
    if n_rows == 1 and n_cols == 1:
        axes = np.array([[axes]])
    elif n_rows == 1 or n_cols == 1:
        axes = axes.reshape(n_rows, n_cols)
    
    fig.suptitle("Analisis de Variables Categoricas - Hidrandina", 
                 fontsize=16, fontweight="bold")
    
    for idx, col in enumerate(cols_cat):
        ax = axes.flatten()[idx]
        
        # Top 15 valores
        top_vals = df[col].value_counts().head(15)
        colors = plt.cm.Set3(np.linspace(0, 1, len(top_vals)))
        
        bars = ax.barh(range(len(top_vals)), top_vals.values, color=colors, edgecolor="black", alpha=0.8)
        ax.set_yticks(range(len(top_vals)))
        ax.set_yticklabels(top_vals.index, fontsize=9)
        ax.set_xlabel("Frecuencia", fontsize=10)
        ax.set_title(f"{col} (Top 15)\n{df[col].nunique()} valores únicos", fontsize=11, fontweight="bold")
        ax.grid(alpha=0.3, axis="x")
        
        # Anotaciones de porcentaje
        for i, (bar, val) in enumerate(zip(bars, top_vals.values)):
            pct = val / len(df) * 100
            ax.text(val, i, f" {pct:.1f}%", va="center", fontsize=8)
    
    # Eliminar subplots vacíos
    for idx in range(len(cols_cat), len(axes.flatten())):
        fig.delaxes(axes.flatten()[idx])
    
    plt.tight_layout(rect=[0, 0, 1, 0.97])
    ruta_cat = os.path.join(os.path.dirname(__file__), "analisis_categoricas.png")
    plt.savefig(ruta_cat, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"\nGrafico categoricas guardado: {ruta_cat}")


def generar_matriz_frecuencias(df):
    """
    Genera matriz de frecuencias entre TARIFA y CARTERA.
    """
    if 'TARIFA' in df.columns and 'CARTERA' in df.columns:
        print("\n=== MATRIZ TARIFA × CARTERA ===")
        
        matriz = pd.crosstab(df['TARIFA'], df['CARTERA'], margins=True)
        print(matriz)
        
        # Gráfico de heatmap
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Matriz sin márgenes para el heatmap
        matriz_sin_margenes = pd.crosstab(df['TARIFA'], df['CARTERA'])
        im = ax.imshow(matriz_sin_margenes.values, cmap='YlOrRd', aspect='auto')
        
        ax.set_xticks(range(len(matriz_sin_margenes.columns)))
        ax.set_yticks(range(len(matriz_sin_margenes.index)))
        ax.set_xticklabels(matriz_sin_margenes.columns, rotation=45, ha='right')
        ax.set_yticklabels(matriz_sin_margenes.index)
        ax.set_xlabel('CARTERA', fontsize=12, fontweight='bold')
        ax.set_ylabel('TARIFA', fontsize=12, fontweight='bold')
        ax.set_title('Matriz de Frecuencias: TARIFA × CARTERA', fontsize=14, fontweight='bold')
        
        # Anotaciones
        for i in range(len(matriz_sin_margenes.index)):
            for j in range(len(matriz_sin_margenes.columns)):
                val = matriz_sin_margenes.iloc[i, j]
                text = ax.text(j, i, f'{int(val):,}', ha='center', va='center', 
                             color='white' if val > matriz_sin_margenes.values.max() / 2 else 'black',
                             fontsize=9, fontweight='bold')
        
        plt.colorbar(im, ax=ax, label='Cantidad de Registros')
        plt.tight_layout()
        
        ruta_matriz = os.path.join(os.path.dirname(__file__), "analisis_matriz_tarifa_cartera.png")
        plt.savefig(ruta_matriz, dpi=150, bbox_inches="tight")
        plt.close()
        print(f"\nMatriz guardada: {ruta_matriz}")
        
        return matriz
    
    return None


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
    print("ANALISIS EXPLORATORIO - Hidrandina (FASE 1.5)")
    print("=" * 60)
    df = cargar_muestra(RUTA_CSV)
    if df.empty:
        print("No hay datos.")
        return
    
    # Análisis numéricos (Fase 1 existente)
    print("\n📊 FASE 1: ANALISIS NUMERICO")
    df = analizar(df)
    generar_graficos(df)
    
    # Análisis categóricos (NUEVO - Fase 1.5)
    print("\n📊 FASE 1.5: ANALISIS CATEGORICO")
    stats_cat = analizar_categoricas(df)
    generar_graficos_categoricas(df)
    matriz = generar_matriz_frecuencias(df)
    
    # Guardar reporte JSON
    reporte = {
        "fase": "1.5 - Calidad de Datos Categóricos",
        "registros_analizados": len(df),
        "variables_categoricas": stats_cat,
        "matriz_tarifa_cartera": matriz.to_dict() if matriz is not None else None
    }
    
    ruta_reporte = os.path.join(os.path.dirname(__file__), "reporte_fase1_5.json")
    with open(ruta_reporte, 'w', encoding='utf-8') as f:
        json.dump(reporte, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ Reporte guardado: {ruta_reporte}")
    print("\nAnalisis completado. Revisa los graficos generados.")


if __name__ == "__main__":
    ejecutar()
