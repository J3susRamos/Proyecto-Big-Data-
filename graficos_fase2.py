import os
import pandas as pd
import matplotlib.pyplot as plt

def generar_graficos():
    print("="*50)
    print("GENERANDO GRAFICOS DE VALIDACION - FASE 2")
    print("="*50)

    batch_dir = os.path.join("serving_layer", "batch_results")
    
    # 1. Gráfico de Segmentación RFM
    rfm_csv_path = os.path.join(batch_dir, "rfm_clientes.csv")
    rfm_parquet_path = os.path.join(batch_dir, "rfm_clientes")
    if os.path.exists(rfm_csv_path) or os.path.isdir(rfm_parquet_path):
        print("Cargando datos de RFM...")
        df_rfm = pd.read_csv(rfm_csv_path) if os.path.exists(rfm_csv_path) else pd.read_parquet(rfm_parquet_path)
        
        # Contar cuántos clientes hay en cada segmento
        segment_counts = df_rfm['segmento'].value_counts()
        
        plt.figure(figsize=(10, 6))
        colors = ['#4CAF50', '#2196F3', '#FF9800', '#F44336']
        bars = plt.bar(segment_counts.index, segment_counts.values, color=colors[:len(segment_counts)])
        plt.title('Distribución de Clientes por Segmento RFM', fontsize=14, fontweight='bold')
        plt.xlabel('Segmento', fontsize=12)
        plt.ylabel('Cantidad de Clientes (NRO_DOC_FAC)', fontsize=12)
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Añadir etiquetas de datos
        for bar in bars:
            yval = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2, yval + (yval*0.01), f'{int(yval):,}', ha='center', va='bottom', fontweight='bold')
            
        plt.tight_layout()
        plt.savefig('analisis_rfm_fase2.png', dpi=300)
        print("[OK] Grafico guardado: analisis_rfm_fase2.png")
    else:
        print(f"[X] No se encontro: {rfm_csv_path} ni {rfm_parquet_path}")

    # 2. Gráfico de Tendencias Históricas
    trend_path = os.path.join(batch_dir, "tendencia_mensual.csv")
    if os.path.exists(trend_path):
        print("Cargando datos de Tendencias...")
        df_trend = pd.read_csv(trend_path)
        
        # Asegurarnos de que está ordenado por PERIODO
        df_trend = df_trend.sort_values('PERIODO')
        # Convertir periodo a string para mejor visualización en X
        df_trend['PERIODO_STR'] = df_trend['PERIODO'].astype(str)
        
        plt.figure(figsize=(12, 6))
        plt.plot(df_trend['PERIODO_STR'], df_trend['total_consumo'] / 1e6, marker='o', linestyle='-', color='#1976D2', linewidth=2)
        plt.title('Tendencia de Consumo Historico por Periodo', fontsize=14, fontweight='bold')
        plt.xlabel('Periodo (YYYYMM)', fontsize=12)
        plt.ylabel('Consumo Total (Millones de kWh)', fontsize=12)
        plt.xticks(rotation=45)
        plt.grid(True, linestyle='--', alpha=0.5)
        
        plt.tight_layout()
        plt.savefig('analisis_tendencia_fase2.png', dpi=300)
        print("[OK] Grafico guardado: analisis_tendencia_fase2.png")
    else:
        print(f"[X] No se encontro: {trend_path}")

if __name__ == "__main__":
    generar_graficos()
