import pandas as pd
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Rutas
RUTA_SPEED = os.environ.get("RUTA_SPEED", "speed_layer")
csv_path = "FACT_ANOMALIAS_STREAM.csv"
output_png = "analisis_riesgo_fase4.png"

def generar_grafico_fase4():
    print(f"Buscando archivo de anomalías en: {csv_path}")
    if not os.path.exists(csv_path):
        print("ERROR: No se encontró el archivo FACT_ANOMALIAS_STREAM.csv")
        return

    # Leer CSV
    df = pd.read_csv(csv_path, encoding='utf-8-sig')
    
    # Configurar estilo
    fig, axes = plt.subplots(1, 2, figsize=(15, 6))
    fig.suptitle('Fase 4: Análisis de Anomalías en Tiempo Real', fontsize=16, fontweight='bold')

    # Gráfico 1: Distribución por Nivel de Riesgo
    riesgo_counts = df['nivel_riesgo'].value_counts()
    colores = {'Riesgo Alto': '#e74c3c', 'Riesgo Medio': '#f39c12', 'Riesgo Bajo': '#27ae60', 'Normal': '#bdc3c7'}
    
    axes[0].pie(riesgo_counts.values, labels=riesgo_counts.index, 
                autopct='%1.1f%%', startangle=90,
                colors=[colores.get(x, '#95a5a6') for x in riesgo_counts.index])
    axes[0].set_title('Distribución de Recibos por Nivel de Riesgo')

    # Gráfico 2: Top Tipos de Anomalía (Barras)
    counts_anomalia = df['tipo_anomalia'].value_counts()
    axes[1].barh(counts_anomalia.index, counts_anomalia.values, color='#3498db')
    axes[1].set_title('Conteo por Tipo de Anomalía')
    axes[1].set_xlabel('Cantidad de Recibos')
    axes[1].set_ylabel('')

    plt.tight_layout()
    plt.savefig(output_png, dpi=300, bbox_inches='tight')
    print(f"Gráfico guardado exitosamente como: {output_png}")

if __name__ == "__main__":
    generar_grafico_fase4()
