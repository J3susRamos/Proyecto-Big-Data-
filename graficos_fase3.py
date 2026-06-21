import os
import pandas as pd
import matplotlib.pyplot as plt

def generar_graficos_fase3():
    print("="*50)
    print("GENERANDO GRAFICOS DE VALIDACION - FASE 3")
    print("="*50)

    eventos_path = os.path.join("data", "eventos_simples.json")
    
    if os.path.exists(eventos_path):
        print("Cargando eventos simulados (esto puede tomar unos segundos)...")
        # Leer el JSON
        df_eventos = pd.read_json(eventos_path)
        
        # Agrupar por FECHA_EMISION para ver el "tráfico" de eventos
        # Convertimos a formato datetime para la gráfica
        df_eventos['FECHA_EMISION'] = pd.to_datetime(df_eventos['FECHA_EMISION'])
        trafico = df_eventos.groupby('FECHA_EMISION').size()
        
        plt.figure(figsize=(12, 6))
        # Plot de área para simular volumen de tráfico de red
        plt.fill_between(trafico.index, trafico.values, color="#4CAF50", alpha=0.4)
        plt.plot(trafico.index, trafico.values, color="#388E3C", linewidth=2)
        
        plt.title('Volumen de Eventos Transmitidos por Fecha (Simulación de Tráfico en Vivo)', fontsize=14, fontweight='bold')
        plt.xlabel('Fecha de Emisión del Recibo', fontsize=12)
        plt.ylabel('Cantidad de Eventos / Recibos', fontsize=12)
        plt.grid(True, linestyle='--', alpha=0.5)
        
        plt.tight_layout()
        plt.savefig('analisis_trafico_fase3.png', dpi=300)
        print("[OK] Grafico guardado: analisis_trafico_fase3.png")
    else:
        print(f"[X] No se encontro: {eventos_path}")

if __name__ == "__main__":
    generar_graficos_fase3()
