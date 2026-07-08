"""
Servidor FastAPI para el dashboard web de Hidrandina y el Simulador en Tiempo Real.
Sirve serving_layer/ en http://localhost:8050/dashboard.html
Y expone la API de simulacion en http://localhost:8050/api/predict
"""
import os
import sys
import json
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
import webbrowser

project_root = os.path.dirname(os.path.abspath(__file__))
serving_path = os.environ.get(
    "RUTA_SERVING",
    os.path.join(project_root, "serving_layer"),
)
data_path = os.environ.get("RUTA_DATA", os.path.join(project_root, "data"))
port = int(os.environ.get("DASHBOARD_PORT", "8050"))

app = FastAPI(title="Hidrandina Real-Time Simulator API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Cargar estadisticas historicas en memoria al iniciar
stats_df = None

def load_stats():
    global stats_df
    csv_stats_path = os.path.join(serving_path, "batch_results", "tmp_estadisticas_historicas.csv")
    parquet_stats_path = os.path.join(serving_path, "batch_results", "tmp_estadisticas_historicas")
    try:
        if os.path.isfile(csv_stats_path):
            stats_df = pd.read_csv(csv_stats_path, encoding="utf-8-sig")
        elif os.path.isdir(parquet_stats_path):
            stats_df = pd.read_parquet(parquet_stats_path)
        else:
            print(f"Advertencia: No se encontro {csv_stats_path} ni {parquet_stats_path}")
            return
        stats_df.columns = [c.lower() for c in stats_df.columns]
        print(f"Estadisticas cargadas: {len(stats_df)} filas.")
    except Exception as e:
        print(f"Error cargando estadisticas: {e}")


# Esquema de la peticion
class PredictRequest(BaseModel):
    distrito: str
    tarifa: str
    cartera: str
    consumo_actual: float
    importe_actual: float

@app.post("/api/predict")
async def predict_anomaly(req: PredictRequest):
    global stats_df
    if stats_df is None or stats_df.empty:
        raise HTTPException(status_code=500, detail="Estadisticas no cargadas en el servidor.")
        
    # Filtrar el distrito, tarifa y cartera en stats_df
    # Si no hay match exacto, hacemos un match solo por distrito
    match = stats_df[
        (stats_df["distrito"].str.upper() == req.distrito.upper()) &
        (stats_df["tarifa"].str.upper() == req.tarifa.upper()) &
        (stats_df["cartera"].str.upper() == req.cartera.upper())
    ]
    
    if match.empty:
        match = stats_df[stats_df["distrito"].str.upper() == req.distrito.upper()]
        
    if match.empty:
        # Si aun no hay match, tomar valores promedio globales
        prom = float(stats_df["consumo_promedio"].mean())
        std = float(stats_df["consumo_std"].mean())
    else:
        prom = float(match.iloc[0]["consumo_promedio"])
        std = float(match.iloc[0]["consumo_std"])
        
    # Evitar division por cero
    std = std if std != 0 else 1.0
    prom_val = prom if prom != 0 else 1.0
    
    # Calcular Z-Score y Variacion
    zscore = round((req.consumo_actual - prom) / std, 4)
    variacion = round(((req.consumo_actual - prom) / prom_val) * 100, 2)
    
    # Clasificar Anomalia (Logica Fase 4)
    if zscore > 3:
        riesgo = "Alto"
        tipo = "Consumo extremadamente alto"
    elif 2 <= zscore <= 3:
        riesgo = "Medio"
        tipo = "Consumo alto"
    elif zscore < -2:
        riesgo = "Bajo"
        tipo = "Consumo sospechosamente bajo"
    elif variacion > 100:
        riesgo = "Medio"
        tipo = "Incremento brusco"
    else:
        riesgo = "Bajo"
        tipo = "Consumo normal"
        
    if req.consumo_actual > 500:
        riesgo = "Crítico"
        if tipo == "Consumo normal":
            tipo = "Alerta consumo critico > 500 kWh"

    return {
        "zscore": zscore,
        "porcentaje_variacion": variacion,
        "consumo_promedio": round(prom, 2),
        "desviacion_estandar": round(std, 2),
        "nivel_riesgo": riesgo,
        "tipo_anomalia": tipo
    }

@app.get("/alertas")
async def get_alertas(limit: int = 200):
    """
    Alertas reales por ventana horaria, generadas por
    speed_layer/spark_streaming.py (create_region_hourly_alerts +
    write_alerts_to_json) en data/region_hourly_alerts.json.
    Solo existen alertas cuando el consumo agregado por distrito en
    una hora supera 500 kWh; si el streaming no esta corriendo o no
    hubo alertas, se devuelve una lista vacia (sin datos de relleno).
    """
    alerts_path = os.path.join(data_path, "region_hourly_alerts.json")
    if not os.path.isfile(alerts_path):
        return {"total": 0, "data": []}

    try:
        with open(alerts_path, "r", encoding="utf-8") as f:
            alerts = json.load(f)
    except Exception as e:
        print(f"Error leyendo region_hourly_alerts.json: {e}")
        return {"total": 0, "data": []}

    alerts.sort(key=lambda a: a.get("window_start", ""), reverse=True)
    alerts = alerts[:limit]

    return {"total": len(alerts), "data": alerts}

# Montar archivos estaticos al final para no sobreescribir la ruta /api
app.mount("/", StaticFiles(directory=serving_path, html=True), name="static")

@app.on_event("startup")
async def startup_event():
    load_stats()
    url = f"http://localhost:{port}/dashboard.html"
    print(f"\n==========================================")
    print(f"API iniciada. Simulador listo en /api/predict")
    print(f"Dashboard disponible en: {url}")
    print(f"==========================================\n")
    try:
        webbrowser.open(url)
    except Exception:
        pass

if __name__ == "__main__":
    if not os.path.isfile(os.path.join(serving_path, "dashboard.html")):
        print(f"ERROR: No se encontro dashboard.html en {serving_path}")
        sys.exit(1)
        
    uvicorn.run("serve_dashboard:app", host="0.0.0.0", port=port, reload=False)
