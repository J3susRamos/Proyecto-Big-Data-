"""
Servidor FastAPI para el dashboard web de Hidrandina y el Simulador en Tiempo Real.
Sirve serving_layer/ en http://localhost:8050/dashboard.html
Y expone la API de simulacion en http://localhost:8050/api/predict
"""
import os
import sys
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
import webbrowser

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SERVING_PATH = os.environ.get(
    "RUTA_SERVING",
    os.path.join(PROJECT_ROOT, "serving_layer"),
)
PORT = int(os.environ.get("DASHBOARD_PORT", "8050"))

app = FastAPI(title="Hidrandina Real-Time Simulator API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Cargar estadisticas historicas en memoria al iniciar
STATS_DF = None

def load_stats():
    global STATS_DF
    ruta_stats = os.path.join(SERVING_PATH, "batch_results", "tmp_estadisticas_historicas.csv")
    if os.path.isfile(ruta_stats):
        try:
            STATS_DF = pd.read_csv(ruta_stats, encoding="utf-8-sig")
            STATS_DF.columns = [c.lower() for c in STATS_DF.columns]
            print(f"Estadisticas cargadas: {len(STATS_DF)} filas.")
        except Exception as e:
            print(f"Error cargando estadisticas: {e}")
    else:
        print(f"Advertencia: No se encontro {ruta_stats}")

# Esquema de la peticion
class PredictRequest(BaseModel):
    distrito: str
    tarifa: str
    cartera: str
    consumo_actual: float
    importe_actual: float

@app.post("/api/predict")
async def predict_anomaly(req: PredictRequest):
    global STATS_DF
    if STATS_DF is None or STATS_DF.empty:
        raise HTTPException(status_code=500, detail="Estadisticas no cargadas en el servidor.")
        
    # Filtrar el distrito, tarifa y cartera en STATS_DF
    # Si no hay match exacto, hacemos un match solo por distrito
    match = STATS_DF[
        (STATS_DF["distrito"].str.upper() == req.distrito.upper()) &
        (STATS_DF["tarifa"].str.upper() == req.tarifa.upper()) &
        (STATS_DF["cartera"].str.upper() == req.cartera.upper())
    ]
    
    if match.empty:
        match = STATS_DF[STATS_DF["distrito"].str.upper() == req.distrito.upper()]
        
    if match.empty:
        # Si aun no hay match, tomar valores promedio globales
        prom = float(STATS_DF["consumo_promedio"].mean())
        std = float(STATS_DF["consumo_std"].mean())
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
async def get_alertas():
    """Endpoint mock para simular alertas conectadas de Kafka"""
    import datetime
    now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    return {
        "total": 1,
        "data": [
            {
                "window_start": now,
                "DEPARTAMENTO": "LA LIBERTAD",
                "DISTRITO": "TRUJILLO",
                "consumo_total_hora": 45000.5,
                "total_eventos": 3,
                "nivel": "CRITICO",
                "mensaje": "Alerta agregada detectada por el servidor en tiempo real"
            }
        ]
    }

# Montar archivos estaticos al final para no sobreescribir la ruta /api
app.mount("/", StaticFiles(directory=SERVING_PATH, html=True), name="static")

@app.on_event("startup")
async def startup_event():
    load_stats()
    url = f"http://localhost:{PORT}/dashboard.html"
    print(f"\n==========================================")
    print(f"API iniciada. Simulador listo en /api/predict")
    print(f"Dashboard disponible en: {url}")
    print(f"==========================================\n")
    try:
        webbrowser.open(url)
    except Exception:
        pass

if __name__ == "__main__":
    if not os.path.isfile(os.path.join(SERVING_PATH, "dashboard.html")):
        print(f"ERROR: No se encontro dashboard.html en {SERVING_PATH}")
        sys.exit(1)
        
    uvicorn.run("serve_dashboard:app", host="0.0.0.0", port=PORT, reload=False)
