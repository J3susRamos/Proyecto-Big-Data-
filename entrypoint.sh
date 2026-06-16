#!/bin/bash
set -e

# =============================================================================
# entrypoint.sh — Punto de entrada del contenedor Docker
# Modos:
#   MODO=dashboard  (defecto) — Solo inicia el dashboard con datos pre-computados
#   MODO=full       — Ejecuta pipeline completo (simulado), luego dashboard
#   MODO=real       — Ejecuta pipeline completo con Kafka real, luego dashboard
#   MODO=api        — Inicia solo la API REST de alertas
# =============================================================================

MODO=${MODO:-dashboard}
cd /app

echo ""
echo "=================================================="
echo "  Hidrandina Dashboard"
echo "  Modo: ${MODO}"
echo "=================================================="
echo ""

if [ "${MODO}" = "api" ]; then
    echo "Iniciando API REST de alertas en puerto ${API_PORT:-8060}..."
    echo ""
    exec python -m api.alert_api

elif [ "${MODO}" = "full" ]; then
    echo "Ejecutando pipeline completo (modo simulado)..."
    python main.py --simulado
    echo ""
    echo "Pipeline completado."

elif [ "${MODO}" = "real" ]; then
    echo "Inicializando topics Kafka..."
    python -m utils.kafka_admin
    echo ""
    echo "Ejecutando pipeline completo con Kafka real..."
    python main.py --kafka
    echo ""
    echo "Pipeline completado."
fi

echo "Iniciando dashboard web en http://localhost:8050 ..."
echo ""
exec python serve_dashboard.py
