#!/bin/bash
set -e

# =============================================================================
# entrypoint.sh — Punto de entrada del contenedor Docker
# Modos:
#   MODO=dashboard  (defecto) — Solo inicia el dashboard con datos pre-computados
#   MODO=full       — Ejecuta pipeline completo (simulado), luego dashboard
#   MODO=real       — Ejecuta pipeline completo con Kafka real, luego dashboard
# =============================================================================

MODO=${MODO:-dashboard}
cd /app

echo ""
echo "=================================================="
echo "  Hidrandina Dashboard"
echo "  Modo: ${MODO}"
echo "=================================================="
echo ""

if [ "${MODO}" = "full" ]; then
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
