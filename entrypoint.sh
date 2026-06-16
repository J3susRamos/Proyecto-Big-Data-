#!/bin/bash
set -e

# =============================================================================
# entrypoint.sh — Punto de entrada del contenedor Docker
# Modos:
#   MODO=dashboard  (defecto) — Solo inicia el dashboard con datos pre-computados
#   MODO=full       — Ejecuta el pipeline completo, luego inicia el dashboard
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
    echo "Ejecutando pipeline completo..."
    python main.py --simulado
    echo ""
    echo "Pipeline completado."
    echo ""
fi

echo "Iniciando dashboard web en http://localhost:8050 ..."
echo ""
exec python serve_dashboard.py
