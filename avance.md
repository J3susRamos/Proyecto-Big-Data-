# Documento de Avance - Pipeline Lambda Hidrandina

**Fecha de inicio:** 2026-06-16  
**Estado:** En ejecución  
**Versión:** 1.0

---

## 📋 Resumen Ejecutivo

Se inicia la ejecución del plan de implementación completo (5 fases) para el pipeline de **Arquitectura Lambda de detección de anomalías en consumo eléctrico** de Hidrandina S.A.

**Tamaño de la data:** ~29.9 millones de registros (CSV mensuales de 31 períodos)

**Decisión sobre datos extensos:** Se agrega **apartado de configuración de límite de datos** para permitir ejecuciones rápidas en desarrollo.

---

## ⚙️ Configuración de Límite de Datos

### Parámetro agregado: `--max-records-per-file`

Se han modificado los siguientes archivos para permitir limitar la cantidad de registros cargados:

#### **1. `utils/loader.py`**
- ✅ Modificada función `load_all_csvs(path=None, max_records_per_file=None)`
- ✅ Añadido contador de filas acumuladas
- ✅ Detiene la carga cuando alcanza `max_records_per_file`
- ✅ Modificada función `execute(max_records_per_file=None)`

#### **2. `main.py`**
- ✅ Modificada función `execute_loader(max_records_per_file=None)`
- ✅ Parseador de argumentos: `--max-records-per-file NUMERO`
- ✅ Documentación en `--help` actualizada
- ✅ Mapa de etapas actualizado

### Uso desde línea de comandos

```bash
# Ejecutar Fase 1 con límite de 500,000 filas por archivo
python main.py --etapa loader --max-records-per-file 500000

# Ejecutar todas las fases con límite de 1,000,000 filas por archivo
python main.py --max-records-per-file 1000000

# Modo simulado con límite (sin Kafka)
python main.py --simulado --max-records-per-file 500000

# Ver ayuda completa
python main.py --help
```

### Valores recomendados de `max_records_per_file`

| Valor | Tiempo estimado | Uso |
|-------|-----------------|-----|
| 100,000 | ~1-2 min | Pruebas rápidas (OE1) |
| 500,000 | ~5-10 min | Desarrollo completo (Fases 1-5) |
| 1,000,000 | ~15-20 min | Validación de KPIs |
| `None` (omitir) | 30-40 min | Producción (todos los datos) |

---

## 📊 Plan de Fases Ejecutadas

### Fase 1: ✅ LOADER (Carga y Limpieza)
**Estado:** Por ejecutar  
**Entrada:** 31 archivos CSV mensuales  
**Salida:** `FACT_CONSUMO.csv`, `DIM_CLIENTE_UBICACION.csv`, `reporte_calidad.json`  
**Métrica KPI OE1:** Tasa de validez ≥ 85%

**Comando:**
```bash
python main.py --etapa loader --max-records-per-file 500000
```

---

### Fase 2: BATCH LAYER (Procesamiento Histórico)
**Estado:** Por ejecutar  
**Entrada:** FACT_CONSUMO + DIM_CLIENTE_UBICACION  
**Salida:** 6 tablas Parquet (estadísticas, KPIs, ranking, tendencia, análisis, RFM)  
**Métrica KPI OE2:** `TMP_ESTADISTICAS_HISTORICAS` con 10 columnas exactas

**Comando:**
```bash
python main.py --etapa batch --max-records-per-file 500000
```

---

### Fase 3: SPEED LAYER (Kafka + Streaming)
**Estado:** Por ejecutar  
**Componentes:**
- **Productor:** Publica eventos al topic `hidrandina-consumo`
- **Streaming:** Consume eventos y calcula z-score en tiempo real

**Salida:** Eventos clasificados por nivel de riesgo  
**Métrica KPI OE3:** Latencia < 5 segundos, precisión ≥ 90%

**Comando (modo simulado):**
```bash
python main.py --etapa producer --etapa streaming --simulado --max-records-per-file 500000
```

---

### Fase 4: SERVING LAYER (Consolidación)
**Estado:** Por ejecutar  
**Entrada:** Batch results + Stream results  
**Salida:**
- `FACT_ANOMALIAS_CONSUMO.csv` (17 columnas)
- 3 tablas de resumen (por DISTRITO, TARIFA, CARTERA)
- `reporte_kpis.json`

**Métrica KPI OE4:** FACT íntegro, 0 nulos en z-score  
**Métrica KPI OE5:** 4 outputs generados

**Comando:**
```bash
python main.py --etapa serving
```

---

### Fase 5: DASHBOARD (Visualización)
**Estado:** Por ejecutar  
**Entrada:** Archivos JSON de data de serving  
**Salida:** Dashboard HTML interactivo + 4 gráficos de negocio

**Gráficos:**
1. Tendencia mensual de consumo
2. Top 10 distritos por anomalías
3. Distribución de niveles de riesgo (Alto/Medio/Bajo)
4. Top z-score por distrito

**Comando:**
```bash
python serve_dashboard.py
# Abrir: http://localhost:8050
```

---

## 📈 Tabla de KPIs Objetivo

| KPI | Descripción | Meta | Estado |
|-----|-------------|------|--------|
| OE1 | Tasa de validez (Loader) | ≥ 85% | ⏳ Por ejecutar |
| OE2 | Columnas en estadísticas históricas | 10 exactas | ⏳ Por ejecutar |
| OE3 | Latencia streaming | ≤ 5 seg | ⏳ Por ejecutar |
| OE3b | Precisión anomalías | ≥ 90% | ⏳ Por ejecutar |
| OE4 | Filas en FACT_ANOMALIAS | 17 columnas | ⏳ Por ejecutar |
| OE5 | Outputs generados | 4 archivos | ⏳ Por ejecutar |

---

## 🛠️ Cambios Realizados

### Modificación 1: `utils/loader.py`
```python
# Refactorizado (por Jesús Ramos)
def load_all_csvs(path=None, max_records_per_file=None):
    # Ahora soporta límite de filas
    # - Usa variables de entorno o parámetro directo
    # - Detiene lectura cuando alcanza max_records_per_file
```

### Modificación 2: `main.py`
```python
# Refactorizado (por Jesús Ramos)
def execute_loader(max_records_per_file=None):
    fact, dim, metrics = loader.execute(max_records_per_file=max_records_per_file)

# Nuevo parseador de argumentos
--max-records-per-file NUMERO
```

---

## 🚀 Próximos Pasos

1. **✅ COMPLETADO:** Agregar configuración de límite de datos
2. ⏳ **EN PROGRESO:** Ejecutar Fase 1 (LOADER)
3. ⏳ Ejecutar Fase 2 (BATCH)
4. ⏳ Ejecutar Fase 3 (SPEED - Producer + Streaming)
5. ⏳ Ejecutar Fase 4 (SERVING)
6. ⏳ Ejecutar Fase 5 (DASHBOARD)
7. ⏳ Validar todos los KPIs
8. ⏳ Generar reportes finales

---

## 📝 Notas

- Los datos se limitan para desarrollo rápido
- En producción, se puede omitir `--max-records-per-file` para procesar todos los ~30M registros
- El modo `--simulado` no requiere Apache Kafka (usa JSON)
- Se mantiene total compatibilidad con el pipeline completo

---

**Última actualización:** 2026-06-16 14:00  
**Responsable:** Equipo de Data Engineering

---

## ✅ ESTADO ACTUAL - FASE 1 + FASE 1.5 COMPLETADAS

### Fase 1: LOADER - VALIDADO
- **Comando ejecutado:** `python main.py --etapa loader`
- **Registros procesados:** 500,000 input → 436,836 válidos
- **KPI OE1:** 87.37% (Requerimiento: >= 85%) ✅
- **Archivos generados:**
  - ✅ `data/FACT_CONSUMO.csv` — 436,836 × 8 columnas
  - ✅ `data/DIM_CLIENTE_UBICACION.csv` — 436,836 × 8 columnas
  - ✅ `data/hidrandina_limpio.csv` — 436,836 × 26 columnas
  - ✅ `serving_layer/reporte_calidad.json` — Métricas de OE1

### Fase 1.5: ANÁLISIS DE CALIDAD - VALIDADO
- **Comando ejecutado:** `python analisis_exploratorio.py`
- **Muestra analizada:** 199,998 registros (3 meses)
- **Variables analizadas:** 7 categóricas + 2 numéricas
- **Archivos generados:**
  - ✅ `analisis_graficos.png` — Histogramas + Boxplots (4 gráficos)
  - ✅ `analisis_categoricas.png` — Distribuciones categóricas (barras)
  - ✅ `analisis_matriz_tarifa_cartera.png` — Heatmap (TARIFA × CARTERA)
  - ✅ `reporte_fase1_5.json` — Estadísticas categóricas

### Hallazgos Clave:
| Métrica | Valor | Status |
|---------|-------|--------|
| Nulos en categóricas | 0 | ✅ Perfecto |
| Blancos en categóricas | 0 | ✅ Perfecto |
| Outliers numéricos | 0.18% | ✅ Aceptable |
| Distritos raros (< 10) | 32 | ⚠️ Investigar |
| BT5B (mayor tarifa) | 99.4% | ⚠️ MUY concentrado |

### Documentación:
- ✅ `fase1_5_plan.md` — Plan completo con gráficos documentados
- ✅ `inconsistencias_mejoras.md` — Registro de issues y estado
- ✅ Checklist de validación manual completado

---

## 🚀 Próximos Pasos

1. ✅ **COMPLETADO:** Fase 1 (LOADER)
2. ✅ **COMPLETADO:** Fase 1.5 (ANÁLISIS CATEGÓRICO)
3. ⏳ **PRÓXIMO:** Fase 2 (BATCH LAYER) — `python main.py --etapa batch`
4. ⏳ Fase 3 (SPEED LAYER - Kafka)
5. ⏳ Fase 4 (SERVING LAYER)
6. ⏳ Fase 5 (DASHBOARD)
7. ⏳ Validar todos los KPIs
8. ⏳ Generar reportes finales

---

**Última actualización:** 2026-06-16 19:50 (✅ Fase 1.5 COMPLETADA)
