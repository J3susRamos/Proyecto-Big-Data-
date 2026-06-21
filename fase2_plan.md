# Fase 2: Batch Layer - Plan de Implementación

**Fecha:** 2026-06-16  
**Estado Previo:** ✅ Fase 1 (Loader) Completada y Validada  
**Entrada:** FACT_CONSUMO.csv y DIM_CLIENTE_UBICACION.csv (436,836 registros)

---

## 📋 OBJETIVO FASE 2

Procesar de forma distribuida (usando PySpark) el historial completo de datos limpios para generar:

1. **TMP_ESTADISTICAS_HISTORICAS** (10 columnas)
   - Agregaciones por DISTRITO, TARIFA, CARTERA
   - consumo_promedio, consumo_std, importe_promedio, importe_std, etc.
   
2. **KPIs Globales** (8 indicadores)
   - facturacion_total_soles, consumo_total_kwh, total_facturas
   - ticket_promedio, tasa_outlier_pct, etc.

3. **Ranking por Departamento** (agregaciones geográficas)

4. **Análisis de Tendencias** (por periodo)

5. **Segmentación RFM** (Recency, Frequency, Monetary)

6. **Análisis Detallado** (por distrito, cartera)

---

## ✅ CHECKLIST PRE-EJECUCION

### 1. **Verificar que loader completó correctamente:**
- [x] FACT_CONSUMO.csv existe (436,836 filas)
- [x] DIM_CLIENTE_UBICACION.csv existe (436,836 filas)
- [x] reporte_calidad.json muestra OE1_cumplido=true

### 2. **Verificar dependencias Spark:**
```bash
python -c "import pyspark; print(f'PySpark {pyspark.__version__} OK')"
python -c "import pandas; print(f'Pandas {pandas.__version__} OK')"
```

### 3. **Crear función `ejecutar()` en spark_batch.py:**
Debe retornar: `(statistics, kpis, ranking, trend, analysis, rfm, oe2)`

---

## 🔴 PROBLEMAS IDENTIFICADOS EN spark_batch.py

1. **FALTA función `ejecutar()`** - main.py intenta llamarla
   - Solución: Crear función que orquesta todas las operaciones batch

2. **NO crea directorio batch_results**
   - Puede fallar al escribir Parquet si directorio no existe

3. **Rutas Parquet inconsistentes**
   - Algunos usan minúsculas, otros mayúsculas
   - Estandarizar a: `tmp_estadisticas_historicas` (minúsculas)

---

## 📊 KPI OE2: VALIDACIÓN

**Métrica Clave (OE2):** TMP_ESTADISTICAS_HISTORICAS debe tener **exactamente 10 columnas**

Columnas esperadas:
1. DISTRITO
2. TARIFA
3. CARTERA
4. consumo_promedio
5. consumo_std
6. importe_promedio
7. importe_std
8. consumo_minimo
9. consumo_maximo
10. total_registros

**Validación adicional:** Todos los valores de `consumo_promedio` deben ser **> 0**

---

## 🛠️ PRÓXIMOS PASOS

1. Corregir spark_batch.py para agregar función `ejecutar()`
2. Verificar creación de directorios
3. Ejecutar: `python main.py --etapa batch`
4. Validar outputs en `serving_layer/batch_results/`
5. Verificar que OE2 se cumple

---

## 📁 OUTPUTS ESPERADOS

```
serving_layer/
├── batch_results/
│   ├── tmp_estadisticas_historicas/      (Parquet)
│   ├── kpis_globales/                    (Parquet)
│   ├── ranking_departamentos/            (Parquet)
│   ├── tendencias_historicas/            (Parquet)
│   ├── analisis_detallado/               (Parquet)
│   └── rfm_segmentacion/                 (Parquet)
└── reporte_batch.json                    (Métricas OE2)
```

---

**Próximo estado:** Avanzar a Fase 3 (Speed Layer - Kafka) una vez validado Fase 2
