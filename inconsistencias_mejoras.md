# Inconsistencias y Aspectos de Mejora - Proyecto Hidrandina Lambda

**Fecha de Revisión:** 2026-06-16  
**Estado del Proyecto:** ✅ **FASE 1 COMPLETADA** - Validación manual exitosa
**Contexto:** Proyecto Docker para múltiples compañeros - NO usar rutas hardcodeadas Windows

---

## ✅ RESULTADOS DE VALIDACIÓN FASE 1

### KPI OE1: TASA DE VALIDEZ
- **Métrica:** 87.37% (Requerimiento: >= 85%)
- **Estado:** ✅ **CUMPLIDO**

---

## ✅ RESULTADOS DE VALIDACIÓN FASE 1.5 (Calidad Categórica)

**Análisis completado:** 16-06-2026  
**Muestra:** 199,998 registros (3 meses)  
**Hallazgos principales:**

### Variables Categóricas Analizadas:
| Variable | Nulos | Blancos | Únicos | Raras (<10) | Estado |
|----------|-------|---------|--------|-------------|--------|
| DEPARTAMENTO | 0 | 0 | 3 | 0 | ✅ Óptimo |
| PROVINCIA | 0 | 0 | 42 | 2 | ⚠️ 2 raras |
| DISTRITO | 0 | 0 | 289 | 32 | ⚠️ 32 raras |
| TARIFA | 0 | 0 | 13 | 0 | ✅ Óptimo |
| CARTERA | 0 | 0 | 2 | 0 | ✅ Óptimo |
| NOMBRE_SERVICIO | 0 | 0 | 1,241 | 606 | ⚠️ Encriptado |

### Análisis de Riesgos:

**🔴 CRÍTICO:**
- NOMBRE_SERVICIO está encriptado (valores como "CXXXXXXXXXXXXXo") → No afecta análisis pero impide desanonimización

**🟠 MODERADO:**
- PROVINCIA: 2 distritos con 1-2 registros (Chota, Cutervo) → Validar dato
- DISTRITO: 32 distritos con < 10 registros → Posibles errores de digitación

**🟢 ACEPTABLE:**
- CARTERA: 99.66% "C" (Cliente regular), 0.34% "M" (Moroso) → Distribución lógica
- TARIFA: 13 tarifas, todas representadas → Esperado para empresa distribuidora

### Matriz TARIFA × CARTERA:
- Hallazgo importante: **Ciertas combinaciones NO existen:**
  - BT5B (Baja Tensión 5B): SOLO con CARTERA "C" (175,756 registros)
  - MT2, MT3, MT4 (Media Tensión): SOLO con CARTERA "M" (414 registros)
  - Esto es CORRECTO (reglas de negocio válidas)

### Archivos Generados:
- ✅ `analisis_categoricas.png` — Gráficos de distribución (Top 15 por variable)
- ✅ `analisis_matriz_tarifa_cartera.png` — Heatmap de TARIFA × CARTERA
- ✅ `reporte_fase1_5.json` — Reporte JSON con estadísticas completas

**Conclusión:** ✅ **FASE 1.5 COMPLETADA** - Datos categóricos VALIDADOS. Proceder a Fase 2.

### Archivos Generados:
| Archivo | Filas | Columnas | Estado |
|---------|-------|----------|--------|
| `FACT_CONSUMO.csv` | 436,836 | 8 | ✅ Correcto |
| `DIM_CLIENTE_UBICACION.csv` | 436,836 | 8 | ✅ Correcto |
| `hidrandina_limpio.csv` | 436,836 | 26 | ✅ Correcto |
| `reporte_calidad.json` | - | - | ✅ Correcto |

### Validaciones de Datos:
- ✅ CONSUMO > 0: 436,836/436,836 registros
- ✅ IMPORTE > 0: 436,836/436,836 registros
- ✅ Nulos en CONSUMO: 0
- ✅ Nulos en IMPORTE: 0
- ✅ Distritos únicos: 287
- ✅ Tarifas: 12
- ✅ Relación FACT-DIM: 100% matches por NRO_DOC_FAC

## 📝 CAMBIOS REALIZADOS (16-06-2026)

✅ **CORREGIDO:** Typo `FECHA_COSNUMO_DESDE` → `FECHA_CONSUMO_DESDE` en:
  - `utils/loader.py` (2 ocurrencias)
  - `speed_layer/spark_streaming.py` (3 ocurrencias)

✅ **CORREGIDO:** Path hardcodeado en `spark_streaming.py`:
  - ❌ `python_path = "C:\\Users\\Roxwell\\..."`
  - ✅ `python_path = sys.executable` (automático en cualquier máquina)
  - ❌ `hadoop_home = "C:\\hadoop"`
  - ✅ `hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")` (compatible con Docker)

🟡 **EN PROGRESO:** Validación manual de Fase 1 (ejecutando Loader, 31 CSVs)

---

## 🔴 INCONSISTENCIAS CRÍTICAS

### 1. **Path hardcodeado en `spark_streaming.py` - INCOMPATIBLE CON DOCKER**
- **Archivo:** `speed_layer/spark_streaming.py`, línea ~45
- **Problema:** 
  ```python
  python_path = "C:\\Users\\Roxwell\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
  hadoop_home = "C:\\hadoop"
  ```
  Contiene ruta específica del desarrollador original. **Incompatible con Docker y otros compañeros.**
- **Impacto:** 🔴 CRÍTICO PARA DOCKER - Pipeline fallará en todos los contenedores
- **Solución:** 
  - Usar `sys.executable` (Python obtiene su propia ruta automáticamente)
  - Usar variables de entorno para `HADOOP_HOME` (ya viene en el contenedor)
  - NO agregar paths absolutos Windows - usar rutas relativas o env vars

### 2. **Nombre de columna incorrecto en `loader.py`**
- **Archivo:** `utils/loader.py`, línea ~169
- **Problema:** Se busca `FECHA_COSNUMO_DESDE` (typo "COSNUMO") pero la columna es `FECHA_CONSUMO_DESDE`
- **Impacto:** ALTO - Las fechas de consumo no se cargarán correctamente
- **Solución:** Cambiar a `FECHA_CONSUMO_DESDE`

### 3. **Falta función `ejecutar()` en `batch_layer/spark_batch.py`**
- **Archivo:** `batch_layer/spark_batch.py`
- **Problema:** `main.py` llama a `spark_batch.ejecutar()` pero el archivo no define esta función
- **Impacto:** CRÍTICO - La etapa batch no se ejecutará
- **Solución:** Implementar función `ejecutar()` que orquesta todas las funciones batch

### 4. **Falta función `execute()` en `speed_layer/kafka_producer.py`**
- **Archivo:** `speed_layer/kafka_producer.py`
- **Problema:** `main.py` llama a `kafka_producer.execute(mode=mode)` pero no existe
- **Impacto:** CRÍTICO - La etapa producer no se ejecutará
- **Solución:** Implementar función `execute(mode)` con lógica de simulado/real

### 5. **Falta función `execute()` en `speed_layer/spark_streaming.py`**
- **Archivo:** `speed_layer/spark_streaming.py`
- **Problema:** `main.py` llama a `spark_streaming.execute(mode=mode)` pero no existe
- **Impacto:** CRÍTICO - La etapa streaming no se ejecutará
- **Solución:** Implementar función `execute(mode)` 

### 6. **Falta función `execute()` en `serving_layer/serving.py`**
- **Archivo:** `serving_layer/serving.py`
- **Problema:** `main.py` llama a `serving.execute()` pero solo hay funciones auxiliares
- **Impacto:** CRÍTICO - La etapa serving no se ejecutará
- **Solución:** Implementar función `execute()` que orquesta carga, generación y guardado

---

## 🟠 INCONSISTENCIAS MODERADAS

### 7. **Variable de entorno no usada consistentemente**
- **Archivos Afectados:** `spark_streaming.py` línea ~33
- **Problema:** 
  ```python
  KAFKA_CHECKPOINT = os.path.join(
      os.environ.get("RUTA_PROYECTO", os.path.dirname(os.path.dirname(__file__))),
      "checkpoint", "streaming"
  )
  ```
  Se usa `RUTA_PROYECTO` que no está documentado en el AGENTS.md
- **Solución:** Usar paths relativos o variables estándar (`RUTA_DATA`, `RUTA_SERVING`)

### 8. **Inconsistencia en nombres de archivos Parquet**
- **Archivos:** `spark_batch.py`, `spark_streaming.py`, `serving.py`
- **Problema:** Rutas Parquet usan minúsculas en algunos lugares y mayúsculas en otros
  - `tmp_estadisticas_historicas` vs `TMP_ESTADISTICAS_HISTORICAS`
  - `FACT_ANOMALIAS_STREAM` vs `fact_anomalias_stream`
- **Impacto:** MODERADO - Posibles fallos de carga por rutas inconsistentes
- **Solución:** Estandarizar a minúsculas: `tmp_estadisticas_historicas`

### 9. **Encoding inconsistente en archivos**
- **Problema:** Mezcla de `utf-8`, `utf-8-sig`, `latin-1` en diferentes lecturas
- **Impacto:** BAJO - Puede haber problemas de caracteres especiales
- **Solución:** Usar `utf-8-sig` sistemáticamente (soporta BOM)

### 10. **Falta manejo de directorios en Spark**
- **Archivo:** `spark_batch.py` línea ~334
- **Problema:** No se crea `RUTA_RESULTADOS` antes de escribir Parquet
- **Impacto:** MODERADO - Posible error si el directorio no existe
- **Solución:** Agregar `os.makedirs(RUTA_RESULTADOS, exist_ok=True)`

---

## 🟡 ASPECTOS DE MEJORA

### 11. **Falta de logging estructurado**
- **Problema:** Mezcla de `print()` sin nivel de severidad
- **Mejora:** Usar `logging` con niveles INFO, WARNING, ERROR
- **Beneficio:** Mejor debugging y trazabilidad

### 12. **Manejo de excepciones genérico**
- **Archivos:** Todos los módulos
- **Problema:** Capturan `Exception` genérica sin detalles específicos
- **Mejora:** Diferenciar errores de lectura, transformación, escritura
- **Beneficio:** Diagnóstico más rápido

### 13. **Falta validación de argumentos en main.py**
- **Problema:** No valida que las etapas existan antes de procesarlas
- **Mejora:** Agregar validación temprana
- **Beneficio:** Mensajes de error más claros

### 14. **Variable de rutas sin consistencia**
- **Problema:** `RUTA_PROYECTO` se usa en algunos archivos pero no está definida
- **Mejora:** Usar solo `RUTA_DATA`, `RUTA_SERVING`, `RUTA_SPEED`
- **Beneficio:** Menos ambigüedad

### 15. **Falta docstring completo en algunas funciones**
- **Archivos:** `spark_streaming.py`, `kafka_producer.py`
- **Mejora:** Documentar todos los parámetros y retornos
- **Beneficio:** Mejor mantenibilidad

### 16. **No hay validación de tipos de datos**
- **Problema:** Las DataFrames de Spark no definen esquemas explícitamente
- **Mejora:** Usar `StructType` para definir esquemas desde el inicio
- **Beneficio:** Detección temprana de errores

### 17. **Falta comentarios en cálculos complejos**
- **Archivos:** `spark_batch.py` (RFM, z-score)
- **Mejora:** Agregar comentarios explicativos de fórmulas
- **Beneficio:** Más fácil de mantener

### 18. **No hay retry logic para operaciones I/O**
- **Problema:** Si falla una lectura/escritura, no hay reintentos
- **Mejora:** Implementar retry exponencial para operaciones de archivo
- **Beneficio:** Mayor robustez en entornos inestables

### 19. **Falta validación de datos antes de operaciones críticas**
- **Ejemplo:** No verificar que `consumo_promedio > 0` antes de KPI OE2
- **Mejora:** Agregar aserciones y validaciones
- **Beneficio:** Prevenir datos inválidos

### 20. **No hay sincronización de etapas**
- **Problema:** Si una etapa falla, el orquestador continúa sin garantías
- **Mejora:** Agregar checkpoints explícitos entre etapas
- **Beneficio:** Pipeline más robusto

---

## 📋 PRIORIDAD DE CORRECCIONES

### ✅ CORREGIDO (Antes de Fase 2):
1. ✅ Path hardcodeado `Roxwell` en `spark_streaming.py` → Reemplazado con `sys.executable`
2. ✅ HADOOP_HOME hardcodeado → Ahora usa variable de entorno con fallback
3. ⚠️  Typo `FECHA_COSNUMO_DESDE` corregido en loader.py pero AÚN PRESENTE en speed_layer - PENDIENTE VERIFICAR

### Debe ser corregido ANTES de Fase 2:
- [ ] Verificar que `FECHA_COSNUMO_DESDE` esté completamente corregido
- [ ] Implementar función `ejecutar()` en `batch_layer/spark_batch.py` 
- [ ] Crear directorio `batch_results` si no existe en `spark_batch.py`

### Debe ser corregido ANTES de Fase 3:
- [ ] Implementar función `execute()` en `kafka_producer.py`
- [ ] Implementar función `execute()` en `spark_streaming.py`
- [ ] Estandarizar nombres de Parquet a minúsculas

### Puede ser mejorado (no bloquea):
- Implementar logging estructurado
- Agregar manejo de excepciones específicas
- Documentar mejor docstrings
- Agregar validación de tipos

---

## ✅ CHECKLIST PARA VALIDACION MANUAL DE FASE 1

Después de ejecutar `python main.py --etapa loader`, verificar:

- [x] `data/FACT_CONSUMO.csv` existe y contiene datos
- [x] `data/DIM_CLIENTE_UBICACION.csv` existe y contiene datos
- [x] `data/hidrandina_limpio.csv` existe
- [x] `serving_layer/reporte_calidad.json` existe
- [x] En `reporte_calidad.json`: `tasa_validez_pct >= 85.0`
- [x] `FACT_CONSUMO.csv` tiene columna `CONSUMO` con valores > 0
- [x] `DIM_CLIENTE_UBICACION.csv` tiene columna `DISTRITO` con valores
- [x] No hay errores en la salida del logger

---

## ✅ CHECKLIST PARA VALIDACION DE FASE 1.5

Después de ejecutar `python analisis_exploratorio.py`, verificar:

- [x] `analisis_graficos.png` generado (histogramas + boxplots)
- [x] `analisis_categoricas.png` generado (distribuciones categóricas)
- [x] `analisis_matriz_tarifa_cartera.png` generado (heatmap)
- [x] `reporte_fase1_5.json` generado (estadísticas)
- [x] DEPARTAMENTO: 3 valores, 0 nulos, 0 raros ✓
- [x] PROVINCIA: 42 valores, 2 raros (Chota, Cutervo) ⚠️
- [x] DISTRITO: 289 valores, 32 raros ⚠️
- [x] CARTERA: 2 valores (C=99.66%, M=0.34%) ✓
- [x] TARIFA: 13 valores distribuidos correctamente ✓

---

## 📝 NOTAS ADICIONALES

- **Encoding:** Los CSVs se guardan con `utf-8-sig` para compatibilidad Windows
- **NRO_SERVICIO:** Está anonimizado a 0 en el dataset, se usa `NRO_DOC_FAC` como clave
- **Variables de entorno:** Se definen en `.env`, con fallbacks a rutas relativas
- **Spark:** Configurado para `local[*]` en Windows; requiere `HADOOP_HOME` para Parquet

---

**Última actualización:** 2026-06-16 (✅ Fase 1 + Fase 1.5 COMPLETADAS)
