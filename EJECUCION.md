# Guía de Ejecución del Pipeline Hidrandina (Arquitectura Lambda)

Este documento contiene las instrucciones precisas para que **cualquier integrante del equipo** pueda clonar el proyecto, procesar los datos de extremo a extremo, y finalmente ver y utilizar el Dashboard interactivo en tiempo real.

> [!IMPORTANT]
> **Requisitos Previos**
> - Asegúrate de tener instalado **Python 3.10+**.
> - Asegúrate de tener instalado los paquetes necesarios. Si es la primera vez, ejecuta:
>   ```bash
>   pip install -r requirements.txt
>   ```

---

## ⚡ Ejecución en un solo paso (Recomendado)

Si deseas correr TODAS las fases del pipeline (Limpieza -> Batch -> Streaming -> Serving) de manera automática en modo simulado (con un tope de 4000 registros para pruebas súper rápidas):

```bash
python main.py --simulado --max-records-per-file 4000
```

*Una vez que termine, puedes pasar directamente a la sección de [Ejecución del Dashboard](#-ejecución-del-dashboard).*

---

## 🛠️ Ejecución Fase por Fase (Modo de Depuración/Explicación)

Si tu objetivo es auditar qué hace cada parte de la arquitectura y generar las gráficas de validación paso a paso, sigue este orden riguroso:

### FASE 1: Limpieza de Datos Crudos (Loader)
Toma los archivos originales y genera la base limpia.
```bash
python main.py --etapa loader --max-records-per-file 4000
```
*Puedes verificar la calidad de esta limpieza generando gráficas opcionales con `python analisis_exploratorio.py`*

### FASE 2: Capa Histórica (Batch Layer)
La "Gran Calculadora" que lee todos los datos históricos con PySpark y calcula el *Libro de Reglas* (medias y desviaciones estándar) usado por el motor de detección de riesgo.
```bash
python main.py --etapa batch
```
*Verifica los resultados visualmente ejecutando `python graficos_fase2.py`*

### FASE 3: Transmisión de Eventos (Kafka Producer / Speed Layer)
Simula que los eventos de consumo eléctrico llegan uno por uno en tiempo real.
```bash
python main.py --etapa producer
```
*Verifica el volumen de eventos ejecutando `python graficos_fase3.py`*

### FASE 4: Detección Inmediata (Streaming Layer)
Atrapa los eventos de la Fase 3, los cruza con el *Libro de Reglas* de la Fase 2, y clasifica su nivel de riesgo y Z-Score instantáneamente.
```bash
python main.py --etapa streaming
```
*Verifica qué anomalías se detectaron ejecutando `python graficos_fase4.py`*

### FASE 5: Consolidación y Reporte (Serving Layer)
Capa final que reúne todo, genera métricas consolidadas (KPIs globales) y empaqueta la información lista para el consumo web.
```bash
python main.py --etapa serving
```

---

## 🚀 Ejecución del Dashboard (Interfaz Gráfica)

Una vez que las 5 fases han concluido (ya sea de golpe o paso a paso), estás listo para encender la interfaz. 

Hemos construido un potente servidor backend con **FastAPI** que carga las reglas a memoria RAM para procesar el "Simulador de Riesgo".

Para lanzar la interfaz y el servidor de inferencias, ejecuta:

```bash
python serve_dashboard.py
```

> [!TIP]
> Al correr este comando:
> 1. Se cargará tu base de datos estadística a la memoria del servidor.
> 2. Se abrirá automáticamente tu navegador apuntando a `http://localhost:8050/dashboard.html`.
> 3. ¡Prueba el **Simulador de Riesgo en Tiempo Real** ubicado debajo de las métricas! Modifica el consumo a un valor altísimo y observa cómo la API y el UI cambian a alerta "Riesgo Crítico" en milisegundos sin recargar la página.

**Para apagar el servidor:** Regresa a la consola donde ejecutaste el comando y presiona `CTRL + C`.
