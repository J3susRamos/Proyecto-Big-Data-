Contexto del Proyecto (Versión Ejecutiva)
¿Qué se quiere desarrollar?

Se desea construir una solución de Big Data basada en Arquitectura Lambda para analizar el consumo de clientes a partir de datos almacenados en archivos CSV.

El sistema permitirá procesar grandes volúmenes de información, generar indicadores de negocio y ofrecer consultas tanto históricas como actualizadas.

Objetivo

Transformar datos de consumo en información útil para responder preguntas como:

¿Qué clientes consumen más?
¿Qué zonas geográficas tienen mayor consumo?
¿Cómo evoluciona el consumo en el tiempo?
¿Cuáles son los indicadores clave del negocio?
Fuentes de Datos
1. FACT_CONSUMO

Tabla principal de transacciones.

Contiene información relacionada con los consumos realizados por los clientes.

Ejemplos de campos:

NRO_DOC_FAC
FECHA_CONSUMO_DESDE
FECHA_CONSUMO_HASTA
CONSUMO
IMPORTE
PRODUCTO
CANAL
Consideraciones
CONSUMO → Float
IMPORTE → Float
NRO_DOC_FAC será la clave principal de integración
2. DIM_CLIENTE_UBICACION

Tabla dimensional que contiene información descriptiva del cliente.

Ejemplos de campos:

NRO_DOC_FAC
DEPARTAMENTO
PROVINCIA
DISTRITO
REGIÓN
ZONA
Consideraciones
NRO_SERVICIO ya no será utilizado
NRO_DOC_FAC será la llave de relación con FACT_CONSUMO
Relación entre tablas
FACT_CONSUMO.NRO_DOC_FAC
            =
DIM_CLIENTE_UBICACION.NRO_DOC_FAC

Esta relación permite enriquecer los consumos con información geográfica del cliente.

Arquitectura Lambda

La solución estará dividida en cuatro capas.

🔵 Input Layer

Recibe los archivos CSV.

Tablas:

FACT_CONSUMO
DIM_CLIENTE_UBICACION

Función:

Ingestar datos.
Validar estructura.
Preparar información para procesamiento.
🟢 Batch Layer

Procesa el histórico completo de datos.

Función:

Calcular métricas históricas.
Generar agregaciones.
Mantener precisión de la información.

Ejemplos:

Consumo acumulado.
Consumo por región.
Consumo por cliente.
🟠 Speed Layer

Procesa datos recientes.

Función:

Actualizar indicadores rápidamente.
Reducir tiempos de espera para consultas.

Ejemplos:

Consumos recientes.
Nuevos registros.
Actualizaciones del día.
🟡 Serving Layer

Presenta los resultados finales.

Función:

Exponer información lista para análisis.
Alimentar dashboards y reportes.

Ejemplos:

Ranking de clientes.
Consumo por departamento.
Indicadores consolidados.
Tipos de Campos en el Output
DIRECTO

El valor proviene directamente de una columna.

Ejemplo:

DEPARTAMENTO =
DIM_CLIENTE_UBICACION.DEPARTAMENTO
CALCULO

El valor requiere transformación o agregación.

Ejemplo:

CONSUMO_TOTAL =
SUM(CONSUMO)
FIJO

Valor constante definido por negocio.

Ejemplo:

PAIS = 'PERU'
Estructura Esperada del Output

Cada campo de salida debe indicar:

Nombre del campo
Descripción
Tipo de dato
Query (DIRECTO, CALCULO o FIJO)
Fórmula o lógica de cálculo
Tabla(s) origen
Capa Lambda donde se genera
Observaciones
Resultado Esperado

Al finalizar el proyecto se tendrá una plataforma capaz de:

Integrar datos de consumo y ubicación de clientes.
Procesar información histórica y reciente.
Generar indicadores de negocio automáticamente.
Consultar información consolidada para análisis y toma de decisiones.
Aplicar correctamente la Arquitectura Lambda diferenciando Input, Batch, Speed y Serving.

En resumen, el proyecto busca convertir los datos de consumo almacenados en CSV en información estratégica para el negocio, utilizando FACT_CONSUMO y DIM_CLIENTE_UBICACION relacionadas mediante NRO_DOC_FAC, y procesadas a través de una Arquitectura Lambda.