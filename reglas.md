# Reglas de Documentación para el Proyecto Hidrandina

Cada vez que se complete o se aborde una fase del proyecto, se deben generar o actualizar dos archivos fundamentales. Ambos están dirigidos a personas no técnicas o nuevos integrantes, por lo que el lenguaje debe ser **breve, sencillo y libre de tecnicismos innecesarios**.

---

## 1. Archivo `EXPLICACION.md` (El "Por qué" y "Para qué")
Este archivo explica conceptualmente la fase abordada, siguiendo estrictamente esta estructura:
1. **¿Qué datos entran? (El Input):** Qué información recibe la fase.
2. **¿Por qué ingresan estos datos?:** La razón de negocio de esta información.
3. **¿Qué se hizo en esta fase?:** Una explicación con analogías cotidianas del procesamiento.
4. **¿Cómo lo medimos?:** El indicador o KPI objetivo de la fase.
5. **¿Cómo se interpreta?:** Qué significa ese KPI o resultado en la vida real.
6. **¿Cómo sabemos si el resultado es bueno?:** Cuál es la meta y si se alcanzó.
7. **¿Cuál es el resultado final? (La Salida):** Qué entregable (archivo/reporte) se generó.
8. **¿Cómo se conecta con lo que sigue?:** Cómo este resultado sirve de "materia prima" para la próxima etapa.

---

## 2. Archivo `EJECUCION.md` (El "Cómo")
Este archivo debe ser la guía práctica paso a paso para replicar lo explicado en `EXPLICACION.md`. 
Debe seguir estrictamente esta estructura:
- **Orden de los pasos:** Estar organizado cronológicamente por fases (Fase 1, Fase 2, etc.).
- **Comando exacto:** Qué comando se debe pegar en la terminal.
- **Archivo ejecutado:** El nombre exacto del archivo `.py` que orquesta la fase.
- **Relación con la Explicación:** Una breve oración vinculando el comando con el resultado descrito en `EXPLICACION.md`.
