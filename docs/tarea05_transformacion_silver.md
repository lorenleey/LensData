# Tarea 5 · Transformación de Bronze a Silver

Implementación preparada en [`scripts/silver_quality.py`](../scripts/silver_quality.py) y [`01_transformar_silver.ipynb`](../notebooks/silver/01_transformar_silver.ipynb). Aún no se ha ejecutado en Databricks.

El notebook lee las dos tablas Bronze definidas en `config/bronze.json`, aplica las reglas aprobadas de `config/silver.json` y separa publicaciones, mediciones y rechazados. Las filas aceptadas conservan `_source_file`, incorporan `_processed_at` y registran advertencias en `_quality_warnings`. Los rechazos conservan un JSON de los valores Bronze, la ruta completa, todos los códigos de error y la fecha de procesamiento. Los duplicados de clave permanecen como advertencia; esta tarea no selecciona una versión definitiva ni ejecuta `MERGE`.

La escritura reconstruye **solo las tres tablas Silver derivadas** mediante `overwrite`. Al volver a ejecutar el notebook, los valores de `_processed_at` cambiarán; no son timestamps de ingesta Bronze. No ejecutar si existen tablas Silver con datos ajenos al flujo, porque serían reemplazadas. Bronze y sus checkpoints no se modifican.

## Validación en Databricks

1. Revisar los conteos actuales de Bronze y ejecutar el notebook Silver desde la raíz del proyecto, con `config/` y `scripts/` accesibles.
2. Comprobar la igualdad `válidas + rechazadas = Bronze` para cada entidad. El notebook la verifica antes de escribir.
3. Inspeccionar `workspace.lensdata_silver.rechazados`: deben aparecer el comentario vacío, `pendiente`, la métrica `-1` y `V999` si los archivos correspondientes ya están en Bronze.
4. Confirmar que `Regalos ` se transformó en `regalos` en publicaciones Silver y que las fechas y métricas tienen los tipos declarados.
5. Tras cargar reenvío y corrección en Bronze, repetir la transformación. Ambas filas deben mantenerse en Silver con `DUPLICATE_BUSINESS_KEY`; la selección de la versión vigente corresponde a la fase de consolidación.

En la muestra CSV local hay 473 mediciones en 15 archivos. La última evidencia remota documentada de Bronze tenía 471 filas en 13 archivos. No se atribuye ningún conteo a Silver hasta ejecutar y verificar el notebook en Databricks.
