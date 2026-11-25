# Tarea 5 · Transformación de Bronze a Silver

Implementación en [`scripts/silver_quality.py`](../scripts/silver_quality.py) y [`01_transformar_silver.ipynb`](../notebooks/silver/01_transformar_silver.ipynb). La salida guardada del notebook muestra 40 publicaciones válidas, 469 mediciones válidas y 4 mediciones rechazadas.

El notebook lee las dos tablas Bronze definidas en `config/bronze.json`, aplica las reglas aprobadas de `config/silver.json` y separa publicaciones, mediciones y rechazados. Las filas aceptadas conservan `_source_file`, incorporan `_processed_at` y registran advertencias en `_quality_warnings`. Los rechazos conservan un JSON de los valores Bronze, la ruta completa, todos los códigos de error y la fecha de procesamiento. Los duplicados de clave permanecen como advertencia; esta tarea no selecciona una versión definitiva ni ejecuta `MERGE`.

La escritura reconstruye **solo las tres tablas Silver intermedias** mediante `overwrite`. `workspace.lensdata_silver.mediciones_vigentes` queda reservada como tabla definitiva y no se escribe aquí. El notebook histórico `01_transformar_silver.ipynb` conserva sus salidas; el contrato actualizado se ejecutará con [`03_transformar_silver_validado.ipynb`](../notebooks/silver/03_transformar_silver_validado.ipynb). Al volver a ejecutar la transformación, `_processed_at` cambiará; no es el timestamp de ingesta Bronze. No ejecutar si las tablas intermedias contienen datos ajenos al flujo, porque serían reemplazadas. Bronze y sus checkpoints no se modifican.

## Validación en Databricks

1. Revisar los conteos actuales de Bronze y ejecutar el notebook Silver desde la raíz del proyecto, con `config/` y `scripts/` accesibles.
2. Comprobar la igualdad `válidas + rechazadas = Bronze` para cada entidad. El notebook la verifica antes de escribir.
3. Inspeccionar `workspace.lensdata_silver.rechazados`: deben aparecer el comentario vacío, `pendiente`, la métrica `-1` y `V999` si los archivos correspondientes ya están en Bronze.
4. Confirmar que `Regalos ` se transformó en `regalos` en publicaciones Silver y que las fechas y métricas tienen los tipos declarados.
5. Tras cargar reenvío y corrección en Bronze, repetir la transformación. Ambas filas deben mantenerse en Silver con `DUPLICATE_BUSINESS_KEY`; la selección de la versión vigente corresponde a la fase de consolidación.

La muestra y la salida Bronze posterior incluyen 473 mediciones en 15 archivos. La ejecución Silver guardada separó esas 473 filas en 469 válidas y 4 rechazadas; los cuatro motivos aparecen en la captura [f10-validacion-rechazos.png](../img/f10-validacion-rechazos.png).
