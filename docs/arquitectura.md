# Arquitectura y transformación

CSV → Auto Loader → Bronze → Silver intermedia → resolución de versiones → MERGE (pendiente) → Gold (pendiente).

# Tarea 5 · Transformación de Bronze a Silver

Implementación en [`scripts/silver_quality.py`](../scripts/silver_quality.py) y [`01_transformar_silver.ipynb`](../docs/historico/01_transformar_silver.ipynb). La salida guardada del notebook muestra 40 publicaciones válidas, 469 mediciones válidas y 4 mediciones rechazadas.

El notebook lee las dos tablas Bronze definidas en `config/bronze.json`, aplica las reglas aprobadas de `config/silver.json` y separa publicaciones, mediciones y rechazados. Las filas aceptadas conservan `_source_file`, incorporan `_processed_at` y registran advertencias en `_quality_warnings`. Los rechazos conservan un JSON de los valores Bronze, la ruta completa, todos los códigos de error y la fecha de procesamiento. Los duplicados de clave permanecen como advertencia; esta tarea no selecciona una versión definitiva ni ejecuta `MERGE`.

La escritura reconstruye **solo las tres tablas Silver intermedias** mediante `overwrite`. `workspace.lensdata_silver.mediciones_vigentes` queda reservada como tabla definitiva y no se escribe aquí. El notebook histórico `01_transformar_silver.ipynb` conserva sus salidas; el contrato actualizado se ejecuta con [`03_transformar_silver_validado.ipynb`](../notebooks/silver/01_transformar_silver.ipynb). Al volver a ejecutar la transformación, `_processed_at` cambiará; no es el timestamp de ingesta Bronze. No ejecutar si las tablas intermedias contienen datos ajenos al flujo, porque serían reemplazadas. Bronze y sus checkpoints no se modifican.

## Validación en Databricks

1. Revisar los conteos actuales de Bronze y ejecutar el notebook Silver desde la raíz del proyecto, con `config/` y `scripts/` accesibles.
2. Comprobar la igualdad `válidas + rechazadas = Bronze` para cada entidad. El notebook la verifica antes de escribir.
3. Inspeccionar `workspace.lensdata_silver.rechazados`: deben aparecer el comentario vacío, `pendiente`, la métrica `-1` y `V999` si los archivos correspondientes ya están en Bronze.
4. Confirmar que `Regalos ` se transformó en `regalos` en publicaciones Silver y que las fechas y métricas tienen los tipos declarados.
5. Tras cargar reenvío y corrección en Bronze, repetir la transformación. Ambas filas deben mantenerse en Silver con `DUPLICATE_BUSINESS_KEY`; la selección de la versión vigente corresponde a la fase de consolidación.

La muestra y la salida Bronze posterior incluyen 473 mediciones en 15 archivos. La ejecución Silver guardada separó esas 473 filas en 469 válidas y 4 rechazadas; los cuatro motivos aparecen en la captura [f10-validacion-rechazos.png](../img/f10-validacion-rechazos.png).


# Tarea 7 · Deduplicación y resolución de versiones

La clave de negocio es (`plataforma`, `video_id`, `fecha_medicion`). La implementación está en [`scripts/deduplicar_mediciones.py`](../scripts/deduplicar_mediciones.py) y el notebook de prueba, de solo lectura, en [`02_resolver_versiones.ipynb`](../tests/test_versiones.ipynb). No crea tablas ni ejecuta `MERGE`.

## Criterio de selección

1. Se comparan las cuatro métricas de cada clave y `version_origen`. Si las métricas son idénticas, las filas adicionales se clasifican como reenvíos. Una ventana con `row_number()` escoge una copia representativa por la ruta `_source_file` ordenada alfabéticamente. Esta ruta conserva la trazabilidad; no determina qué versión prevalece.
2. Si la misma clave y versión tienen métricas diferentes, todas esas filas aparecen en `conflictos` con `SAME_KEY_VERSION_DIFFERENT_METRICS`. No se escoge una de ellas por orden de lectura ni por ruta.
3. Una ventana calcula la versión máxima por clave. Solo hay candidata si esa versión no tiene conflicto. Si el conflicto afecta únicamente a una versión inferior y la máxima es coherente, la máxima sí puede ser candidata; el conflicto inferior permanece registrado.
4. Las versiones inferiores se exponen en `superadas`. Un reenvío de v1 recibido después de v2 sigue siendo inferior a v2, con independencia del momento en que llegó el archivo.

Las salidas son DataFrames `candidatas`, `reenvios`, `conflictos` y `superadas`. `candidatas` conserva `_source_file` y añade `resolution` (`ORIGINAL` o `CORRECCION`). Cada clave aparece a lo sumo una vez. Bronze y Silver de entrada permanecen intactos.

`dropDuplicates` por sí solo no resuelve este problema: solo elimina filas según las columnas elegidas y no garantiza cuál de las filas distintas de una misma clave conservará. Si se usa únicamente la clave, podría retener un reenvío v1 y descartar una corrección v2. Si se incluye la versión, tampoco detecta métricas contradictorias dentro de esa versión. La selección exige comparar el contenido y ordenar explícitamente por `version_origen`.

## Pruebas preparadas

El notebook exige los registros de `lote_01`, `lote_reenvio` y `lote_correccion` en la tabla Silver. Verifica que el reenvío v1 es idéntico al original, que la candidata es la corrección v2 con 950 reproducciones y que `_source_file` apunta a `correccion_dia_1.csv`. Después crea en memoria una copia v1 tardía y un conflicto de métricas en v2. Comprueba que la copia tardía no sustituye a v2 y que el conflicto bloquea la candidatura de esa clave.

Las salidas guardadas del notebook muestran 469 filas Silver, 467 candidatas, un reenvío idéntico y ningún conflicto real. La prueba en memoria de versión tardía y conflicto terminó con sus aserciones correctas; el conflicto simulado no se escribió. No hay en Bronze una marca de llegada fiable por fila para demostrar el orden real de recepción: la llegada tardía se simuló en memoria y la regla no depende del orden.
