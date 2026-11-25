# Tarea 7 · Deduplicación y resolución de versiones

La clave de negocio es (`plataforma`, `video_id`, `fecha_medicion`). La implementación está en [`scripts/deduplicar_mediciones.py`](../scripts/deduplicar_mediciones.py) y el notebook de prueba, de solo lectura, en [`02_resolver_versiones.ipynb`](../notebooks/silver/02_resolver_versiones.ipynb). No crea tablas ni ejecuta `MERGE`.

## Criterio de selección

1. Se comparan las cuatro métricas de cada clave y `version_origen`. Si las métricas son idénticas, las filas adicionales se clasifican como reenvíos. Una ventana con `row_number()` escoge una copia representativa por la ruta `_source_file` ordenada alfabéticamente. Esta ruta conserva la trazabilidad; no determina qué versión prevalece.
2. Si la misma clave y versión tienen métricas diferentes, todas esas filas aparecen en `conflictos` con `SAME_KEY_VERSION_DIFFERENT_METRICS`. No se escoge una de ellas por orden de lectura ni por ruta.
3. Una ventana calcula la versión máxima por clave. Solo hay candidata si esa versión no tiene conflicto. Si el conflicto afecta únicamente a una versión inferior y la máxima es coherente, la máxima sí puede ser candidata; el conflicto inferior permanece registrado.
4. Las versiones inferiores se exponen en `superadas`. Un reenvío de v1 recibido después de v2 sigue siendo inferior a v2, con independencia del momento en que llegó el archivo.

Las salidas son DataFrames `candidatas`, `reenvios`, `conflictos` y `superadas`. `candidatas` conserva `_source_file` y añade `resolution` (`ORIGINAL` o `CORRECCION`). Cada clave aparece a lo sumo una vez. Bronze y Silver de entrada permanecen intactos.

`dropDuplicates` por sí solo no resuelve este problema: solo elimina filas según las columnas elegidas y no garantiza cuál de las filas distintas de una misma clave conservará. Si se usa únicamente la clave, podría retener un reenvío v1 y descartar una corrección v2. Si se incluye la versión, tampoco detecta métricas contradictorias dentro de esa versión. La selección exige comparar el contenido y ordenar explícitamente por `version_origen`.

## Pruebas preparadas

El notebook exige los registros de `lote_01`, `lote_reenvio` y `lote_correccion` en la tabla Silver. Verifica que el reenvío v1 es idéntico al original, que la candidata es la corrección v2 con 950 reproducciones y que `_source_file` apunta a `correccion_dia_1.csv`. Después crea en memoria una copia v1 tardía y un conflicto de métricas en v2. Comprueba que la copia tardía no sustituye a v2 y que el conflicto bloquea la candidatura de esa clave.

Estas pruebas **no se han ejecutado en Databricks** desde esta revisión. Los CSV locales sí contienen el original v1, el reenvío v1 y la corrección v2 descritos. No hay en Bronze una marca de llegada fiable por fila para demostrar el orden real de recepción; la llegada tardía se simula en memoria y la regla no depende del orden.
