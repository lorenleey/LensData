# LensData

Análisis del rendimiento de vídeos de una tienda ficticia en Databricks. La fase actual carga publicaciones y mediciones en Bronze; Silver queda pendiente de validación.

## Ingesta Bronze

La configuración de rutas, tabla, checkpoint y esquema de entrada está en [config/bronze.json](config/bronze.json). El volumen existente es `workspace.lensdata_bronze.archivos`. Las mediciones se reciben en `recepcion/mediciones/lote_01/`, `lote_02/`, `lote_03/`, `lote_correccion/` y `lote_reenvio/`.

1. Ejecutar [01_ingestar_publicaciones.ipynb](notebooks/bronze/01_ingestar_publicaciones.ipynb) para las 40 publicaciones.
2. Copiar cada CSV de mediciones a su subcarpeta del volumen, sin sobrescribir los ya procesados, y ejecutar [02_ingestar_mediciones.ipynb](notebooks/bronze/02_ingestar_mediciones.ipynb).
3. Ejecutar [03_validar_mediciones_bronze.ipynb](notebooks/bronze/03_validar_mediciones_bronze.ipynb) para comprobar la tabla. La última celda se ejecuta después de cargar reenvío y corrección.

Auto Loader (`cloudFiles`) usa Structured Streaming para descubrir los CSV nuevos en las subcarpetas. `availableNow=True` procesa los archivos disponibles y termina. El checkpoint persistente recuerda los archivos ya procesados y permite repetir la ejecución sin volver a insertarlos. Bronze mantiene los ocho campos originales como cadenas y `_source_file` con la ruta de `_metadata.file_path`; no aplica deduplicación ni limpieza, ya que de eso nos encargaremos en la siguiente fase.

## Estado comprobado

La usuaria validó manualmente la conexión remota con Databricks Connect, la lectura recursiva, las cabeceras, la ingesta de los lotes 01–03 y la repetición sin archivos nuevos. Las salidas guardadas del notebook antes de su limpieza muestran **13 CSV**, **471 filas** y **471 → 471** en la repetición. Esos 13 archivos incluyen `lote_03/video_desconocido.csv`, con una fila. El reenvío y la corrección aún no figuran en esa relación de archivos.

Queda por ejecutar la carga de `lote_reenvio` y `lote_correccion`, comprobar los tres registros de `tiktok/V000/2026-11-01`, verificar el formato Delta y guardar capturas reales. El procedimiento y las comprobaciones pendientes están en [desarrollo.md](desarrollo.md).
