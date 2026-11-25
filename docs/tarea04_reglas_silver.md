# Tarea 4 · Esquemas y reglas de calidad de Silver

> Reglas aprobadas para implementar Silver. Este documento define el contrato; la implementación está en la [tarea 5](tarea05_transformacion_silver.md).

## Base revisada

Los notebooks Bronze declaran los campos de entrada como cadenas y conservan `_source_file`. La propuesta de LensData exige categorías comparables, versiones de corrección y rechazos trazables. En los CSV locales hay 40 publicaciones y 473 filas de mediciones repartidas en 15 archivos, incluidos el reenvío y la corrección. Este conteo **es local**: la última salida remota documentada de Bronze mostraba 471 filas y 13 archivos, antes de esos dos lotes.

## Esquemas propuestos

| Entidad | Campo | Tipo Silver | Obligatorio | Tratamiento |
| --- | --- | --- | --- | --- |
| Publicaciones | `plataforma` | `STRING` | Sí | `lower(trim)` |
| Publicaciones | `video_id` | `STRING` | Sí | `upper(trim)` |
| Publicaciones | `fecha_publicacion` | `DATE` | Sí | ISO `yyyy-MM-dd`, `try_cast` |
| Publicaciones | `categoria` | `STRING` | Sí | `lower(trim)` |
| Publicaciones | `titulo` | `STRING` | No | `trim`; vacío → `NULL` |
| Mediciones | `plataforma`, `video_id` | `STRING` | Sí | Igual que publicaciones |
| Mediciones | `fecha_medicion` | `DATE` | Sí | ISO `yyyy-MM-dd`, `try_cast` |
| Mediciones | `version_origen` | `INT` | Sí | Entero positivo, `try_cast` |
| Mediciones | `reproducciones`, `me_gusta`, `comentarios`, `compartidos` | `BIGINT` | Sí | Enteros no negativos, `try_cast` |
| Ambas | `_source_file` | `STRING` | Sí | Copia de Bronze, sin recortar la ruta |
| Ambas | `_processed_at` | `TIMESTAMP` | Sí | Momento de procesamiento en Silver, distinto de la ingesta Bronze |
| Ambas | `_quality_warnings` | `ARRAY<STRING>` | No | Códigos de advertencias sin rechazo |

La clave de publicación es (`plataforma`, `video_id`). La clave de medición es (`plataforma`, `video_id`, `fecha_medicion`); `version_origen` permite distinguir revisiones de esa medición, pero **no forma parte de la clave de negocio**. Un reenvío de la misma versión puede existir en Bronze y queda señalado como duplicado para su resolución posterior. Esta tarea no decide ni ejecuta el `MERGE`.

## Reglas propuestas

`Rechazar` significa separar la fila del conjunto Silver válido, manteniéndola intacta en Bronze y registrando todos los motivos aplicables. `Advertir` conserva la fila con un código para revisión. Los ejemplos inválidos ilustran la regla; algunos proceden de los CSV locales y otros son casos de prueba propuestos.

| Regla y clase | Justificación y acción | Ejemplo válido | Ejemplo inválido |
| --- | --- | --- | --- |
| `CSV_HEADER_MISMATCH`, `CSV_WRONG_FIELD_COUNT`, `CSV_UNREADABLE` · estructural | Detectar un archivo que no respeta el contrato CSV. Detener o aislar el archivo antes de Silver; no convertir posiciones erróneas. | Cabecera con las ocho columnas esperadas | Cabecera sin `compartidos` |
| `REQUIRED_FIELD` · calidad | Claves, fechas, categoría y métricas deben tener valor después de `trim`. Rechazar; `titulo` es opcional. | `comentarios="0"` | `comentarios=""` en lote 01, `tiktok/V001` |
| `INVALID_PLATFORM` · calidad | Solo se comparan TikTok e Instagram. Normalizar `trim` + minúsculas y rechazar otra plataforma. | `" TikTok "` → `tiktok` | `"youtube"` |
| `INVALID_VIDEO_ID` · calidad | Necesario para la unión y clave estable. `upper(trim)` y patrón `^V[0-9]{3}$`. Rechazar. | `" v001 "` → `V001` | `"video-1"` |
| `INVALID_CATEGORY` · calidad | Garantizar categorías comparables. `lower(trim)` y pertenencia a regalos, comida asiática, belleza o juguetes. Rechazar. | `"Regalos "` → `regalos` | `"otros"` |
| `INVALID_DATE` · calidad | Calcular edad del vídeo requiere fecha real. Exigir ISO `yyyy-MM-dd`; `try_cast(... AS DATE)` devuelve `NULL` si falla. Rechazar. | `2026-11-02` | `2026-02-30` o `02/11/2026` |
| `INVALID_NUMBER` · calidad | `try_cast` a `BIGINT` debe detectar texto u overflow sin abortar el lote. Rechazar. | `me_gusta="8"` | `comentarios="pendiente"` en lote 02 |
| `NEGATIVE_METRIC` · calidad | Las métricas son conteos acumulados no negativos. Rechazar. | `compartidos="0"` | `compartidos="-1"` en lote 02 |
| `INVALID_VERSION` · calidad | La revisión debe ser un `INT` mayor o igual que 1. Rechazar nulo, texto, cero, negativo u overflow. | `version_origen="2"` | `version_origen="0"` |
| `UNKNOWN_VIDEO` · calidad referencial | La medición debe referir a una publicación por plataforma y vídeo normalizados. Rechazar de Silver válido. | `tiktok/V000` | `tiktok/V999` en `video_desconocido.csv` |
| `MEASUREMENT_BEFORE_PUBLICATION` · calidad temporal | No puede medirse antes de publicar el vídeo. Rechazar. | Publicación 01/11, medición 02/11 | Publicación 02/11, medición 01/11 |
| `METRIC_EXCEEDS_VIEWS` · advertencia de negocio | Una interacción mayor que reproducciones merece revisión, pero podría reflejar otra definición de métrica. Conservar. | 50 me gusta / 1000 reproducciones | 120 me gusta / 100 reproducciones |
| `DUPLICATE_BUSINESS_KEY` · advertencia de negocio | Reenvíos y correcciones de mediciones comparten clave. Conservar la evidencia y señalar la resolución por versión. | Una fila de `tiktok/V000/2026-11-01` | Original v1 + reenvío v1 + corrección v2 |
| `IDENTICAL_PUBLICATION_COPY` · copia apartada | Una clave de publicación solo debe aparecer una vez en el conjunto válido; conservar la copia en rechazados con procedencia. | Un registro `tiktok/V000` | Dos registros idénticos `tiktok/V000` |
| `CONFLICTING_PUBLICATION` · calidad | La misma clave con fecha, categoría o título distintos no admite elegir una fila arbitrariamente. Rechazar ambas. | Dos copias idénticas | `tiktok/V000` con fechas distintas |
| `AMBIGUOUS_PUBLICATION` · calidad referencial | Una medición no debe asociarse a una publicación conflictiva. Rechazarla hasta resolver la referencia. | Medición de una publicación única | Medición de `tiktok/V000` si hay dos publicaciones contradictorias |

Los espacios exteriores y las diferencias de mayúsculas se normalizan en los campos indicados; no se corrigen tildes ni se inventan alias de plataformas o categorías. Se guarda el valor original en Bronze. Las métricas y la versión se validan tras `trim`: un `NULL` por ausencia genera `REQUIRED_FIELD`; un valor no vacío que `try_cast` convierta en `NULL` genera `INVALID_NUMBER`, `INVALID_VERSION` o `INVALID_DATE` según la columna. Para las fechas, el patrón ISO se comprueba antes del cast seguro. Los errores estructurales de cabecera y lectura ocurren antes de la validación de filas; el modo `FAILFAST` de Bronze puede detener la lectura de un CSV mal formado.

## Registro de rechazados

La estructura propuesta contiene `entity STRING`, `reason_codes ARRAY<STRING>`, `reason_details ARRAY<STRING>`, `raw_record STRING` (JSON de todos los campos Bronze originales), `source_file STRING` y `processed_at TIMESTAMP`. Una fila con varios fallos se registra una vez con todos sus códigos. `source_file` conserva la ruta completa de Bronze y `processed_at` indica cuándo se evaluó la regla; no se presenta como hora de llegada del archivo. Si dos filas idénticas proceden del mismo archivo, estos campos no garantizan un identificador único de línea: Bronze no contiene número de línea.

Las decisiones y parámetros reutilizables están centralizados en [`config/silver.json`](../config/silver.json). La obligatoriedad de las cuatro métricas, el rechazo de fechas anteriores a la publicación y el tratamiento de claves duplicadas forman parte de las reglas aprobadas.
