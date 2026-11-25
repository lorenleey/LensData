"""Resolución de versiones Silver sin escrituras ni MERGE."""

from pyspark.sql import Window, functions as F


KEY = ["plataforma", "video_id", "fecha_medicion"]
METRICS = ["reproducciones", "me_gusta", "comentarios", "compartidos"]
VERSION = "version_origen"


def resolver_versiones(mediciones):
    """Devuelve candidatas, reenvíos, conflictos y versiones superadas.

    Una clave cuya versión máxima tiene métricas contradictorias no tiene candidata.
    Una versión inferior conflictiva queda registrada, pero no bloquea una versión
    superior coherente. La ruta solo desempata copias de contenido idéntico.
    """
    required = set(KEY + METRICS + [VERSION, "_source_file"])
    missing = required.difference(mediciones.columns)
    if missing:
        raise ValueError(f"Faltan columnas Silver: {sorted(missing)}")

    payload = F.to_json(F.struct(*[F.col(c) for c in METRICS]), {"ignoreNullFields": "false"})
    base = mediciones.withColumn("_payload", payload)
    group_cols = KEY + [VERSION]
    stats = base.groupBy(*group_cols).agg(
        F.count(F.lit(1)).alias("_copies"),
        F.countDistinct("_payload").alias("_variants"),
    )
    rows = base.join(stats, group_cols, "inner")
    per_key = Window.partitionBy(*KEY)
    per_version = Window.partitionBy(*group_cols, "_payload").orderBy(F.col("_source_file").asc())
    rows = (rows.withColumn("_max_version", F.max(VERSION).over(per_key))
            .withColumn("_copy_rank", F.row_number().over(per_version)))

    conflicts = (rows.filter(F.col("_variants") > 1)
                 .withColumn("conflict_code", F.lit("SAME_KEY_VERSION_DIFFERENT_METRICS"))
                 .drop("_payload", "_copy_rank", "_copies", "_variants", "_max_version"))
    reenvios = (rows.filter((F.col("_variants") == 1) & (F.col("_copy_rank") > 1))
                .withColumn("duplicate_code", F.lit("IDENTICAL_REDELIVERY"))
                .drop("_payload", "_copy_rank", "_copies", "_variants", "_max_version"))
    superadas = (rows.filter(F.col(VERSION) < F.col("_max_version"))
                 .drop("_payload", "_copy_rank", "_copies", "_variants", "_max_version"))
    candidates = (rows.filter((F.col(VERSION) == F.col("_max_version")) &
                              (F.col("_variants") == 1) & (F.col("_copy_rank") == 1))
                  .withColumn("resolution", F.when(F.col(VERSION) > 1, F.lit("CORRECCION"))
                              .otherwise(F.lit("ORIGINAL")))
                  .drop("_payload", "_copy_rank", "_copies", "_variants", "_max_version"))
    return candidates, reenvios, conflicts, superadas
