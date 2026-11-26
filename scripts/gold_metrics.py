"""Métricas Gold a partir de publicaciones y mediciones vigentes Silver."""
from pyspark.sql import functions as F

KEY = ["plataforma", "video_id"]
METRICS = ["reproducciones", "me_gusta", "comentarios", "compartidos"]


def _unique(df, keys, label):
    if df.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(f"Claves duplicadas en {label}: {keys}")


def _ratio(numerator, denominator):
    return F.when(denominator > 0, numerator.cast("double") * 100.0 / denominator)


def rendimiento_video(publicaciones, mediciones):
    """Una fila por vídeo y fecha de medición; conserva las métricas acumuladas."""
    _unique(publicaciones, KEY, "publicaciones")
    _unique(mediciones, KEY + ["fecha_medicion"], "mediciones vigentes")
    pubs = publicaciones.select(*KEY, "fecha_publicacion", "categoria", "titulo")
    med = mediciones.select(*KEY, "fecha_medicion", "version_origen", *METRICS)
    if med.join(pubs, KEY, "left_anti").limit(1).count():
        raise ValueError("Hay mediciones vigentes sin publicación válida")
    video = (med.join(pubs, KEY, "inner")
             .withColumn("dia_video", F.datediff("fecha_medicion", "fecha_publicacion") + 1)
             .withColumn("interacciones", F.col("me_gusta") + F.col("comentarios") + F.col("compartidos"))
             .withColumn("tasa_interaccion", _ratio(F.col("interacciones"), F.col("reproducciones"))))
    if video.filter(F.col("dia_video").isNull() | (F.col("dia_video") < 1)).limit(1).count():
        raise ValueError("Hay fechas de medición anteriores a la publicación o nulas")
    return video.select(*KEY, "categoria", "titulo", "fecha_publicacion", "fecha_medicion",
                        "dia_video", "version_origen", *METRICS, "interacciones", "tasa_interaccion")


def resumen_dia(publicaciones, video, grupos, dia=3):
    """Una fila por grupo; ratio de sumas solo de vídeos con denominador positivo."""
    if not isinstance(dia, int) or dia < 1:
        raise ValueError("El día objetivo debe ser un entero positivo")
    base = publicaciones.groupBy(*grupos).agg(F.count("*").alias("videos_publicados"))
    snapshot = video.filter(F.col("dia_video") == dia)
    _unique(snapshot, KEY, "mediciones del día objetivo")
    valid = F.col("reproducciones") > 0
    measured = snapshot.groupBy(*grupos).agg(
        F.count("*").alias("videos_medidos"),
        F.sum(F.when(valid, 1).otherwise(0)).alias("videos_con_tasa"),
        F.sum(F.when(valid, F.col("reproducciones")).otherwise(0)).alias("reproducciones"),
        F.sum(F.when(valid, F.col("interacciones")).otherwise(0)).alias("interacciones"),
    )
    cols = ["videos_medidos", "videos_con_tasa", "reproducciones", "interacciones"]
    return (base.join(measured, grupos, "left").fillna(0, subset=cols)
            .withColumn("dia_video", F.lit(dia))
            .withColumn("videos_sin_medicion", F.col("videos_publicados") - F.col("videos_medidos"))
            .withColumn("videos_sin_tasa", F.col("videos_medidos") - F.col("videos_con_tasa"))
            .withColumn("cobertura_pct", _ratio(F.col("videos_medidos"), F.col("videos_publicados")))
            .withColumn("tasa_interaccion", _ratio(F.col("interacciones"), F.col("reproducciones")))
            .select(*grupos, "dia_video", "videos_publicados", "videos_medidos", "videos_con_tasa",
                    "videos_sin_medicion", "videos_sin_tasa", "cobertura_pct", "reproducciones",
                    "interacciones", "tasa_interaccion"))


def generar_gold(publicaciones, mediciones, dia=3):
    video = rendimiento_video(publicaciones, mediciones)
    return {"rendimiento_video": video,
            "rendimiento_categoria": resumen_dia(publicaciones, video, ["plataforma", "categoria"], dia),
            "rendimiento_plataforma": resumen_dia(publicaciones, video, ["plataforma"], dia)}
