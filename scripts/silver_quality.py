"""Transformaciones puras de Bronze a Silver; no escribe tablas ni toca checkpoints."""

from pyspark.sql import Window, functions as F


def _empty():
    return F.expr("cast(array() as array<string>)")


def _issue(condition, code, detail):
    return F.when(condition, F.array(F.lit(f"{code}: {detail}"))).otherwise(_empty())


def _normalize(raw, entity, config):
    fields = config["entities"][entity]["fields"]
    df = raw.withColumn("_raw_record", F.to_json(F.struct(*[F.col(name) for name in fields]), {"ignoreNullFields": "false"}))
    for name, spec in fields.items():
        value = F.trim(F.col(name))
        if name in ("plataforma", "categoria"):
            value = F.lower(value)
        elif name == "video_id":
            value = F.upper(value)
        if spec["type"] == "DATE":
            pattern = config["normalization"]["date_pattern"]
            value = F.when(value.rlike(pattern), F.expr(f"try_cast(trim(`{name}`) AS DATE)"))
        elif spec["type"] in ("INT", "BIGINT"):
            value = F.expr(f"try_cast(trim(`{name}`) AS {spec['type']})")
        else:
            value = F.when(value != "", value)
        df = df.withColumn(f"_typed_{name}", value)
    return df


def _base_errors(df, entity, config):
    fields = config["entities"][entity]["fields"]
    errors = []
    for name, spec in fields.items():
        raw_value = F.trim(F.col(name))
        typed = F.col(f"_typed_{name}")
        missing = F.col(name).isNull() | (raw_value == "")
        if spec["required"]:
            errors.append(_issue(missing, "REQUIRED_FIELD", name))
        if spec["type"] == "DATE":
            errors.append(_issue(~missing & typed.isNull(), "INVALID_DATE", name))
        elif spec["type"] in ("INT", "BIGINT"):
            code = "INVALID_VERSION" if name == "version_origen" else "INVALID_NUMBER"
            errors.append(_issue(~missing & typed.isNull(), code, name))
            minimum = spec.get("minimum")
            if minimum is not None:
                negative_code = "INVALID_VERSION" if name == "version_origen" else "NEGATIVE_METRIC"
                errors.append(_issue(typed < minimum, negative_code, name))
    norms = config["normalization"]
    for name, code in (("plataforma", "INVALID_PLATFORM"), ("categoria", "INVALID_CATEGORY")):
        if name in fields:
            errors.append(_issue(F.col(f"_typed_{name}").isNotNull() & ~F.col(f"_typed_{name}").isin(norms[name]["allowed"]), code, name))
    errors.append(_issue(F.col("_typed_video_id").isNotNull() & ~F.col("_typed_video_id").rlike(norms["video_id"]["pattern"]), "INVALID_VIDEO_ID", "video_id"))
    return df.withColumn("_errors", F.concat(*errors))


def _finish(df, entity, config):
    fields = list(config["entities"][entity]["fields"])
    selected = [F.col(f"_typed_{name}").alias(name) for name in fields]
    selected += [F.col("_source_file"), F.current_timestamp().alias("_processed_at"),
                 F.transform("_warnings", lambda x: F.split(x, ": ").getItem(0)).alias("_quality_warnings")]
    accepted = df.filter(F.size("_errors") == 0).select(*selected)
    rejected = df.filter(F.size("_errors") > 0).select(
        F.lit(entity).alias("entity"),
        F.transform("_errors", lambda x: F.split(x, ": ").getItem(0)).alias("reason_codes"),
        F.col("_errors").alias("reason_details"),
        F.col("_raw_record").alias("raw_record"),
        F.col("_source_file").alias("source_file"),
        F.current_timestamp().alias("processed_at"),
    )
    return accepted, rejected


def transform_publicaciones(raw, config):
    entity = "publicaciones"
    df = _base_errors(_normalize(raw, entity, config), entity, config)
    key = [F.col("_typed_plataforma"), F.col("_typed_video_id")]
    df = df.withColumn("_duplicate_count", F.count(F.lit(1)).over(Window.partitionBy(*key)))
    df = df.withColumn("_warnings", _issue(F.col("_duplicate_count") > 1, "DUPLICATE_BUSINESS_KEY", "publicaciones"))
    accepted, rejected = _finish(df, entity, config)
    return accepted, rejected


def transform_mediciones(raw, valid_publicaciones, config):
    entity = "mediciones"
    df = _base_errors(_normalize(raw, entity, config), entity, config)
    publications = valid_publicaciones.select(
        F.col("plataforma").alias("_pub_platform"), F.col("video_id").alias("_pub_video_id"),
        F.col("fecha_publicacion").alias("_pub_date"),
    ).groupBy("_pub_platform", "_pub_video_id").agg(F.min("_pub_date").alias("_pub_date"))
    df = df.join(publications,
        (F.col("_typed_plataforma") == F.col("_pub_platform")) &
        (F.col("_typed_video_id") == F.col("_pub_video_id")), "left")
    df = df.withColumn("_errors", F.concat(F.col("_errors"),
        _issue(F.col("_typed_plataforma").isNotNull() & F.col("_typed_video_id").isNotNull() & F.col("_pub_video_id").isNull(), "UNKNOWN_VIDEO", "plataforma,video_id"),
        _issue(F.col("_typed_fecha_medicion") < F.col("_pub_date"), "MEASUREMENT_BEFORE_PUBLICATION", "fecha_medicion")))
    metrics = ("me_gusta", "comentarios", "compartidos")
    exceeding = None
    for metric in metrics:
        condition = F.col(f"_typed_{metric}") > F.col("_typed_reproducciones")
        exceeding = condition if exceeding is None else exceeding | condition
    key = [F.col("_typed_plataforma"), F.col("_typed_video_id"), F.col("_typed_fecha_medicion")]
    df = df.withColumn("_duplicate_count", F.count(F.lit(1)).over(Window.partitionBy(*key)))
    df = df.withColumn("_warnings", F.concat(
        _issue(exceeding, "METRIC_EXCEEDS_VIEWS", "interacciones"),
        _issue(F.col("_duplicate_count") > 1, "DUPLICATE_BUSINESS_KEY", "mediciones")))
    return _finish(df, entity, config)
