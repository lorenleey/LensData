"""MERGE incremental de mediciones Silver; Bronze y sus checkpoints son de solo lectura."""

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from deduplicar_mediciones import KEY, METRICS, VERSION, resolver_versiones


def _require_unique(df, columns, label):
    if df.groupBy(*columns).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(f"{label}: hay claves duplicadas en {columns}")


def _create_target(spark, table, sample):
    if not spark.catalog.tableExists(table):
        columns = ", ".join(
            f"`{field.name}` {field.dataType.simpleString()}"
            for field in sample.schema.fields)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({columns}, `_merged_at` TIMESTAMP) USING DELTA")
    expected = {field.name: field.dataType.simpleString() for field in sample.schema.fields}
    expected["_merged_at"] = "timestamp"
    actual = {field.name: field.dataType.simpleString()
              for field in spark.table(table).schema.fields}
    if actual != expected:
        raise ValueError(f"La tabla {table} no tiene el esquema esperado para MERGE")
    _require_unique(spark.table(table), KEY, table)


def _create_conflict_table(spark, table):
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {table} (
        conflict_id STRING, conflict_code STRING,
        plataforma STRING, video_id STRING, fecha_medicion DATE,
        version_origen INT, source_file STRING, source_metrics STRING,
        target_source_file STRING, target_metrics STRING,
        processed_at TIMESTAMP
    ) USING DELTA""")


def _conflict_rows(df, code, target_file=None, target_metrics=None):
    source_metrics = F.to_json(F.struct(*[F.col(name) for name in METRICS]))
    rows = df.select(
        F.lit(code).alias("conflict_code"),
        *[F.col(name) for name in KEY],
        F.col(VERSION),
        F.col("_source_file").alias("source_file"),
        source_metrics.alias("source_metrics"),
        (target_file if target_file is not None else F.lit(None).cast("string")).alias("target_source_file"),
        (target_metrics if target_metrics is not None else F.lit(None).cast("string")).alias("target_metrics"),
        F.current_timestamp().alias("processed_at"),
    )
    identity = ["conflict_code", *KEY, VERSION, "source_file", "source_metrics",
                "target_source_file", "target_metrics"]
    return rows.withColumn("conflict_id", F.sha2(F.to_json(F.struct(*identity)), 256))


def _record_conflicts(spark, rows, table):
    # Una repetición tras un fallo no duplica los conflictos ya registrados.
    unique_rows = rows.dropDuplicates(["conflict_id"])
    if unique_rows.limit(1).count():
        _require_unique(unique_rows, ["conflict_id"], "Conflictos")
        (DeltaTable.forName(spark, table).alias("t")
         .merge(unique_rows.alias("s"), "t.conflict_id = s.conflict_id")
         .whenNotMatchedInsertAll().execute())


def merge_candidatas(spark, entradas, target_table, conflict_table, policy):
    """Resuelve un lote y aplica MERGE. El lote se puede repetir tras cualquier fallo."""
    candidates, reenvios, internal_conflicts, superadas = resolver_versiones(entradas)
    _require_unique(candidates, KEY, "Fuente del MERGE")
    _create_target(spark, target_table, candidates)
    _create_conflict_table(spark, conflict_table)

    target = spark.table(target_table).select(
        *KEY, F.col(VERSION).alias("target_version"),
        F.col("_source_file").alias("target_source_file"),
        *[F.col(name).alias(f"target_{name}") for name in METRICS],
    )
    compared = candidates.join(target, KEY, "left")
    different = None
    for name in METRICS:
        condition = ~F.col(name).eqNullSafe(F.col(f"target_{name}"))
        different = condition if different is None else different | condition
    same_version_conflicts = compared.filter(
        (F.col(VERSION) == F.col("target_version")) & different)
    target_metrics = F.to_json(F.struct(*[
        F.col(f"target_{name}").alias(name) for name in METRICS]))
    conflict_rows = _conflict_rows(
        internal_conflicts, policy["same_version_conflict"])
    conflict_rows = conflict_rows.unionByName(_conflict_rows(
        same_version_conflicts, policy["same_version_target_conflict"],
        F.col("target_source_file"), target_metrics))
    _record_conflicts(spark, conflict_rows, conflict_table)

    source = candidates.withColumn("_merged_at", F.current_timestamp())
    (DeltaTable.forName(spark, target_table).alias("t")
     .merge(source.alias("s"), " AND ".join(f"t.{name} = s.{name}" for name in KEY))
     .whenMatchedUpdateAll(condition=f"s.{VERSION} > t.{VERSION}")
     .whenNotMatchedInsertAll().execute())
    _require_unique(spark.table(target_table), KEY, "Destino del MERGE")
    return {
        "candidatas": candidates.count(),
        "reenvios_en_lote": reenvios.count(),
        "conflictos_en_lote": internal_conflicts.count(),
        "versiones_superadas_en_lote": superadas.count(),
        "conflictos_con_destino": same_version_conflicts.count(),
    }


def validar_cobertura(spark, bronze_table, valid_table, rejected_table):
    """Exige que la transformación intermedia cubra exactamente cada archivo Bronze."""
    bronze = (spark.table(bronze_table).groupBy("_source_file")
              .count().withColumnRenamed("count", "bronze_rows"))
    valid = (spark.table(valid_table).groupBy("_source_file")
             .count().withColumnRenamed("count", "valid_rows"))
    rejected = (spark.table(rejected_table).filter(F.col("entity") == "mediciones")
                .withColumnRenamed("source_file", "_source_file")
                .groupBy("_source_file")
                .count().withColumnRenamed("count", "rejected_rows"))
    coverage = (bronze.join(valid, "_source_file", "full")
                .join(rejected, "_source_file", "full")
                .fillna(0, ["bronze_rows", "valid_rows", "rejected_rows"]))
    bad = coverage.filter(
        F.col("_source_file").isNull() |
        (F.col("bronze_rows") != F.col("valid_rows") + F.col("rejected_rows")))
    if bad.limit(1).count():
        raise ValueError("Silver intermedia no cubre los archivos Bronze; reconstruye Silver antes del MERGE")
    return coverage


def _create_ledger(spark, table):
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {table} (
        _source_file STRING, bronze_row_count BIGINT, processed_at TIMESTAMP
    ) USING DELTA""")
    _require_unique(spark.table(table), ["_source_file"], table)


def procesar_incremental(spark, bronze_config, silver_config, reprocesar_archivos=()):
    """Procesa archivos aún no confirmados; el ledger se confirma solo al final."""
    tables = silver_config["tables"]
    policy = silver_config["incremental_merge"]
    coverage = validar_cobertura(
        spark, bronze_config["tables"]["mediciones"],
        tables["mediciones"], tables["rechazados"])
    _create_ledger(spark, tables[policy["ledger"]])
    ledger = spark.table(tables[policy["ledger"]])
    processed = ledger.select("_source_file")
    pending = coverage.join(processed, "_source_file", "left_anti")
    replay = tuple(dict.fromkeys(reprocesar_archivos))
    changed = (coverage.join(ledger, "_source_file", "inner")
               .filter(F.col("bronze_rows") != F.col("bronze_row_count")))
    unexpected = changed if not replay else changed.filter(~F.col("_source_file").isin(*replay))
    if unexpected.limit(1).count():
        raise ValueError("Cambió el número de filas de un archivo ya confirmado; reprocesa su ruta explícitamente")
    if replay:
        known = coverage.filter(F.col("_source_file").isin(*replay))
        if known.count() != len(replay):
            raise ValueError("Se solicitó reprocesar un archivo que no existe en Bronze")
        pending = pending.unionByName(known).dropDuplicates(["_source_file"])
    pending = pending.cache()
    pending_count = pending.count()
    if not pending_count:
        pending.unpersist()
        if not spark.catalog.tableExists(tables[policy["target"]]):
            raise ValueError("Falta la tabla definitiva; reprocesa explícitamente los archivos Bronze")
        return {"archivos": 0, "estado": "sin entradas nuevas"}
    try:
        entries = spark.table(tables["mediciones"]).join(
            pending.select("_source_file"), "_source_file", "inner")
        result = merge_candidatas(
            spark, entries, tables[policy["target"]],
            tables[policy["conflicts"]], policy)

        # El marcador persistente se confirma al final. Si falla antes, el
        # siguiente intento repite el lote de forma idempotente.
        confirmed = pending.select(
            "_source_file", F.col("bronze_rows").alias("bronze_row_count"),
            F.current_timestamp().alias("processed_at"))
        (DeltaTable.forName(spark, tables[policy["ledger"]]).alias("t")
         .merge(confirmed.alias("s"), "t._source_file = s._source_file")
         .whenMatchedUpdate(condition="t.bronze_row_count <> s.bronze_row_count",
                            set={"bronze_row_count": "s.bronze_row_count", "processed_at": "s.processed_at"})
         .whenNotMatchedInsertAll().execute())
        result["archivos"] = pending_count
        return result
    finally:
        pending.unpersist()


def historial_merge(spark, table, limit=10):
    """Historia real de Delta, incluidas métricas cuando el runtime las ofrece."""
    return (spark.sql(f"DESCRIBE HISTORY {table}")
            .select("version", "timestamp", "operation", "operationMetrics")
            .orderBy(F.col("version").desc()).limit(limit))
