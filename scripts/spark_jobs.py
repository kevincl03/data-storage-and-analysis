"""PySpark batch jobs for Hyperflix curated analytics.

Pandas is used by the ingestion layer for light, single-node normalization.
PySpark is used here for partitioned, parallel transformations over Parquet:
the same jobs can run locally today and on a Spark cluster later.
"""

from __future__ import annotations

import glob
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, LongType, StringType, TimestampType


DEFAULT_LAKE_ROOT = Path(os.getenv("DATA_LAKE_ROOT", "data_lake"))
SPARK_OUTPUT_ROOT = "spark_jobs"


def crear_spark_session() -> SparkSession:
    """Create a local Spark session suitable for the Windows lab environment."""
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--master local[*] pyspark-shell")
    return (
        SparkSession.builder
        .appName("Hyperflix_BigData_SparkJobs")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        # PyArrow in the existing Pandas ETL writes nanosecond timestamps.
        # Spark reads them as epoch nanoseconds, then _timestamp_column normalizes them.
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .getOrCreate()
    )


def obtener_ultimo_archivo_parquet(ruta_carpeta: str | Path) -> str | None:
    """Return the newest Parquet file for compatibility with the job specification."""
    archivos = glob.glob(str(Path(ruta_carpeta) / "**" / "*.parquet"), recursive=True)
    if not archivos:
        return None
    return max(archivos, key=os.path.getctime)


def _read_required(spark: SparkSession, path: Path, required: Iterable[str]) -> DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required Parquet path does not exist: {path}")
    frame = spark.read.parquet(str(path))
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise ValueError(f"Missing columns in {path}: {', '.join(missing)}")
    return frame


def _write(frame: DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.write.mode("overwrite").option("compression", "snappy").parquet(str(output_path))


def _timestamp_column(frame: DataFrame, column: str) -> DataFrame:
    data_type = frame.schema[column].dataType
    if isinstance(data_type, LongType):
        return frame.withColumn(column, F.timestamp_micros((F.col(column) / F.lit(1000)).cast("long")))
    if isinstance(data_type, TimestampType):
        return frame.withColumn(column, F.col(column).cast("timestamp"))
    return frame.withColumn(column, F.to_timestamp(F.col(column)))


def _column_or_null(frame: DataFrame, column: str, data_type: str = "string"):
    return F.col(column).cast(data_type) if column in frame.columns else F.lit(None).cast(data_type)


def _facts(spark: SparkSession, lake_root: Path) -> DataFrame:
    frame = _read_required(
        spark,
        lake_root / "curated" / "fact_reproducciones.parquet",
        ["event_id", "user_id", "content_id", "fecha_inicio", "duration_minutes"],
    )
    return (
        _timestamp_column(frame, "fecha_inicio")
        .withColumn("duration_minutes", F.coalesce(F.col("duration_minutes").cast("double"), F.lit(0.0)))
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("duration_minutes") >= 0)
    )


def ejecutar_job1_popularidad_contenido(spark: SparkSession, lake_root: Path) -> DataFrame:
    """Aggregate completed viewing by content, type and genre/category."""
    facts = _facts(spark, lake_root)
    content = _read_required(spark, lake_root / "curated" / "dim_contenido.parquet", ["content_id"])
    genres_type = ArrayType(StringType())
    content = (
        content
        .withColumn("generos_array", F.from_json(_column_or_null(content, "generos"), genres_type))
        .withColumn("genero", F.explode_outer(F.col("generos_array")))
        .withColumn("genero", F.coalesce(F.col("genero"), _column_or_null(content, "categoria")))
        .withColumn("content_name", F.coalesce(_column_or_null(content, "titulo"), _column_or_null(content, "nombre")))
        .select("content_id", "content_type", "content_name", "genero")
    )
    result = (
        facts.join(content, "content_id", "left")
        .groupBy("content_id", "content_type", "content_name", "genero")
        .agg(
            F.countDistinct("event_id").alias("total_reproducciones"),
            F.countDistinct("user_id").alias("usuarios_unicos"),
            F.round(F.sum("duration_minutes"), 2).alias("minutos_totales"),
            F.round(F.sum("duration_minutes") / F.lit(60), 2).alias("horas_totales"),
            F.round(F.avg("duration_minutes"), 2).alias("promedio_minutos"),
        )
        .orderBy(F.col("horas_totales").desc(), F.col("total_reproducciones").desc())
        .withColumn("fecha_procesamiento", F.current_timestamp())
    )
    output = lake_root / "curated" / SPARK_OUTPUT_ROOT / "metricas_contenido"
    _write(result, output)
    return result


def ejecutar_job2_segmentacion_usuarios(spark: SparkSession, lake_root: Path) -> DataFrame:
    """Build user engagement profiles and business-friendly consumption segments."""
    facts = _facts(spark, lake_root)
    users_path = lake_root / "curated" / "dim_usuario.parquet"
    users = _read_required(spark, users_path, ["user_id"])
    activity = (
        facts.withColumn("calendar_day", F.to_date("fecha_inicio"))
        .groupBy("user_id")
        .agg(
            F.countDistinct("event_id").alias("total_reproducciones"),
            F.countDistinct("content_id").alias("contenidos_unicos"),
            F.countDistinct("calendar_day").alias("dias_activos"),
            F.round(F.sum("duration_minutes"), 2).alias("minutos_consumidos"),
            F.round(F.sum("duration_minutes") / F.lit(60), 2).alias("horas_consumidas"),
            F.round(F.avg("duration_minutes"), 2).alias("promedio_minutos"),
            F.max("fecha_inicio").alias("ultima_fecha_reproduccion"),
        )
    )
    profile_columns = [column for column in ["user_id", "pais", "ciudad"] if column in users.columns]
    result = (
        users.select(*profile_columns)
        .join(activity, "user_id", "left")
        .fillna(0, subset=["total_reproducciones", "contenidos_unicos", "dias_activos", "minutos_consumidos", "horas_consumidas", "promedio_minutos"])
        .withColumn(
            "segmento",
            F.when(F.col("horas_consumidas") > 80, F.lit("VIP Binge Watcher"))
            .when(F.col("horas_consumidas") > 25, F.lit("Streamer Frecuente"))
            .when(F.col("horas_consumidas") > 5, F.lit("Casual Streamer"))
            .otherwise(F.lit("Usuario Ocasional")),
        )
        .withColumn("fecha_procesamiento", F.current_timestamp())
        .orderBy(F.col("horas_consumidas").desc(), F.col("user_id"))
    )
    output = lake_root / "curated" / SPARK_OUTPUT_ROOT / "segmentacion_usuarios"
    _write(result, output)
    return result


def ejecutar_job3_tendencias_temporales(spark: SparkSession, lake_root: Path) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Create consumption trends and an independent subscription payment summary."""
    facts = (
        _facts(spark, lake_root)
        .withColumn("fecha", F.to_date("fecha_inicio"))
        .withColumn("anio", F.year("fecha_inicio"))
        .withColumn("mes", F.month("fecha_inicio"))
        .withColumn("dia", F.dayofmonth("fecha_inicio"))
        .withColumn("dia_semana", F.date_format("fecha_inicio", "EEEE"))
        .withColumn("hora", F.hour("fecha_inicio"))
    )
    monthly = (
        facts.groupBy("anio", "mes")
        .agg(
            F.countDistinct("event_id").alias("total_reproducciones"),
            F.countDistinct("user_id").alias("usuarios_unicos"),
            F.countDistinct("content_id").alias("contenidos_unicos"),
            F.round(F.sum("duration_minutes"), 2).alias("minutos_totales"),
            F.round(F.sum("duration_minutes") / F.lit(60), 2).alias("horas_totales"),
            F.round(F.avg("duration_minutes"), 2).alias("promedio_minutos"),
        )
        .withColumn("fecha_procesamiento", F.current_timestamp())
        .orderBy("anio", "mes")
    )
    daily = (
        facts.groupBy("fecha", "anio", "mes", "dia_semana")
        .agg(
            F.countDistinct("event_id").alias("total_reproducciones"),
            F.countDistinct("user_id").alias("usuarios_unicos"),
            F.countDistinct("content_id").alias("contenidos_unicos"),
            F.round(F.sum("duration_minutes") / F.lit(60), 2).alias("horas_totales"),
        )
        .withColumn("fecha_procesamiento", F.current_timestamp())
        .orderBy("fecha")
    )
    payments_path = lake_root / "raw" / "pagos.parquet"
    payments = _read_required(spark, payments_path, ["usuario_id", "suscripcion_id", "fecha_pago", "monto", "moneda", "estado"])
    payments_summary = (
        _timestamp_column(payments, "fecha_pago")
        .filter(F.lower(F.col("estado")) == F.lit("completado"))
        .withColumn("anio", F.year("fecha_pago"))
        .withColumn("mes", F.month("fecha_pago"))
        .groupBy("usuario_id", "suscripcion_id", "anio", "mes", "moneda")
        .agg(
            F.count("*").alias("pagos_completados"),
            F.round(F.sum(F.col("monto").cast("double")), 2).alias("monto_total"),
            F.max("fecha_pago").alias("fecha_ultimo_pago"),
        )
        .orderBy("anio", "mes", "usuario_id")
    )
    output_root = lake_root / "curated" / SPARK_OUTPUT_ROOT
    _write(monthly, output_root / "tendencias_temporales" / "mensual")
    _write(daily, output_root / "tendencias_temporales" / "diaria")
    _write(payments_summary, output_root / "pagos_resumen" / "mensual")
    return monthly, daily, payments_summary


def ejecutar_jobs(lake_root: Path = DEFAULT_LAKE_ROOT) -> None:
    spark = crear_spark_session()
    try:
        ejecutar_job1_popularidad_contenido(spark, lake_root)
        ejecutar_job2_segmentacion_usuarios(spark, lake_root)
        ejecutar_job3_tendencias_temporales(spark, lake_root)
        print(f"Spark jobs completed successfully. Outputs: {lake_root / 'curated' / SPARK_OUTPUT_ROOT}")
    finally:
        spark.stop()


if __name__ == "__main__":
    print("=" * 58)
    print("STARTING HYPERFLIX BIG DATA ENGINE (PYSPARK)")
    print("=" * 58)
    ejecutar_jobs(DEFAULT_LAKE_ROOT)