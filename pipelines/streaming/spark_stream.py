from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, when
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from src.config import settings
from src.logging_utils import setup_logging


LOGGER = logging.getLogger("streaming")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("oilgas-telemetry-stream")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def telemetry_schema() -> StructType:
    return StructType(
        [
            StructField("asset_id", StringType(), False),
            StructField("event_ts", StringType(), False),
            StructField("temperature_c", DoubleType(), True),
            StructField("vibration_mm_s", DoubleType(), True),
            StructField("pressure_kpa", DoubleType(), True),
            StructField("production_rate_bbl_hr", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("unit_temperature", StringType(), True),
            StructField("unit_pressure", StringType(), True),
        ]
    )


def silver_transform(bronze_df: DataFrame) -> DataFrame:
    cleaned = (
        bronze_df.withColumn("event_ts", to_timestamp(col("event_ts")))
        .withColumn("temperature_c", when(col("temperature_c").isNull(), 0.0).otherwise(col("temperature_c")))
        .withColumn("pressure_kpa", when(col("pressure_kpa") < 0, None).otherwise(col("pressure_kpa")))
        .dropDuplicates(["asset_id", "event_ts"])
    )
    return cleaned


def run_stream() -> None:
    setup_logging(settings.log_level)
    spark = build_spark()

    schema = telemetry_schema()
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    bronze_query = (
        parsed.writeStream.format("parquet")
        .option("path", str(settings.bronze_dir))
        .option("checkpointLocation", str(settings.checkpoint_dir / "bronze"))
        .outputMode("append")
        .start()
    )

    silver_df = silver_transform(parsed)
    silver_query = (
        silver_df.writeStream.format("parquet")
        .option("path", str(settings.silver_dir))
        .option("checkpointLocation", str(settings.checkpoint_dir / "silver"))
        .outputMode("append")
        .start()
    )

    LOGGER.info("Streaming started. Writing bronze=%s silver=%s", settings.bronze_dir, settings.silver_dir)
    spark.streams.awaitAnyTermination()
    bronze_query.stop()
    silver_query.stop()


if __name__ == "__main__":
    run_stream()

