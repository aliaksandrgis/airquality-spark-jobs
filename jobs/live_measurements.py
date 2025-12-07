from __future__ import annotations

import logging
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from jobs.config import JobConfig, load_job_config
from jobs.transformations import (
    clean_measurements,
    deduplicate_measurements,
    parse_kafka_value,
)

LOGGER = logging.getLogger("airquality.spark")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

SUPABASE_COLUMNS: Iterable[str] = [
    "station_id",
    "pollutant",
    "value",
    "unit",
    "country",
    "city",
    "location_name",
    "lat",
    "lon",
    "observed_at",
    "source",
    "ingested_at",
]


def build_spark(app_name: str = "airquality-live-measurements") -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    return spark


def configure_r2_access(spark: SparkSession, config: JobConfig) -> None:
    if not config.r2_endpoint_url:
        LOGGER.info("R2 endpoint not configured; skipping S3A overrides")
        return

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", config.r2_endpoint_url)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    if config.r2_region:
        hadoop_conf.set("fs.s3a.endpoint.region", config.r2_region)
    if config.r2_access_key_id and config.r2_secret_access_key:
        hadoop_conf.set("fs.s3a.access.key", config.r2_access_key_id)
        hadoop_conf.set("fs.s3a.secret.key", config.r2_secret_access_key)
        hadoop_conf.set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    LOGGER.info("Configured Cloudflare R2 endpoint for S3A access")


def start_bronze_sink(df: DataFrame, config: JobConfig) -> StreamingQuery:
    writer = (
        df.writeStream.format("parquet")
        .option("path", config.bronze_path)
        .option("checkpointLocation", config.bronze_checkpoint_path)
        .outputMode("append")
    )
    if config.bronze_partition_columns:
        writer = writer.partitionBy(*config.bronze_partition_columns)
    if config.bronze_trigger_seconds:
        writer = writer.trigger(processingTime=f"{config.bronze_trigger_seconds} seconds")
    LOGGER.info("Starting bronze sink at %s", config.bronze_path)
    return writer.start()


def start_supabase_sink(df: DataFrame, config: JobConfig) -> StreamingQuery:
    LOGGER.info(
        "Starting Supabase sink to table %s using checkpoint %s",
        config.supabase_table,
        config.supabase_checkpoint_path,
    )
    return (
        df.writeStream.outputMode("append")
        .foreachBatch(lambda batch_df, batch_id: _write_batch_to_supabase(batch_df, config, batch_id))
        .option("checkpointLocation", config.supabase_checkpoint_path)
        .start()
    )


def _write_batch_to_supabase(batch_df: DataFrame, config: JobConfig, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        LOGGER.debug("Batch %s empty; skipping Supabase write", batch_id)
        return
    rows = batch_df.count()
    LOGGER.info("Writing %s curated rows to Supabase for batch %s", rows, batch_id)
    (
        batch_df.select(*SUPABASE_COLUMNS)
        .write.format("jdbc")
        .option("url", config.supabase_jdbc_url)
        .option("dbtable", config.supabase_table)
        .option("user", config.supabase_user)
        .option("password", config.supabase_password)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", str(config.supabase_batch_size))
        .mode("append")
        .save()
    )


def run() -> None:
    config = load_job_config()
    spark = build_spark()
    configure_r2_access(spark, config)

    kafka_df = (
        spark.readStream.format("kafka")
        .options(**config.kafka_options())
        .load()
    )

    parsed_df = parse_kafka_value(kafka_df)
    clean_df = clean_measurements(parsed_df)
    curated_df = deduplicate_measurements(clean_df, config.dedup_watermark_minutes)

    bronze_query = start_bronze_sink(clean_df, config)
    supabase_query = start_supabase_sink(curated_df, config)

    LOGGER.info(
        "Streaming queries started (bronze id=%s, supabase id=%s)",
        bronze_query.id,
        supabase_query.id,
    )
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
