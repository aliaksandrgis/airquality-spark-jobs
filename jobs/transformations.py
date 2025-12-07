from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DoubleType, StringType

from .schemas import MEASUREMENT_SCHEMA


def parse_kafka_value(df: DataFrame) -> DataFrame:
    """Extract JSON payload from Kafka records."""
    return (
        df.selectExpr("CAST(value AS STRING) AS value_str")
        .withColumn("payload", F.from_json("value_str", MEASUREMENT_SCHEMA))
        .select("payload.*")
    )


def clean_measurements(df: DataFrame) -> DataFrame:
    """Normalize measurements dataframe and add ingestion metadata."""

    ts_with_timezone = F.regexp_replace(F.col("timestamp"), "Z$", "+00:00")
    station_col = F.trim(F.col("station_id"))
    pollutant_col = F.trim(F.col("pollutant"))
    unit_col = F.trim(F.col("unit"))

    normalized = (
        df.withColumn(
            "station_id",
            F.when(F.length(station_col) == 0, None)
            .otherwise(station_col)
            .cast(StringType()),
        )
        .withColumn(
            "pollutant",
            F.when(F.length(pollutant_col) == 0, None).otherwise(pollutant_col),
        )
        .withColumn("pollutant", F.lower(F.col("pollutant")))
        .withColumn(
            "unit",
            F.lower(
                F.when(F.length(unit_col) == 0, None).otherwise(unit_col),
            ),
        )
        .withColumn("country", F.upper(F.trim(F.col("country"))))
        .withColumn("city", F.initcap(F.col("city")))
        .withColumn(
            "location_name",
            F.coalesce(
                F.col("location_name"),
                F.col("city"),
                F.col("station_id"),
            ),
        )
        .withColumn(
            "observed_at",
            F.to_timestamp(ts_with_timezone),
        )
        .withColumn("value", F.col("value").cast(DoubleType()))
        .withColumn("lat", F.col("lat").cast(DoubleType()))
        .withColumn("lon", F.col("lon").cast(DoubleType()))
        .withColumn("source", F.lower(F.col("source")))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("event_date", F.to_date("observed_at"))
    )

    cleaned = normalized.filter(
        F.col("station_id").isNotNull()
        & F.col("pollutant").isNotNull()
        & F.col("observed_at").isNotNull()
        & F.col("value").isNotNull()
    )
    quality_filtered = cleaned.filter(F.col("value") >= 0)

    ordered_cols: List[str] = [
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
        "event_date",
    ]
    return quality_filtered.select(*ordered_cols)


def deduplicate_measurements(df: DataFrame, watermark_minutes: int) -> DataFrame:
    """Drop duplicates using watermark on observed_at."""
    minutes = max(1, watermark_minutes)
    return (
        df.withWatermark("observed_at", f"{minutes} minutes")
        .dropDuplicates(["station_id", "pollutant", "observed_at"])
    )
