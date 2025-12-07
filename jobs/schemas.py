from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

MEASUREMENT_SCHEMA = StructType(
    [
        StructField("station_id", StringType(), True),
        StructField("pollutant", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("location_name", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("source", StringType(), True),
    ]
)
