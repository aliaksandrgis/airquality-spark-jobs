from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from jobs.schemas import MEASUREMENT_SCHEMA
from jobs.transformations import clean_measurements, deduplicate_measurements


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("airquality-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()


def test_clean_measurements_filters_invalid_and_normalizes(spark: SparkSession) -> None:
    rows = [
        (
            "DE001",
            "NO2",
            12.5,
            "UG/M3",
            "de",
            "berlin",
            "Alexanderplatz",
            52.5,
            13.4,
            "2024-01-01T00:00:00+00:00",
            "DE",
        ),
        (
            "",
            "pm10",
            10.0,
            "ug/m3",
            "de",
            "berlin",
            None,
            52.5,
            13.4,
            "2024-01-01T00:00:00+00:00",
            "DE",
        ),
        (
            "PL001",
            "pm25",
            -5.0,
            "ug/m3",
            "pl",
            "warsaw",
            None,
            51.0,
            21.0,
            "2024-01-01T00:00:00+00:00",
            "PL",
        ),
    ]
    df = spark.createDataFrame(rows, schema=MEASUREMENT_SCHEMA)

    cleaned = clean_measurements(df).collect()
    assert len(cleaned) == 1
    row = cleaned[0]
    assert row.station_id == "DE001"
    assert row.pollutant == "no2"
    assert row.country == "DE"
    assert row.city == "Berlin"
    assert row.location_name == "Alexanderplatz"
    assert row.event_date is not None


def test_deduplicate_measurements_drops_duplicates(spark: SparkSession) -> None:
    raw_rows = [
        (
            "DE001",
            "no2",
            15.0,
            "ug/m3",
            "DE",
            "berlin",
            "Berlin",
            52.5,
            13.4,
            "2024-01-01T00:00:00+00:00",
            "de",
        ),
        (
            "DE001",
            "pm10",
            20.0,
            "ug/m3",
            "DE",
            "berlin",
            "Berlin",
            52.5,
            13.4,
            "2024-01-01T00:10:00+00:00",
            "de",
        ),
    ]
    clean_df = clean_measurements(
        spark.createDataFrame(raw_rows, schema=MEASUREMENT_SCHEMA)
    )
    duplicated = clean_df.union(clean_df.filter(clean_df.pollutant == "no2"))
    deduped = deduplicate_measurements(duplicated, watermark_minutes=60)
    assert deduped.count() == 2
