# AirQuality Spark Jobs

Databricks/Spark Structured Streaming jobs that ingest air-quality measurements from Kafka, land raw/validated data in Cloudflare R2 (S3 compatible) and upsert curated tables into Supabase Postgres/PostGIS.

## Layout
- jobs/ - Python package with the streaming driver (live_measurements.py), configuration loader and transformation helpers.
- configs/ - sample Databricks Jobs API payloads (JSON) for deploying to the hosted workspace.
- 	ests/ - PySpark unit tests for transformations (pytest).

## Live measurements streaming job
jobs/live_measurements.py performs the full ingestion loop:

1. Reads JSON payloads from Kafka (irquality.raw topic) with optional SASL/SASL_SSL credentials (Confluent Cloud ready).
2. Parses and validates each record (station_id, pollutant, alue, coordinates, timestamp, etc.), normalising casing and dropping negative/NULL values.
3. Writes every valid row to Cloudflare R2/MinIO as partitioned Parquet (event_date, country) - this is the bronze/raw history, suitable for replay.
4. Applies a watermark-based deduplication by (station_id, pollutant, observed_at) and uses oreachBatch to upsert curated data into Supabase over JDBC (PostgreSQL driver).

Two streaming queries are started (bronze + Supabase) and share the same parsed dataset, so bronze always contains the superset of curated records.

## Configuration
All settings are read from environment variables; Databricks secrets/environment configs map 1:1.

### Kafka / Confluent
- KAFKA_BOOTSTRAP or KAFKA_BOOTSTRAP_SERVERS - comma-separated brokers (required).
- KAFKA_TOPIC - topic name (default irquality.raw).
- KAFKA_STARTING_OFFSETS - earliest or latest (default latest).
- KAFKA_FAIL_ON_DATA_LOSS - 	rue/alse (default alse).
- KAFKA_SECURITY_PROTOCOL, KAFKA_SASL_MECHANISM, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD, KAFKA_SASL_JAAS_CONFIG - optional SASL/SASL_SSL configuration for Confluent Cloud.
- KAFKA_MAX_OFFSETS_PER_TRIGGER - limit records per micro-batch (optional).

### Cloudflare R2 / bronze lake
- BRONZE_PATH - target Parquet path, e.g. s3a://air-quality-bronze/measurements (required).
- BRONZE_CHECKPOINT_PATH - checkpoint path for bronze query (required).
- BRONZE_TRIGGER_SECONDS - processing interval (default 60).
- BRONZE_PARTITION_COLUMNS - comma-separated list, default event_date,country.
- R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_REGION - optional overrides for Cloudflare R2/MinIO S3-compatible storage. When unset the job relies on cluster-wide credentials (e.g. Databricks secrets-backed instance profile).

### Supabase / curated table
- SUPABASE_JDBC_URL - e.g. jdbc:postgresql://db.supabase.co:5432/postgres?sslmode=require (required).
- SUPABASE_USER, SUPABASE_PASSWORD - service role or PgBouncer credentials (required).
- SUPABASE_TABLE - target table (default public.measurements_curated).
- SUPABASE_CHECKPOINT_PATH - checkpoint directory for the oreachBatch sink (required).
- SUPABASE_BATCH_SIZE - JDBC batch size (default 5000).

### Data quality / dedupe
- DEDUP_WATERMARK_MINUTES - watermark horizon for duplicate detection (default 120 minutes).

## Local development
`ash
python -m venv .venv && source .venv/bin/activate  # .venv\Scripts\activate on Windows
pip install -r requirements.txt

# Run unit tests
pytest

# Example: run the streaming job against local Kafka/MinIO/Postgres
export KAFKA_BOOTSTRAP=localhost:29092
export BRONZE_PATH=file:///tmp/bronze/measurements
export BRONZE_CHECKPOINT_PATH=file:///tmp/bronze/_checkpoint
export SUPABASE_CHECKPOINT_PATH=file:///tmp/bronze/_supabase_checkpoint
export SUPABASE_JDBC_URL=jdbc:postgresql://localhost:5432/airquality?sslmode=disable
export SUPABASE_USER=airuser
export SUPABASE_PASSWORD=airpassword

spark-submit \
  --packages org.postgresql:postgresql:42.7.1 \
  jobs/live_measurements.py
`

## Deploying on Databricks
1. Upload the package (jobs/) to DBFS (e.g. dbfs:/FileStore/airquality/jobs or create a wheel).
2. Create a job using configs/live_measurements_job.json as a starting payload (fill cluster ID, libraries, and paths).
3. Store Kafka/Supabase/R2 secrets inside Databricks secret scopes and wire them via job environment variables.
4. Add org.postgresql:postgresql:42.7.1 and org.apache.hadoop:hadoop-aws:3.3.4 as cluster libraries when targeting Cloudflare R2.

## Docker image
Для публичного GitHub репозитория добавлен Dockerfile, который сворачивает готовое окружение Spark 3.5.0:

```bash
docker build -t airquality-spark-job .

docker run --rm \
  -e KAFKA_BOOTSTRAP=pkc-xxxx.us-east-1.aws.confluent.cloud:9092 \
  -e KAFKA_SECURITY_PROTOCOL=SASL_SSL \
  -e KAFKA_SASL_MECHANISM=PLAIN \
  -e KAFKA_SASL_USERNAME=... \
  -e KAFKA_SASL_PASSWORD=... \
  -e SUPABASE_JDBC_URL=jdbc:postgresql://... \
  -e SUPABASE_USER=... \
  -e SUPABASE_PASSWORD=... \
  -e BRONZE_PATH=s3a://air-quality-bronze/measurements \
  -e BRONZE_CHECKPOINT_PATH=s3a://air-quality-bronze/_chk/bronze \
  -e SUPABASE_CHECKPOINT_PATH=s3a://air-quality-bronze/_chk/supabase \
  -e R2_ENDPOINT_URL=https://<account>.r2.cloudflarestorage.com \
  -e R2_ACCESS_KEY_ID=... \
  -e R2_SECRET_ACCESS_KEY=... \
  airquality-spark-job
```

EntryPoint уже вызывает `spark-submit --packages org.postgresql:postgresql:42.7.1,org.apache.hadoop:hadoop-aws:3.3.4 jobs/live_measurements.py`, поэтому достаточно передать переменные окружения (можно переопределить `CMD`, если нужны дополнительные аргументы).

### Supabase table hint
`sql
CREATE TABLE IF NOT EXISTS public.measurements_curated (
    id bigint generated always as identity primary key,
    station_id text not null,
    pollutant text not null,
    value double precision not null,
    unit text,
    country text,
    city text,
    location_name text,
    lat double precision,
    lon double precision,
    observed_at timestamptz not null,
    source text,
    ingested_at timestamptz default now(),
    unique (station_id, pollutant, observed_at)
);
`

## CI
The GitHub Actions workflow lints and runs pytest on every push/PR to main. Add Databricks deployment steps only after secrets are available.

