# AirQuality Spark Jobs

Spark Structured Streaming job that reads from Kafka and writes to bronze parquet and Supabase.

## Runtime
- Input: Kafka topic airquality.raw
- Bronze: parquet at BRONZE_PATH
- Curated: Supabase table public.measurements_curated

## Environment
KAFKA_*, SUPABASE_*, BRONZE_*, DEDUP_WATERMARK_MINUTES.

## Docker
```bash
docker build -t airquality-spark-jobs .
docker run --env-file .env airquality-spark-jobs
```
