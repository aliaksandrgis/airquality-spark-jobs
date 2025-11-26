# AirQuality Spark Jobs

Databricks/Spark Structured Streaming jobs: read from Kafka (Confluent), validate/enrich, write Parquet bronze (R2/S3) and curated tables to Supabase Postgres/PostGIS.

## Layout
- `jobs/` — notebooks or Python packages for streaming/batch.
- `configs/` — job JSON/YAML for Databricks.
- `tests/` — lightweight unit tests for transformations.

## Development
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pytest
```

## Deployment
- Package job (wheel/notebook) and submit via Databricks Jobs API.
- Configure Kafka SASL/SSL and Supabase creds via Databricks secrets scopes.
- Output paths: Parquet bronze in R2/S3; JDBC to Supabase for curated tables.

## CI (template)
- Lint with `ruff`, run unit tests.
- Optional: build wheel artifact.
- No cloud submission unless secrets are present.
