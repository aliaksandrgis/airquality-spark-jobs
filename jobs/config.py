from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, List


def _getenv(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default or "")
    return value.strip()


@dataclass
class JobConfig:
    """Runtime configuration for the streaming job."""

    kafka_bootstrap_servers: str
    kafka_topic: str = "airquality.raw"
    kafka_starting_offsets: str = "latest"
    kafka_fail_on_data_loss: bool = False
    kafka_security_protocol: str = ""
    kafka_sasl_mechanism: str = ""
    kafka_sasl_username: str = ""
    kafka_sasl_password: str = ""
    kafka_sasl_jaas_config: str = ""
    kafka_ssl_endpoint_identification_algorithm: str = ""
    kafka_max_offsets_per_trigger: int | None = None

    bronze_path: str = ""
    bronze_checkpoint_path: str = ""
    bronze_trigger_seconds: int = 60
    bronze_partition_columns: List[str] = field(
        default_factory=lambda: ["event_date", "country"]
    )

    supabase_checkpoint_path: str = ""
    supabase_jdbc_url: str = ""
    supabase_user: str = ""
    supabase_password: str = ""
    supabase_table: str = "public.measurements_curated"
    supabase_batch_size: int = 5000

    dedup_watermark_minutes: int = 120

    r2_endpoint_url: str = ""
    r2_access_key_id: str = ""
    r2_secret_access_key: str = ""
    r2_region: str = "auto"

    def kafka_options(self) -> Dict[str, str]:
        """Build Spark readStream options for Kafka."""
        opts: Dict[str, str] = {
            "kafka.bootstrap.servers": self.kafka_bootstrap_servers,
            "subscribe": self.kafka_topic,
            "startingOffsets": self.kafka_starting_offsets,
            "failOnDataLoss": str(self.kafka_fail_on_data_loss).lower(),
        }
        if self.kafka_security_protocol:
            opts["kafka.security.protocol"] = self.kafka_security_protocol
        if self.kafka_sasl_mechanism:
            opts["kafka.sasl.mechanism"] = self.kafka_sasl_mechanism
        jaas = self.kafka_sasl_jaas_config
        if not jaas and self.kafka_sasl_username and self.kafka_sasl_password:
            jaas = (
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                f'username="{self.kafka_sasl_username}" '
                f'password="{self.kafka_sasl_password}";'
            )
        if jaas:
            opts["kafka.sasl.jaas.config"] = jaas
        if self.kafka_ssl_endpoint_identification_algorithm:
            opts["kafka.ssl.endpoint.identification.algorithm"] = (
                self.kafka_ssl_endpoint_identification_algorithm
            )
        if self.kafka_max_offsets_per_trigger is not None:
            opts["maxOffsetsPerTrigger"] = str(self.kafka_max_offsets_per_trigger)
        return opts


def load_job_config() -> JobConfig:
    """Load configuration from environment variables and validate required fields."""

    cfg = JobConfig(
        kafka_bootstrap_servers=_getenv("KAFKA_BOOTSTRAP") or _getenv(
            "KAFKA_BOOTSTRAP_SERVERS"
        ),
        kafka_topic=_getenv("KAFKA_TOPIC", "airquality.raw") or "airquality.raw",
        kafka_starting_offsets=_getenv("KAFKA_STARTING_OFFSETS", "latest") or "latest",
        kafka_fail_on_data_loss=_getenv("KAFKA_FAIL_ON_DATA_LOSS", "false")
        .lower()
        .strip()
        == "true",
        kafka_security_protocol=_getenv("KAFKA_SECURITY_PROTOCOL", ""),
        kafka_sasl_mechanism=_getenv("KAFKA_SASL_MECHANISM", ""),
        kafka_sasl_username=_getenv("KAFKA_SASL_USERNAME", ""),
        kafka_sasl_password=_getenv("KAFKA_SASL_PASSWORD", ""),
        kafka_sasl_jaas_config=_getenv("KAFKA_SASL_JAAS_CONFIG", ""),
        kafka_ssl_endpoint_identification_algorithm=_getenv(
            "KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", ""
        ),
        kafka_max_offsets_per_trigger=_parse_int("KAFKA_MAX_OFFSETS_PER_TRIGGER"),
        bronze_path=_getenv("BRONZE_PATH"),
        bronze_checkpoint_path=_getenv("BRONZE_CHECKPOINT_PATH"),
        bronze_trigger_seconds=_parse_int("BRONZE_TRIGGER_SECONDS", default=60),
        bronze_partition_columns=_parse_list(
            "BRONZE_PARTITION_COLUMNS", ["event_date", "country"]
        ),
        supabase_checkpoint_path=_getenv("SUPABASE_CHECKPOINT_PATH"),
        supabase_jdbc_url=_getenv("SUPABASE_JDBC_URL"),
        supabase_user=_getenv("SUPABASE_USER"),
        supabase_password=_getenv("SUPABASE_PASSWORD"),
        supabase_table=_getenv("SUPABASE_TABLE", "public.measurements_curated")
        or "public.measurements_curated",
        supabase_batch_size=_parse_int("SUPABASE_BATCH_SIZE", default=5000),
        dedup_watermark_minutes=_parse_int("DEDUP_WATERMARK_MINUTES", default=120),
        r2_endpoint_url=_getenv("R2_ENDPOINT_URL"),
        r2_access_key_id=_getenv("R2_ACCESS_KEY_ID"),
        r2_secret_access_key=_getenv("R2_SECRET_ACCESS_KEY"),
        r2_region=_getenv("R2_REGION", "auto") or "auto",
    )

    missing = [
        name
        for name, value in [
            ("KAFKA_BOOTSTRAP", cfg.kafka_bootstrap_servers),
            ("BRONZE_PATH", cfg.bronze_path),
            ("BRONZE_CHECKPOINT_PATH", cfg.bronze_checkpoint_path),
            ("SUPABASE_CHECKPOINT_PATH", cfg.supabase_checkpoint_path),
            ("SUPABASE_JDBC_URL", cfg.supabase_jdbc_url),
            ("SUPABASE_USER", cfg.supabase_user),
            ("SUPABASE_PASSWORD", cfg.supabase_password),
        ]
        if not value
    ]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    return cfg


def _parse_int(name: str, default: int | None = None) -> int | None:
    raw = _getenv(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _parse_list(name: str, default: List[str]) -> List[str]:
    raw = _getenv(name)
    if not raw:
        return list(default)
    return [item.strip() for item in raw.split(",") if item.strip()]
