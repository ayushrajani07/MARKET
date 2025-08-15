from pathlib import Path
import json
from typing import Optional, Any, Dict

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # ===== InfluxDB connection =====
    influxdb_url: str = Field(default="http://localhost:8086", alias="INFLUXDB_URL")
    influxdb_token: str = Field(default="", alias="INFLUXDB_TOKEN")
    influxdb_org: str = Field(default="your-org", alias="INFLUXDB_ORG")
    influxdb_bucket: str = Field(default="your-bucket", alias="INFLUXDB_BUCKET")

    # ===== Config paths =====
    schema_config_path: str = Field(default="config/schema_config.json", alias="SCHEMA_CONFIG_PATH")

    # ===== Logging =====
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    # ===== Backup =====
    backup_influx_token: Optional[str] = Field(default=None, alias="BACKUP_INFLUX_TOKEN")
    backup_dir: str = Field(default="backups", alias="BACKUP_DIR")

    # ===== Pydantic model config =====
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"  # Allow other env vars that aren't explicitly defined
    )

    # --- Load full schema config (JSON or YAML in the future) ---
    def load_schema_config(self) -> Dict[str, Any]:
        cfg_path = Path(self.schema_config_path)
        if not cfg_path.exists():
            raise FileNotFoundError(f"Schema config not found: {cfg_path}")
        if cfg_path.suffix.lower() in [".yaml", ".yml"]:
            import yaml
            with cfg_path.open("r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        else:
            with cfg_path.open("r", encoding="utf-8") as f:
                return json.load(f)

    # --- Shortcut property for collector schedule ---
    @property
    def collector_schedule(self) -> Dict[str, Any]:
        return self.load_schema_config()["app"]["collector_schedule"]


# Create a singleton settings instance for global import
settings = Settings()
