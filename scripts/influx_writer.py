# influx_writer.py
from datetime import datetime, UTC
from typing import Optional, Dict, Any
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, WriteOptions

import os
from dotenv import load_dotenv

# --- Load environment variables from .env ---
load_dotenv()

class InfluxWriter:
    def __init__(self, on_success=None, on_error=None, on_retry=None):
        # Fetch credentials & configs from env
        url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
        token = os.getenv("INFLUXDB_TOKEN", "")
        org = os.getenv("INFLUXDB_ORG", "")
        bucket = os.getenv("INFLUXDB_BUCKET", "")
        batch_size = int(os.getenv("INFLUXDB_WRITE_BATCH_SIZE", "0"))
        flush_ms = int(os.getenv("INFLUXDB_WRITE_FLUSH_MS", "1000"))

        if not url or not token or not org or not bucket:
            raise RuntimeError(
                "Missing InfluxDB credentials in environment. "
                "Please set INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET in your .env"
            )

        self.bucket = bucket
        self.client = InfluxDBClient(url=url, token=token, org=org)

        if batch_size > 0:
            write_options = WriteOptions(
                batch_size=batch_size,
                flush_interval=flush_ms,
                jitter_interval=500,
                retry_interval=2000,
                max_retries=5,
                max_retry_delay=60000,
                exponential_base=2,
            )
            self.write_api = self.client.write_api(
                write_options=write_options,
                success_callback=on_success,
                error_callback=on_error,
                retry_callback=on_retry,
            )
        else:
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def health_check(self):
        """Write a health indicator point."""
        p = (
            Point("pipeline_health")
            .tag("check", "startup")
            .field("ok", 1)
            .time(datetime.now(UTC), WritePrecision.NS)
        )
        self.write_api.write(bucket=self.bucket, record=p)

    def write_point(
        self,
        measurement: str,
        tags: Dict[str, str],
        fields: Dict[str, Any],
        ts_utc: Optional[datetime] = None
    ):
        """Write a single InfluxDB point"""
        ts = ts_utc or datetime.now(UTC)
        p = Point(measurement)
        for k, v in (tags or {}).items():
            if v is not None:
                p = p.tag(k, str(v))
        for k, v in (fields or {}).items():
            if v is None:
                continue
            try:
                if isinstance(v, (int, float, bool)):
                    p = p.field(k, v)
                else:
                    p = p.field(k, float(v))
            except Exception:
                p = p.field(k, str(v))
        p = p.time(ts, WritePrecision.NS)
        self.write_api.write(bucket=self.bucket, record=p)

    def close(self):
        """Close client gracefully"""
        self.client.close()
