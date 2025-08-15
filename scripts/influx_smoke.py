#!/usr/bin/env python3
import os
import sys
from datetime import datetime, UTC
from pathlib import Path

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influx_writer import InfluxWriter
from app.utils.time_utils import rounded_half_minute

def query_last_points(client, bucket, measurement, minutes=5, limit=3):
    flux = f'''
    from(bucket: "{bucket}")
      |> range(start: -{minutes}m)
      |> filter(fn: (r) => r._measurement == "{measurement}")
      |> sort(columns: ["_time"], desc: true)
      |> limit(n:{limit})
    '''
    tables = client.query_api().query(flux)
    out = []
    for table in tables:
        for record in table.records:
            t = record.get_time()
            val = record.get_value()
            # pull non-internal values
            tags = {k: v for k, v in record.values.items() if not k.startswith("_")}
            out.append((t, val, tags))
    return out

def main():
    env_path = Path(".env")
    if env_path.exists():
        load_dotenv(env_path)
    else:
        load_dotenv()

    url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
    token = os.getenv("INFLUXDB_TOKEN", "")
    org = os.getenv("INFLUXDB_ORG", "")
    bucket = os.getenv("INFLUXDB_BUCKET", "")

    print(f"[INFO] Testing Influx connection to bucket: {bucket}")

    # Write a rounded debug point using your existing writer (respects your env config)
    writer = InfluxWriter()
    try:
        now_iso = rounded_half_minute(datetime.now(UTC))
        ts = datetime.fromisoformat(now_iso)
        p = Point("debug_test") \
            .tag("suite", "smoke") \
            .tag("env", os.getenv("ENV", "local")) \
            .field("value", 42.0) \
            .field("note", "smoke_test") \
            .time(ts, WritePrecision.NS)
        writer.write_api.write(bucket=writer.bucket, record=p)
        # If batching is enabled, flush; if sync mode, this is no-op
        try:
            writer.write_api.flush()
        except Exception:
            pass
        print(f"[OK] Wrote debug_test point at {now_iso}")
    finally:
        try:
            writer.close()
        except Exception:
            pass

    # Use a direct client for queries
    client = InfluxDBClient(url=url, token=token, org=org)
    try:
        # Show last debug_test points
        recs = query_last_points(client, bucket, "debug_test", minutes=10, limit=5)
        if recs:
            print("[INFO] Last debug_test points:")
            for t, val, tags in recs:
                print(f"  {t}  value={val}, tags={tags}")
        else:
            print("[WARN] No debug_test points found in last 10m!")

        # Check live measurements
        for meas in ["index_overview", "atm_option_quote"]:
            pts = query_last_points(client, bucket, meas, minutes=5, limit=5)
            if pts:
                latest_time = pts[0][0]
                print(f"[OK] {meas}: Found {len(pts)} points in last 5m (latest {latest_time})")
            else:
                print(f"[WARN] {meas}: No points in last 5m — feed may be stale!")
    finally:
        try:
            client.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
