#!/usr/bin/env python3
import os, sys
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from influxdb_client import InfluxDBClient
from influx_writer import InfluxWriter
from app.monitors.health_writer import write_monitor_status

def query_latest_ts(client, bucket, measurement, minutes=5):
    flux = f'''
    from(bucket: "{bucket}")
      |> range(start: -{minutes}m)
      |> filter(fn: (r) => r._measurement == "{measurement}")
      |> keep(columns: ["_time"])
      |> sort(columns: ["_time"], desc: true)
      |> limit(n:1)
    '''
    tables = client.query_api().query(flux)
    for table in tables:
        for record in table.records:
            return record.get_time()
    return None

def main():
    load_dotenv()
    url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
    token = os.getenv("INFLUXDB_TOKEN", "")
    org = os.getenv("INFLUXDB_ORG", "")
    bucket = os.getenv("INFLUXDB_BUCKET", "")
    status = "ok"

    now = datetime.now(timezone.utc)
    lags = {}
    ok = {}

    client = InfluxDBClient(url=url, token=token, org=org)
    for meas in ["index_overview", "atm_option_quote"]:
        ts = query_latest_ts(client, bucket, meas, minutes=5)
        if ts is None:
            ok[meas] = False
            lags[meas] = None
            status = "warn"
        else:
            lag = (now - ts).total_seconds()
            lags[meas] = lag
            ok[meas] = lag <= 180.0
            if lag > 180.0:
                status = "warn"

    client.close()

    for meas in ["index_overview", "atm_option_quote"]:
        if ok[meas]:
            print(f"[OK] {meas} lag={lags[meas]:.1f}s")
        else:
            print(f"[WARN] {meas} stale/missing (lag={lags[meas]})")

    # Write health status
    writer = InfluxWriter()
    write_monitor_status(writer,
                         env=os.getenv("ENV", "local"),
                         app="health_monitor",
                         status=status,
                         lag_sec_index_overview=lags.get("index_overview"),
                         lag_sec_atm_option_quote=lags.get("atm_option_quote"),
                         errors_in_tick=0)
    writer.close()

    sys.exit(0 if status == "ok" else 1)

if __name__ == "__main__":
    main()
