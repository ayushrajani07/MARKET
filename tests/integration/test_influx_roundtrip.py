from datetime import datetime, UTC
from influxdb_client import InfluxDBClient, Point, WritePrecision
import os

def test_roundtrip():
    url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
    token = os.getenv("INFLUXDB_TOKEN", "dev-token-please-change")
    org = os.getenv("INFLUXDB_ORG", "your-org")
    bucket = os.getenv("INFLUXDB_BUCKET", "market_data")

    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api()
    p = Point("debug_test").tag("suite", "integration").field("value", 1.0).time(datetime.now(UTC), WritePrecision.NS)
    write_api.write(bucket=bucket, record=p)

    q = client.query_api()
    flux = f'''
    from(bucket: "{bucket}")
      |> range(start: -5m)
      |> filter(fn: (r) => r._measurement == "debug_test")
      |> limit(n:1)
    '''
    tables = q.query(flux)
    found = any(True for _ in tables)
    assert found
