import streamlit as st
from influxdb_client import InfluxDBClient
from app.config.settings import settings

st.set_page_config(page_title="Market Monitor", layout="wide")
st.title("Market Monitor (IST view)")

client = InfluxDBClient(url=settings.influxdb_url, token=settings.influxdb_token, org=settings.influx_org)
query_api = client.query_api()

# Flux query to render in IST
flux = f"""
import "timezone"
option location = timezone.location(name: "Asia/Kolkata")

from(bucket: "{settings.influx_bucket}")
  |> range(start: -30m)
  |> filter(fn: (r) => r._measurement == "index_overview")
  |> keep(columns: ["_time","_value","_field","index"])
  |> sort(columns: ["_time"])
"""

try:
    tables = query_api.query(flux)
    rows = []
    for table in tables:
        for rec in table.records:
            rows.append({
                "time_IST": rec.get_time(),
                "index": rec.values.get("index"),
                "field": rec.get_field(),
                "value": rec.get_value(),
            })
    st.dataframe(rows, use_container_width=True)
except Exception as e:
    st.error(f"Query failed: {e}")
