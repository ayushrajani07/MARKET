from datetime import datetime, timezone
from influxdb_client import Point, WritePrecision
import socket

def _now():
    return datetime.now(timezone.utc)

def _host():
    try:
        return socket.gethostname()
    except Exception:
        return "unknown-host"

def write_monitor_status(writer,
                        env="prod",
                        app="logger",
                        status="ok",
                        loop_duration_ms=None,
                        legs_count=None,
                        overview_count=None,
                        lag_sec_index_overview=None,
                        lag_sec_atm_option_quote=None,
                        errors_in_tick=None,
                        kite_429_count=None,
                        kite_error_count=None,
                        influx_points_attempted=None,
                        influx_points_written=None,
                        influx_points_rejected=None,
                        json_write_errors=None):
    ts = _now()
    p = Point("monitor_status") \
        .tag("env", env) \
        .tag("host", _host()) \
        .tag("app", app) \
        .tag("status", status) \
        .field("loop_duration_ms", float(loop_duration_ms) if loop_duration_ms is not None else None) \
        .field("legs_count", int(legs_count) if legs_count is not None else None) \
        .field("overview_count", int(overview_count) if overview_count is not None else None) \
        .field("lag_sec_index_overview", float(lag_sec_index_overview) if lag_sec_index_overview is not None else None) \
        .field("lag_sec_atm_option_quote", float(lag_sec_atm_option_quote) if lag_sec_atm_option_quote is not None else None) \
        .field("errors_in_tick", int(errors_in_tick) if errors_in_tick is not None else None) \
        .field("kite_429_count", int(kite_429_count) if kite_429_count is not None else None) \
        .field("kite_error_count", int(kite_error_count) if kite_error_count is not None else None) \
        .field("influx_points_attempted", int(influx_points_attempted) if influx_points_attempted is not None else None) \
        .field("influx_points_written", int(influx_points_written) if influx_points_written is not None else None) \
        .field("influx_points_rejected", int(influx_points_rejected) if influx_points_rejected is not None else None) \
        .field("json_write_errors", int(json_write_errors) if json_write_errors is not None else None) \
        .time(ts, WritePrecision.NS)
    writer.write_api.write(bucket=writer.bucket, record=p)

def write_pipeline_tick(writer,
                        env="prod",
                        app="logger",
                        loop_duration_ms=None,
                        atm_collect_ms=None,
                        overview_collect_ms=None,
                        kite_quote_calls=None,
                        kite_batch_size_avg=None,
                        records_written_index_overview=None,
                        records_written_atm_legs=None,
                        json_files_written=None,
                        influx_points_written=None,
                        retry_count=None,
                        backoff_applied=None):
    ts = _now()
    p = Point("pipeline_tick") \
        .tag("env", env) \
        .tag("host", _host()) \
        .tag("app", app) \
        .field("loop_duration_ms", float(loop_duration_ms) if loop_duration_ms is not None else None) \
        .field("atm_collect_ms", float(atm_collect_ms) if atm_collect_ms is not None else None) \
        .field("overview_collect_ms", float(overview_collect_ms) if overview_collect_ms is not None else None) \
        .field("kite_quote_calls", int(kite_quote_calls) if kite_quote_calls is not None else None) \
        .field("kite_batch_size_avg", float(kite_batch_size_avg) if kite_batch_size_avg is not None else None) \
        .field("records_written_index_overview", int(records_written_index_overview) if records_written_index_overview is not None else None) \
        .field("records_written_atm_legs", int(records_written_atm_legs) if records_written_atm_legs is not None else None) \
        .field("json_files_written", int(json_files_written) if json_files_written is not None else None) \
        .field("influx_points_written", int(influx_points_written) if influx_points_written is not None else None) \
        .field("retry_count", int(retry_count) if retry_count is not None else None) \
        .field("backoff_applied", int(1 if backoff_applied else 0) if backoff_applied is not None else None) \
        .time(ts, WritePrecision.NS)
    writer.write_api.write(bucket=writer.bucket, record=p)

def write_influx_write_stats(writer,
                             env="prod",
                             measurement="unknown",
                             points_attempted=0,
                             points_written=0,
                             points_rejected=0,
                             flush_latency_ms=None,
                             batch_queue_depth=None):
    ts = _now()
    p = Point("influx_write_stats") \
        .tag("env", env) \
        .tag("host", _host()) \
        .tag("measurement", measurement) \
        .field("points_attempted", int(points_attempted)) \
        .field("points_written", int(points_written)) \
        .field("points_rejected", int(points_rejected)) \
        .field("flush_latency_ms", float(flush_latency_ms) if flush_latency_ms is not None else None) \
        .field("batch_queue_depth", int(batch_queue_depth) if batch_queue_depth is not None else None) \
        .time(ts, WritePrecision.NS)
    writer.write_api.write(bucket=writer.bucket, record=p)

def write_broker_health(writer,
                        env="prod",
                        api="kite",
                        quote_latency_ms=None,
                        http_429_count=None,
                        error_rate_percent=None,
                        tokens_refresh_count=None):
    ts = _now()
    p = Point("broker_health") \
        .tag("env", env) \
        .tag("host", _host()) \
        .tag("api", api) \
        .field("quote_latency_ms", float(quote_latency_ms) if quote_latency_ms is not None else None) \
        .field("http_429_count", int(http_429_count) if http_429_count is not None else None) \
        .field("error_rate_percent", float(error_rate_percent) if error_rate_percent is not None else None) \
        .field("tokens_refresh_count", int(tokens_refresh_count) if tokens_refresh_count is not None else None) \
        .time(ts, WritePrecision.NS)
    writer.write_api.write(bucket=writer.bucket, record=p)

def write_latency_metric(writer,
                         env="prod",
                         stream="index_overview",
                         ingest_delay_sec=None,
                         write_delay_ms=None,
                         end_to_end_latency_sec=None):
    ts = _now()
    p = Point("latency_metrics") \
        .tag("env", env) \
        .tag("host", _host()) \
        .tag("stream", stream) \
        .field("ingest_delay_sec", float(ingest_delay_sec) if ingest_delay_sec is not None else None) \
        .field("write_delay_ms", float(write_delay_ms) if write_delay_ms is not None else None) \
        .field("end_to_end_latency_sec", float(end_to_end_latency_sec) if end_to_end_latency_sec is not None else None) \
        .time(ts, WritePrecision.NS)
    writer.write_api.write(bucket=writer.bucket, record=p)
