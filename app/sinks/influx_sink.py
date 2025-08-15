from datetime import datetime
from influxdb_client import Point, WritePrecision

def _ffloat(val):
    """Return float if numeric, else None."""
    return float(val) if isinstance(val, (int, float)) else None

def _fint(val):
    """Return int if numeric, else None."""
    return int(val) if isinstance(val, (int, float)) else None


def write_atm_leg(rec, writer):
    """Write ATM option leg to InfluxDB with strict types."""
    try:
        ts = datetime.fromisoformat(rec["timestamp"])
        tags = {
            "index": rec.get("index"),
            "option_type": "CE" if rec.get("side") == "CALL" else "PE",
            "expiration": rec.get("expiry"),
            "strike": str(rec.get("atm_strike")),
            "bucket": rec.get("bucket"),
        }
        fields = {
            "last_price": _ffloat(rec.get("last_price")),
            "average_price": _ffloat(rec.get("average_price")),
            "ohlc_open": _ffloat(rec.get("ohlc.open")),
            "ohlc_high": _ffloat(rec.get("ohlc.high")),
            "ohlc_low": _ffloat(rec.get("ohlc.low")),
            "ohlc_close": _ffloat(rec.get("ohlc.close")),
            "net_change": _ffloat(rec.get("net_change")),
            "net_change_percent": _ffloat(rec.get("net_change_percent")),
            "day_change": _ffloat(rec.get("day_change")),
            "day_change_percent": _ffloat(rec.get("day_change_percent")),
            "iv": _ffloat(rec.get("iv")),
            "volume": _fint(rec.get("volume")),
            "oi": _fint(rec.get("oi")),
            "oi_open": _fint(rec.get("oi_open")),
            "oi_change": _fint(rec.get("oi_change")),
            "days_to_expiry": _fint(rec.get("days_to_expiry")),
            "atm_strike_val": _fint(rec.get("atm_strike")),
        }
        fields = {k: v for k, v in fields.items() if v is not None}
        if not fields:
            return
        p = Point("atm_option_quote")
        for k, v in tags.items():
            if v is not None:
                p = p.tag(k, str(v))
        for k, v in fields.items():
            p = p.field(k, v)
        p = p.time(ts, WritePrecision.NS)
        writer.write_api.write(bucket=writer.bucket, record=p)
    except Exception as e:
        print(f"[WARN] Influx write atm_option_quote failed: {e}")


def write_index_overview(rec, writer):
    """Write index overview to InfluxDB with strict types."""
    try:
        ts = datetime.fromisoformat(rec["timestamp"])
        tags = {
            "index": rec.get("symbol"),
        }
        fields = {
            "atm_strike": _fint(rec.get("atm_strike")),
            "last_price": _ffloat(rec.get("last_price")),
            "open": _ffloat(rec.get("open")),
            "high": _ffloat(rec.get("high")),
            "low": _ffloat(rec.get("low")),
            "close": _ffloat(rec.get("close")),
            "net_change": _ffloat(rec.get("net_change")),
            "net_change_percent": _ffloat(rec.get("net_change_percent")),
            "day_change": _ffloat(rec.get("day_change")),
            "day_change_percent": _ffloat(rec.get("day_change_percent")),
            "day_width": _ffloat(rec.get("day_width")),
            "day_width_percent": _ffloat(rec.get("day_width_percent")),
        }
        for k, v in rec.items():
            if k.endswith("_TP") or k.endswith("_OI_CALL") or k.endswith("_OI_PUT"):
                fv = _ffloat(v)
                if fv is not None:
                    fields[k] = fv
            elif k.startswith("pcr_") or k.endswith("_iv_open") or k.endswith("_iv_day_change") or k.endswith("_atm_iv"):
                fv = _ffloat(v)
                if fv is not None:
                    fields[k] = fv
            elif k.endswith("_days_to_expiry"):
                iv = _fint(v)
                if iv is not None:
                    fields[k] = iv
        fields = {k: v for k, v in fields.items() if v is not None}
        if not fields:
            return
        p = Point("index_overview")
        for k, v in tags.items():
            if v is not None:
                p = p.tag(k, str(v))
        for k, v in fields.items():
            p = p.field(k, v)
        p = p.time(ts, WritePrecision.NS)
        writer.write_api.write(bucket=writer.bucket, record=p)
    except Exception as e:
        print(f"[WARN] Influx write index_overview failed: {e}")
