from datetime import datetime

def rounded_half_minute(ts: datetime, to_str: bool = True):
    """
    Round a datetime object to nearest 0 or 30 seconds.

    Args:
        ts (datetime): datetime (tz-aware or naive)
        to_str (bool): if True returns ISO8601 string, else datetime

    Returns:
        str | datetime: Rounded ISO string or datetime
    """
    sec = 0 if ts.second < 30 else 30
    ts_rounded = ts.replace(second=sec, microsecond=0)
    return ts_rounded.isoformat() if to_str else ts_rounded
