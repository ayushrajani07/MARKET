def to_influx_index(row):
    tags = {"index": row["index"]}
    fields = {
        "price": row["price"],
        "change": row["change"],
        "change_pct": row["change_pct"],
        "volume": row["volume"],
    }
    return tags, fields, row["ts"]

def to_influx_option(row):
    tags = {
        "symbol": row["symbol"],
        "expiration": row["expiration"],
        "option_type": row["option_type"],
        "strike": str(row["strike"]),
    }
    fields = {
        "last_price": row["last_price"],
        "volume": row["volume"],
        "open_interest": row["open_interest"],
        "iv": row["iv"],
    }
    return tags, fields, row["ts"]
