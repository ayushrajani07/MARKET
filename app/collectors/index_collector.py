from datetime import datetime, UTC
import random

def collect_index_snapshot():
    now = datetime.now(UTC)
    rows = []
    for idx, base in [("NIFTY", 20000), ("BANKNIFTY", 40000), ("FINNIFTY", 30000)]:
        price = base + random.uniform(-500, 500)
        change = random.uniform(-2, 2)
        change_pct = (change / base) * 100
        rows.append({
            "index": idx,
            "price": price,
            "change": change,
            "change_pct": change_pct,
            "volume": random.randint(10_000, 50_000),
            "ts": now
        })
    return rows
