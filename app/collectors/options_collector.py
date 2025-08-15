from datetime import datetime, UTC
import random

def collect_options_snapshot():
    now = datetime.now(UTC)
    data = []
    indices = ["NIFTY", "BANKNIFTY"]
    option_types = ["CE", "PE"]
    expirations = ["2025-08-14", "2025-08-21", "2025-08-28"]
    for idx in indices:
        base_price = 20000 if idx == "NIFTY" else 40000
        for exp in expirations:
            for opt_type in option_types:
                for strike_offset in range(-5, 6):
                    strike = base_price + (strike_offset * 100)
                    price_factor = abs(strike_offset) / 10
                    price = max(10, (1 - price_factor) * (100 if opt_type == "CE" else 120))
                    data.append({
                        "symbol": idx,
                        "expiration": exp,
                        "strike": strike,
                        "option_type": opt_type,
                        "last_price": price + random.uniform(-5, 5),
                        "volume": random.randint(1000, 10000),
                        "open_interest": random.randint(5000, 50000),
                        "iv": random.uniform(10, 30),
                        "ts": now
                    })
    return data
