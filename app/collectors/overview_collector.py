import json
from pathlib import Path
from typing import List, Dict, Any

from app.brokers.kite_helpers import safe_call, get_now
from app.utils.time_utils import rounded_half_minute
from app.sinks.influx_sink import write_index_overview

SPOT_SYMBOL = {
    "NIFTY": "NSE:NIFTY 50",
    "SENSEX": "BSE:SENSEX",
    "BANKNIFTY": "NSE:NIFTY BANK",
}
STEP = {"NIFTY": 50, "SENSEX": 100, "BANKNIFTY": 100}

class OverviewCollector:
    def __init__(self, kite_client, ensure_token, atm_collector,
                 raw_dir="data/raw_snapshots/overview",
                 influx_writer=None):
        self.kite = kite_client
        self.ensure_token = ensure_token
        self.atm_collector = atm_collector
        self.raw_dir = Path(raw_dir)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.influx_writer = influx_writer

    def collect(self) -> List[Dict[str, Any]]:
        atm_result = self.atm_collector.collect()
        atm_aggs = atm_result.get("overview_aggs", {})
        snapshot_time = get_now().strftime("%Y%m%d_%H%M%S")
        qdata = safe_call(self.kite, self.ensure_token, "quote", list(SPOT_SYMBOL.values())) or {}
        results = []

        for idx, mkt in SPOT_SYMBOL.items():
            q = qdata.get(mkt, {})
            ltp = q.get("last_price")
            ohlc = q.get("ohlc", {})
            atm_strike = round(ltp / STEP[idx]) * STEP[idx] if isinstance(ltp, (int, float)) else None
            prev_close = ohlc.get("close")
            open_px = ohlc.get("open")

            rec = {
                "timestamp": rounded_half_minute(get_now()),
                "symbol": "NIFTY 50" if idx == "NIFTY" else "SENSEX" if idx=="SENSEX" else "NIFTY BANK",
                "atm_strike": atm_strike,
                "last_price": ltp,
                "open": open_px,
                "high": ohlc.get("high"),
                "low": ohlc.get("low"),
                "close": prev_close,
                "net_change": (ltp - prev_close) if isinstance(ltp, (int, float)) and isinstance(prev_close, (int, float)) else None,
                "net_change_percent": ((ltp - prev_close) / prev_close * 100) if isinstance(ltp, (int, float)) and isinstance(prev_close, (int, float)) and prev_close != 0 else None,
                "day_change": (ltp - open_px) if isinstance(ltp, (int, float)) and isinstance(open_px, (int, float)) else None,
                "day_change_percent": ((ltp - open_px) / open_px * 100) if isinstance(ltp, (int, float)) and isinstance(open_px, (int, float)) and open_px != 0 else None,
                "day_width": (ohlc.get("high") - ohlc.get("low")) if isinstance(ohlc.get("high"), (int, float)) and isinstance(ohlc.get("low"), (int, float)) else None,
                "day_width_percent": ((ohlc.get("high") - ohlc.get("low")) / open_px * 100) if isinstance(open_px, (int, float)) and isinstance(ohlc.get("high"), (int, float)) and isinstance(ohlc.get("low"), (int, float)) and open_px != 0 else None,
            }
            if idx in atm_aggs:
                for bucket, vals in atm_aggs[idx].items():
                    rec[f"{bucket.upper()}_TP"] = vals.get("TP")
                    rec[f"{bucket.upper()}_OI_CALL"] = vals.get("OI_CALL")
                    rec[f"{bucket.upper()}_OI_PUT"] = vals.get("OI_PUT")
                    rec[f"pcr_{bucket}"] = vals.get("PCR")
                    rec[f"{bucket}_iv_open"] = vals.get("iv_open")
                    rec[f"{bucket}_iv_day_change"] = vals.get("iv_day_change")
                    rec[f"{bucket}_atm_iv"] = vals.get("atm_iv")
                    rec[f"{bucket}_days_to_expiry"] = vals.get("days_to_expiry")

            results.append(rec)
            # save JSON
            fname = f"{rec['symbol'].replace(' ', '_')}_{snapshot_time}.json"
            with open(self.raw_dir / fname, "w") as f:
                json.dump(rec, f, indent=2)

            # safe Influx write
            if self.influx_writer:
                try:
                    write_index_overview(rec, self.influx_writer)
                except Exception as e:
                    print(f"[WARN] Influx write failed for index_overview: {e}")

        return results
