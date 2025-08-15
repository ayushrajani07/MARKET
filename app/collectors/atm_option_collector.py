import json
from datetime import date
from pathlib import Path
from typing import Dict, List, Any, Optional
import pytz

from app.brokers.kite_helpers import safe_call, parse_expiry, get_now
from app.brokers.expiry_discovery import discover_weeklies_for_index, discover_monthlies_for_index
from app.utils.time_utils import rounded_half_minute
from app.sinks.influx_sink import write_atm_leg  # 🔹 centralised Influx mapping

IST = pytz.timezone("Asia/Kolkata")

SPOT_SYMBOL = {
    "NIFTY":     "NSE:NIFTY 50",
    "SENSEX":    "BSE:SENSEX",
    "BANKNIFTY": "NSE:NIFTY BANK",
}
STEP = {"NIFTY": 50, "SENSEX": 100, "BANKNIFTY": 100}


class ATMOptionCollector:
    def __init__(self, kite_client, ensure_token,
                 raw_dir="data/raw_snapshots/options",
                 use_dynamic_expiries=True,
                 influx_writer=None):
        self.kite = kite_client
        self.ensure_token = ensure_token
        self.raw_dir = Path(raw_dir)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.use_dynamic_expiries = use_dynamic_expiries
        self.influx_writer = influx_writer  # can be None

        try:
            self.insts_nfo = self.kite.instruments(exchange="NFO") or []
        except Exception:
            self.insts_nfo = []
        try:
            self.insts_bfo = self.kite.instruments(exchange="BFO") or []
        except Exception:
            self.insts_bfo = []

        self.oi_open_map: Dict[str, int] = {}
        self.iv_open_map: Dict[str, float] = {}

    def _spot_price(self, idx: str) -> Optional[float]:
        q = safe_call(self.kite, self.ensure_token, "quote", [SPOT_SYMBOL[idx]]) or {}
        return q.get(SPOT_SYMBOL[idx], {}).get("last_price")

    def _pool_for_idx(self, idx) -> list:
        return self.insts_bfo if idx == "SENSEX" else self.insts_nfo

    def _discover_expiries_for_idx(self, idx: str, atm: int) -> Dict[str, List[date]]:
        weeklies = discover_weeklies_for_index(self.insts_nfo, self.insts_bfo, idx, atm)
        m_this, m_next = discover_monthlies_for_index(self.insts_nfo, self.insts_bfo, idx, atm)
        monthly = [d for d in [m_this, m_next] if d]
        return {"weekly": weeklies, "monthly": monthly}

    def _get_tokens(self, pool, idx, atm, exp):
        opts = [
            i for i in pool
            if str(i.get("segment", "")).endswith("-OPT")
            and i.get("strike") == atm
            and idx in str(i.get("tradingsymbol", ""))
        ]
        for o in opts:
            try:
                o["_exp"] = parse_expiry(o)
            except Exception:
                o["_exp"] = None
        ce = next((o for o in opts if o.get("_exp") == exp and o.get("instrument_type") == "CE"), None)
        pe = next((o for o in opts if o.get("_exp") == exp and o.get("instrument_type") == "PE"), None)
        return (str(ce["instrument_token"]), str(pe["instrument_token"])) if ce and pe else (None, None)

    def collect(self) -> Dict[str, Any]:
        snapshot_time = get_now().strftime("%Y%m%d_%H%M%S")
        legs = []
        overview_aggs: Dict[str, Any] = {}

        for idx in ["NIFTY", "SENSEX", "BANKNIFTY"]:
            spot = self._spot_price(idx)
            if spot is None:
                continue
            atm = round(spot / STEP[idx]) * STEP[idx]
            pool = self._pool_for_idx(idx)
            exps = self._discover_expiries_for_idx(idx, atm) if self.use_dynamic_expiries else {}

            if idx == "BANKNIFTY":
                pairs = [("this_month", exps["monthly"][0] if len(exps["monthly"]) > 0 else None),
                         ("next_month", exps["monthly"][1] if len(exps["monthly"]) > 1 else None)]
            else:
                pairs = [("this_week", exps["weekly"][0] if exps.get("weekly") else None),
                         ("next_week", exps["weekly"][1] if len(exps.get("weekly") or []) > 1 else None),
                         ("this_month", exps["monthly"][0] if exps.get("monthly") else None),
                         ("next_month", exps["monthly"][1] if len(exps.get("monthly") or []) > 1 else None)]

            overview_aggs[idx] = {}
            for label, exp in pairs:
                if not exp:
                    continue
                ce_tkn, pe_tkn = self._get_tokens(pool, idx, atm, exp)
                if not ce_tkn or not pe_tkn:
                    continue
                qdata = safe_call(self.kite, self.ensure_token, "quote", [ce_tkn, pe_tkn]) or {}
                ce_q = qdata.get(str(ce_tkn), {})
                pe_q = qdata.get(str(pe_tkn), {})

                # seed maps
                for tkn, q in [(ce_tkn, ce_q), (pe_tkn, pe_q)]:
                    if tkn not in self.oi_open_map and isinstance(q.get("oi"), int):
                        self.oi_open_map[tkn] = q.get("oi")
                    if tkn not in self.iv_open_map and isinstance(q.get("iv"), (int, float)):
                        self.iv_open_map[tkn] = q.get("iv")

                iv_avg = None
                if isinstance(ce_q.get("iv"), (int, float)) and isinstance(pe_q.get("iv"), (int, float)):
                    iv_avg = (ce_q.get("iv") + pe_q.get("iv")) / 2
                iv_key = f"{idx}_{label}_iv_open"
                if iv_avg is not None and iv_key not in self.iv_open_map:
                    self.iv_open_map[iv_key] = iv_avg
                iv_open_val = self.iv_open_map.get(iv_key)
                iv_day_change = (iv_open_val - iv_avg) if (isinstance(iv_open_val, (int, float)) and isinstance(iv_avg, (int, float))) else None

                overview_aggs[idx][label] = {
                    "TP": sum(x for x in [ce_q.get("last_price"), pe_q.get("last_price")] if isinstance(x, (int, float))),
                    "OI_CALL": ce_q.get("oi"),
                    "OI_PUT": pe_q.get("oi"),
                    "PCR": (pe_q.get("oi") / ce_q.get("oi")) if isinstance(pe_q.get("oi"), int) and 
                           isinstance(ce_q.get("oi"), int) and ce_q.get("oi") != 0 else None,
                    "atm_iv": iv_avg,
                    "iv_open": iv_open_val,
                    "iv_day_change": iv_day_change,
                    "days_to_expiry": (exp - date.today()).days
                }

                for side, tkn, q in [("CALL", ce_tkn, ce_q), ("PUT", pe_tkn, pe_q)]:
                    oi_open = self.oi_open_map.get(tkn)
                    oi_curr = q.get("oi")
                    oi_change = (oi_open - oi_curr) if (isinstance(oi_open, int) and isinstance(oi_curr, int)) else None

                    rec = {
                        "timestamp": rounded_half_minute(get_now()),
                        "index": idx,
                        "bucket": label,
                        "side": side,
                        "expiry": exp.isoformat(),
                        "atm_strike": atm,
                        "last_price": q.get("last_price"),
                        "days_to_expiry": (exp - date.today()).days,
                        "average_price": q.get("average_price"),
                        "volume": q.get("volume"),
                        "oi": oi_curr,
                        "oi_open": oi_open,
                        "oi_change": oi_change,
                        "ohlc.open": q.get("ohlc", {}).get("open"),
                        "ohlc.high": q.get("ohlc", {}).get("high"),
                        "ohlc.low": q.get("ohlc", {}).get("low"),
                        "ohlc.close": q.get("ohlc", {}).get("close"),
                        "net_change": (q.get("last_price") - q.get("ohlc", {}).get("close"))
                            if isinstance(q.get("last_price"), (int, float))
                            and isinstance(q.get("ohlc", {}).get("close"), (int, float)) else None,
                        "net_change_percent": ((q.get("last_price") - q.get("ohlc", {}).get("close")) /
                                               q.get("ohlc", {}).get("close") * 100)
                            if isinstance(q.get("last_price"), (int, float))
                            and isinstance(q.get("ohlc", {}).get("close"), (int, float))
                            and q.get("ohlc", {}).get("close") != 0 else None,
                        "day_change": (q.get("last_price") - q.get("ohlc", {}).get("open"))
                            if isinstance(q.get("last_price"), (int, float))
                            and isinstance(q.get("ohlc", {}).get("open"), (int, float)) else None,
                        "day_change_percent": ((q.get("last_price") - q.get("ohlc", {}).get("open")) /
                                                q.get("ohlc", {}).get("open") * 100)
                            if isinstance(q.get("last_price"), (int, float))
                            and isinstance(q.get("ohlc", {}).get("open"), (int, float))
                            and q.get("ohlc", {}).get("open") != 0 else None,
                        "iv": q.get("iv")
                    }
                    legs.append(rec)
                    # save JSON
                    bdir = self.raw_dir / label
                    bdir.mkdir(parents=True, exist_ok=True)
                    fname = f"{idx}_{side}_{label}_{exp.isoformat()}_{snapshot_time}.json"
                    with open(bdir / fname, "w") as f:
                        json.dump(rec, f, indent=2)

                    # safe Influx write
                    if self.influx_writer:
                        try:
                            write_atm_leg(rec, self.influx_writer)
                        except Exception as e:
                            print(f"[WARN] Influx write failed for atm_option_quote: {e}")

        return {"legs": legs, "overview_aggs": overview_aggs}
