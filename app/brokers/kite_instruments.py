from typing import Dict, Any, Optional, List
from kiteconnect import KiteConnect

CANONICAL_INDEX_ALIASES = {
    "NIFTY 50": ["NIFTY 50", "NIFTY50"],
    "NIFTY BANK": ["NIFTY BANK", "BANKNIFTY", "NIFTYBANK"],
    "SENSEX": ["SENSEX"]
}


def get_instruments_map(kite: KiteConnect) -> Dict[str, Any]:
    instruments = kite.instruments()
    by_tradingsymbol = {ins.get("tradingsymbol"): ins for ins in instruments}
    by_token = {ins.get("instrument_token"): ins for ins in instruments}
    indices = [ins for ins in instruments if ins.get("instrument_type") == "INDEX"]
    return {
        "all": instruments,
        "by_tradingsymbol": by_tradingsymbol,
        "by_token": by_token,
        "indices": indices,
    }


def list_index_heads(kite_maps: Dict[str, Any], limit: int = 20) -> List[Dict[str, Any]]:
    return [
        {
            "exchange": ins.get("exchange"),
            "name": ins.get("name"),
            "tradingsymbol": ins.get("tradingsymbol"),
            "instrument_token": ins.get("instrument_token"),
        }
        for ins in kite_maps["indices"][:limit]
    ]


def find_index_token(kite_maps: Dict[str, Any], requested: str) -> Optional[int]:
    req_upper = requested.upper().replace("_", " ").strip()
    if req_upper == "BANKNIFTY":
        req_canon = "NIFTY BANK"
    else:
        req_canon = req_upper

    aliases = CANONICAL_INDEX_ALIASES.get(req_canon, [req_canon])
    aliases_upper = [a.upper() for a in aliases]

    for ins in kite_maps["indices"]:
        name = (ins.get("name") or "").upper()
        ts = (ins.get("tradingsymbol") or "").upper()
        if name in aliases_upper or ts in aliases_upper:
            return ins.get("instrument_token")

    for alias in aliases:
        ins = kite_maps["by_tradingsymbol"].get(alias)
        if ins and ins.get("instrument_type") == "INDEX":
            return ins.get("instrument_token")
    return None


def nearest_strike(price: float, step: int) -> int:
    return int(round(price / step) * step)
