from datetime import date
from typing import Dict, List, Any, Optional, Tuple

from app.brokers.kite_helpers import parse_expiry

# Indices and their exchange pools
# - NIFTY, BANKNIFTY use NFO
# - SENSEX uses BFO
POOL_FOR = {
    "NIFTY": "NFO",
    "BANKNIFTY": "NFO",
    "SENSEX": "BFO",
}

def _pool_for_index(insts_nfo: List[Dict[str, Any]], insts_bfo: List[Dict[str, Any]], idx: str) -> List[Dict[str, Any]]:
    pool = POOL_FOR.get(idx)
    if pool == "NFO":
        return insts_nfo or []
    if pool == "BFO":
        return insts_bfo or []
    return []

def discover_weeklies_for_index(insts_nfo: List[Dict[str, Any]], insts_bfo: List[Dict[str, Any]], idx: str, spot_atm: int) -> List[date]:
    """
    Return next two weekly expiries for a given index, deduced from available ATM option contracts.
    It filters contracts that:
      - belong to the correct pool
      - are options (segment endswith '-OPT')
      - match the ATM strike
      - include the index tag in tradingsymbol
    Then it collects unique expiry dates >= today and returns the nearest two.
    """
    pool = _pool_for_index(insts_nfo, insts_bfo, idx)
    if not pool:
        return []

    today = date.today()
    opts = [
        inst for inst in pool
        if str(inst.get("segment","")).endswith("-OPT")
        and inst.get("strike") == spot_atm
        and idx in str(inst.get("tradingsymbol",""))
    ]
    if not opts:
        return []

    # Parse expiries and dedupe
    exps = []
    seen = set()
    for o in opts:
        try:
            e = parse_expiry(o)
            if e >= today and e not in seen:
                seen.add(e)
                exps.append(e)
        except Exception:
            continue

    exps.sort()
    # Return first two upcoming
    return exps[:2]

def discover_monthlies_for_index(insts_nfo: List[Dict[str, Any]], insts_bfo: List[Dict[str, Any]], idx: str, spot_atm: int) -> Tuple[Optional[date], Optional[date]]:
    """
    Return a tuple (this_month_expiry, next_month_expiry) for the index.
    We infer monthlies by grouping expiries by (year, month) and taking the last expiry in each month.
    """
    pool = _pool_for_index(insts_nfo, insts_bfo, idx)
    if not pool:
        return (None, None)

    today = date.today()
    opts = [
        inst for inst in pool
        if str(inst.get("segment","")).endswith("-OPT")
        and inst.get("strike") == spot_atm
        and idx in str(inst.get("tradingsymbol",""))
    ]
    if not opts:
        return (None, None)

    # Gather expiries by month
    by_month: Dict[Tuple[int,int], List[date]] = {}
    for o in opts:
        try:
            e = parse_expiry(o)
            if e >= today:
                key = (e.year, e.month)
                by_month.setdefault(key, []).append(e)
        except Exception:
            continue

    if not by_month:
        return (None, None)

    # Monthly expiry = max expiry date within the month (last tradable expiry of that month)
    monthly_list = []
    for ym, arr in by_month.items():
        monthly_list.append(max(arr))
    monthly_list.sort()

    this_m = monthly_list[0] if monthly_list else None
    next_m = monthly_list[1] if len(monthly_list) > 1 else None
    return (this_m, next_m)
