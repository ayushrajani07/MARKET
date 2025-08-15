import json
import csv
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

MAPPING_CSV = Path("data/raw_snapshots/field_mapping.csv")
CONFIG_PATH = Path("config/schema_config.json")

def load_mapping():
    mapping = {}
    with open(MAPPING_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            mapping.setdefault(row["source_type"], {})[row["source_field_path"]] = {
                "target_schema_field": row["target_schema_field"].strip(),
                "transform_notes": row["transform_notes"].strip()
            }
    return mapping

def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def flatten_json(obj, prefix=""):
    flat = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{prefix}.{k}" if prefix else k
            flat.update(flatten_json(v, new_key))
    elif isinstance(obj, list) and obj and isinstance(obj[0], dict):
        flat.update(flatten_json(obj[0], prefix))
    else:
        flat[prefix] = obj
    return flat

def normalise_file(raw: dict, src_type: str, mapping: dict, derived_rules: dict) -> dict:
    flat = flatten_json(raw)
    norm = {}

    for path, val in flat.items():
        map_info = mapping.get(src_type, {}).get(path)
        if not map_info:
            continue
        tgt = map_info["target_schema_field"]
        if not tgt:
            continue
        notes = map_info["transform_notes"].lower()
        try:
            if "int" in notes:
                val = int(val)
            elif "float" in notes:
                val = float(val)
            elif "str" in notes:
                val = str(val)
        except:
            pass
        norm[tgt] = val

    # Derived fields
    ts_rule = derived_rules.get("timestamp_rounded")
    if ts_rule and "timestamp" in norm:
        try:
            dt = datetime.fromisoformat(norm["timestamp"].replace("Z", "+00:00"))
            if ts_rule["round_to"] == "30s":
                sec = 0 if dt.second < 30 else 30
            norm["timestamp_rounded"] = dt.replace(second=sec, microsecond=0).strftime("%H:%M:%S")
        except:
            pass

    atm_rule = derived_rules.get("atm_strike")
    if atm_rule and atm_rule["source_field"] in norm:
        nearest = atm_rule["by_index"].get(norm.get("index"))
        if nearest:
            try:
                price = float(norm[atm_rule["source_field"]])
                norm["atm_strike"] = int(round(price / nearest) * nearest)
            except:
                pass

    if "prev_close" in norm and "last_price" in norm:
        try:
            pc, lp = float(norm["prev_close"]), float(norm["last_price"])
            norm["net_change"] = lp - pc
            norm["net_change_percent"] = (lp - pc) / pc * 100 if pc else None
        except: pass

    if "open" in norm and "last_price" in norm:
        try:
            op, lp = float(norm["open"]), float(norm["last_price"])
            norm["day_change"] = lp - op
            norm["day_change_percent"] = (lp - op) / op * 100 if op else None
        except: pass

    if "high" in norm and "low" in norm and "open" in norm:
        try:
            hi, lo, op = float(norm["high"]), float(norm["low"]), float(norm["open"])
            norm["day_width"] = hi - lo
            norm["day_width_percent"] = (hi - lo) / op * 100 if op else None
        except: pass

    if "iv_open" in norm and "atm_strike_iv" in norm:
        try:
            norm["iv_day_change"] = float(norm["iv_open"]) - float(norm["atm_strike_iv"])
        except: pass

    if "oi_open" in norm and "oi" in norm:
        try:
            norm["oi_change"] = float(norm["oi_open"]) - float(norm["oi"])
        except: pass

    return norm

def normalise_cycle(raw_overview: list, raw_options: list):
    mapping = load_mapping()
    config = load_config()
    derived_rules = config["app"]["derived_rules"]
    ov_records = [normalise_file(rec, "overview", mapping, derived_rules) for rec in raw_overview]
    opt_records = [normalise_file(rec, "options", mapping, derived_rules) for rec in raw_options]
    return ov_records, opt_records
