#!/usr/bin/env python3
import json
from pathlib import Path
from datetime import datetime
import pandas as pd

from app.utils.time_utils import rounded_half_minute  # ✅ import helper

DATE_STR = datetime.now().strftime("%Y-%m-%d")
RAW_BASE = Path("data/raw_snapshots")
PROC_BASE = Path("data/PROCESSED")

def load_json_files(folder: Path):
    recs = []
    if not folder.exists():
        return recs
    for f in folder.glob("*.json"):
        if DATE_STR.replace("-", "") in f.name:
            try:
                with open(f, "r", encoding="utf-8") as fp:
                    rec = json.load(fp)
                # ✅ Ensure timestamp values are rounded (if they exist)
                if "timestamp" in rec:
                    try:
                        rec["timestamp"] = rounded_half_minute(datetime.fromisoformat(rec["timestamp"]))
                    except Exception:
                        pass
                recs.append(rec)
            except Exception as e:
                print(f"[WARN] Failed reading {f}: {e}")
    return recs

def save_csv_append_sorted(records, csv_path: Path):
    if not records:
        return
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df_new = pd.DataFrame(records)
    if csv_path.exists():
        df_old = pd.read_csv(csv_path)
        old_set = {json.dumps(r, sort_keys=True, default=str) for r in df_old.to_dict(orient="records")}
        add_rows = [r for r in records if json.dumps(r, sort_keys=True, default=str) not in old_set]
        if not add_rows:
            return
        df_new = pd.concat([df_old, pd.DataFrame(add_rows)], ignore_index=True)
    if "timestamp" in df_new.columns:
        df_new["timestamp"] = pd.to_datetime(df_new["timestamp"], errors="coerce")
        df_new = df_new.sort_values("timestamp").reset_index(drop=True)
    df_new.to_csv(csv_path, index=False)
    print(f"[OK] {csv_path} ({len(df_new)} rows)")

def process_overview():
    ovr_raw = RAW_BASE / "overview"
    recs = load_json_files(ovr_raw)
    if not recs:
        print("[INFO] No overview records for today.")
        return
    buckets = {}
    for r in recs:
        sym = r.get("symbol", "UNKNOWN")
        idx = "NIFTY" if sym == "NIFTY 50" else "SENSEX" if sym == "SENSEX" else "BANKNIFTY" if sym == "NIFTY BANK" else "UNKNOWN"
        buckets.setdefault(idx, []).append(r)
    for idx, rows in buckets.items():
        out_path = PROC_BASE / idx / "overview" / f"{idx}_{DATE_STR}.csv"
        save_csv_append_sorted(rows, out_path)

def process_options():
    opt_raw = RAW_BASE / "options"
    if not opt_raw.exists():
        print("[INFO] No options raw directory.")
        return
    for bucket_dir in opt_raw.iterdir():
        if not bucket_dir.is_dir():
            continue
        bucket = bucket_dir.name
        recs = load_json_files(bucket_dir)
        if not recs:
            continue
        by_idx = {}
        for r in recs:
            idx = r.get("index", "UNKNOWN")
            by_idx.setdefault(idx, []).append(r)
        for idx, rows in by_idx.items():
            out_path = PROC_BASE / idx / bucket / f"{idx}_{DATE_STR}.csv"
            save_csv_append_sorted(rows, out_path)

def main():
    process_overview()
    process_options()
    print("[DONE] Processed CSVs written to data/PROCESSED/...")

if __name__ == "__main__":
    main()
