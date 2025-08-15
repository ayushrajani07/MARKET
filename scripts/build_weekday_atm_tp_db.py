from pathlib import Path
from datetime import date, datetime
import pandas as pd

# Processed input base
PROC_BASE = Path("data/PROCESSED")
# Master averages output base
MASTER_BASE = Path("data/MASTER")

INDEXES = ["NIFTY", "SENSEX", "BANKNIFTY"]
BUCKETS_BY_INDEX = {
    "NIFTY": ["this_week", "next_week", "this_month", "next_month"],
    "SENSEX": ["this_week", "next_week", "this_month", "next_month"],
    "BANKNIFTY": ["this_month", "next_month"],
}
RESAMPLE_RULE = "30S"

def load_today_tp(idx: str, bucket: str, day: date):
    """Load today's processed bucket CSV and return total premium per 30s bin."""
    f = PROC_BASE / idx / bucket / f"{idx}_{day.strftime('%Y-%m-%d')}.csv"
    if not f.exists():
        return None
    df = pd.read_csv(f)
    if "timestamp" not in df.columns or "last_price" not in df.columns:
        return None
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
    # Sum CALL + PUT last_price
    tp = df.groupby("timestamp", as_index=False)["last_price"].sum()
    tp = tp.rename(columns={"last_price": "today_tp"}).set_index("timestamp")
    tp = tp.resample(RESAMPLE_RULE).last().ffill().reset_index()
    return tp

def update_master_avg(idx: str, bucket: str, today_df: pd.DataFrame, weekday_name: str):
    """Incrementally update master_avg.csv for this weekday/index/bucket."""
    out_dir = MASTER_BASE / idx / weekday_name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{idx}_{bucket}_master_avg.csv"

    if out_file.exists():
        master = pd.read_csv(out_file, parse_dates=["timestamp"])
    else:
        # Initialize with today's TP
        master = today_df.copy()
        master = master.rename(columns={"today_tp": "avg_tp"})
        master["count"] = 1
        master.to_csv(out_file, index=False)
        print(f"[INIT] Created {out_file} with {len(master)} rows")
        return

    # Merge on timestamp
    m = master.merge(today_df, on="timestamp", how="outer")
    m = m.sort_values("timestamp")
    m["count"] = m["count"].fillna(0).astype(int)
    m["avg_tp"] = m["avg_tp"].fillna(0)
    m["today_tp"] = m["today_tp"].fillna(0)

    # Incremental update: only update non-zero today_tp
    mask = m["today_tp"] != 0
    m.loc[mask, "count"] = m.loc[mask, "count"] + 1
    m.loc[mask, "avg_tp"] = (
        (m.loc[mask, "avg_tp"] * (m.loc[mask, "count"] - 1) + m.loc[mask, "today_tp"])
        / m.loc[mask, "count"]
    )

    m = m.drop(columns=["today_tp"])
    m.to_csv(out_file, index=False)
    print(f"[UPDATE] {out_file} updated, count={m['count'].max()}")

def main():
    today = date.today()
    wname = today.strftime("%A")
    for idx in INDEXES:
        for bucket in BUCKETS_BY_INDEX[idx]:
            tp_df = load_today_tp(idx, bucket, today)
            if tp_df is None or tp_df.empty:
                print(f"[SKIP] No data for {idx} {bucket}")
                continue
            update_master_avg(idx, bucket, tp_df, wname)
    print("[DONE] Master averages incrementally updated.")

if __name__ == "__main__":
    main()
