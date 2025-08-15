import streamlit as st
from pathlib import Path
from datetime import date
import pandas as pd

PROC_BASE = Path("data/PROCESSED")
MASTER_BASE = Path("data/MASTER")

INDEXES = ["NIFTY", "SENSEX", "BANKNIFTY"]
BUCKETS_BY_INDEX = {
    "NIFTY": ["this_week", "next_week", "this_month", "next_month"],
    "SENSEX": ["this_week", "next_week", "this_month", "next_month"],
    "BANKNIFTY": ["this_month", "next_month"],  # no weekly buckets
}

RESAMPLE_RULE = "30S"

def load_today_series(idx: str, bucket: str, day: date):
    f = PROC_BASE / idx / bucket / f"{idx}_{day.strftime('%Y-%m-%d')}.csv"
    if not f.exists():
        return None
    try:
        df = pd.read_csv(f)
    except Exception:
        return None
    if "timestamp" not in df.columns or "last_price" not in df.columns:
        return None
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
    grp = df.groupby("timestamp", as_index=False)["last_price"].sum().rename(columns={"last_price": "today_tp"})
    grp = grp.set_index("timestamp").resample(RESAMPLE_RULE).last().ffill().reset_index()
    return grp

def load_weekday_master_avg(idx: str, bucket: str, weekday_name: str):
    f = MASTER_BASE / idx / weekday_name / f"{idx}_{bucket}_master_avg.csv"
    if not f.exists():
        return None
    try:
        df = pd.read_csv(f)
    except Exception:
        return None
    if "timestamp" not in df.columns or "avg_tp" not in df.columns or "count" not in df.columns:
        return None
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
    return df

def main():
    st.title("ATM Total Premium: Today vs Weekday Average (Incremental)")

    idx = st.selectbox("Index", INDEXES, index=0)
    bucket = st.selectbox("Expiry bucket", BUCKETS_BY_INDEX[idx], index=0)
    day = st.date_input("Date", value=date.today())

    weekday_name = day.strftime("%A")

    today_df = load_today_series(idx, bucket, day)
    if today_df is None or today_df.empty:
        st.warning("No processed data found for the selected date/index/bucket.")
        return

    avg_df = load_weekday_master_avg(idx, bucket, weekday_name)
    if avg_df is None or avg_df.empty:
        st.warning("No master average available for this weekday/index/bucket yet. Showing today only.")
        st.line_chart(today_df.set_index("timestamp")["today_tp"])
        return

    merged = pd.merge(today_df, avg_df[["timestamp", "avg_tp", "count"]], on="timestamp", how="left")
    merged = merged.set_index("timestamp")

    st.line_chart(merged[["today_tp", "avg_tp"]])

    last_row = merged.dropna(subset=["today_tp", "avg_tp"]).iloc[-1] if not merged.dropna(subset=["today_tp", "avg_tp"]).empty else None
    if last_row is not None:
        diff = last_row["today_tp"] - last_row["avg_tp"]
        ratio = (last_row["today_tp"] / last_row["avg_tp"] - 1.0) if last_row["avg_tp"] != 0 else None
        st.subheader("Latest stats")
        st.metric("Diff (today - avg)", f"{diff:.2f}")
        st.metric("Ratio (today/avg - 1)", f"{ratio:.2%}" if ratio is not None else "NA")
        st.caption(f"Average based on count={int(merged['count'].dropna().max()) if 'count' in merged.columns else 'NA'} days")

if __name__ == "__main__":
    main()
