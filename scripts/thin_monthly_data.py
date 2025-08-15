import pandas as pd
from pathlib import Path
from datetime import datetime
import zipfile
import shutil

# Base raw_snapshots path
BASE_DIR = Path("data/raw_snapshots")
ARCHIVE_DIR = Path("archive")
ARCHIVE_DIR.mkdir(exist_ok=True)

# Downsampling intervals (minutes)
OVERVIEW_INTERVAL = 1
MONTHLY_INTERVAL = 2

# Buckets for monthly options
MONTHLY_BUCKETS = ["this_month", "next_month"]

def thin_csv(file_path: Path, interval_min: int):
    """Thin a CSV file to one row per 'interval_min' minutes."""
    try:
        df = pd.read_csv(file_path, parse_dates=['timestamp'])
    except Exception as e:
        print(f"[WARN] Could not read {file_path}: {e}")
        return

    if 'timestamp' not in df.columns:
        print(f"[WARN] No 'timestamp' in {file_path}")
        return

    df = df.sort_values('timestamp').set_index('timestamp')
    thinned = df.resample(f"{interval_min}T").first().dropna(how="all").reset_index()
    thinned.to_csv(file_path, index=False)
    print(f"[OK] Thinned {file_path.name} → {len(thinned)} rows at {interval_min}‑min intervals")

def zip_and_remove_jsons(folder: Path, year: int, month: int, archive_name_prefix: str):
    """Zip JSONs for given year-month in a folder, then delete originals."""
    json_files = [f for f in folder.glob("*.json") if _file_matches_month(f, year, month)]
    if not json_files:
        return

    zip_name = f"{archive_name_prefix}_{year}-{month:02d}.zip"
    zip_path = ARCHIVE_DIR / zip_name

    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        for f in json_files:
            zf.write(f, arcname=f.name)
    print(f"[ARCHIVE] Zipped {len(json_files)} JSONs → {zip_path}")

    # Remove originals after zipping
    for f in json_files:
        try:
            f.unlink()
        except Exception as e:
            print(f"[WARN] Failed to delete {f}: {e}")

def _file_matches_month(f: Path, year: int, month: int) -> bool:
    """Check if the file has a YYYYMMDD date within the given year, month."""
    stem = f.stem
    try:
        # Try to find an 8-digit date like YYYYMMDD in the name
        for token in stem.split("_"):
            if token.isdigit() and len(token) == 8:
                fdate = datetime.strptime(token, "%Y%m%d").date()
                if fdate.year == year and fdate.month == month:
                    return True
    except Exception:
        pass
    return False

def process_overview():
    ovr_dir = BASE_DIR / "overview"
    if not ovr_dir.exists():
        return

    for csv_file in ovr_dir.glob("*.csv"):
        try:
            date_str = csv_file.stem.split("_")[-1]
            file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            print(f"[SKIP] Could not parse date from {csv_file.name}")
            continue

        today = datetime.today().date()
        if file_date.month != today.month or file_date.year != today.year:
            thin_csv(csv_file, OVERVIEW_INTERVAL)
            zip_and_remove_jsons(ovr_dir, file_date.year, file_date.month, "overview")

def process_monthlies():
    opt_dir = BASE_DIR / "options"
    if not opt_dir.exists():
        return

    for bucket in MONTHLY_BUCKETS:
        bucket_dir = opt_dir / bucket
        if not bucket_dir.exists():
            continue

        for csv_file in bucket_dir.glob("*.csv"):
            try:
                date_str = csv_file.stem.split("_")[-1]
                file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except Exception:
                print(f"[SKIP] Could not parse date from {csv_file.name}")
                continue

            today = datetime.today().date()
            if file_date.month != today.month or file_date.year != today.year:
                thin_csv(csv_file, MONTHLY_INTERVAL)
                zip_and_remove_jsons(bucket_dir, file_date.year, file_date.month, bucket)

def main():
    print(f"[{datetime.now()}] Starting month-end thinning & archiving...")
    process_overview()
    process_monthlies()
    print("[DONE] Thinning & archiving complete.")

if __name__ == "__main__":
    main()
