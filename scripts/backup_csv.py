import shutil
from datetime import datetime
from pathlib import Path

src_dir = Path("data_csv")
dest_dir = Path("backups")
dest_dir.mkdir(exist_ok=True)
backup_file = dest_dir / f"csv_backup_{datetime.now():%Y%m%d}.zip"

if src_dir.exists():
    shutil.make_archive(str(backup_file.with_suffix('')), 'zip', "data_csv")
    print(f"CSV backup created at {backup_file}")
else:
    print("No CSV directory found to backup.")
