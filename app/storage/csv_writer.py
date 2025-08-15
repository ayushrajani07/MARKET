import csv
from datetime import datetime
from pathlib import Path

class CsvWriter:
    def __init__(self, base_dir="data_csv"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def write(self, measurement: str, rows: list[dict]):
        if not rows:
            return
        csv_path = self.base_dir / f"{measurement}_{datetime.now().strftime('%Y%m%d')}.csv"
        write_header = not csv_path.exists()
        with open(csv_path, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            if write_header:
                writer.writeheader()
            writer.writerows(rows)
