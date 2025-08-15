import sys
from pathlib import Path
import json
import csv
from typing import Any

# --- Ensure project root on sys.path so package imports work ---
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

RAW_DIRS = {
    "overview": Path("data/raw_snapshots/overview"),
    "options": Path("data/raw_snapshots/options")
}

OUT_FILE = Path("data/raw_snapshots/field_mapping.csv")

def flatten_json(obj: Any, prefix=""):
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

def collect_fields():
    rows = []
    seen_paths = set()

    for src_type, dir_path in RAW_DIRS.items():
        if not dir_path.exists():
            print(f"⚠ No directory: {dir_path}")
            continue

        for file in sorted(dir_path.glob("*.json")):
            try:
                with open(file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                flat = flatten_json(data)
                for path, value in flat.items():
                    if (src_type, path) not in seen_paths:
                        seen_paths.add((src_type, path))
                        rows.append({
                            "source_type": src_type,
                            "source_field_path": path,
                            "example_value": value,
                            "target_schema_field": "",
                            "transform_notes": ""
                        })
            except Exception as e:
                print(f"Error reading {file}: {e}")
    return rows

def main():
    print("Scanning raw snapshot JSONs...")
    field_rows = collect_fields()
    if not field_rows:
        print("No fields found. Did you run the raw collectors?")
        return

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=[
            "source_type", "source_field_path", "example_value", "target_schema_field", "transform_notes"
        ])
        writer.writeheader()
        writer.writerows(field_rows)

    print(f"Mapping CSV created: {OUT_FILE} ({len(field_rows)} rows)")
    print("Fill in 'target_schema_field' and 'transform_notes' for reconciliation.")

if __name__ == "__main__":
    main()
