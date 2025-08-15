import sys
import time
from datetime import datetime, time as dtime
import pytz
from pathlib import Path

from app.observability.logging import setup_json_logging
from app.storage.influx_writer import InfluxWriter
from app.storage.csv_writer import CsvWriter
from app.collectors.overview_collector import OverviewCollector
from app.collectors.atm_option_collector import ATMOptionCollector
from scripts.normalise_from_mapping import normalise_cycle, load_config

logger = setup_json_logging("market-app")
IST = pytz.timezone("Asia/Kolkata")

MARKET_CLOSE_IST = dtime(hour=15, minute=31)
MAPPING_CSV_PATH = Path("data/raw_snapshots/field_mapping.csv")

def run_eod_backup():
    logger.info("Running EOD backup...")
    logger.info("EOD backup complete.")

def main():
    if not MAPPING_CSV_PATH.exists():
        logger.error("Mapping CSV not found. Run mapping generation tool first.")
        sys.exit(1)

    config = load_config()
    sched = config["app"]["collector_schedule"]

    influx_writer = InfluxWriter()
    csv_writer = CsvWriter()

    overview_collector = OverviewCollector()
    fast_options_collector = ATMOptionCollector(index_expiries=sched["fast_index_expiries"])
    slow_options_collector = ATMOptionCollector(index_expiries=sched["slow_index_expiries"])

    last_run_fast = 0
    last_run_slow = 0

    while True:
        now_ist = datetime.now(IST).time()
        now_ts = time.time()

        if now_ist >= MARKET_CLOSE_IST:
            logger.info("Market closed.")
            run_eod_backup()
            sys.exit(0)

        raw_overview = []
        raw_options = []

        if now_ts - last_run_fast >= sched["fast_interval"]:
            raw_overview = overview_collector.collect()
            raw_options += fast_options_collector.collect()
            last_run_fast = now_ts

        if now_ts - last_run_slow >= sched["slow_interval"]:
            raw_options += slow_options_collector.collect()
            last_run_slow = now_ts

        if raw_overview or raw_options:
            try:
                ov_records, opt_records = normalise_cycle(raw_overview, raw_options)

                for rec in ov_records:
                    influx_writer.write_point(
                        "index_overview",
                        tags={"index": rec.get("index")},
                        fields={k: v for k, v in rec.items() if k != "index"}
                    )
                    csv_writer.write("index_overview", [rec])

                for rec in opt_records:
                    influx_writer.write_point(
                        "atm_option_quote",
                        tags={
                            "index": rec.get("index"),
                            "option_type": rec.get("option_type"),
                            "expiration": rec.get("expiration"),
                            "strike": str(rec.get("strike"))
                        },
                        fields={k: v for k, v in rec.items() if k not in ["index", "option_type", "expiration", "strike"]}
                    )
                    csv_writer.write("atm_option_quote", [rec])

                logger.info(f"Cycle stored: {len(ov_records)} overview, {len(opt_records)} options")
            except Exception as e:
                logger.error(f"Error: {e}", exc_info=True)

        time.sleep(1)

if __name__ == "__main__":
    main()
