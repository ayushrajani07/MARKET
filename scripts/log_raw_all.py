#!/usr/bin/env python3
import os, sys, time
from pathlib import Path
from datetime import datetime, time as dtime
import pytz
from dotenv import load_dotenv
from socket import gethostname

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv()

from app.brokers.kite_client import get_kite_client, _oauth_login
from app.collectors.atm_option_collector import ATMOptionCollector
from app.collectors.overview_collector import OverviewCollector
from app.utils.time_utils import rounded_half_minute
from influx_writer import InfluxWriter
from app.monitors.health_writer import write_monitor_status, write_pipeline_tick

IST = pytz.timezone("Asia/Kolkata")
MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)

EXPECTED_BUCKETS = {
    "NIFTY": ["this_week", "next_week", "this_month", "next_month"],
    "SENSEX": ["this_week", "next_week", "this_month", "next_month"],
    "BANKNIFTY": ["this_month", "next_month"]
}

def market_is_open():
    now = datetime.now(IST)
    return now.weekday() < 5 and MARKET_OPEN <= now.time() <= MARKET_CLOSE

def wait_until_open():
    while True:
        now = datetime.now(IST)
        if now.weekday() >= 5:
            time.sleep(60)
            continue
        if now.time() >= MARKET_OPEN:
            break
        print(f"[{now}] waiting for market open…")
        time.sleep(30)
    print(f"[{datetime.now(IST)}] market open")

def main():
    kite = get_kite_client()
    def ensure_token():
        fresh = _oauth_login(kite)
        return getattr(fresh, "_KiteConnect__access_token", "")

    writer = InfluxWriter()
    atm_collector = ATMOptionCollector(kite_client=kite, ensure_token=ensure_token, influx_writer=writer)
    overview_collector = OverviewCollector(kite_client=kite, ensure_token=ensure_token,
                                           atm_collector=atm_collector, influx_writer=writer)

    loop_interval = 30
    wait_until_open()
    print(f"[{datetime.now(IST)}] Starting main loop… ({loop_interval}s interval)")

    try:
        while True:
            if not market_is_open():
                if datetime.now(IST).time() >= MARKET_CLOSE:
                    break
                time.sleep(5)
                continue

            loop_start = time.time()
            legs_result = atm_collector.collect()
            atm_collect_ms = (time.time() - loop_start) * 1000
            legs_count = len(legs_result.get("legs", []))

            counts = {}
            for leg in legs_result.get("legs", []):
                idx = leg.get("index")
                bucket = leg.get("bucket")
                counts.setdefault(idx, {}).setdefault(bucket, 0)
                counts[idx][bucket] += 1
            for idx, bucket_map in counts.items():
                bucket_msgs = []
                for bucket in EXPECTED_BUCKETS[idx]:
                    cnt = bucket_map.get(bucket, 0)
                    if cnt == 0:
                        bucket_msgs.append(f"{bucket}:0⚠")
                        print(f"[WARN]   {idx} bucket '{bucket}' missing legs this tick!")
                    else:
                        bucket_msgs.append(f"{bucket}:{cnt}")
                print(f"[INFO]   {idx} buckets: {{{', '.join(bucket_msgs)}}}")

            t2 = time.time()
            overview_data = overview_collector.collect()
            overview_collect_ms = (time.time() - t2) * 1000
            overview_count = len(overview_data)

            loop_duration_ms = (time.time() - loop_start) * 1000

            # Emit monitor metrics
            write_pipeline_tick(writer,
                                env=os.getenv("ENV", "local"),
                                app="logger",
                                loop_duration_ms=loop_duration_ms,
                                atm_collect_ms=atm_collect_ms,
                                overview_collect_ms=overview_collect_ms,
                                records_written_index_overview=overview_count,
                                records_written_atm_legs=legs_count)

            write_monitor_status(writer,
                                 env=os.getenv("ENV", "local"),
                                 app="logger",
                                 status="ok",
                                 loop_duration_ms=loop_duration_ms,
                                 legs_count=legs_count,
                                 overview_count=overview_count,
                                 lag_sec_index_overview=0,
                                 lag_sec_atm_option_quote=0,
                                 errors_in_tick=0)

            time.sleep(loop_interval)

    except KeyboardInterrupt:
        print("Stopping logger…")
    finally:
        try:
            writer.close()
        except Exception as e:
            print(f"[WARN] Error closing InfluxWriter: {e}")

if __name__ == "__main__":
    main()
