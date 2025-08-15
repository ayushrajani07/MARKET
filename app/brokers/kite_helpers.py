import calendar
from datetime import datetime, date
from typing import Any, Dict, List
import pytz
import time
import math
import requests
from kiteconnect import KiteConnect
from kiteconnect.exceptions import TokenException, InputException

IST = pytz.timezone("Asia/Kolkata")

_token_refreshed = False

import time
import math
import requests
from kiteconnect.exceptions import KiteException

def safe_call(kite, ensure_token_fn, method_name, *args, **kwargs):
    """
    Wrapper around Kite API calls to auto-refresh token on auth error
    and retry lightly on rate limiting (429 Too Many Requests).
    """
    max_retries = 3
    base_delay = 0.2  # 200 ms
    attempt = 0

    while True:
        try:
            m = getattr(kite, method_name)
            return m(*args, **kwargs)

        except KiteException as ex:
            # Check for authentication issues — refresh token once
            if "TokenException" in str(type(ex)) or "TokenException" in str(ex):
                try:
                    ensure_token_fn()
                except Exception as e2:
                    print(f"[ERROR] Token refresh failed: {e2}")
                attempt += 1
                if attempt > max_retries:
                    print(f"[ERROR] {method_name} failed after {max_retries} retries (auth).")
                    return {}
                continue

            # Handle rate limit (429)
            if "Too many requests" in str(ex) or getattr(ex, "code", None) == 429:
                retry_after = None
                # Expose headers if available
                if hasattr(ex, "response") and hasattr(ex.response, "headers"):
                    try:
                        retry_after = int(ex.response.headers.get("Retry-After", 0))
                    except Exception:
                        pass

                # Respect Retry-After if present, else do exponential backoff
                if retry_after and retry_after > 0:
                    delay = min(retry_after, 2)  # cap at 2s
                else:
                    delay = min(base_delay * (2 ** attempt), 2.0)

                print(f"[WARN] {method_name}: 429 Too Many Requests — retrying in {delay:.3f}s...")
                time.sleep(delay)
                attempt += 1
                if attempt > max_retries:
                    print(f"[ERROR] {method_name} failed after {max_retries} retries (rate limit).")
                    return {}
                continue

            # Other Kite API exceptions
            print(f"❌ Kite call error in {method_name} : {ex}")
            return {}

        except requests.exceptions.HTTPError as http_ex:
            # Direct HTTP error — check status code
            if http_ex.response is not None and http_ex.response.status_code == 429:
                retry_after = http_ex.response.headers.get("Retry-After")
                try:
                    retry_after = int(retry_after)
                except Exception:
                    retry_after = None
                delay = min(retry_after or base_delay * (2 ** attempt), 2.0)
                print(f"[WARN] {method_name}: HTTP 429 — retrying in {delay:.3f}s...")
                time.sleep(delay)
                attempt += 1
                if attempt > max_retries:
                    print(f"[ERROR] {method_name} failed after {max_retries} retries (HTTP429).")
                    return {}
                continue
            else:
                print(f"❌ HTTP error in {method_name}: {http_ex}")
                return {}

        except Exception as e:
            print(f"❌ Error in {method_name}: {e}")
            return {}

def safe_ltp(kite_client: KiteConnect, ensure_token_fn, tokens):
    return safe_call(kite_client, ensure_token_fn, "ltp", tokens) or {}

def get_now():
    return datetime.now(IST)

def parse_expiry(inst: Dict[str, Any]) -> date:
    e = inst["expiry"]
    return e if isinstance(e, date) else datetime.strptime(e, "%Y-%m-%d").date()

def this_month_expiry() -> date:
    t = date.today()
    cal = calendar.monthcalendar(t.year, t.month)
    thurs = [w[3] for w in cal if w[3] != 0]
    return date(t.year, t.month, thurs[-1])

def next_month_expiry() -> date:
    t = date.today()
    m = (t.month % 12) + 1
    y = t.year + (t.month == 12)
    cal = calendar.monthcalendar(y, m)
    thurs = [w[3] for w in cal if w[3] != 0]
    return date(y, m, thurs[-1])
