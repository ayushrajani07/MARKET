import os
import json
import time
from pathlib import Path
from typing import Optional, Dict, Any, Callable
from functools import wraps

from kiteconnect import KiteConnect, exceptions as kite_ex
from flask import Flask, request
from dotenv import load_dotenv

load_dotenv()

TOKEN_STORE = Path(".secrets/kite_token.json")
TOKEN_STORE.parent.mkdir(parents=True, exist_ok=True)


def _save_token(data: Dict[str, Any]):
    TOKEN_STORE.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _load_token() -> Optional[Dict[str, Any]]:
    if not TOKEN_STORE.exists():
        return None
    try:
        return json.loads(TOKEN_STORE.read_text(encoding="utf-8"))
    except Exception:
        return None


def start_local_callback_server(expected_state: str, host="127.0.0.1", port=5000, timeout=120) -> Optional[str]:
    app = Flask(__name__)
    result = {"request_token": None, "state": None}

    @app.route("/success")
    def success():
        result["request_token"] = request.args.get("request_token")
        result["state"] = request.args.get("state")
        return "Login successful. You can close this tab.", 200

    from threading import Thread
    t = Thread(target=lambda: app.run(host=host, port=port, debug=False, use_reloader=False))
    t.daemon = True
    t.start()

    start_time = time.time()
    while time.time() - start_time < timeout:
        if result["request_token"]:
            break
        time.sleep(0.2)

    if not result["request_token"]:
        return None
    if expected_state and result["state"] != expected_state:
        return None
    return result["request_token"]


def _oauth_login(kite: KiteConnect) -> KiteConnect:
    api_secret = os.environ.get("KITE_API_SECRET")
    redirect_uri = os.environ.get("REDIRECT_URI", "http://127.0.0.1:5000/success")

    login_url = kite.login_url()
    print(f"[ACTION] Open this URL in a browser to login:\n{login_url}\n")
    print(f"[INFO] After login, Zerodha will redirect to: {redirect_uri}")
    print("[INFO] Starting local callback server to capture request_token...")

    request_token = start_local_callback_server(expected_state=None, host="127.0.0.1", port=5000, timeout=180)
    if not request_token:
        raise RuntimeError("Failed to receive request_token via local callback. "
                           "Check your Kite app config.")

    session_data = kite.generate_session(request_token, api_secret=api_secret)
    access_token = session_data["access_token"]
    kite.set_access_token(access_token)
    _save_token({"access_token": access_token, "created_at": int(time.time())})
    print("[INFO] Kite access token stored.")
    return kite


def _wrap_with_self_heal(kite: KiteConnect) -> KiteConnect:
    def make_wrapper(method: Callable):
        @wraps(method)
        def wrapper(*args, **kwargs):
            try:
                return method(*args, **kwargs)
            except (kite_ex.TokenException, kite_ex.GeneralException) as e:
                print(f"[SELF-HEAL] Token failed during '{method.__name__}': {e}")
                print("[SELF-HEAL] Initiating browser login to refresh token...")
                fresh_kite = _oauth_login(kite)
                return getattr(fresh_kite, method.__name__)(*args, **kwargs)
        return wrapper

    for attr_name in dir(kite):
        attr_val = getattr(kite, attr_name)
        if callable(attr_val) and not attr_name.startswith("_"):
            setattr(kite, attr_name, make_wrapper(attr_val))
    return kite


def get_kite_client() -> KiteConnect:
    api_key = os.environ.get("KITE_API_KEY")
    api_secret = os.environ.get("KITE_API_SECRET")

    if not api_key or not api_secret:
        raise RuntimeError("KITE_API_KEY/KITE_API_SECRET not set in environment.")

    kite = KiteConnect(api_key=api_key)
    kite.set_session_expiry_hook(lambda: print("[WARN] Kite session expired."))

    # Step 1: manual token option
    if input("[PROMPT] Do you already have a valid Access Token? (y/n): ").strip().lower() == "y":
        manual_token = input("[PROMPT] Enter your existing Access Token: ").strip()
        if manual_token:
            try:
                kite.set_access_token(manual_token)
                kite.profile()
                print("[INFO] Provided Access Token is valid. Skipping login.")
                _save_token({"access_token": manual_token, "created_at": int(time.time())})
                return kite
            except Exception as e:
                print(f"[WARN] Provided token failed validation: {e}")
                if input("[PROMPT] Use this token anyway without validation? (y/n): ").strip().lower() == "y":
                    print("[INFO] Using provided token without validation. Will auto-heal if it fails later.")
                    kite.set_access_token(manual_token)
                    _save_token({"access_token": manual_token, "created_at": int(time.time()), "forced": True})
                    return _wrap_with_self_heal(kite)
                print("[INFO] Continuing with normal login flow...")

    # Step 2: stored token
    saved = _load_token()
    if saved and saved.get("access_token"):
        try:
            kite.set_access_token(saved["access_token"])
            kite.profile()
            print("[INFO] Using stored access token.")
            return kite
        except Exception:
            print("[INFO] Stored access token invalid/expired. Re‑authenticating...")

    # Step 3: OAuth login
    return _oauth_login(kite)
