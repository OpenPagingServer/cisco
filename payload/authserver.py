import base64
import json
import os
import threading
import time
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pymysql
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR.parent.parent / ".env"
load_dotenv(ENV_PATH)

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
MODULE_LOG_DIR = Path(os.getenv("OPS_ENDPOINT_MODULE_LOG_DIR", "/var/log/openpagingserver/endpointmodules"))
LOG_FILE = MODULE_LOG_DIR / "cisco" / "authserver.log"
AUTH_IPC_TOKEN = os.getenv("CISCO_AUTH_IPC_TOKEN", "")
CISCO_USERNAME = os.getenv("CISCO_USERNAME", "admin")
CISCO_PASSWORD = os.getenv("CISCO_PASSWORD", "admin")
try:
    AUTH_TTL_SECONDS = int(os.getenv("CISCO_AUTH_TTL_SECONDS", "300"))
except ValueError:
    AUTH_TTL_SECONDS = 300
AUTH_STORE = {}
AUTH_STORE_LOCK = threading.Lock()


def auth_log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(LOG_FILE, "a", encoding="utf-8") as handle:
            handle.write(f"[{timestamp}] {message}\n")
    except Exception:
        pass


def db():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor,
    )


def normalize_device_id(value):
    if value is None:
        return ""
    return "".join(ch for ch in str(value).upper() if ch.isalnum())


def first_query_value(params, names):
    lowered = {key.lower(): values for key, values in params.items()}
    for name in names:
        values = lowered.get(name.lower())
        if values:
            return values[0]
    return ""


def basic_auth_credentials(header):
    if not header or not header.lower().startswith("basic "):
        return "", ""
    try:
        decoded = base64.b64decode(header.split(" ", 1)[1]).decode("utf-8", errors="ignore")
    except Exception:
        return "", ""
    if ":" not in decoded:
        return "", ""
    return decoded.split(":", 1)


def legacy_auth_model(model):
    return str(model or "").strip().startswith("79")


def read_form_params(handler):
    try:
        length = int(handler.headers.get("Content-Length", "0"))
    except ValueError:
        length = 0
    if length <= 0 or length > 65536:
        return {}
    content_type = (handler.headers.get("Content-Type", "") or "").lower()
    body = handler.rfile.read(length).decode("utf-8", errors="ignore")
    if "application/x-www-form-urlencoded" in content_type or "=" in body:
        return parse_qs(body)
    return {}


def merged_params(query_params, body_params):
    merged = dict(query_params)
    for key, values in body_params.items():
        if key in merged:
            merged[key].extend(values)
        else:
            merged[key] = list(values)
    return merged


def store_summary():
    with AUTH_STORE_LOCK:
        devices = sorted(AUTH_STORE.keys())
        return {
            "count": len(AUTH_STORE),
            "devices": devices[:10],
            "ips": {device: AUTH_STORE[device].get("ip") for device in devices[:10]},
            "models": {device: AUTH_STORE[device].get("model") for device in devices[:10]},
        }


def registered_device_for_ip(ip):
    with AUTH_STORE_LOCK:
        for device, credentials in AUTH_STORE.items():
            if str(credentials.get("ip") or "") == str(ip):
                return device
    return ""


def register_credentials(payload):
    credentials = payload.get("credentials") if isinstance(payload, dict) else None
    if not isinstance(credentials, list):
        auth_log("register rejected reason=credentials_not_list")
        return 0
    now = int(time.time())
    next_store = {}
    skipped = 0
    for item in credentials:
        if not isinstance(item, dict):
            skipped += 1
            continue
        device_key = normalize_device_id(item.get("device"))
        ip = str(item.get("ip") or "").strip()
        username = str(item.get("username") or "")
        password = str(item.get("password") or "")
        if not device_key or not ip or not username or not password:
            skipped += 1
            continue
        next_store[device_key] = {
            "username": username,
            "password": password,
            "ip": ip,
            "model": str(item.get("model") or "").strip(),
            "message_key": str(item.get("message_key") or ""),
            "updated": int(item.get("updated") or now),
        }
    with AUTH_STORE_LOCK:
        stale_before = now - AUTH_TTL_SECONDS
        for device_key, item in list(AUTH_STORE.items()):
            try:
                updated = int(item.get("updated") or 0)
            except (TypeError, ValueError):
                updated = 0
            if updated < stale_before:
                AUTH_STORE.pop(device_key, None)
        AUTH_STORE.update(next_store)
        count = len(AUTH_STORE)
    registered_ips = {device: next_store[device].get("ip") for device in sorted(next_store.keys())[:10]}
    registered_models = {device: next_store[device].get("model") for device in sorted(next_store.keys())[:10]}
    auth_log(
        "register accepted "
        f"received={len(credentials)} stored_now={count} added={len(next_store)} skipped={skipped} "
        f"devices={sorted(next_store.keys())[:10]} "
        f"ips={registered_ips} models={registered_models}"
    )
    return count


def clear_credentials(payload):
    devices = payload.get("devices") if isinstance(payload, dict) else None
    message_key = str(payload.get("message_key") or "") if isinstance(payload, dict) else ""
    cleared = 0
    with AUTH_STORE_LOCK:
        if isinstance(devices, list) and devices:
            wanted = {normalize_device_id(device) for device in devices}
            for device_key in list(AUTH_STORE.keys()):
                if device_key in wanted and (not message_key or AUTH_STORE[device_key].get("message_key") == message_key):
                    AUTH_STORE.pop(device_key, None)
                    cleared += 1
        elif message_key:
            for device_key, credentials in list(AUTH_STORE.items()):
                if credentials.get("message_key") == message_key:
                    AUTH_STORE.pop(device_key, None)
                    cleared += 1
    auth_log(f"clear credentials message_key={message_key or '<any>'} cleared={cleared}")
    return cleared


def authrelay_value():
    try:
        conn = db()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT value FROM `endpoints-modulesettings-cisco` "
                    "WHERE parameter=%s LIMIT 1",
                    ("authrelay",),
                )
                row = cur.fetchone()
                return "" if not row else str(row.get("value") or "").strip()
        finally:
            conn.close()
    except Exception:
        return ""


def authorization_result(device_name, username, password, remote_ip):
    device_key = normalize_device_id(device_name)
    if not device_key:
        return False, "missing_device"
    if not username or not password:
        missing = []
        if not username:
            missing.append("username")
        if not password:
            missing.append("password")
        return False, f"missing_{'_'.join(missing)}"

    with AUTH_STORE_LOCK:
        credentials = AUTH_STORE.get(device_key)
        store_count = len(AUTH_STORE)

    if not isinstance(credentials, dict):
        relay = authrelay_value()
        if relay and username == CISCO_USERNAME and password == CISCO_PASSWORD:
             return True, f"authorized method=authrelay relay_val={relay}"
        return False, f"no_credentials_for_device store_count={store_count}"

    model = str(credentials.get("model") or "").strip()
    temp_match = credentials.get("username") == username and credentials.get("password") == password
    legacy_match = legacy_auth_model(model) and CISCO_USERNAME == username and CISCO_PASSWORD == password
    
    if not temp_match and not legacy_match:
        if credentials.get("username") != username and CISCO_USERNAME != username:
            return False, "username_mismatch"
        return False, "password_mismatch"
        
    message_key = credentials.get("message_key", "")
    method = "legacy_79xx" if legacy_match else "temporary"
    return True, f"authorized method={method} device={device_key} model={model or '<unknown>'} message_key={message_key or '<none>'}"


def is_authorized(device_name, username, password, remote_ip):
    authorized, _reason = authorization_result(device_name, username, password, remote_ip)
    return authorized


def send_phone_auth_response(handler, body):
    payload = body.encode("ascii")
    handler.send_response(200)
    handler.send_header("Content-Type", "text/plain")
    handler.send_header("Content-Length", str(len(payload)))
    handler.send_header("Connection", "close")
    handler.end_headers()
    handler.wfile.write(payload)


def reject(handler, reason=""):
    auth_log(f"auth reject action=un-authorized status=200 reason={reason}")
    send_phone_auth_response(handler, "UN-AUTHORIZED")


class AuthHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

    def do_POST(self):
        parsed = urlparse(self.path)
        if not parsed.path.startswith("/__ops/"):
            self.handle_phone_auth(parsed, body_params=read_form_params(self))
            return
        if parsed.path not in ("/__ops/register-auth", "/__ops/clear-auth"):
            auth_log(f"post rejected path={parsed.path} remote={self.client_address[0]} reason=unknown_path")
            self.send_response(404)
            self.end_headers()
            return
        remote_ip = self.client_address[0]
        if remote_ip not in ("127.0.0.1", "::1") or not AUTH_IPC_TOKEN:
            auth_log(
                f"register rejected remote={remote_ip} "
                f"has_token={bool(AUTH_IPC_TOKEN)} reason=forbidden_remote_or_missing_token"
            )
            self.send_response(403)
            self.end_headers()
            return
        if self.headers.get("X-OpenPagingServer-Token", "") != AUTH_IPC_TOKEN:
            auth_log(f"register rejected remote={remote_ip} reason=bad_ipc_token")
            self.send_response(403)
            self.end_headers()
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        if length <= 0 or length > 131072:
            auth_log(f"register rejected remote={remote_ip} length={length} reason=bad_length")
            self.send_response(400)
            self.end_headers()
            return
        try:
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            if parsed.path == "/__ops/clear-auth":
                count = clear_credentials(payload)
            else:
                count = register_credentials(payload)
        except Exception as exc:
            auth_log(f"auth ipc rejected remote={remote_ip} path={parsed.path} reason=parse_error error={exc.__class__.__name__}")
            self.send_response(400)
            self.end_headers()
            return
        auth_log(f"auth ipc response remote={remote_ip} path={parsed.path} status=200 result={count}")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(str(count).encode("ascii"))

    def do_GET(self):
        parsed = urlparse(self.path)
        if not parsed.path.startswith("/__ops/"):
            self.handle_phone_auth(parsed)
        else:
            auth_log(f"get rejected path={parsed.path} remote={self.client_address[0]} reason=ops_path")
            self.send_response(404)
            self.end_headers()

    def handle_phone_auth(self, parsed, body_params=None):
        body_params = body_params or {}
        query_params = parse_qs(parsed.query)
        params = merged_params(query_params, body_params)
        query_keys = sorted(query_params.keys())
        body_keys = sorted(body_params.keys())
        username = first_query_value(params, ("UserID", "userid", "username", "user"))
        password = first_query_value(params, ("Password", "password", "pwd"))
        auth_source = "params"
        basic_username, basic_password = basic_auth_credentials(self.headers.get("Authorization", ""))
        if basic_username or basic_password:
            auth_source = "basic"
            username = basic_username or username
            password = basic_password or password
        device_name = first_query_value(
            params,
            ("devicename", "deviceName", "DeviceName", "device", "name", "mac", "macaddr", "MACAddress"),
        )
        remote_ip = self.client_address[0]
        summary = store_summary()
        registered_device = registered_device_for_ip(remote_ip)
        auth_header = self.headers.get("Authorization", "")
        user_agent = (self.headers.get("User-Agent", "") or "").replace("\r", " ").replace("\n", " ")[:120]
        auth_log(
            "auth request "
            f"method={self.command} remote={remote_ip} path={parsed.path} "
            f"query_keys={query_keys} body_keys={body_keys} "
            f"device_raw={device_name or '<missing>'} device={normalize_device_id(device_name) or '<missing>'} "
            f"username_present={bool(username)} password_present={bool(password)} auth_source={auth_source} "
            f"authorization_header_present={bool(auth_header)} user_agent={user_agent or '<missing>'} "
            f"registered_device_for_remote={registered_device or '<none>'} "
            f"store_count={summary['count']} store_devices={summary['devices']} "
            f"store_ips={summary['ips']} store_models={summary['models']}"
        )

        authorized, reason = authorization_result(device_name, username, password, remote_ip)
        if authorized:
            auth_log(
                f"auth accepted method={self.command} remote={remote_ip} "
                f"device={normalize_device_id(device_name) or '<missing>'} reason={reason}"
            )
            send_phone_auth_response(self, "AUTHORIZED")
        else:
            reject(self, reason)


def main():
    auth_log(
        f"authserver starting bind=0.0.0.0:8082 has_ipc_token={bool(AUTH_IPC_TOKEN)} ttl={AUTH_TTL_SECONDS}"
    )
    HTTPServer(("0.0.0.0", 8082), AuthHandler).serve_forever()


if __name__ == "__main__":
    main()
