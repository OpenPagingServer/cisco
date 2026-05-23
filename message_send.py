
import os
import json
import locale
import random
import secrets
import socket
import struct
import threading
import time
import urllib.parse
import uuid
import xml.sax.saxutils as saxutils
from collections import deque
from datetime import datetime
from pathlib import Path

import pymysql
import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth, HTTPDigestAuth
from active_broadcast_store import fetch_active_broadcast
from broadcasts import legacy_type

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR.parent.parent / ".env"
load_dotenv(ENV_PATH)

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
DEBUG = os.getenv("DEBUG", "").strip().lower() == "true"
LOG_FILE = BASE_DIR / "cisco_debug.log"
DETAILS_STORE_DIR = BASE_DIR / "details_store"
ENDPOINT_TABLE = "endpoints-output-cisco"
SPA_MULTICAST_TABLE = "endpoints-output-cisco-spamulticast"
SPA_XML_EXE_TABLE = "endpoints-output-cisco-spaxmlexe"

USERNAME = os.getenv("CISCO_USERNAME", "admin")
PASSWORD = os.getenv("CISCO_PASSWORD", "admin")
PAYLOAD_TYPE = 0
IPC_PORT = 50000
FRAME_SIZE = 160
SILENCE_FRAME = b"\xff" * FRAME_SIZE
SILENCE_INTERVAL = FRAME_SIZE / 8000
STREAM_IDLE_TIMEOUT = 3.0
WATCHDOG_INTERVAL = 0.1
PRE_AUDIO_GRACE_SECONDS = 6.0
SPA_XML_EXE_AUDIO_DELAY = 0.5
MODULE_SETTINGS_TABLE = "endpoints-modulesettings-cisco"
SOURCE_QUEUE_MAX_FRAMES = 24
LIVE_PAGE_SOURCE_KIND = "livepage"
LIVE_PAGE_MIX_WEIGHT = 5.0
LIVE_PAGE_BACKGROUND_WEIGHT = 0.35
LIVE_PAGE_OVERLAP_BOOST = 1.25

DEFAULT_IMAGE_RESOLUTION = "298x168"
MODEL_EXACT_IMAGE_RESOLUTIONS = {
    "8845": "600x300",
}
MODEL_PREFIX_IMAGE_RESOLUTIONS = {
    "88": "600x300",
}
LEGACY_MONO_IMAGE_MODELS = {"7940", "7941", "7942", "7960", "7961", "7962"}

rtp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
rtp_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
rtp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 262144)
try:
    rtp_sock.setsockopt(socket.IPPROTO_IP, socket.IP_TOS, 0xB8)
except OSError:
    pass

active_streams = {}
streams_lock = threading.Lock()
multicast_sessions = {}
phone_multicast_session = {}
multicast_sessions_lock = threading.Lock()
unicast_sessions = {}
unicast_sessions_lock = threading.Lock()
column_cache = {}
column_cache_lock = threading.Lock()
phone_auth_lock = threading.Lock()
phone_auth_by_ip = {}
missing_streams_logged = set()
spa_xml_server = None

ULAW_DECODE_TABLE = []
for _ulaw in range(256):
    _value = ~_ulaw & 0xFF
    _sign = _value & 0x80
    _exponent = (_value >> 4) & 0x07
    _mantissa = _value & 0x0F
    _sample = ((_mantissa << 3) + 0x84) << _exponent
    _sample -= 0x84
    ULAW_DECODE_TABLE.append(-_sample if _sign else _sample)


def linear_to_ulaw(sample):
    sample = max(-32768, min(32767, int(sample)))
    sign = 0x80 if sample < 0 else 0
    if sample < 0:
        sample = -sample
    sample = min(sample + 0x84, 32635)
    exponent = 7
    mask = 0x4000
    while exponent > 0 and not (sample & mask):
        exponent -= 1
        mask >>= 1
    mantissa = (sample >> (exponent + 3)) & 0x0F
    return (~(sign | (exponent << 4) | mantissa)) & 0xFF


def source_mix_weight(source_kind):
    return LIVE_PAGE_MIX_WEIGHT if source_kind == LIVE_PAGE_SOURCE_KIND else 1.0


def mix_ulaw_frames(weighted_frames):
    items = []
    for item in weighted_frames:
        if not item:
            continue
        if isinstance(item, tuple):
            frame, weight = item
        else:
            frame, weight = item, 1.0
        if frame:
            items.append((frame, float(weight or 1.0)))
    if not items:
        return SILENCE_FRAME
    if len(items) == 1:
        return items[0][0].ljust(FRAME_SIZE, b"\xff")[:FRAME_SIZE]

    has_live = any(weight >= LIVE_PAGE_MIX_WEIGHT for _frame, weight in items)
    if has_live:
        adjusted = [
            (frame, weight if weight >= LIVE_PAGE_MIX_WEIGHT else LIVE_PAGE_BACKGROUND_WEIGHT)
            for frame, weight in items
        ]
        live_weight_total = sum(weight for _frame, weight in adjusted if weight >= LIVE_PAGE_MIX_WEIGHT)
        divisor = max(1.0, live_weight_total / LIVE_PAGE_OVERLAP_BOOST)
    else:
        adjusted = items
        divisor = max(1.0, sum(weight for _frame, weight in adjusted))

    mixed = bytearray(FRAME_SIZE)
    for idx in range(FRAME_SIZE):
        total = 0
        for frame, weight in adjusted:
            value = frame[idx] if idx < len(frame) else 0xFF
            total += ULAW_DECODE_TABLE[value] * weight
        mixed[idx] = linear_to_ulaw(total / divisor)
    return bytes(mixed)


def init_spa_xml_server(server_module):
    global spa_xml_server
    spa_xml_server = server_module


def debug_log(message):
    if not DEBUG:
        return
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as handle:
        handle.write(f"[{timestamp}] {message}\n")


def xml_document(body):
    return f'<?xml version="1.0" encoding="utf-8"?>{body}'


def xml_text_content(value):
    text = "" if value is None else str(value)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return saxutils.escape(text).replace("\n", "&#10;")


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


def normalize_model_number(value):
    token = normalize_device_id(value)
    digits = ""
    started = False
    for ch in token:
        if ch.isdigit():
            digits += ch
            started = True
        elif started:
            break
    return digits or token


def image_resolution_for_model(model_value):
    model = normalize_model_number(model_value)
    if model in MODEL_EXACT_IMAGE_RESOLUTIONS:
        return MODEL_EXACT_IMAGE_RESOLUTIONS[model]
    for prefix, resolution in sorted(MODEL_PREFIX_IMAGE_RESOLUTIONS.items(), key=lambda item: len(item[0]), reverse=True):
        if model.startswith(prefix):
            return resolution
    return DEFAULT_IMAGE_RESOLUTION


def model_uses_legacy_mono_image(model_value):
    return normalize_model_number(model_value) in LEGACY_MONO_IMAGE_MODELS


def image_width_height(resolution):
    try:
        width, height = str(resolution).lower().split("x", 1)
        return int(width), int(height)
    except (TypeError, ValueError):
        return 320, 212


def random_auth_token():
    return "".join(secrets.choice("0123456789") for _ in range(32))


def auth_register_url():
    return os.getenv("CISCO_AUTH_REGISTER_URL", "http://127.0.0.1:8082/__ops/register-auth")


def auth_clear_url():
    return auth_register_url().replace("/__ops/register-auth", "/__ops/clear-auth")


def auth_ipc_token():
    return os.getenv("CISCO_AUTH_IPC_TOKEN", "")


def register_auth_credentials(credentials):
    if not credentials:
        debug_log("auth_register skipped no_credentials")
        return
    token = auth_ipc_token()
    if not token:
        debug_log("auth_register skipped missing_token")
        return
    payload = {"credentials": credentials}
    headers = {"X-OpenPagingServer-Token": token}
    last_error = ""
    debug_log(
        "auth_register start "
        f"url={auth_register_url()} credentials={len(credentials)} "
        f"devices={[item.get('device') for item in credentials[:10]]} "
        f"ips={[item.get('ip') for item in credentials[:10]]}"
    )
    for attempt in range(1, 4):
        try:
            response = requests.post(auth_register_url(), json=payload, headers=headers, timeout=1)
            if response.status_code in (200, 204):
                debug_log(
                    f"auth_register ok attempt={attempt} credentials={len(credentials)} "
                    f"server_body={(response.text or '')[:80]}"
                )
                return
            last_error = f"status={response.status_code} body={(response.text or '')[:80]}"
            debug_log(f"auth_register retry attempt={attempt} error={last_error}")
        except requests.exceptions.RequestException as exc:
            last_error = exc.__class__.__name__
            debug_log(f"auth_register retry attempt={attempt} error={last_error}")
        time.sleep(0.1)
    debug_log(f"auth_register failed credentials={len(credentials)} error={last_error}")


def clear_auth_credentials(message_key, devices=None):
    token = auth_ipc_token()
    if not message_key:
        return
    with phone_auth_lock:
        cleared_local = []
        for ip, credentials in list(phone_auth_by_ip.items()):
            if credentials.get("message_key") != message_key:
                continue
            if devices and credentials.get("device") not in devices:
                continue
            cleared_local.append(credentials.get("device"))
            phone_auth_by_ip.pop(ip, None)
    if not token:
        debug_log(f"auth_clear skipped missing_token message_key={message_key} local={cleared_local}")
        return
    payload = {"message_key": message_key}
    if devices:
        payload["devices"] = list(devices)
    headers = {"X-OpenPagingServer-Token": token}
    try:
        response = requests.post(auth_clear_url(), json=payload, headers=headers, timeout=1)
        debug_log(
            f"auth_clear message_key={message_key} devices={devices or '<all>'} "
            f"local={cleared_local} status={response.status_code} body={(response.text or '')[:80]}"
        )
    except requests.exceptions.RequestException as exc:
        debug_log(
            f"auth_clear failed message_key={message_key} devices={devices or '<all>'} "
            f"local={cleared_local} error={exc.__class__.__name__}"
        )


def mark_auth_used(ip):
    with phone_auth_lock:
        credentials = phone_auth_by_ip.get(str(ip))
    if credentials:
        message_key = credentials.get("message_key")
        device = credentials.get("device")
        debug_log(f"auth_used ip={ip} device={device} message_key={message_key} retained_until_message_end")


def legacy_auth_model(model):
    return str(model or "").strip().startswith("79")


def prepare_auth_credentials(endpoints, message_key):
    now = int(time.time())
    registered = []
    skipped = 0
    reused = 0
    created = 0
    with phone_auth_lock:
        for endpoint in endpoints:
            device_name = normalize_device_id(endpoint.get("macaddr"))
            ip = endpoint.get("ipv4")
            if not device_name or not ip:
                skipped += 1
                continue
            ip = str(ip)
            model = str(endpoint.get("model") or "").strip()
            existing = phone_auth_by_ip.get(ip)
            if (
                existing
                and existing.get("device") == device_name
                and existing.get("ip") == ip
                and existing.get("message_key") == message_key
            ):
                credentials = existing
                credentials["model"] = model
                reused += 1
            else:
                credentials = {
                    "device": device_name,
                    "message_key": message_key,
                    "model": model,
                    "username": random_auth_token(),
                    "password": random_auth_token(),
                    "ip": ip,
                    "updated": now,
                }
                created += 1
            phone_auth_by_ip[str(ip)] = credentials
            registered.append(
                {
                    "device": device_name,
                    "message_key": message_key,
                    "ip": ip,
                    "username": credentials["username"],
                    "password": credentials["password"],
                    "model": model,
                    "updated": credentials.get("updated", now),
                }
            )
    debug_log(
        "prepare_auth_credentials "
        f"message_key={message_key} endpoints={len(endpoints)} registered={len(registered)} "
        f"skipped={skipped} reused={reused} created={created} "
        f"devices={[item.get('device') for item in registered[:10]]}"
    )
    register_auth_credentials(registered)


def credentials_for_ip(ip):
    with phone_auth_lock:
        credentials = phone_auth_by_ip.get(str(ip))
    return credentials


def auth_for_ip(ip):
    credentials = credentials_for_ip(ip)
    if credentials:
        return HTTPBasicAuth(credentials["username"], credentials["password"])
    debug_log(f"auth_for_ip fallback_default ip={ip}")
    return HTTPBasicAuth(USERNAME, PASSWORD)


def auth_debug_for_ip(ip):
    credentials = credentials_for_ip(ip)
    if not credentials:
        return "default"
    username = str(credentials.get("username", ""))
    device = credentials.get("device", "")
    message_key = credentials.get("message_key", "")
    model = credentials.get("model", "")
    return f"dynamic device={device} model={model} message_key={message_key} user_tail={username[-6:] if username else '<empty>'}"


def auth_attempts_for_ip(ip):
    credentials = credentials_for_ip(ip)
    attempts = []
    if credentials:
        username = credentials["username"]
        password = credentials["password"]
        model = credentials.get("model", "")
        if legacy_auth_model(model):
            attempts.append(("dynamic_legacy_basic", HTTPBasicAuth(username, password)))
            attempts.append(("dynamic_legacy_digest", HTTPDigestAuth(username, password)))
        else:
            attempts.append((auth_debug_for_ip(ip), HTTPBasicAuth(username, password)))
    else:
        debug_log(f"auth_for_ip fallback_default ip={ip}")
    attempts.append(("fallback_default_basic", HTTPBasicAuth(USERNAME, PASSWORD)))
    attempts.append(("fallback_default_digest", HTTPDigestAuth(USERNAME, PASSWORD)))
    return attempts


def response_success(response):
    lowered = (response.text or "").lower()
    return (
        response.status_code == 200
        and "xml error" not in lowered
        and "error[4]" not in lowered
        and "ciscoipphoneerror" not in lowered
        and "number=\"4\"" not in lowered
        and "number='4'" not in lowered
        and "protected object" not in lowered
        and "request too large" not in lowered
        and "status=\"4\"" not in lowered
        and "status='4'" not in lowered
    )


def post_phone_execute(ip, xml, auth, auth_label, timeout_seconds):
    response = requests.post(
        f"http://{ip}/CGI/Execute",
        data={"XML": xml},
        auth=auth,
        timeout=timeout_seconds,
    )
    preview = (response.text or "")[:200].replace("\r", " ").replace("\n", " ")
    debug_log(f"POST {ip} auth={auth_label} status={response.status_code} body={preview}")
    return response


def send_phone_request_with_result(ip, xml, timeout_seconds=5):
    try:
        last_status = None
        for auth_label, auth in auth_attempts_for_ip(ip):
            response = post_phone_execute(ip, xml, auth, auth_label, timeout_seconds)
            last_status = response.status_code
            success = response_success(response)
            if success:
                mark_auth_used(ip)
                return True, response.status_code
            if response.status_code != 401 and "ciscoipphoneerror" not in (response.text or "").lower():
                break
        return False, last_status
    except requests.exceptions.RequestException as exc:
        debug_log(f"POST {ip} auth={auth_debug_for_ip(ip)} request_failed error={exc.__class__.__name__}: {exc}")
        return False, None


def table_columns(table_name):
    with column_cache_lock:
        cached = column_cache.get(table_name)
    if cached is not None:
        return cached
    conn = db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                (DB_NAME, table_name),
            )
            columns = {row["COLUMN_NAME"] for row in cur.fetchall()}
    finally:
        conn.close()
    with column_cache_lock:
        column_cache[table_name] = columns
    return columns


def truthy(value):
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    return str(value or "").strip().lower() in ("true", "1", "yes", "on")


def first_nonempty_setting(settings, *names):
    for name in names:
        if name in settings:
            value = settings.get(name)
            if value is not None and str(value).strip() != "":
                return value
    return ""


def format_message_timestamp(value):
    raw = "" if value is None else str(value).strip()
    if not raw:
        return ""
    iso_text = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        dt = datetime.fromisoformat(iso_text)
    except ValueError:
        dt = None
    for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        if dt is not None:
            break
        try:
            dt = datetime.strptime(raw, pattern)
        except ValueError:
            pass
    if dt is None:
        return raw
    local_dt = dt.astimezone()
    if system_prefers_12_hour_time():
        return f"{local_dt.strftime('%Y-%m-%d')} {local_dt.strftime('%I:%M:%S %p').lstrip('0')}"
    return local_dt.strftime("%Y-%m-%d %H:%M:%S")


def system_prefers_12_hour_time():
    try:
        locale.setlocale(locale.LC_TIME, "")
    except Exception:
        pass
    try:
        time_format = locale.nl_langinfo(locale.T_FMT)
        if any(token in time_format for token in ("%I", "%r", "%p")):
            return True
        if any(token in time_format for token in ("%H", "%T")):
            return False
    except Exception:
        pass
    sample = time.strftime("%X")
    return "AM" in sample.upper() or "PM" in sample.upper()


def message_text(shortmessage, longmessage):
    short_text = str(shortmessage or "").strip()
    long_text = str(longmessage or "").strip()
    if not long_text:
        return short_text
    if not short_text:
        return long_text
    if short_text == long_text or long_text.startswith(short_text):
        return long_text
    return f"{short_text}\n\n{long_text}"


def load_cisco_message_settings():
    conn = db()
    try:
        with conn.cursor() as cur:
            settings = {
                "product-name": "",
            }
            try:
                cur.execute("SELECT parameter, value FROM `endpoints-modulesettings-cisco`")
                for row in cur.fetchall():
                    parameter_value = row.get("parameter")
                    if isinstance(parameter_value, bytes):
                        parameter_value = parameter_value.decode("utf-8", errors="ignore")
                    parameter = str(parameter_value or "").strip().lower()
                    if not parameter:
                        continue
                    settings[parameter] = row.get("value")
            except pymysql.MySQLError:
                pass
            try:
                cur.execute(
                    "SELECT value FROM systemsettings WHERE parameter=%s LIMIT 1",
                    ("product_name",),
                )
                row = cur.fetchone()
                if row:
                    settings["product-name"] = str(row.get("value") or "").strip()
            except pymysql.MySQLError:
                pass
            settings["messageinfo-enabled"] = truthy(
                first_nonempty_setting(settings, "messageinfo-enabled")
            )
            settings["messageinfo-showsender"] = truthy(
                first_nonempty_setting(settings, "messageinfo-showsender")
            )
            settings["messageinfo-productname"] = truthy(
                first_nonempty_setting(settings, "messageinfo-productname")
            )
            return settings
    finally:
        conn.close()


def merge_missing_message_fields(message, fallback):
    merged = dict(message or {})
    for key, value in dict(fallback or {}).items():
        if key not in merged:
            merged[key] = value
            continue
        current = merged.get(key)
        if current is None:
            merged[key] = value
            continue
        if isinstance(current, str):
            if not current.strip() and value is not None:
                merged[key] = value
            continue
        if current == "" and value is not None:
            merged[key] = value
    return merged


def hydrate_messageinfo_fields(message, message_id=None):
    enriched = dict(message or {})
    broadcast_id = str(enriched.get("id") or message_id or "").strip()
    if broadcast_id and not str(enriched.get("id") or "").strip():
        enriched["id"] = broadcast_id
    if broadcast_id and broadcast_id != "-1":
        try:
            active_message = fetch_active_broadcast(broadcast_id)
            if active_message:
                enriched = merge_missing_message_fields(enriched, active_message)
        except Exception:
            pass
    try:
        broadcast_columns = table_columns("broadcasts")
        wanted = ["id", "template_id", "sender", "issued", "expires"]
        selected = [column for column in wanted if column in broadcast_columns]
        if not selected:
            return enriched
        conn = db()
        try:
            with conn.cursor() as cur:
                row = None
                if broadcast_id and "id" in broadcast_columns:
                    cur.execute(
                        f"SELECT {', '.join(f'`{column}`' for column in selected)} "
                        "FROM broadcasts WHERE id=%s LIMIT 1",
                        (broadcast_id,),
                    )
                    row = cur.fetchone()
                if row is None:
                    match_clauses = []
                    params = []
                    name = str(enriched.get("name") or "").strip()
                    shortmessage = str(enriched.get("shortmessage") or "").strip()
                    longmessage = str(enriched.get("longmessage") or "").strip()
                    if name and "name" in broadcast_columns:
                        match_clauses.append("`name`=%s")
                        params.append(name)
                    if shortmessage and "shortmessage" in broadcast_columns:
                        match_clauses.append("`shortmessage`=%s")
                        params.append(shortmessage)
                    if longmessage and "longmessage" in broadcast_columns:
                        match_clauses.append("`longmessage`=%s")
                        params.append(longmessage)
                    if match_clauses:
                        order_column = "issued" if "issued" in broadcast_columns else ("id" if "id" in broadcast_columns else None)
                        order_sql = f" ORDER BY `{order_column}` DESC" if order_column else ""
                        cur.execute(
                            f"SELECT {', '.join(f'`{column}`' for column in selected)} "
                            f"FROM broadcasts WHERE {' AND '.join(match_clauses)}{order_sql} LIMIT 1",
                            tuple(params),
                        )
                        row = cur.fetchone()
                if row:
                    enriched = merge_missing_message_fields(enriched, row)
        finally:
            conn.close()
    except Exception:
        return enriched
    return enriched


def send_phone(ip, xml, results, idx):
    success, _status = send_phone_request_with_result(ip, xml, timeout_seconds=5)
    results[idx] = success


def send_parallel_and_wait(ips, xml):
    if not ips:
        debug_log("send_parallel_and_wait called with no IPs")
        return True
    debug_log(f"send_parallel_and_wait ips={ips} xml={xml[:200]}")
    threads = []
    results = [False] * len(ips)
    for idx, ip in enumerate(ips):
        thread = threading.Thread(target=send_phone, args=(ip, xml, results, idx), daemon=True)
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
    debug_log(f"send_parallel_and_wait results={results}")
    return all(results)


def send_parallel_results(ips, xml):
    if not ips:
        debug_log("send_parallel_results called with no IPs")
        return {}
    debug_log(f"send_parallel_results ips={ips} xml={xml[:200]}")
    threads = []
    results = {}
    lock = threading.Lock()

    def worker(ip):
        success, status = send_phone_request_with_result(ip, xml, timeout_seconds=5)
        with lock:
            results[ip] = {"success": success, "status": status}

    for ip in ips:
        thread = threading.Thread(target=worker, args=(ip,), daemon=True)
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
    debug_log(f"send_parallel_results results={results}")
    return results


def send_phone_request(ip, xml, timeout_seconds=5):
    success, _status = send_phone_request_with_result(ip, xml, timeout_seconds=timeout_seconds)
    return success


def send_ready_signal(module_name, stream_id):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        sock.connect(("127.0.0.1", IPC_PORT))
        sock.sendall(f"READY {module_name} {stream_id}\n".encode("utf-8"))
        sock.recv(16)
        sock.close()
        debug_log(f"READY sent module={module_name} stream={stream_id}")
    except Exception:
        debug_log(f"READY failed module={module_name} stream={stream_id}")


def xml_text_message(name, text):
    title = saxutils.escape("" if name is None else str(name))
    body = xml_text_content(text)
    return xml_document(
        "<CiscoIPPhoneText>"
        f"<Title>{title}</Title>"
        "<Prompt>Message text</Prompt>"
        f"<Text>{body}</Text>"
        "</CiscoIPPhoneText>"
    )


def xml_text_message_with_back(name, text, back_url):
    title = saxutils.escape("" if name is None else str(name))
    body = xml_text_content(text)
    url = saxutils.escape(back_url)
    return xml_document(
        "<CiscoIPPhoneText>"
        f"<Title>{title}</Title>"
        "<Prompt>Message text</Prompt>"
        f"<Text>{body}</Text>"
        "<SoftKeyItem>"
        "<Name>Back</Name>"
        f"<URL>{url}</URL>"
        "<Position>1</Position>"
        "</SoftKeyItem>"
        "</CiscoIPPhoneText>"
    )


def local_ip_for_phone(phone_ip):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect((phone_ip, 9))
        return sock.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        sock.close()


def normalize_visual_mode(value):
    token = "" if value is None else str(value).strip().lower()
    if token in ("", "none", "null"):
        return "none"
    if token == "image":
        return "image"
    return "text"


def model_supports_visual(model_value):
    model = "" if model_value is None else str(model_value).strip()
    return model.startswith("79") or model.startswith("88") or bool(normalize_model_number(model))


def xml_image_message(name, short_text, bg_color, symbol, image_url, resolution):
    title = saxutils.escape("" if name is None else str(name))
    url = saxutils.escape(image_url)
    width, height = image_width_height(resolution)
    return xml_document(
        "<CiscoIPPhoneImageFile>"
        f"<Title>{title}</Title>"
        "<Prompt>Select an action</Prompt>"
        f"<Width>{width}</Width>"
        f"<Height>{height}</Height>"
        "<LocationX>0</LocationX>"
        "<LocationY>0</LocationY>"
        f"<URL>{url}</URL>"
        "</CiscoIPPhoneImageFile>"
    )


def build_image_url(phone_ip, short_text, bg_color, symbol, model_value=None):
    base_ip = local_ip_for_phone(phone_ip)
    resolution = image_resolution_for_model(model_value)
    legacy_mono = model_uses_legacy_mono_image(model_value)
    params = {
        "resolution": resolution,
        "bg": "FFFFFF" if legacy_mono else (bg_color or "FFFFFF"),
        "text": "" if short_text is None else str(short_text),
    }
    if legacy_mono:
        params["mono"] = "1"
        params["fg"] = "000000"
    if symbol:
        params["symbol"] = str(symbol)
    return f"http://{base_ip}:6975/thumb?{urllib.parse.urlencode(params)}"


def details_server_url(phone_ip, path, snapshot_id):
    base_ip = local_ip_for_phone(phone_ip)
    port = os.getenv("CISCO_DETAILS_PORT", "6967")
    return f"http://{base_ip}:{port}/{path}?id={urllib.parse.quote(snapshot_id)}"


def build_snapshot_image_url(phone_ip, snapshot_id):
    return details_server_url(phone_ip, "thumb", snapshot_id)


def xml_execute_url(url):
    safe_url = saxutils.escape(url)
    return xml_document(
        "<CiscoIPPhoneExecute>"
        f"<ExecuteItem Priority=\"0\" URL=\"{safe_url}\"/>"
        "</CiscoIPPhoneExecute>"
    )


def persist_details_snapshot(phone_ip, message, settings=None, message_id=None):
    DETAILS_STORE_DIR.mkdir(parents=True, exist_ok=True)
    snapshot_id = uuid.uuid4().hex
    cisco_settings = settings or {}
    source_message_id = str(message_id or message.get("id") or "").strip()
    message = hydrate_messageinfo_fields(message, source_message_id)
    payload = {
        "broadcast_id": message.get("id", "") or "",
        "message_id": source_message_id,
        "template_id": message.get("template_id", "") or "",
        "server_ip": local_ip_for_phone(phone_ip),
        "name": message.get("name", "") or "",
        "shortmessage": message.get("shortmessage", "") or "",
        "longmessage": message.get("longmessage", "") or "",
        "color": (message.get("color") or "").strip() or "FFFFFF",
        "icon": (message.get("icon") or "").strip(),
        "sender": message.get("sender", "") or "",
        "issued": "" if message.get("issued") is None else str(message.get("issued")),
        "expires": "" if message.get("expires") is None else str(message.get("expires")),
        "messageinfo-enabled": bool(cisco_settings.get("messageinfo-enabled")),
        "messageinfo-showsender": bool(cisco_settings.get("messageinfo-showsender")),
        "messageinfo-productname": bool(cisco_settings.get("messageinfo-productname")),
        "product-name": cisco_settings.get("product-name", "") or "",
    }
    debug_log(
        f"persist_details_snapshot snapshot={snapshot_id} broadcast_id={payload['broadcast_id']} "
        f"sender={payload['sender']!r} issued={payload['issued']!r} expires={payload['expires']!r} "
        f"showsender={payload['messageinfo-showsender']}"
    )
    with open(DETAILS_STORE_DIR / f"{snapshot_id}.json", "w", encoding="utf-8") as handle:
        json.dump(payload, handle)
    return snapshot_id


def build_visual_payloads(endpoint, message, cisco_settings=None, message_id=None):
    visual_mode = normalize_visual_mode(endpoint.get("visual"))
    if visual_mode == "none":
        return []
    name = message.get("name", "")
    shortmessage = message.get("shortmessage", "")
    longmessage = message.get("longmessage", "")
    text_body = message_text(shortmessage, longmessage)
    cisco_settings = cisco_settings or load_cisco_message_settings()
    messageinfo_enabled = bool(cisco_settings.get("messageinfo-enabled"))
    model = endpoint.get("model")
    if visual_mode == "image" and model_supports_visual(model):
        color = (message.get("color") or "").strip() or "FFFFFF"
        symbol = (message.get("icon") or "").strip()
        short_text = shortmessage or ""
        resolution = image_resolution_for_model(model)
        image_url = build_image_url(endpoint.get("ipv4"), short_text, color, symbol, model)
        payloads = []
        if str(longmessage or "").strip() or messageinfo_enabled:
            snapshot_id = persist_details_snapshot(endpoint.get("ipv4"), message, cisco_settings, message_id=message_id)
            payloads.append(
                (
                    "image_details",
                    xml_execute_url(details_server_url(endpoint.get("ipv4"), "image", snapshot_id)),
                )
            )
        payloads.append(("image", xml_image_message(name, short_text, color, symbol, image_url, resolution)))
        if longmessage and str(longmessage).strip():
            payloads.append(("text", xml_text_message(name, longmessage)))
        return payloads
    if messageinfo_enabled:
        snapshot_id = persist_details_snapshot(endpoint.get("ipv4"), message, cisco_settings, message_id=message_id)
        return [
            (
                "text_details",
                xml_execute_url(details_server_url(endpoint.get("ipv4"), "text", snapshot_id)),
            ),
            ("text", xml_text_message(name, text_body)),
        ]
    return [("text", xml_text_message(name, text_body))]


def send_visual_payload_sequence(ip, payloads):
    for label, xml in payloads:
        debug_log(f"send_visual_payload_sequence ip={ip} mode={label} xml={xml[:240]}")
        if send_phone_request(ip, xml):
            return True
    return False


def send_endpoint_visuals(endpoints, message, message_id=None):
    cisco_settings = load_cisco_message_settings()
    visual_targets = []
    for endpoint in endpoints:
        ip = endpoint.get("ipv4")
        if not ip:
            continue
        payloads = build_visual_payloads(endpoint, message, cisco_settings, message_id=message_id)
        if not payloads:
            continue
        visual_targets.append((ip, payloads))
    if not visual_targets:
        debug_log("no_visual_targets")
        return True
    debug_log(f"send_endpoint_visuals targets={[(ip, [label for label, _ in payloads]) for ip, payloads in visual_targets]}")
    results = [False] * len(visual_targets)
    for idx, (ip, payloads) in enumerate(visual_targets):
        results[idx] = send_visual_payload_sequence(ip, payloads)
    debug_log(f"send_endpoint_visuals results={results}")
    return all(results)


def xml_spa_execute_url(url):
    safe_url = saxutils.escape(url)
    return (
        "<CiscoIPPhoneExecute>"
        f"<ExecuteItem URL=\"{safe_url}\"/>"
        "</CiscoIPPhoneExecute>"
    )


def post_spa_xml_execute(target, xml):
    ip = target.get("ipv4")
    username = target.get("username")
    password = target.get("password")
    if not ip:
        return False
    auth_attempts = []
    if username or password:
        auth_attempts.append(("basic", HTTPBasicAuth(username, password)))
        auth_attempts.append(("digest", HTTPDigestAuth(username, password)))
    else:
        auth_attempts.append(("none", None))
    for auth_label, auth in auth_attempts:
        try:
            response = requests.post(
                f"http://{ip}/CGI/Execute",
                data={"XML": xml},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                auth=auth,
                timeout=5,
            )
            preview = (response.text or "")[:200].replace("\r", " ").replace("\n", " ")
            debug_log(f"SPA XML EXE POST {ip} auth={auth_label} status={response.status_code} body={preview}")
            if response_success(response):
                return True
            if response.status_code != 401:
                break
        except requests.exceptions.RequestException as exc:
            debug_log(f"SPA XML EXE POST {ip} auth={auth_label} error={exc.__class__.__name__}: {exc}")
            return False
    return False


def send_spa_xml_exe_visuals(targets, message):
    if not targets:
        return True
    if spa_xml_server is None:
        debug_log("send_spa_xml_exe_visuals skipped missing_spa_xml_server")
        return False
    allowed_macs = [target["macaddress"] for target in targets]
    message_id = spa_xml_server.store_text_message(
        message.get("name") or "",
        message.get("shortmessage") or "",
        message.get("longmessage") or "",
        allowed_macs,
    )
    results = []
    for target in targets:
        server_ip = local_ip_for_phone(target["ipv4"])
        port = os.getenv("CISCO_SPA_XML_PORT", "6989")
        url = f"http://{server_ip}:{port}/{message_id}.xml"
        xml = xml_spa_execute_url(url)
        debug_log(
            f"send_spa_xml_exe_visual target={target.get('macaddress')} ip={target.get('ipv4')} url={url}"
        )
        results.append(post_spa_xml_execute(target, xml))
    debug_log(f"send_spa_xml_exe_visuals results={results}")
    return all(results)


def xml_start_multicast(mcast_ip, mcast_port):
    return xml_document(
        "<CiscoIPPhoneExecute>"
        f"<ExecuteItem Priority=\"0\" URL=\"RTPMRx:{mcast_ip}:{mcast_port}\"/>"
        "</CiscoIPPhoneExecute>"
    )


def xml_start_unicast(server_ip, rtp_port):
    return xml_document(
        "<CiscoIPPhoneExecute>"
        f"<ExecuteItem Priority=\"0\" URL=\"RTPRx:{server_ip}:{rtp_port}\"/>"
        "</CiscoIPPhoneExecute>"
    )


def xml_stop_multicast():
    return xml_document(
        "<CiscoIPPhoneExecute>"
        "<ExecuteItem Priority=\"0\" URL=\"RTPMRx:Stop\"/>"
        "</CiscoIPPhoneExecute>"
    )


def xml_stop_unicast():
    return xml_document(
        "<CiscoIPPhoneExecute>"
        "<ExecuteItem Priority=\"0\" URL=\"RTPRx:Stop\"/>"
        "</CiscoIPPhoneExecute>"
    )


def parse_targets(targets):
    target_info = {
        "all": False,
        "endpoint_targets": [],
        "spa_multicast_ids": [],
        "spa_exe_macs": [],
    }
    for target in targets:
        token = str(target).strip()
        if not token:
            continue
        lowered = token.lower()
        if lowered == "all":
            target_info["all"] = True
            continue
        if lowered.startswith("spa-multicast-"):
            value = token[len("spa-multicast-"):].strip()
            if value and value not in target_info["spa_multicast_ids"]:
                target_info["spa_multicast_ids"].append(value)
            continue
        if lowered.startswith("spa-exe-"):
            value = normalize_device_id(token[len("spa-exe-"):].strip())
            if value and value not in target_info["spa_exe_macs"]:
                target_info["spa_exe_macs"].append(value)
            continue
        if token not in target_info["endpoint_targets"]:
            target_info["endpoint_targets"].append(token)
    return target_info


def fetch_spa_multicast_targets(target_info):
    if not target_info["all"] and not target_info["spa_multicast_ids"]:
        return []
    conn = db()
    try:
        with conn.cursor() as cur:
            if target_info["all"]:
                cur.execute(
                    f"SELECT `id`, `address`, `port` FROM `{SPA_MULTICAST_TABLE}` "
                    "WHERE `address` IS NOT NULL AND `address` <> '' AND `port` IS NOT NULL"
                )
                rows = cur.fetchall()
            else:
                placeholders = ",".join(["%s"] * len(target_info["spa_multicast_ids"]))
                cur.execute(
                    f"SELECT `id`, `address`, `port` FROM `{SPA_MULTICAST_TABLE}` "
                    f"WHERE `id` IN ({placeholders})",
                    tuple(target_info["spa_multicast_ids"]),
                )
                rows = cur.fetchall()
    except pymysql.MySQLError as exc:
        debug_log(f"fetch_spa_multicast_targets error={exc}")
        return []
    finally:
        conn.close()
    spa_targets = []
    for row in rows:
        try:
            spa_targets.append(
                {
                    "id": str(row.get("id")),
                    "address": str(row.get("address")),
                    "port": int(row.get("port")),
                }
            )
        except (TypeError, ValueError):
            continue
    return spa_targets


def fetch_spa_xml_exe_targets(target_info):
    if not target_info["all"] and not target_info["spa_exe_macs"]:
        return []
    conn = db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT `id`, `ipv4`, `username`, `password`, `macaddress`, `status` FROM `{SPA_XML_EXE_TABLE}` "
                "WHERE `ipv4` IS NOT NULL AND `ipv4` <> '' AND `macaddress` IS NOT NULL AND `macaddress` <> ''"
            )
            rows = cur.fetchall()
    except pymysql.MySQLError as exc:
        debug_log(f"fetch_spa_xml_exe_targets error={exc}")
        return []
    finally:
        conn.close()
    allowed_macs = set(target_info["spa_exe_macs"])
    targets = []
    for row in rows:
        macaddress = normalize_device_id(row.get("macaddress"))
        if not target_info["all"] and macaddress not in allowed_macs:
            continue
        ipv4 = str(row.get("ipv4") or "").strip()
        status = str(row.get("status") or "").strip()
        if status.lower() == "offline":
            debug_log(f"fetch_spa_xml_exe_targets skip_offline mac={macaddress} ip={ipv4}")
            continue
        if not macaddress or not ipv4:
            continue
        targets.append(
            {
                "id": str(row.get("id")),
                "ipv4": ipv4,
                "username": str(row.get("username") or ""),
                "password": str(row.get("password") or ""),
                "macaddress": macaddress,
                "status": status,
            }
        )
    return targets


def fetch_endpoints_and_message(targets, msg_id):
    endpoint_columns = table_columns(ENDPOINT_TABLE)
    message_columns = table_columns("messages")
    broadcast_columns = table_columns("broadcasts")
    endpoint_select = ["macaddr", "ipv4", "status", "audio"]
    if "model" in endpoint_columns:
        endpoint_select.append("model")
    if "visual" in endpoint_columns:
        endpoint_select.append("visual")
    message_select = ["name", "longmessage", "type"]
    if "shortmessage" in message_columns:
        message_select.append("shortmessage")
    if "color" in message_columns:
        message_select.append("color")
    if "icon" in message_columns:
        message_select.append("icon")
    broadcast_select = ["id", "name", "longmessage", "type", "sender", "issued", "expires"]
    if "template_id" in broadcast_columns:
        broadcast_select.append("template_id")
    if "shortmessage" in broadcast_columns:
        broadcast_select.append("shortmessage")
    if "color" in broadcast_columns:
        broadcast_select.append("color")
    if "icon" in broadcast_columns:
        broadcast_select.append("icon")
    if "expires_rule" in broadcast_columns:
        broadcast_select.append("expires_rule")
    conn = db()
    try:
        with conn.cursor() as cur:
            target_info = parse_targets(targets)
            use_all = target_info["all"]
            cur.execute(
                f"SELECT {', '.join(endpoint_select)} FROM `{ENDPOINT_TABLE}` "
                "WHERE ipv4 IS NOT NULL AND ipv4 <> ''"
            )
            endpoints = cur.fetchall()
            if not use_all:
                normalized_targets = {normalize_device_id(target) for target in target_info["endpoint_targets"] if target}
                endpoints = [
                    endpoint
                    for endpoint in endpoints
                    if normalize_device_id(endpoint.get("macaddr")) in normalized_targets
                ]
            message = fetch_active_broadcast(msg_id)
            history_message = None
            if "id" in broadcast_columns:
                cur.execute(
                    f"SELECT {', '.join(broadcast_select)} FROM broadcasts WHERE id=%s LIMIT 1",
                    (msg_id,),
                )
                history_message = cur.fetchone()
            if message:
                if history_message:
                    message = merge_missing_message_fields(message, history_message)
                if not str(message.get("id") or "").strip():
                    message["id"] = str(msg_id or "").strip()
                message["name"] = message.get("name") or "Broadcast"
                message["type"] = legacy_type(message.get("type"))
            else:
                message = history_message
                if message:
                    if not str(message.get("id") or "").strip():
                        message["id"] = str(msg_id or "").strip()
                    message["name"] = message.get("name") or "Broadcast"
                    message["type"] = legacy_type(message.get("type"))
                if not message:
                    cur.execute(f"SELECT {', '.join(message_select)} FROM messages WHERE messageid=%s", (msg_id,))
                    message = cur.fetchone()
                    if message:
                        message["type"] = legacy_type(message.get("type"))
    finally:
        conn.close()
    if message:
        message = hydrate_messageinfo_fields(message, msg_id)
    debug_log(
        f"fetch_endpoints_and_message targets={targets} "
        f"matched={[(ep.get('macaddr'), ep.get('ipv4'), ep.get('status'), ep.get('audio'), ep.get('model'), ep.get('visual')) for ep in endpoints]} "
        f"message_found={bool(message)} sender={'' if not message else message.get('sender', '')!r} "
        f"message_id={'' if not message else message.get('id', '')!r}"
    )
    return endpoints or [], message


def update_endpoint_status(ip, status):
    conn = db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE `{ENDPOINT_TABLE}` SET status=%s WHERE ipv4=%s",
                (status, ip),
            )
        conn.commit()
    finally:
        conn.close()


def stream_watchdog(stream_id):
    while True:
        with streams_lock:
            stream = active_streams.get(stream_id)
            if stream is None:
                break
            should_send_silence = (
                not stream.get("received_audio")
                and time.time() < stream.get("pre_audio_until", 0)
            )
        if should_send_silence:
            send_stream_frame(stream_id, SILENCE_FRAME, mark_audio=False)
            time.sleep(SILENCE_INTERVAL)
            continue
        with streams_lock:
            stream = active_streams.get(stream_id)
            if stream is None:
                break
            if time.time() - stream["last_seen"] <= STREAM_IDLE_TIMEOUT:
                sleep_for = WATCHDOG_INTERVAL
            else:
                sleep_for = None
        if sleep_for is None:
            stop_stream(stream_id)
            break
        time.sleep(sleep_for)


def allocate_unicast_port():
    used = {int(session.get("port") or 0) for session in unicast_sessions.values()}
    for _ in range(100):
        port = random.randrange(20480, 32768, 2)
        if port not in used:
            return port
    return random.randrange(20480, 32768, 2)


def allocate_multicast_destination():
    used = {
        (session.get("mcast_ip"), int(session.get("mcast_port") or 0))
        for session in multicast_sessions.values()
    }
    for _ in range(100):
        mcast_ip = f"239.{random.randint(1,254)}.{random.randint(1,254)}.{random.randint(1,254)}"
        mcast_port = random.randrange(20480, 32768, 2)
        if (mcast_ip, mcast_port) not in used:
            return mcast_ip, mcast_port
    return f"239.{random.randint(1,254)}.{random.randint(1,254)}.{random.randint(1,254)}", random.randrange(20480, 32768, 2)


def multicast_mixer_loop(session_id):
    next_send_time = time.perf_counter()
    while True:
        with multicast_sessions_lock:
            session = multicast_sessions.get(session_id)
            if not session:
                return
            sources = set(session.get("sources") or set())
            phones = set(session.get("phones") or set())
            if not sources or not phones:
                multicast_sessions.pop(session_id, None)
                stop_phones = [
                    ip for ip in phones
                    if phone_multicast_session.get(ip) == session_id
                ]
                for ip in stop_phones:
                    phone_multicast_session.pop(ip, None)
                debug_log(f"multicast_mixer stop session={session_id} phones={sorted(stop_phones)}")
                if stop_phones:
                    threading.Thread(
                        target=send_parallel_and_wait,
                        args=(stop_phones, xml_stop_multicast()),
                        daemon=True,
                    ).start()
                return
            queues_by_source = session.setdefault("frames", {})
            weights_by_source = session.setdefault("source_weights", {})
            frames = []
            for source in sorted(sources):
                queue = queues_by_source.get(source)
                if queue:
                    frame = queue.popleft()
                    frames.append((frame, weights_by_source.get(source, 1.0)))
            if not frames:
                frames = [(SILENCE_FRAME, 1.0)]
            seq = session["seq"]
            ts = session["ts"]
            ssrc = session["ssrc"]
            destination = (session["mcast_ip"], int(session["mcast_port"]))
            session["seq"] = (seq + 1) % 65536
            session["ts"] = (ts + FRAME_SIZE) % 4294967296
        frame = mix_ulaw_frames(frames)
        packet = struct.pack("!BBHII", 0x80, PAYLOAD_TYPE, seq, ts, ssrc) + frame
        try:
            rtp_sock.sendto(packet, destination)
        except OSError as exc:
            debug_log(
                f"multicast_mixer send failed session={session_id} "
                f"address={destination[0]} port={destination[1]} error={exc}"
            )
        next_send_time += SILENCE_INTERVAL
        sleep_time = next_send_time - time.perf_counter()
        if sleep_time > 0:
            time.sleep(sleep_time)
        else:
            next_send_time = time.perf_counter()


def add_multicast_sources(phone_ips, stream_id, source_kind=None):
    phone_ips = [str(ip or "").strip() for ip in phone_ips or [] if str(ip or "").strip()]
    if not phone_ips:
        return []
    start_groups = []
    session_ids = set()
    with multicast_sessions_lock:
        free_ips = []
        for phone_ip in phone_ips:
            session_id = phone_multicast_session.get(phone_ip)
            session = multicast_sessions.get(session_id) if session_id else None
            if session is None:
                free_ips.append(phone_ip)
                continue
            session.setdefault("sources", set()).add(stream_id)
            session.setdefault("source_weights", {})[stream_id] = source_mix_weight(source_kind)
            session.setdefault("frames", {}).setdefault(stream_id, deque(maxlen=SOURCE_QUEUE_MAX_FRAMES))
            session.setdefault("phones", set()).add(phone_ip)
            session_ids.add(session_id)
            debug_log(f"multicast_mux add_source existing phone={phone_ip} session={session_id} stream={stream_id}")
        if free_ips:
            mcast_ip, mcast_port = allocate_multicast_destination()
            session_id = uuid.uuid4().hex
            session = {
                "id": session_id,
                "mcast_ip": mcast_ip,
                "mcast_port": mcast_port,
                "seq": 0,
                "ts": 0,
                "ssrc": random.randint(1, 0xFFFFFFFF),
                "phones": set(free_ips),
                "sources": {stream_id},
                "source_weights": {stream_id: source_mix_weight(source_kind)},
                "frames": {stream_id: deque(maxlen=SOURCE_QUEUE_MAX_FRAMES)},
            }
            multicast_sessions[session_id] = session
            for phone_ip in free_ips:
                phone_multicast_session[phone_ip] = session_id
            session_ids.add(session_id)
            start_groups.append((dict(session), list(free_ips)))
            threading.Thread(target=multicast_mixer_loop, args=(session_id,), daemon=True).start()
            debug_log(
                f"multicast_mux create session={session_id} stream={stream_id} "
                f"phones={sorted(free_ips)} multicast={mcast_ip}:{mcast_port}"
            )
    with streams_lock:
        stream = active_streams.get(stream_id)
        if stream is not None:
            stream.setdefault("mcast_session_ids", set()).update(session_ids)
    return start_groups


def enqueue_multicast_frame(stream_id, session_ids, frame):
    if not session_ids:
        return
    with multicast_sessions_lock:
        for session_id in session_ids:
            session = multicast_sessions.get(session_id)
            if session is None or stream_id not in session.get("sources", set()):
                continue
            queue = session.setdefault("frames", {}).setdefault(
                stream_id,
                deque(maxlen=SOURCE_QUEUE_MAX_FRAMES),
            )
            queue.append(frame)


def remove_multicast_sources(stream_id, session_ids):
    if not session_ids:
        return
    with multicast_sessions_lock:
        for session_id in session_ids:
            session = multicast_sessions.get(session_id)
            if session is None:
                continue
            session.setdefault("sources", set()).discard(stream_id)
            session.setdefault("frames", {}).pop(stream_id, None)
            session.setdefault("source_weights", {}).pop(stream_id, None)
            debug_log(
                f"multicast_mux remove_source session={session_id} stream={stream_id} "
                f"remaining_sources={sorted(session.get('sources', set()))}"
            )


def remove_multicast_phones(session_id, phone_ips):
    with multicast_sessions_lock:
        session = multicast_sessions.get(session_id)
        if session is None:
            return
        for phone_ip in phone_ips:
            session.setdefault("phones", set()).discard(phone_ip)
            if phone_multicast_session.get(phone_ip) == session_id:
                phone_multicast_session.pop(phone_ip, None)
        debug_log(
            f"multicast_mux remove_phones session={session_id} phones={sorted(phone_ips)} "
            f"remaining_phones={sorted(session.get('phones', set()))}"
        )


def count_multicast_source_phones(stream_id):
    with multicast_sessions_lock:
        total = 0
        for session in multicast_sessions.values():
            if stream_id in session.get("sources", set()):
                total += len(session.get("phones", set()))
        return total


def start_multicast_phone_sessions(phone_ips, stream_id, context_label, source_kind=None):
    start_groups = add_multicast_sources(phone_ips, stream_id, source_kind)
    for session, ips in start_groups:
        session_id = session["id"]
        xml = xml_start_multicast(session["mcast_ip"], session["mcast_port"])
        start_results = send_parallel_results(ips, xml)
        failed_ips = [ip for ip, result in start_results.items() if not result.get("success")]
        if failed_ips:
            remove_multicast_phones(session_id, failed_ips)
            with streams_lock:
                active_stream = active_streams.get(stream_id)
                if active_stream is not None:
                    active_stream.get("phones", set()).difference_update(failed_ips)
            for ip in failed_ips:
                debug_log(
                    f"{context_label} removed failed multicast start ip={ip} "
                    f"status={start_results[ip].get('status')} device_status_unchanged=true"
                )
    return count_multicast_source_phones(stream_id)


def unicast_mixer_loop(phone_ip):
    next_send_time = time.perf_counter()
    while True:
        with unicast_sessions_lock:
            session = unicast_sessions.get(phone_ip)
            if not session:
                return
            sources = set(session.get("sources") or set())
            if not sources:
                unicast_sessions.pop(phone_ip, None)
                stop_xml = xml_stop_unicast()
                threading.Thread(target=send_phone_request, args=(phone_ip, stop_xml), daemon=True).start()
                debug_log(f"unicast_mixer stop phone={phone_ip}")
                return
            queues_by_source = session.setdefault("frames", {})
            weights_by_source = session.setdefault("source_weights", {})
            frames = []
            for source in sorted(sources):
                queue = queues_by_source.get(source)
                if queue:
                    frame = queue.popleft()
                    frames.append((frame, weights_by_source.get(source, 1.0)))
            if not frames:
                frames = [(SILENCE_FRAME, 1.0)]
            seq = session["seq"]
            ts = session["ts"]
            ssrc = session["ssrc"]
            port = session["port"]
            session["seq"] = (seq + 1) % 65536
            session["ts"] = (ts + FRAME_SIZE) % 4294967296
        frame = mix_ulaw_frames(frames)
        packet = struct.pack("!BBHII", 0x80, PAYLOAD_TYPE, seq, ts, ssrc) + frame
        try:
            rtp_sock.sendto(packet, (phone_ip, port))
        except OSError as exc:
            debug_log(f"unicast_mixer send failed phone={phone_ip} port={port} error={exc}")
        next_send_time += SILENCE_INTERVAL
        sleep_time = next_send_time - time.perf_counter()
        if sleep_time > 0:
            time.sleep(sleep_time)
        else:
            next_send_time = time.perf_counter()


def ensure_unicast_session(phone_ip, server_ip):
    with unicast_sessions_lock:
        session = unicast_sessions.get(phone_ip)
        created = False
        if session is None:
            session = {
                "phone_ip": phone_ip,
                "server_ip": server_ip,
                "port": allocate_unicast_port(),
                "seq": 0,
                "ts": 0,
                "ssrc": random.randint(1, 0xFFFFFFFF),
                "sources": set(),
                "source_weights": {},
                "frames": {},
            }
            unicast_sessions[phone_ip] = session
            created = True
        return dict(session), created


def add_unicast_source(phone_ip, server_ip, stream_id, source_kind=None):
    session, created = ensure_unicast_session(phone_ip, server_ip)
    with unicast_sessions_lock:
        current = unicast_sessions.get(phone_ip)
        if current is not None:
            current.setdefault("sources", set()).add(stream_id)
            current.setdefault("source_weights", {})[stream_id] = source_mix_weight(source_kind)
            current.setdefault("frames", {}).setdefault(stream_id, deque(maxlen=SOURCE_QUEUE_MAX_FRAMES))
    if created:
        threading.Thread(target=unicast_mixer_loop, args=(phone_ip,), daemon=True).start()
    return session, created


def enqueue_unicast_frame(stream_id, phone_ips, frame):
    if not phone_ips:
        return
    with unicast_sessions_lock:
        for phone_ip in phone_ips:
            session = unicast_sessions.get(phone_ip)
            if session is None or stream_id not in session.get("sources", set()):
                continue
            queue = session.setdefault("frames", {}).setdefault(
                stream_id,
                deque(maxlen=SOURCE_QUEUE_MAX_FRAMES),
            )
            queue.append(frame)


def remove_unicast_sources(stream_id, phone_ips):
    if not phone_ips:
        return
    with unicast_sessions_lock:
        for phone_ip in phone_ips:
            session = unicast_sessions.get(phone_ip)
            if session is None:
                continue
            session.setdefault("sources", set()).discard(stream_id)
            session.setdefault("frames", {}).pop(stream_id, None)
            session.setdefault("source_weights", {}).pop(stream_id, None)


def build_rtp_destinations(stream, spa_multicast_targets):
    destinations = []
    for target in spa_multicast_targets or []:
        destination = (target["address"], int(target["port"]))
        if destination not in destinations:
            destinations.append(destination)
    return destinations


def send_stream_frame(stream_id, frame, mark_audio=True):
    with streams_lock:
        stream = active_streams.get(stream_id)
        if stream is None:
            return False
        seq = stream["seq"]
        ts = stream["ts"]
        ssrc = stream["ssrc"]
        destinations = list(stream.get("rtp_destinations") or [])
        multicast_session_ids = list(stream.get("mcast_session_ids") or [])
        unicast_phone_ips = list(stream.get("unicast_phone_ips") or [])
        stream["seq"] = (seq + 1) % 65536
        stream["ts"] = (ts + FRAME_SIZE) % 4294967296
        if mark_audio:
            stream["received_audio"] = True
            stream["last_seen"] = time.time()
    packet = struct.pack("!BBHII", 0x80, PAYLOAD_TYPE, seq, ts, ssrc) + frame
    for address, port in destinations:
        try:
            rtp_sock.sendto(packet, (address, port))
        except OSError as exc:
            debug_log(f"send_stream_frame failed stream={stream_id} address={address} port={port} error={exc}")
    enqueue_multicast_frame(stream_id, multicast_session_ids, frame)
    enqueue_unicast_frame(stream_id, unicast_phone_ips, frame)
    return bool(destinations or multicast_session_ids or unicast_phone_ips)


def message_auth_key(stream_id, msg_id):
    return f"{stream_id}:{msg_id}"


def ensure_stream(stream_id, audio_ips, message_key=None, spa_multicast_targets=None, unicast_endpoints=None, source_kind=None):
    spa_multicast_targets = spa_multicast_targets or []
    unicast_endpoints = unicast_endpoints or []
    unicast_phone_ips = [endpoint["ipv4"] for endpoint in unicast_endpoints if endpoint.get("ipv4")]
    with streams_lock:
        stream = active_streams.get(stream_id)
        if stream is None:
            missing_streams_logged.discard(stream_id)
            mcast_ip = f"239.{random.randint(1,254)}.{random.randint(1,254)}.{random.randint(1,254)}"
            mcast_port = random.randrange(20480, 32768, 2)
            stream = {
                "seq": 0,
                "ts": 0,
                "ssrc": random.randint(1, 0xFFFFFFFF),
                "last_seen": time.time(),
                "mcast_ip": mcast_ip,
                "mcast_port": mcast_port,
                "phones": set(),
                "mcast_session_ids": set(),
                "unicast_phone_ips": set(),
                "spa_multicast_targets": list(spa_multicast_targets),
                "received_audio": False,
                "pre_audio_until": time.time() + PRE_AUDIO_GRACE_SECONDS,
                "message_key": message_key,
                "source_kind": source_kind or "broadcast",
            }
            stream["rtp_destinations"] = build_rtp_destinations(stream, spa_multicast_targets)
            active_streams[stream_id] = stream
            threading.Thread(target=stream_watchdog, args=(stream_id,), daemon=True).start()
        elif message_key and not stream.get("message_key"):
            stream["message_key"] = message_key
        if spa_multicast_targets:
            by_id = {target["id"]: target for target in stream.get("spa_multicast_targets", [])}
            for target in spa_multicast_targets:
                by_id[target["id"]] = target
            stream["spa_multicast_targets"] = list(by_id.values())
            stream["rtp_destinations"] = build_rtp_destinations(stream, stream["spa_multicast_targets"])
        new_ips = [ip for ip in audio_ips if ip not in stream["phones"]]
        stream["phones"].update(audio_ips)
        new_unicast_ips = [ip for ip in unicast_phone_ips if ip not in stream["unicast_phone_ips"]]
        stream["unicast_phone_ips"].update(unicast_phone_ips)
        return stream.copy(), new_ips, new_unicast_ips


def remove_stream_phone(stream_id, ip):
    with streams_lock:
        stream = active_streams.get(stream_id)
        if stream is None:
            return
        stream["phones"].discard(ip)
        debug_log(f"remove_stream_phone stream={stream_id} ip={ip} remaining={sorted(stream['phones'])}")


def handle_dispatch(action, stream_id, msg_id, targets):
    normalized_targets = []
    for target in targets:
        if target and target not in normalized_targets:
            normalized_targets.append(target)
    if not normalized_targets:
        if action == "prepare_audio":
            send_ready_signal("cisco", stream_id)
        return
    debug_log(f"handle_dispatch action={action} stream={stream_id} msg={msg_id} targets={normalized_targets}")
    target_info = parse_targets(normalized_targets)
    spa_multicast_targets = fetch_spa_multicast_targets(target_info)
    spa_xml_exe_targets = fetch_spa_xml_exe_targets(target_info)
    endpoints, message = fetch_endpoints_and_message(normalized_targets, msg_id)
    message_key = message_auth_key(stream_id, msg_id)
    prepare_auth_credentials(endpoints, message_key)
    if not message:
        if action == "prepare_audio":
            send_ready_signal("cisco", stream_id)
        debug_log(f"message_not_found msg={msg_id}")
        clear_auth_credentials(message_key)
        return
    msg_type = message.get("type", "text+audio")
    name = message.get("name", "")
    longmessage = message.get("longmessage", "")
    online_endpoints = [
        endpoint
        for endpoint in endpoints
        if endpoint.get("ipv4") and endpoint.get("status") in ("Unchecked", "Online")
    ]
    debug_log(
        f"online_endpoints={[(ep.get('macaddr'), ep.get('ipv4'), ep.get('audio'), ep.get('model'), ep.get('visual')) for ep in online_endpoints]} "
        f"spa_multicast_targets={[(target.get('id'), target.get('address'), target.get('port')) for target in spa_multicast_targets]} "
        f"spa_xml_exe_targets={[(target.get('id'), target.get('ipv4'), target.get('macaddress')) for target in spa_xml_exe_targets]} "
        f"msg_type={msg_type}"
    )
    text_success = True
    if msg_type in ("text", "text+audio"):
        if online_endpoints:
            text_success = send_endpoint_visuals(online_endpoints, message, message_id=msg_id)
        else:
            debug_log("no_text_endpoints")
        if msg_type == "text":
            spa_text_success = send_spa_xml_exe_visuals(spa_xml_exe_targets, message)
            text_success = text_success and spa_text_success
    if msg_type not in ("audio", "text+audio"):
        if action == "prepare_audio":
            send_ready_signal("cisco", stream_id)
        if text_success:
            clear_auth_credentials(message_key)
        else:
            debug_log(f"text message did not fully succeed; retaining auth until TTL message_key={message_key}")
        return
    audio_ips = [
        endpoint["ipv4"]
        for endpoint in online_endpoints
        if endpoint.get("audio") == "Multicast"
    ]
    unicast_endpoints = [
        endpoint
        for endpoint in online_endpoints
        if endpoint.get("audio") == "Unicast"
    ]
    if action != "prepare_audio":
        debug_log("audio message arrived on non-prepare action")
        clear_auth_credentials(message_key)
        return
    if not audio_ips and not unicast_endpoints and not spa_multicast_targets:
        debug_log("no_audio_ips")
        if spa_xml_exe_targets:
            send_spa_xml_exe_visuals(spa_xml_exe_targets, message)
        send_ready_signal("cisco", stream_id)
        clear_auth_credentials(message_key)
        return
    stream, new_ips, new_unicast_ips = ensure_stream(
        stream_id,
        audio_ips,
        message_key,
        spa_multicast_targets,
        unicast_endpoints,
        "broadcast",
    )
    debug_log(
        f"prepare_audio stream={stream_id} multicast={stream['mcast_ip']}:{stream['mcast_port']} "
        f"new_ips={new_ips} new_unicast_ips={new_unicast_ips} "
        f"spa_multicast_targets={[(target.get('id'), target.get('address'), target.get('port')) for target in spa_multicast_targets]}"
    )
    active_multicast_phone_count = start_multicast_phone_sessions(audio_ips, stream_id, "prepare_audio", "broadcast")
    if audio_ips and active_multicast_phone_count == 0 and not new_unicast_ips and not spa_multicast_targets:
        debug_log(f"prepare_audio no active Cisco phones after multicast start failures stream={stream_id}")
        stop_stream(stream_id)
        send_ready_signal("cisco", stream_id)
        return
    for endpoint in unicast_endpoints:
        ip = endpoint.get("ipv4")
        if not ip or ip not in new_unicast_ips:
            continue
        server_ip = local_ip_for_phone(ip)
        session, created = add_unicast_source(ip, server_ip, stream_id, "broadcast")
        if created:
            result = send_phone_request_with_result(ip, xml_start_unicast(server_ip, session["port"]))
            if not result.get("success"):
                remove_unicast_sources(stream_id, [ip])
                with streams_lock:
                    active_stream = active_streams.get(stream_id)
                    if active_stream is not None:
                        active_stream.get("unicast_phone_ips", set()).discard(ip)
                debug_log(
                    f"prepare_audio removed failed unicast start ip={ip} status={result.get('status')} "
                    "device_status_unchanged=true"
                )
        else:
            debug_log(f"prepare_audio multiplexed unicast ip={ip} stream={stream_id} port={session['port']}")
    if msg_type == "text+audio" and spa_xml_exe_targets:
        debug_log(f"prepare_audio delaying spa xml exe seconds={SPA_XML_EXE_AUDIO_DELAY}")
        time.sleep(SPA_XML_EXE_AUDIO_DELAY)
        send_spa_xml_exe_visuals(spa_xml_exe_targets, message)
    send_ready_signal("cisco", stream_id)


def handle_api(command_string):
    parts = str(command_string).strip().split()
    if len(parts) < 4:
        return
    handle_dispatch(parts[0], parts[2], parts[3], [parts[1]])


def receive_audio(chunk, stream_id):
    with streams_lock:
        stream = active_streams.get(stream_id)
        if stream is None:
            if stream_id not in missing_streams_logged:
                missing_streams_logged.add(stream_id)
                debug_log(f"receive_audio missing_stream stream={stream_id} bytes={len(chunk)}")
            return
    offset = 0
    while offset < len(chunk):
        frame = chunk[offset:offset + FRAME_SIZE]
        if len(frame) < FRAME_SIZE:
            frame = frame.ljust(FRAME_SIZE, b"\xff")
        send_stream_frame(stream_id, frame, mark_audio=True)
        offset += FRAME_SIZE
    with streams_lock:
        if stream_id in active_streams:
            seq = active_streams[stream_id]["seq"]
            ts = active_streams[stream_id]["ts"]
        else:
            seq = None
            ts = None
    debug_log(f"receive_audio stream={stream_id} bytes={len(chunk)} seq={seq} ts={ts}")


def stop_stream(stream_id):
    with streams_lock:
        stream = active_streams.pop(stream_id, None)
    if not stream:
        debug_log(f"stop_stream missing stream={stream_id}")
        return
    phones = sorted(stream["phones"])
    multicast_session_ids = list(stream.get("mcast_session_ids") or [])
    unicast_phones = sorted(stream.get("unicast_phone_ips") or [])
    message_key = stream.get("message_key")
    debug_log(
        f"stop_stream stream={stream_id} phones={phones} "
        f"multicast_sessions={multicast_session_ids} unicast_phones={unicast_phones} "
        f"message_key={message_key}"
    )
    remove_multicast_sources(stream_id, multicast_session_ids)
    remove_unicast_sources(stream_id, unicast_phones)
    stopped = True
    if message_key and stopped:
        clear_auth_credentials(message_key)
    elif message_key:
        debug_log(f"stop_stream did not fully succeed; retaining auth until TTL message_key={message_key}")


def end_stream(stream_id):
    stop_stream(stream_id)
