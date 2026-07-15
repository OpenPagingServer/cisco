
import json
import ipaddress
import locale
import os
import time
import sqlite3
import shutil
import threading
import urllib.parse
import urllib.request
import xml.sax.saxutils as saxutils
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pymysql
from dotenv import load_dotenv
from active_broadcast_store import DB_PATH as ACTIVE_BROADCAST_DB_PATH, fetch_active_broadcast

BASE_DIR = Path(__file__).resolve().parent
STORE_DIR = BASE_DIR / "details_store"
server = None
thread = None
ENV_PATH = BASE_DIR.parent.parent / ".env"
load_dotenv(ENV_PATH)
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
MODULE_SETTINGS_TABLE = "endpoints-modulesettings-cisco"
DEBUG = os.getenv("DEBUG", "").strip().lower() == "true"
MODULE_LOG_DIR = Path(os.getenv("OPS_ENDPOINT_MODULE_LOG_DIR", "/var/log/openpagingserver/endpointmodules"))
LOG_FILE = MODULE_LOG_DIR / "cisco" / "details_server.log"


def debug_log(message):
    if not DEBUG:
        return
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(LOG_FILE, "a", encoding="utf-8") as handle:
            handle.write(f"[{timestamp}] details_server {message}\n")
    except Exception:
        pass


def xml_document(body):
    return f'<?xml version="1.0" encoding="utf-8"?>{body}'


def xml_text_content(value):
    text = "" if value is None else str(value)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return saxutils.escape(text).replace("\n", "&#10;")


def xml_services_exit_uri():
    return "Init:Services"


def reset_store():
    shutil.rmtree(STORE_DIR, ignore_errors=True)
    STORE_DIR.mkdir(parents=True, exist_ok=True)


def store_snapshot(snapshot_id, payload):
    STORE_DIR.mkdir(parents=True, exist_ok=True)
    with open(STORE_DIR / f"{snapshot_id}.json", "w", encoding="utf-8") as handle:
        json.dump(payload, handle)


def load_snapshot(snapshot_id):
    path = STORE_DIR / f"{snapshot_id}.json"
    if not path.is_file():
        return None
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def db():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor,
    )


def table_columns(table_name):
    conn = db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                (DB_NAME, table_name),
            )
            return {row["COLUMN_NAME"] for row in cur.fetchall()}
    finally:
        conn.close()


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


def first_nonempty_value(*values):
    for value in values:
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    return ""


def is_ipv6_address(value):
    try:
        return ipaddress.ip_address(str(value or "").strip()).version == 6
    except ValueError:
        return False


def http_host(value):
    host = str(value or "").strip()
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1].strip()
    if is_ipv6_address(host):
        return f"[{host}]"
    return host


def http_url(server_ip, path, port):
    return f"http://{http_host(server_ip)}:{port}{path}"


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


def parse_message_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    raw = str(value).strip()
    if not raw:
        return None
    iso_text = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        return datetime.fromisoformat(iso_text)
    except ValueError:
        pass
    for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(raw, pattern)
        except ValueError:
            continue
    return None


def format_message_timestamp(value):
    dt = parse_message_datetime(value)
    if dt is None:
        return "" if value is None else str(value).strip()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    local_dt = dt.astimezone()
    if system_prefers_12_hour_time():
        date_part = local_dt.strftime("%Y-%m-%d")
        time_part = local_dt.strftime("%I:%M:%S %p").lstrip("0")
        return f"{date_part} {time_part}"
    return local_dt.strftime("%Y-%m-%d %H:%M:%S")


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


def load_live_messageinfo_settings():
    settings = {
        "messageinfo-enabled": False,
        "messageinfo-showsender": False,
        "messageinfo-productname": False,
        "product-name": "",
        "_settings-loaded": False,
    }
    try:
        conn = db()
        try:
            with conn.cursor() as cur:
                try:
                    cur.execute("SELECT parameter, value FROM `endpoints-modulesettings-cisco`")
                    raw_settings = {}
                    for row in cur.fetchall():
                        parameter_value = row.get("parameter")
                        if isinstance(parameter_value, bytes):
                            parameter_value = parameter_value.decode("utf-8", errors="ignore")
                        parameter = str(parameter_value or "").strip().lower()
                        if parameter:
                            raw_settings[parameter] = row.get("value")
                    if "messageinfo-showsender" not in raw_settings:
                        cur.execute(
                            "SELECT value FROM `endpoints-modulesettings-cisco` "
                            "WHERE `parameter` = %s LIMIT 1",
                            ("messageinfo-showsender",),
                        )
                        row = cur.fetchone()
                        if row:
                            raw_settings["messageinfo-showsender"] = row.get("value")
                    settings["messageinfo-enabled"] = truthy(
                        first_nonempty_setting(raw_settings, "messageinfo-enabled")
                    )
                    settings["messageinfo-showsender"] = truthy(
                        first_nonempty_setting(raw_settings, "messageinfo-showsender")
                    )
                    settings["messageinfo-productname"] = truthy(
                        first_nonempty_setting(raw_settings, "messageinfo-productname")
                    )
                    settings["_settings-loaded"] = True
                    debug_log(
                        "settings_loaded "
                        f"enabled={settings['messageinfo-enabled']} "
                        f"showsender={settings['messageinfo-showsender']} "
                        f"productname={settings['messageinfo-productname']} "
                        f"raw={raw_settings!r}"
                    )
                except Exception:
                    debug_log("settings_load_error")
                try:
                    cur.execute(
                        "SELECT value FROM systemsettings WHERE parameter=%s LIMIT 1",
                        ("product_name",),
                    )
                    row = cur.fetchone()
                    if row:
                        settings["product-name"] = str(row.get("value") or "").strip()
                except Exception:
                    pass
        finally:
            conn.close()
    except Exception:
        return settings
    return settings


def messageinfo_flag_enabled(settings, snapshot, key):
    return bool((settings or {}).get(key) or (snapshot or {}).get(key))


def messageinfo_setting_enabled(settings, snapshot, key):
    if (settings or {}).get("_settings-loaded"):
        return bool((settings or {}).get(key))
    return messageinfo_flag_enabled(settings, snapshot, key)


def messageinfo_visible(settings, snapshot):
    if messageinfo_setting_enabled(settings, snapshot, "messageinfo-enabled"):
        return True
    return bool(messageinfo_setting_enabled(settings, snapshot, "messageinfo-productname"))


def build_messageinfo_lines(settings, snapshot):
    lines = []
    show_sender = messageinfo_setting_enabled(settings, snapshot, "messageinfo-showsender")
    sender = first_nonempty_value(
        snapshot.get("sender"),
        snapshot.get("message_sender"),
        snapshot.get("from"),
        snapshot.get("caller_id"),
    )
    if show_sender:
        sender = sender or "Unknown"
        lines.append(f'Sent by: {sender}')
    issued = format_message_timestamp(snapshot.get("issued"))
    if issued:
        lines.append(f"Sent at: {issued}")
    expires = format_message_timestamp(snapshot.get("expires"))
    if expires:
        lines.append(f"Expires at: {expires}")
    if messageinfo_setting_enabled(settings, snapshot, "messageinfo-productname"):
        product_name = str(settings.get("product-name") or snapshot.get("product-name") or "").strip()
        if product_name:
            if lines:
                lines.append("")
            lines.append(product_name)
    return lines


def hydrate_from_active_store_snapshot_match(snapshot):
    enriched = dict(snapshot or {})
    if not ACTIVE_BROADCAST_DB_PATH.exists():
        return enriched
    try:
        conn = sqlite3.connect(str(ACTIVE_BROADCAST_DB_PATH))
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                "SELECT id, template_id, sender, issued, expires, payload FROM active_broadcasts "
                "ORDER BY issued DESC LIMIT 100"
            ).fetchall()
        finally:
            conn.close()
    except Exception:
        return enriched

    broadcast_id = first_nonempty_value(enriched.get("broadcast_id"), enriched.get("id"))
    template_id = first_nonempty_value(enriched.get("template_id"), enriched.get("message_id"))
    name = str(enriched.get("name") or "").strip()
    shortmessage = str(enriched.get("shortmessage") or "").strip()
    longmessage = str(enriched.get("longmessage") or "").strip()
    for row in rows:
        try:
            payload = json.loads(row["payload"] or "{}")
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        row_id = first_nonempty_value(row["id"] if "id" in row.keys() else "", payload.get("id"))
        row_template_id = first_nonempty_value(
            row["template_id"] if "template_id" in row.keys() else "",
            payload.get("template_id"),
        )
        id_matches = bool(broadcast_id and row_id == broadcast_id)
        template_matches = bool(template_id and row_template_id == template_id)
        payload_name = str(payload.get("name") or "").strip()
        payload_shortmessage = str(payload.get("shortmessage") or "").strip()
        payload_longmessage = str(payload.get("longmessage") or "").strip()
        content_can_match = bool(name or shortmessage or longmessage)
        if not id_matches and not template_matches:
            if not content_can_match:
                continue
            if name and payload_name and name != payload_name:
                continue
            if shortmessage and payload_shortmessage and shortmessage != payload_shortmessage:
                continue
            if longmessage and payload_longmessage and longmessage != payload_longmessage:
                continue
        for key in ("id", "template_id", "sender", "issued", "expires"):
            if not str(enriched.get(key) or "").strip():
                if key == "id":
                    value = row["id"]
                elif key == "template_id":
                    value = row["template_id"] if "template_id" in row.keys() else None
                else:
                    value = payload.get(key, row[key] if key in row.keys() else None)
                if value is not None:
                    enriched[key] = value
        break
    return enriched


def hydrate_snapshot_message_fields(snapshot):
    enriched = dict(snapshot or {})
    broadcast_id = first_nonempty_value(enriched.get("broadcast_id"), enriched.get("id"))
    template_id = first_nonempty_value(enriched.get("template_id"), enriched.get("message_id"))
    if broadcast_id:
        try:
            active_message = fetch_active_broadcast(broadcast_id)
            if active_message:
                for key in ("id", "template_id", "sender", "issued", "expires"):
                    if not str(enriched.get(key) or "").strip():
                        value = active_message.get(key)
                        if value is not None:
                            enriched[key] = value
                if not str(enriched.get("broadcast_id") or "").strip() and str(active_message.get("id") or "").strip():
                    enriched["broadcast_id"] = active_message.get("id")
        except Exception:
            pass
    if not str(enriched.get("sender") or "").strip():
        enriched = hydrate_from_active_store_snapshot_match(enriched)
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
                if row is None and template_id and "template_id" in broadcast_columns:
                    order_column = "issued" if "issued" in broadcast_columns else ("id" if "id" in broadcast_columns else None)
                    order_sql = f" ORDER BY `{order_column}` DESC" if order_column else ""
                    cur.execute(
                        f"SELECT {', '.join(f'`{column}`' for column in selected)} "
                        f"FROM broadcasts WHERE template_id=%s{order_sql} LIMIT 1",
                        (template_id,),
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
                if not row:
                    return enriched
                for key in selected:
                    if not str(enriched.get(key) or "").strip():
                        value = row.get(key)
                        if value is not None:
                            enriched[key] = value
                if not str(enriched.get("broadcast_id") or "").strip() and str(row.get("id") or "").strip():
                    enriched["broadcast_id"] = row.get("id")
        finally:
            conn.close()
    except Exception:
        return enriched
    return enriched


def thumb_source_url(server_ip, snapshot):
    resolution = snapshot.get("resolution") or "600x300"
    params = {
        "resolution": resolution,
        "bg": snapshot.get("color") or "FFFFFF",
        "text": snapshot.get("shortmessage") or "",
    }
    icon = snapshot.get("icon") or ""
    if icon:
        params["symbol"] = icon
    return f"{http_url(server_ip, '/thumb', 6975)}?{urllib.parse.urlencode(params)}"


def image_url(server_ip, snapshot_id):
    return f"{http_url(server_ip, '/thumb', 6967)}?id={urllib.parse.quote(snapshot_id)}"


def image_page(server_ip, snapshot_id, snapshot):
    live_settings = load_live_messageinfo_settings()
    snapshot = hydrate_snapshot_message_fields(snapshot)
    title = saxutils.escape(snapshot.get("name") or "")
    image = saxutils.escape(image_url(server_ip, snapshot_id))
    has_text = bool((snapshot.get("longmessage") or "").strip())
    has_info = messageinfo_visible(live_settings, snapshot)
    has_details = has_text or has_info
    model = snapshot.get("model") or ""
    parts = [
        "<CiscoIPPhoneImageFile>",
        f"<Title>{title}</Title>",
    ]
    if model not in ("9811", "9851", "9861", "9871"):
        parts.append("<Prompt>Select an action</Prompt>")
    parts.extend([
        "<LocationX>-1</LocationX>",
        "<LocationY>-1</LocationY>",
        f"<URL>{image}</URL>",
        "<SoftKeyItem>",
        "<Name>Exit</Name>",
        f"<URL>{xml_services_exit_uri()}</URL>",
        "<Position>1</Position>",
        "</SoftKeyItem>",
    ])
    if has_details:
        detail_path = "details" if has_text else "info"
        detail_url = saxutils.escape(f"{http_url(server_ip, f'/{detail_path}', 6967)}?id={urllib.parse.quote(snapshot_id)}")
        parts.extend(
            [
                "<SoftKeyItem>",
                "<Name>Details</Name>",
                f"<URL>{detail_url}</URL>",
                "<Position>4</Position>",
                "</SoftKeyItem>",
            ]
        )
    parts.append("</CiscoIPPhoneImageFile>")
    return xml_document("".join(parts))


def details_page(server_ip, snapshot_id, snapshot):
    live_settings = load_live_messageinfo_settings()
    snapshot = hydrate_snapshot_message_fields(snapshot)
    title = saxutils.escape(snapshot.get("name") or "")
    body = xml_text_content(snapshot.get("longmessage") or "")
    back_url = saxutils.escape(f"{http_url(server_ip, '/image', 6967)}?id={urllib.parse.quote(snapshot_id)}")
    parts = [
        "<CiscoIPPhoneText>",
        f"<Title>{title}</Title>",
        "<Prompt>Message text</Prompt>",
        f"<Text>{body}</Text>",
        "<SoftKeyItem>",
        "<Name>Back</Name>",
        f"<URL>{back_url}</URL>",
        "<Position>1</Position>",
        "</SoftKeyItem>",
    ]
    if messageinfo_visible(live_settings, snapshot):
        info_url = saxutils.escape(f"{http_url(server_ip, '/info', 6967)}?id={urllib.parse.quote(snapshot_id)}")
        parts.extend(
            [
                "<SoftKeyItem>",
                "<Name>Info</Name>",
                f"<URL>{info_url}</URL>",
                "<Position>4</Position>",
                "</SoftKeyItem>",
            ]
        )
    parts.append("</CiscoIPPhoneText>")
    return xml_document("".join(parts))


def text_page(server_ip, snapshot_id, snapshot):
    live_settings = load_live_messageinfo_settings()
    snapshot = hydrate_snapshot_message_fields(snapshot)
    title = saxutils.escape(snapshot.get("name") or "")
    body = xml_text_content(message_text(snapshot.get("shortmessage"), snapshot.get("longmessage")))
    parts = [
        "<CiscoIPPhoneText>",
        f"<Title>{title}</Title>",
        "<Prompt>Message text</Prompt>",
        f"<Text>{body}</Text>",
        "<SoftKeyItem>",
        "<Name>Exit</Name>",
        "<URL>SoftKey:Exit</URL>",
        "<Position>1</Position>",
        "</SoftKeyItem>",
    ]
    if messageinfo_visible(live_settings, snapshot):
        info_url = saxutils.escape(f"{http_url(server_ip, '/info', 6967)}?id={urllib.parse.quote(snapshot_id)}&source=text")
        parts.extend(
            [
                "<SoftKeyItem>",
                "<Name>Info</Name>",
                f"<URL>{info_url}</URL>",
                "<Position>4</Position>",
                "</SoftKeyItem>",
            ]
        )
    parts.append("</CiscoIPPhoneText>")
    return xml_document("".join(parts))


def info_page(server_ip, snapshot_id, snapshot, source="image"):
    live_settings = load_live_messageinfo_settings()
    snapshot = hydrate_snapshot_message_fields(snapshot)
    title = saxutils.escape(snapshot.get("name") or "")
    is_text_source = str(source or "").strip().lower() == "text"
    has_long_text = bool(str(snapshot.get("longmessage") or "").strip())
    back_target = "text" if is_text_source else "image"
    back_url = saxutils.escape(f"{http_url(server_ip, f'/{back_target}', 6967)}?id={urllib.parse.quote(snapshot_id)}")
    text_url = saxutils.escape(f"{http_url(server_ip, '/details', 6967)}?id={urllib.parse.quote(snapshot_id)}")
    lines = build_messageinfo_lines(live_settings, snapshot)
    debug_log(
        f"info_page snapshot={snapshot_id} source={source!r} "
        f"sender={first_nonempty_value(snapshot.get('sender'), snapshot.get('message_sender'), snapshot.get('from'), snapshot.get('caller_id'))!r} "
        f"showsender={messageinfo_setting_enabled(live_settings, snapshot, 'messageinfo-showsender')} "
        f"enabled={messageinfo_setting_enabled(live_settings, snapshot, 'messageinfo-enabled')} "
        f"lines={lines!r}"
    )
    body = xml_text_content("\n".join(lines))
    parts = [
        "<CiscoIPPhoneText>",
        f"<Title>{title}</Title>",
        "<Prompt>Info</Prompt>",
        f"<Text>{body}</Text>",
        "<SoftKeyItem>",
        f"<Name>{'Exit' if is_text_source else 'Back'}</Name>",
        f"<URL>{'SoftKey:Exit' if is_text_source else back_url}</URL>",
        "<Position>1</Position>",
        "</SoftKeyItem>",
    ]
    if is_text_source or has_long_text:
        parts.extend(
            [
                "<SoftKeyItem>",
                "<Name>Text</Name>",
                f"<URL>{back_url if is_text_source else text_url}</URL>",
                "<Position>4</Position>",
                "</SoftKeyItem>",
            ]
        )
    parts.append("</CiscoIPPhoneText>")
    return xml_document("".join(parts))


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path not in ("/image", "/details", "/info", "/text", "/thumb"):
            self.send_error(404)
            return
        snapshot_id = urllib.parse.parse_qs(parsed.query).get("id", [""])[0]
        snapshot = load_snapshot(snapshot_id)
        if snapshot is None:
            self.send_error(404)
            return
        server_ip = snapshot.get("server_ip") or self.server.server_address[0]
        if parsed.path == "/thumb":
            try:
                with urllib.request.urlopen(thumb_source_url(server_ip, snapshot), timeout=5) as response:
                    body = response.read()
                    content_type = response.headers.get_content_type()
            except Exception:
                self.send_error(502)
                return
            self.send_response(200)
            self.send_header("Content-Type", content_type or "image/png")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path == "/image":
            body = image_page(server_ip, snapshot_id, snapshot)
        elif parsed.path == "/info":
            source = urllib.parse.parse_qs(parsed.query).get("source", ["image"])[0]
            body = info_page(server_ip, snapshot_id, snapshot, source=source)
        elif parsed.path == "/text":
            body = text_page(server_ip, snapshot_id, snapshot)
        else:
            body = details_page(server_ip, snapshot_id, snapshot)
        encoded = body.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/xml; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format, *args):
        return


def start():
    global server, thread
    if server is not None:
        return
    reset_store()
    port = int(os.getenv("CISCO_DETAILS_PORT", "6967"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()


def stop():
    global server, thread
    if server is None:
        return
    server.shutdown()
    server.server_close()
    server = None
    if thread is not None:
        thread.join(timeout=1)
        thread = None
