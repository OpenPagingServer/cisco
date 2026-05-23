
import os
import sys
import time
import threading
import subprocess
import importlib.util
import secrets
import requests
import pymysql
import re
import html
from dotenv import load_dotenv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
AUTH_IPC_TOKEN = os.getenv("CISCO_AUTH_IPC_TOKEN") or secrets.token_urlsafe(32)
AUTH_REGISTER_URL = os.getenv("CISCO_AUTH_REGISTER_URL") or "http://127.0.0.1:8082/__ops/register-auth"
os.environ["CISCO_AUTH_IPC_TOKEN"] = AUTH_IPC_TOKEN
os.environ["CISCO_AUTH_REGISTER_URL"] = AUTH_REGISTER_URL


def load_message_send():
    module_name = "cisco_message_send_runtime"
    existing = sys.modules.get(module_name)
    if existing is not None:
        return existing
    module_path = BASE_DIR / "message_send.py"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


message_send = load_message_send()


def load_page_handler():
    module_path = BASE_DIR / "page_handler.py"
    spec = importlib.util.spec_from_file_location("cisco_page_handler", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if hasattr(module, "init"):
        module.init(message_send)
    return module


page_handler = load_page_handler()


def load_details_server():
    module_path = BASE_DIR / "details_server.py"
    spec = importlib.util.spec_from_file_location("cisco_details_server", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


details_server = load_details_server()


def load_spa_xml_server():
    module_path = BASE_DIR / "spa_xml_server.py"
    spec = importlib.util.spec_from_file_location("cisco_spa_xml_server", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


spa_xml_server = load_spa_xml_server()
if hasattr(message_send, "init_spa_xml_server"):
    message_send.init_spa_xml_server(spa_xml_server)

ENV_PATH = BASE_DIR.parent.parent / ".env"
load_dotenv(ENV_PATH)

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
AUTH_URL_MARKER = os.getenv("CISCO_AUTH_EXPECTED_MARKER", ":8082").strip()

core = None
running = False
thread = None
INTERVAL = 60

imggen_proc = None
authserver_proc = None
ucm_sync_proc = None
ucm_sync_log_handle = None

def init(core_obj):
    global core, running, thread, imggen_proc, authserver_proc, ucm_sync_proc, ucm_sync_log_handle
    core = core_obj
    running = True
    ensure_database_schema()

    imggen_path = BASE_DIR / "imggen.py"
    authserver_path = BASE_DIR / "authserver.py"
    ucm_sync_path = BASE_DIR / "ucm_sync.py"

    if imggen_path.exists():
        imggen_proc = subprocess.Popen([sys.executable, str(imggen_path)], cwd=BASE_DIR)
    
    if authserver_path.exists():
        auth_env = os.environ.copy()
        auth_env["CISCO_AUTH_IPC_TOKEN"] = AUTH_IPC_TOKEN
        auth_env["CISCO_AUTH_REGISTER_URL"] = AUTH_REGISTER_URL
        authserver_proc = subprocess.Popen([sys.executable, str(authserver_path)], cwd=BASE_DIR, env=auth_env)

    if ucm_sync_path.exists():
        ucm_sync_log_handle = open(BASE_DIR / "cisco_ucm_sync.log", "a", encoding="utf-8")
        ucm_sync_proc = subprocess.Popen(
            [sys.executable, str(ucm_sync_path)],
            cwd=BASE_DIR,
            stdout=ucm_sync_log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        log(f"cisco ucm sync started pid={ucm_sync_proc.pid}")

    try:
        details_server.start()
    except Exception as exc:
        log(f"cisco details server error: {exc}")

    try:
        spa_xml_server.start()
    except Exception as exc:
        log(f"cisco spa xml server error: {exc}")

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()

def log(msg):
    if core and hasattr(core, "log"):
        core.log(msg)
    else:
        print(msg)

def db():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
    )


def split_sql_statements(sql):
    statements = []
    current = []
    quote = None
    escape = False
    for char in sql:
        current.append(char)
        if quote:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == quote:
                quote = None
            continue
        if char in ("'", '"', "`"):
            quote = char
            continue
        if char == ";":
            statement = "".join(current).strip()
            if statement:
                statements.append(statement[:-1].strip())
            current = []
    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return statements


def enum_values_from_type(column_type):
    return re.findall(r"'((?:[^'\\\\]|\\\\.)*)'", str(column_type or ""))


def table_column_defs(cur, table):
    cur.execute(f"SHOW COLUMNS FROM `{table}`")
    return {row["Field"]: row for row in cur.fetchall()}


def ensure_enum_column(cur, table, column, values, default, after_column=None):
    definitions = table_column_defs(cur, table)
    enum_sql = ",".join(f"'{value}'" for value in values)
    column_def = definitions.get(column)
    if column_def is None:
        after_sql = f" AFTER `{after_column}`" if after_column else ""
        cur.execute(
            f"ALTER TABLE `{table}` "
            f"ADD COLUMN `{column}` ENUM({enum_sql}) NOT NULL DEFAULT %s{after_sql}",
            (default,),
        )
        return

    placeholders = ",".join(["%s"] * len(values))
    cur.execute(
        f"UPDATE `{table}` SET `{column}`=%s "
        f"WHERE `{column}` IS NULL OR `{column}` NOT IN ({placeholders})",
        tuple([default, *values]),
    )
    current_type = str(column_def.get("Type", ""))
    current_values = enum_values_from_type(current_type)
    if current_values == list(values) and str(column_def.get("Default")) == str(default):
        return
    cur.execute(
        f"ALTER TABLE `{table}` "
        f"MODIFY COLUMN `{column}` ENUM({enum_sql}) NOT NULL DEFAULT %s",
        (default,),
    )


def ensure_cisco_endpoint_schema(cur):
    ensure_enum_column(
        cur,
        ENDPOINT_TABLE,
        "status",
        ("New", "Unchecked", "Offline", "Online"),
        "Unchecked",
    )
    ensure_enum_column(
        cur,
        ENDPOINT_TABLE,
        "audio",
        ("Multicast", "Unicast", "Disabled"),
        "Multicast",
        after_column="status",
    )
    ensure_enum_column(
        cur,
        ENDPOINT_TABLE,
        "visual",
        ("None", "Text", "Image"),
        "Image",
        after_column="model",
    )
    ensure_enum_column(
        cur,
        ENDPOINT_TABLE,
        "volume",
        ("0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100", "asis"),
        "asis",
        after_column="visual",
    )


def ensure_cisco_spa_xml_exe_schema(cur):
    ensure_enum_column(
        cur,
        SPA_XML_EXE_TABLE,
        "status",
        ("New", "Unchecked", "Offline", "Online"),
        "Unchecked",
    )


def ensure_database_schema():
    schema_path = BASE_DIR / "install.sql"
    if not schema_path.exists():
        log(f"cisco schema file missing: {schema_path}")
        return
    statements = split_sql_statements(schema_path.read_text(encoding="utf-8"))
    if not statements:
        return
    conn = db()
    try:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            ensure_cisco_endpoint_schema(cur)
            ensure_cisco_spa_xml_exe_schema(cur)
        conn.commit()
        log(f"cisco database schema checked statements={len(statements)}")
    finally:
        conn.close()


ENDPOINT_TABLE = "endpoints-output-cisco"
SPA_MULTICAST_TABLE = "endpoints-output-cisco-spamulticast"
SPA_XML_EXE_TABLE = "endpoints-output-cisco-spaxmlexe"

def fetch_endpoints():
    conn = db()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT ipv4, status FROM `{ENDPOINT_TABLE}`")
            return cur.fetchall()
    finally:
        conn.close()


def fetch_spa_xml_exe_endpoints():
    conn = db()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(f"SHOW COLUMNS FROM `{SPA_XML_EXE_TABLE}`")
            columns = {row["Field"] for row in cur.fetchall()}
            if "status" not in columns:
                return []
            cur.execute(
                f"SELECT `id`, `ipv4`, `status` FROM `{SPA_XML_EXE_TABLE}` "
                "WHERE `ipv4` IS NOT NULL AND `ipv4` <> ''"
            )
            return cur.fetchall()
    except pymysql.MySQLError as exc:
        log(f"cisco spa xml exe fetch status error: {exc}")
        return []
    finally:
        conn.close()

def update_status(ip, status):
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


def update_spa_xml_exe_status(endpoint_id, status):
    conn = db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE `{SPA_XML_EXE_TABLE}` SET status=%s WHERE id=%s",
                (status, endpoint_id),
            )
        conn.commit()
    finally:
        conn.close()


def get_endpoint_status():
    conn = db()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(f"SHOW COLUMNS FROM `{ENDPOINT_TABLE}`")
            columns = {row["Field"] for row in cur.fetchall()}
            wanted = ["macaddr", "name", "ipv4", "status", "audio", "model", "visual"]
            selected = [column for column in wanted if column in columns]
            rows = []
            if selected:
                cur.execute(
                    f"SELECT {', '.join(selected)} FROM `{ENDPOINT_TABLE}` "
                    "ORDER BY macaddr ASC, ipv4 ASC"
                )
                rows = cur.fetchall()
            try:
                cur.execute(
                    f"SELECT `id`, `name`, `address`, `port` FROM `{SPA_MULTICAST_TABLE}` "
                    "ORDER BY `name` ASC, `id` ASC"
                )
                spa_multicast_rows = cur.fetchall()
            except pymysql.MySQLError as exc:
                log(f"cisco spa multicast endpoint status error: {exc}")
                spa_multicast_rows = []
            try:
                cur.execute(
                    f"SELECT `id`, `ipv4`, `macaddress`, `status` FROM `{SPA_XML_EXE_TABLE}` "
                    "ORDER BY `macaddress` ASC, `ipv4` ASC"
                )
                spa_xml_rows = cur.fetchall()
            except pymysql.MySQLError as exc:
                log(f"cisco spa xml exe endpoint status error: {exc}")
                spa_xml_rows = []
    finally:
        conn.close()

    endpoints = []
    for row in rows:
        macaddr = row.get("macaddr") or ""
        endpoint_name = row.get("name") or ""
        if endpoint_name and macaddr and endpoint_name != macaddr:
            endpoint_name = f"{endpoint_name} ({macaddr})"
        else:
            endpoint_name = endpoint_name or macaddr or row.get("ipv4") or "Cisco Endpoint"
        model = row.get("model") or ""
        display_model = f"Cisco {model}" if model else "Cisco"
        endpoints.append(
            {
                "id": macaddr,
                "name": endpoint_name,
                "address": row.get("ipv4") or "",
                "model": display_model,
                "status": row.get("status") or "Unknown",
                "type": "IP Phone",
                "direction": "Output",
                            "bell_capable": True,
                            "capabilities": ["bells"],
            }
        )
    for row in spa_multicast_rows:
        endpoint_id = row.get("id")
        endpoint_name = row.get("name") or f"SPA Multicast {endpoint_id}"
        endpoints.append(
            {
                "id": f"spa-multicast-{endpoint_id}",
                "name": endpoint_name,
                "address": f"{row.get('address') or ''}:{row.get('port') or ''}",
                "model": "Cisco/Sipura SPA/MPP",
                "status": "Configured",
                "type": "Cisco/Sipura SPA/MPP Multicast RTP Group",
                "direction": "Output",
                            "bell_capable": True,
                            "capabilities": ["bells"],
            }
        )
    for row in spa_xml_rows:
        macaddress = row.get("macaddress") or ""
        mac_token = message_send.normalize_device_id(macaddress)
        endpoint_name = macaddress or row.get("ipv4") or f"SPA XML EXE {row.get('id')}"
        endpoints.append(
            {
                "id": f"spa-exe-{mac_token}",
                "name": endpoint_name,
                "address": row.get("ipv4") or "",
                "model": "Cisco/Sipura SPA/MPP",
                "status": row.get("status") or "Unknown",
                "type": "Cisco/Sipura SPA/MPP XML EXE Endpoint",
                "direction": "Output",
                            "bell_capable": True,
                            "capabilities": ["bells"],
            }
        )
    return {
        "module": "cisco",
        "display_name": "Cisco",
        "endpoints": endpoints,
    }


def check_phone(ip):
    url = f"http://{ip}/CGI/Java/Serviceability?adapter=device.statistics.configuration"
    try:
        r = requests.get(url, timeout=3)
        if r.status_code != 200:
            log(f"cisco auth url check {ip} -> Offline http_status={r.status_code}")
            return "Offline"

        text = r.text
        match = re.search(
            r"Authentication URL</B></TD><td[^>]*></TD><TD><B>(.*?)</B>",
            text,
            re.IGNORECASE | re.DOTALL
        )

        if not match:
            log(f"cisco auth url check {ip} -> NoAuthURL reason=not_found")
            return "NoAuthURL"

        value = html.unescape(match.group(1)).strip()

        if value == "":
            log(f"cisco auth url check {ip} -> NoAuthURL reason=empty")
            return "NoAuthURL"

        if AUTH_URL_MARKER and AUTH_URL_MARKER not in value:
            log(f"cisco auth url check {ip} -> Online reason=marker_missing marker={AUTH_URL_MARKER} value={value}")
            return "Online"

        log(f"cisco auth url check {ip} -> Online value={value}")
        return "Online"
    except Exception as exc:
        log(f"cisco auth url check {ip} -> Offline error={type(exc).__name__}: {exc}")
        return "Offline"


def ping_device(ip):
    if not ip:
        return "Offline"
    if os.name == "nt":
        cmd = ["ping", "-n", "1", "-w", "1000", ip]
    else:
        cmd = ["ping", "-c", "1", "-W", "1", ip]
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=3,
        )
        return "Online" if result.returncode == 0 else "Offline"
    except Exception:
        return "Offline"

def loop():
    while running:
        try:
            endpoints = fetch_endpoints()
            for ip, status in endpoints:
                if status == "Unchecked":
                    continue

                result = check_phone(ip)
                if result != status:
                    update_status(ip, result)
                    log(f"{ip} -> {result}")
            for endpoint in fetch_spa_xml_exe_endpoints():
                status = endpoint.get("status")
                if str(status or "").strip().lower() == "unchecked":
                    continue
                result = ping_device(endpoint.get("ipv4"))
                if result != status:
                    update_spa_xml_exe_status(endpoint.get("id"), result)
                    log(f"spa xml exe {endpoint.get('ipv4')} -> {result}")
        except Exception as e:
            log(f"cisco error: {e}")

        time.sleep(INTERVAL)

def shutdown():
    global running, imggen_proc, authserver_proc, ucm_sync_proc, ucm_sync_log_handle
    running = False
    try:
        details_server.stop()
    except Exception:
        pass
    try:
        spa_xml_server.stop()
    except Exception:
        pass
    if imggen_proc:
        imggen_proc.terminate()
    if authserver_proc:
        authserver_proc.terminate()
    if ucm_sync_proc:
        ucm_sync_proc.terminate()
    if ucm_sync_log_handle:
        try:
            ucm_sync_log_handle.close()
        except Exception:
            pass
        ucm_sync_log_handle = None

def api_endpoint(command_string):
    message_send.handle_api(command_string)

def handle_dispatch(action, stream_id, msg_id, targets, metadata=None):
    if action == "prepare_livepage":
        page_handler.handle_dispatch(action, stream_id, msg_id, targets, metadata)
        return
    message_send.handle_dispatch(action, stream_id, msg_id, targets)

def receive_audio(chunk, stream_id):
    message_send.receive_audio(chunk, stream_id)

def end_stream(stream_id):
    message_send.end_stream(stream_id)
