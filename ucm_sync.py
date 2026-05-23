
import os
import re
import time
import html
import socket
import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from xml.sax.saxutils import escape

import pymysql
import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR.parent.parent / ".env"
load_dotenv(ENV_PATH)

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

ENDPOINT_TABLE = "endpoints-output-cisco"
SETTINGS_TABLE = "endpoints-modulesettings-cisco"
LOG_FILE = BASE_DIR / "cisco_ucm_sync.log"
DEFAULT_INTERVAL = 300
SETTINGS_POLL_INTERVAL = 5
RIS_BATCH_SIZE = 1000
SOAP_PREVIEW_BYTES = 2000
DEFAULT_AXL_VERSION = "14.0"
FALLBACK_AXL_VERSIONS = ("12.5", "12.0", "11.5", "11.0")
AUTH_URL_MARKER = os.getenv("CISCO_AUTH_EXPECTED_MARKER", ":8082").strip()
DEFAULT_SUPPORTED_MODELS = {
    "7821", "7841", "7861",
    "7925", "7926", "7931", "7940", "7941", "7942", "7945",
    "7960", "7961", "7962", "7965", "7970", "7971", "7975",
    "8811", "8841", "8845", "8851", "8861", "8865",
}


def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as handle:
        handle.write(f"[{timestamp}] {message}\n")


def log_exception(prefix, exc):
    log(f"{prefix}: {type(exc).__name__}: {exc}")


def db():
    log(f"opening database connection db={DB_NAME or '<missing>'} host={DB_HOST or '<missing>'}")
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor,
    )


def truthy(value):
    return str(value or "").strip().lower() in ("true", "1", "yes", "on")


def setting(settings, name, default=""):
    return str(settings.get(name, default) or "").strip()


def settings_fingerprint(settings):
    items = sorted((str(key), str(value or "")) for key, value in settings.items())
    raw = repr(items).encode("utf-8", "replace")
    return hashlib.sha256(raw).hexdigest()


def cucm_servers(value):
    servers = []
    seen = set()
    for item in str(value or "").split(","):
        server = item.strip()
        if not server:
            continue
        key = server.lower()
        if key in seen:
            continue
        seen.add(key)
        servers.append(server)
    return servers


def resolve_cucm_server(server):
    server = str(server or "").strip()
    if not server:
        return []
    host = server
    port = None
    if server.startswith("[") and "]" in server:
        host = server[1:server.index("]")]
        rest = server[server.index("]") + 1:]
        if rest.startswith(":"):
            port = rest[1:]
    elif server.count(":") == 1 and re.match(r"^[^:]+:\d+$", server):
        host, port = server.rsplit(":", 1)
    try:
        infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        log_exception(f"CUCM DNS lookup failed server={server}", exc)
        return [server]
    resolved = []
    seen = set()
    for family, _, _, _, sockaddr in infos:
        address = sockaddr[0]
        target = f"[{address}]" if family == socket.AF_INET6 else address
        if port:
            target = f"{target}:{port}"
        if target not in seen:
            seen.add(target)
            resolved.append(target)
    log(f"CUCM DNS lookup server={server} resolved={resolved or [server]}")
    return resolved or [server]


def cucm_connection_targets(server):
    targets = [server]
    for resolved in resolve_cucm_server(server):
        if resolved not in targets:
            targets.append(resolved)
    return targets


def load_settings():
    conn = db()
    try:
        with conn.cursor() as cur:
            try:
                cur.execute(f"SELECT parameter, value FROM `{SETTINGS_TABLE}`")
            except pymysql.MySQLError as exc:
                log_exception("settings unavailable", exc)
                return {}
            settings = {}
            skipped_rows = 0
            for row in cur.fetchall():
                parameter = row.get("parameter")
                if parameter is None:
                    skipped_rows += 1
                    continue
                settings[str(parameter)] = row.get("value")
            visible = {
                key: ("<set>" if "password" in str(key).lower() and value else value)
                for key, value in settings.items()
            }
            if skipped_rows:
                log(f"skipped malformed UCM settings rows missing parameter={skipped_rows}")
            log(f"loaded UCM settings keys={sorted(settings)} values={visible}")
            return settings
    finally:
        conn.close()


def sync_interval(settings):
    raw = setting(settings, "ucmsync-interval")
    try:
        value = int(raw)
        interval = value if value > 0 else DEFAULT_INTERVAL
    except (TypeError, ValueError):
        interval = DEFAULT_INTERVAL
    log(f"UCM sync interval raw={raw or '<missing>'} resolved={interval}")
    return interval


def table_columns(cur, table):
    cur.execute(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s",
        (DB_NAME, table),
    )
    columns = {row["COLUMN_NAME"] for row in cur.fetchall()}
    log(f"table columns table={table} columns={sorted(columns)}")
    return columns


def table_column_defs(cur, table):
    cur.execute(f"SHOW COLUMNS FROM `{table}`")
    definitions = {row["Field"]: row for row in cur.fetchall()}
    definition_summary = {
        name: data.get("Type")
        for name, data in definitions.items()
    }
    log(f"table column definitions table={table} defs={definition_summary}")
    return definitions


def ensure_addedby_column():
    conn = db()
    try:
        with conn.cursor() as cur:
            columns = table_columns(cur, ENDPOINT_TABLE)
            if "addedby" not in columns:
                log(f"adding addedby column to {ENDPOINT_TABLE}")
                cur.execute(
                    f"ALTER TABLE `{ENDPOINT_TABLE}` "
                    "ADD COLUMN addedby ENUM('MANUAL','UCM') NOT NULL DEFAULT 'MANUAL'"
                )
            else:
                log(f"addedby column already exists on {ENDPOINT_TABLE}")
        conn.commit()
    finally:
        conn.close()


def ensure_ucm_columns():
    conn = db()
    try:
        with conn.cursor() as cur:
            definitions = table_column_defs(cur, ENDPOINT_TABLE)
            if "addedby" not in definitions:
                log(f"adding addedby column to {ENDPOINT_TABLE}")
                cur.execute(
                    f"ALTER TABLE `{ENDPOINT_TABLE}` "
                    "ADD COLUMN addedby ENUM('MANUAL','UCM') NOT NULL DEFAULT 'MANUAL'"
                )
            ensure_enum_member(cur, ENDPOINT_TABLE, "status", "New")
            model_def = definitions.get("model")
            model_type = str(model_def.get("Type", "") if model_def else "")
            if model_def and not model_type.lower().startswith("enum("):
                enum_sql = ",".join(f"'{model}'" for model in sorted(DEFAULT_SUPPORTED_MODELS))
                log(f"restoring model column to supported-model enum from {model_type}")
                cur.execute(
                    f"UPDATE `{ENDPOINT_TABLE}` SET model='' "
                    f"WHERE model IS NOT NULL AND model NOT IN ({','.join(['%s'] * len(DEFAULT_SUPPORTED_MODELS))})",
                    tuple(sorted(DEFAULT_SUPPORTED_MODELS)),
                )
                cur.execute(
                    f"ALTER TABLE `{ENDPOINT_TABLE}` "
                    f"MODIFY COLUMN model ENUM('',{enum_sql}) NOT NULL DEFAULT ''"
                )
        conn.commit()
    finally:
        conn.close()


def enum_values_from_type(column_type):
    if not str(column_type or "").lower().startswith("enum("):
        return set()
    return set(re.findall(r"'((?:[^'\\\\]|\\\\.)*)'", column_type))


def ensure_enum_member(cur, table, column, value):
    definitions = table_column_defs(cur, table)
    column_def = definitions.get(column)
    if not column_def:
        return
    current_type = str(column_def.get("Type", ""))
    values = list(re.findall(r"'((?:[^'\\\\]|\\\\.)*)'", current_type))
    if not values or value in values:
        return
    values.append(value)
    null_sql = "NOT NULL" if column_def.get("Null") == "NO" else "NULL"
    default = column_def.get("Default")
    default_sql = f" DEFAULT '{default}'" if default is not None else ""
    enum_sql = ",".join(f"'{item}'" for item in values)
    log(f"adding enum value {value} to {table}.{column}")
    cur.execute(f"ALTER TABLE `{table}` MODIFY COLUMN `{column}` ENUM({enum_sql}) {null_sql}{default_sql}")


def supported_models():
    conn = db()
    try:
        with conn.cursor() as cur:
            definitions = table_column_defs(cur, ENDPOINT_TABLE)
            model_def = definitions.get("model")
            values = enum_values_from_type(model_def.get("Type", "") if model_def else "")
            models = {value for value in values if re.fullmatch(r"\d{4}", str(value or ""))}
            if models:
                log(f"using model enum as supported model list models={sorted(models)}")
                return models
            log(f"model column is not a usable enum; using default supported models={sorted(DEFAULT_SUPPORTED_MODELS)}")
            return set(DEFAULT_SUPPORTED_MODELS)
    finally:
        conn.close()


def local_name(tag):
    return tag.rsplit("}", 1)[-1]


def text_of_child(element, name):
    for child in list(element):
        if local_name(child.tag).lower() == name.lower():
            return (child.text or "").strip()
    return ""


def normalize_device_name(value):
    token = "".join(ch for ch in str(value or "").upper() if ch.isalnum())
    if token and not token.startswith("SEP"):
        token = "SEP" + token
    return token


def model_number(value):
    match = re.search(r"(\d{4})", str(value or ""))
    return match.group(1) if match else ""


def axl_operation(name, body, version=DEFAULT_AXL_VERSION):
    namespace = f"http://www.cisco.com/AXL/API/{version}"
    return (
        f'<axlapi:{name} sequence="1" '
        f'xmlns:axlapi="{namespace}" '
        f'xmlns:axl="{namespace}" '
        'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        f'xsi:schemaLocation="{namespace} axlsoap.xsd">'
        f"{body}"
        f"</axlapi:{name}>"
    )


def axl_envelope(body, version=DEFAULT_AXL_VERSION):
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" '
        'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        'xmlns:xsd="http://www.w3.org/2001/XMLSchema">'
        f"<SOAP-ENV:Body>{body}</SOAP-ENV:Body>"
        "</SOAP-ENV:Envelope>"
    )


def ris_envelope(body):
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<soapenv:Envelope '
        'xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" '
        'xmlns:xsd="http://www.w3.org/2001/XMLSchema" '
        'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        'xmlns:soapenc="http://schemas.xmlsoap.org/soap/encoding/" '
        'xmlns:soap="http://schemas.cisco.com/ast/soap/">'
        f"<soapenv:Body>{body}</soapenv:Body>"
        "</soapenv:Envelope>"
    )


def soap_post(url, body, username, password, soap_action=""):
    headers = {
        "Accept": "text/*",
        "Content-Type": "text/xml",
    }
    if soap_action:
        if soap_action.startswith("CUCM:DB"):
            headers["SOAPAction"] = f'"{soap_action}"'
        else:
            headers["SOAPAction"] = soap_action
    log(f"SOAP request url={url} action={soap_action or '<none>'} bytes={len(body.encode('utf-8'))}")
    try:
        response = requests.post(
            url,
            data=body.encode("utf-8"),
            headers=headers,
            auth=HTTPBasicAuth(username, password),
            verify=False,
            timeout=30,
        )
    except Exception as exc:
        log_exception(f"SOAP request failed url={url} action={soap_action or '<none>'}", exc)
        raise

    preview = response.text[:SOAP_PREVIEW_BYTES].replace("\n", " ").replace("\r", " ")
    log(
        f"SOAP response url={url} action={soap_action or '<none>'} "
        f"status={response.status_code} bytes={len(response.content)} preview={preview}"
    )
    response.raise_for_status()
    try:
        return ET.fromstring(response.content)
    except ET.ParseError as exc:
        log_exception(f"SOAP XML parse failed url={url} action={soap_action or '<none>'}", exc)
        raise


def row_value(row, name):
    for child in list(row):
        if local_name(child.tag).lower() == name.lower():
            return (child.text or "").strip()
    return ""


def axl_versions(settings):
    configured = setting(settings, "ucmsync-axl-version")
    versions = []
    if configured:
        versions.append(configured)
    for version in (DEFAULT_AXL_VERSION, *FALLBACK_AXL_VERSIONS):
        if version not in versions:
            versions.append(version)
    log(f"AXL versions to try={versions}")
    return versions


def list_cucm_phones_sql(cucm_ip, username, password, axl_version):
    url = f"https://{cucm_ip}:8443/axl/"
    sql = (
        "select d.name, d.description, tm.name as model "
        "from device d left join typemodel tm on d.tkmodel = tm.enum "
        "where d.name like 'SEP%'"
    )
    body = axl_envelope(
        axl_operation(
            "executeSQLQuery",
            f"<sql>{escape(sql)}</sql>",
            axl_version,
        ),
        axl_version,
    )
    log(f"listing CUCM phones with executeSQLQuery axl_version={axl_version} sql={sql}")
    root = soap_post(
        url,
        body,
        username,
        password,
        f"CUCM:DB ver={axl_version} executeSQLQuery",
    )
    phones = {}
    for element in root.iter():
        if local_name(element.tag).lower() != "row":
            continue
        name = normalize_device_name(row_value(element, "name"))
        if not name:
            continue
        model = model_number(row_value(element, "model"))
        phones[name] = {
            "macaddr": name,
            "name": row_value(element, "description") or name,
            "model": model,
            "ipv4": "",
            "status": "Offline",
        }
    log(f"executeSQLQuery returned phones={len(phones)} sample={list(sorted(phones))[:5]}")
    return phones


def list_cucm_phones_listphone(cucm_ip, username, password, axl_version):
    url = f"https://{cucm_ip}:8443/axl/"
    log(f"listing CUCM phones with AXL listPhone fallback axl_version={axl_version}")
    body = axl_envelope(
        axl_operation(
            "listPhone",
            "<searchCriteria><name>SEP%</name></searchCriteria>"
            "<returnedTags><name/><description/><model/></returnedTags>",
            axl_version,
        ),
        axl_version,
    )
    root = soap_post(url, body, username, password, f"CUCM:DB ver={axl_version} listPhone")
    phones = {}
    for element in root.iter():
        if local_name(element.tag) != "phone":
            continue
        name = normalize_device_name(text_of_child(element, "name"))
        if not name:
            continue
        model = model_number(text_of_child(element, "model"))
        phones[name] = {
            "macaddr": name,
            "name": text_of_child(element, "description") or name,
            "model": model,
            "ipv4": "",
            "status": "Offline",
        }
    log(f"listPhone returned phones={len(phones)} sample={list(sorted(phones))[:5]}")
    return phones


def list_cucm_phones(cucm_ip, username, password, settings):
    last_error = None
    for axl_version in axl_versions(settings):
        try:
            phones = list_cucm_phones_sql(cucm_ip, username, password, axl_version)
            if phones:
                return phones
            log(f"executeSQLQuery returned no phones for AXL {axl_version}; trying listPhone fallback")
        except Exception as exc:
            last_error = exc
            log_exception(f"executeSQLQuery phone list failed for AXL {axl_version}; trying listPhone fallback", exc)

        try:
            return list_cucm_phones_listphone(cucm_ip, username, password, axl_version)
        except Exception as exc:
            last_error = exc
            log_exception(f"listPhone fallback failed for AXL {axl_version}", exc)
    if last_error:
        raise last_error
    return {}


def chunks(values, size):
    values = list(values)
    for index in range(0, len(values), size):
        yield values[index:index + size]


def first_ip_from_device(device):
    direct = text_of_child(device, "IpAddress") or text_of_child(device, "IPAddress")
    if direct:
        return direct
    for child in device.iter():
        if local_name(child.tag).lower() in ("ip", "ipaddress") and child.text:
            value = child.text.strip()
            if value:
                return value
    return ""


def ris_status_from_device(device):
    status = text_of_child(device, "Status")
    return status or ""


def ris_targets(cucm_ip):
    return (
        (
            f"https://{cucm_ip}/realtimeservice/services/RisPort70",
            "http://schemas.cisco.com/ast/soap/action/#RisPort70#SelectCmDevice",
        ),
        (
            f"https://{cucm_ip}:8443/realtimeservice/services/RisPort70",
            "http://schemas.cisco.com/ast/soap/action/#RisPort70#SelectCmDevice",
        ),
    )


def parse_ris_devices(root):
    devices = {}
    for device in root.iter():
        is_cm_device = local_name(device.tag) == "CmDevice" or any(
            str(value).endswith(":CmDevice") or str(value) == "CmDevice"
            for value in device.attrib.values()
        )
        if not is_cm_device:
            continue
        name = normalize_device_name(text_of_child(device, "Name"))
        if not name:
            continue
        ip = first_ip_from_device(device)
        status = ris_status_from_device(device)
        devices[name] = {
            "ipv4": ip,
            "cucm_status": status,
        }
    return devices


def ris_rpc_criteria(select_items, select_items_type, item_multirefs, device_class="Phone"):
    return ris_envelope(
        '<ns1:SelectCmDevice '
        'soapenv:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/" '
        'xmlns:ns1="http://schemas.cisco.com/ast/soap/">'
        '<StateInfo xsi:type="xsd:string"/>'
        '<CmSelectionCriteria href="#criteria"/>'
        "</ns1:SelectCmDevice>"
        '<multiRef id="criteria" '
        'soapenc:root="0" '
        'soapenv:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/" '
        'xsi:type="ns2:CmSelectionCriteria" '
        'xmlns:soapenc="http://schemas.xmlsoap.org/soap/encoding/" '
        'xmlns:ns2="http://schemas.cisco.com/ast/soap/">'
        '<MaxReturnedDevices xsi:type="xsd:unsignedInt">1000</MaxReturnedDevices>'
        f'<Class xsi:type="xsd:string">{device_class}</Class>'
        '<Model xsi:type="xsd:unsignedInt">255</Model>'
        '<Status xsi:type="xsd:string">Registered</Status>'
        '<NodeName xsi:type="xsd:string" xsi:nil="true"/>'
        '<SelectBy xsi:type="xsd:string">Name</SelectBy>'
        f'<SelectItems soapenc:arrayType="{select_items_type}" xsi:type="soapenc:Array">'
        f"{select_items}"
        "</SelectItems>"
        "</multiRef>"
        f"{item_multirefs}"
    )


def ris_rpc_criteria_with_selectitems_ref(select_items_array, item_multirefs, device_class="Phone"):
    return ris_envelope(
        '<ns1:SelectCmDevice '
        'soapenv:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/" '
        'xmlns:ns1="http://schemas.cisco.com/ast/soap/">'
        '<StateInfo xsi:type="xsd:string"/>'
        '<CmSelectionCriteria href="#criteria"/>'
        "</ns1:SelectCmDevice>"
        '<multiRef id="criteria" '
        'soapenc:root="0" '
        'soapenv:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/" '
        'xsi:type="ns2:CmSelectionCriteria" '
        'xmlns:soapenc="http://schemas.xmlsoap.org/soap/encoding/" '
        'xmlns:ns2="http://schemas.cisco.com/ast/soap/">'
        '<MaxReturnedDevices xsi:type="xsd:unsignedInt">1000</MaxReturnedDevices>'
        f'<Class xsi:type="xsd:string">{device_class}</Class>'
        '<Model xsi:type="xsd:unsignedInt">255</Model>'
        '<Status xsi:type="xsd:string">Registered</Status>'
        '<NodeName xsi:type="xsd:string" xsi:nil="true"/>'
        '<SelectBy xsi:type="xsd:string">Name</SelectBy>'
        '<SelectItems href="#selectItems"/>'
        "</multiRef>"
        f"{select_items_array}"
        f"{item_multirefs}"
    )


def ris_request_variants(device_names):
    for status in ("Registered", "Any"):
        yield (
            f"doc_literal_SelectCmDevice_class_wildcard_{status.lower()}",
            ris_envelope(
                "<soap:SelectCmDevice>"
                "<soap:StateInfo/>"
                "<soap:CmSelectionCriteria>"
                "<soap:MaxReturnedDevices>1000</soap:MaxReturnedDevices>"
                "<soap:Class>Phone</soap:Class>"
                "<soap:Model>255</soap:Model>"
                f"<soap:Status>{status}</soap:Status>"
                '<soap:NodeName xsi:nil="true"/>'
                "<soap:SelectBy>Name</soap:SelectBy>"
                "<soap:SelectItems><soap:Item>*</soap:Item></soap:SelectItems>"
                "<soap:Protocol>Any</soap:Protocol>"
                "<soap:DownloadStatus>Any</soap:DownloadStatus>"
                "</soap:CmSelectionCriteria>"
                "</soap:SelectCmDevice>"
            ),
        )
        yield (
            f"doc_literal_SelectCmDevice_class_wildcard_wrapped_{status.lower()}",
            ris_envelope(
                "<soap:SelectCmDevice>"
                "<soap:StateInfo/>"
                "<soap:CmSelectionCriteria>"
                "<soap:MaxReturnedDevices>1000</soap:MaxReturnedDevices>"
                "<soap:Class>Phone</soap:Class>"
                "<soap:Model>255</soap:Model>"
                f"<soap:Status>{status}</soap:Status>"
                '<soap:NodeName xsi:nil="true"/>'
                "<soap:SelectBy>Name</soap:SelectBy>"
                "<soap:SelectItems><soap:item><soap:Item>*</soap:Item></soap:item></soap:SelectItems>"
                "<soap:Protocol>Any</soap:Protocol>"
                "<soap:DownloadStatus>Any</soap:DownloadStatus>"
                "</soap:CmSelectionCriteria>"
                "</soap:SelectCmDevice>"
            ),
        )

    item_refs = "".join(f'<item href="#item{index}"/>' for index, _ in enumerate(device_names))
    item_refs_capital = "".join(f'<Item href="#item{index}"/>' for index, _ in enumerate(device_names))
    item_multirefs = "".join(
        '<multiRef '
        f'id="item{index}" '
        'soapenc:root="0" '
        'soapenv:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/" '
        'xsi:type="ns3:SelectItem" '
        'xmlns:ns3="http://schemas.cisco.com/ast/soap/" '
        'xmlns:soapenc="http://schemas.xmlsoap.org/soap/encoding/">'
        f'<Item xsi:type="xsd:string">{escape(name)}</Item>'
        "</multiRef>"
        for index, name in enumerate(device_names)
    )
    select_items_array = (
        '<multiRef id="selectItems" '
        'soapenc:root="0" '
        'soapenv:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/" '
        f'soapenc:arrayType="ns2:SelectItem[{len(device_names)}]" '
        'xsi:type="soapenc:Array" '
        'xmlns:soapenc="http://schemas.xmlsoap.org/soap/encoding/" '
        'xmlns:ns2="http://schemas.cisco.com/ast/soap/">'
        f"{item_refs}"
        "</multiRef>"
    )
    yield (
        "rpc_selectitems_href_array_phone_registered",
        ris_rpc_criteria_with_selectitems_ref(select_items_array, item_multirefs, "Phone"),
    )
    select_items_array_capital = select_items_array.replace(item_refs, item_refs_capital)
    yield (
        "rpc_selectitems_href_array_capital_item_phone_registered",
        ris_rpc_criteria_with_selectitems_ref(select_items_array_capital, item_multirefs, "Phone"),
    )

    literal_item_wrappers = "".join(
        f"<soap:item><soap:Item>{escape(name)}</soap:Item></soap:item>"
        for name in device_names
    )
    literal_direct_items = "".join(
        f"<soap:Item>{escape(name)}</soap:Item>"
        for name in device_names
    )
    for operation in ("SelectCmDevice", "selectCmDevice"):
        yield (
            f"doc_literal_{operation}_class_direct_items_registered",
            ris_envelope(
                f"<soap:{operation}>"
                "<soap:StateInfo/>"
                "<soap:CmSelectionCriteria>"
                "<soap:MaxReturnedDevices>1000</soap:MaxReturnedDevices>"
                "<soap:Class>Phone</soap:Class>"
                "<soap:Model>255</soap:Model>"
                "<soap:Status>Registered</soap:Status>"
                '<soap:NodeName xsi:nil="true"/>'
                "<soap:SelectBy>Name</soap:SelectBy>"
                f"<soap:SelectItems>{literal_direct_items}</soap:SelectItems>"
                "<soap:Protocol>Any</soap:Protocol>"
                "<soap:DownloadStatus>Any</soap:DownloadStatus>"
                f"</soap:CmSelectionCriteria>"
                f"</soap:{operation}>"
            ),
        )
        yield (
            f"doc_literal_{operation}_class_phone_registered",
            ris_envelope(
                f"<soap:{operation}>"
                "<soap:StateInfo/>"
                "<soap:CmSelectionCriteria>"
                "<soap:MaxReturnedDevices>1000</soap:MaxReturnedDevices>"
                "<soap:Class>Phone</soap:Class>"
                "<soap:Model>255</soap:Model>"
                "<soap:Status>Registered</soap:Status>"
                '<soap:NodeName xsi:nil="true"/>'
                "<soap:SelectBy>Name</soap:SelectBy>"
                f"<soap:SelectItems>{literal_item_wrappers}</soap:SelectItems>"
                "<soap:Protocol>Any</soap:Protocol>"
                "<soap:DownloadStatus>Any</soap:DownloadStatus>"
                f"</soap:CmSelectionCriteria>"
                f"</soap:{operation}>"
            ),
        )
    yield (
        "rpc_multiref_array_phone_registered",
        ris_rpc_criteria(
            item_refs,
            f"ns2:SelectItem[{len(device_names)}]",
            item_multirefs,
            "Phone",
        ),
    )

    repeated_items = "".join(f'<Item xsi:type="xsd:string">{escape(name)}</Item>' for name in device_names)
    single_item_multiref = (
        '<multiRef id="item0" '
        'soapenc:root="0" '
        'soapenv:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/" '
        'xsi:type="ns3:SelectItem" '
        'xmlns:ns3="http://schemas.cisco.com/ast/soap/" '
        'xmlns:soapenc="http://schemas.xmlsoap.org/soap/encoding/">'
        f"{repeated_items}"
        "</multiRef>"
    )
    yield (
        "rpc_single_selectitem_repeated_items_phone_registered",
        ris_rpc_criteria(
            '<item href="#item0"/>',
            "ns2:SelectItem[1]",
            single_item_multiref,
            "Phone",
        ),
    )


def lookup_registered_ips_batch(cucm_ip, username, password, device_names):
    requested_names = {normalize_device_name(name) for name in device_names}
    last_error = None
    for variant_name, body in ris_request_variants(device_names):
        for url, soap_action in ris_targets(cucm_ip):
            try:
                log(f"trying RIS target variant={variant_name} url={url} action={soap_action}")
                root = soap_post(url, body, username, password, soap_action)
                devices = {
                    name: device
                    for name, device in parse_ris_devices(root).items()
                    if name in requested_names
                }
                log(f"RIS target succeeded variant={variant_name} url={url} devices={len(devices)}")
                if devices:
                    return devices
                log(f"RIS target returned zero requested devices variant={variant_name}; trying next variant")
            except Exception as exc:
                last_error = exc
                log_exception(f"RIS target failed variant={variant_name} url={url}", exc)
    if last_error:
        raise last_error
    return {}


def lookup_registered_devices(cucm_ip, username, password, device_names):
    device_names = sorted(device_names)
    if not device_names:
        log("RIS lookup skipped: no CUCM device names")
        return {}

    devices = {}
    failed_batches = 0
    total_batches = (len(device_names) + RIS_BATCH_SIZE - 1) // RIS_BATCH_SIZE
    log(f"RIS lookup starting devices={len(device_names)} batches={total_batches}")
    for batch_number, batch in enumerate(chunks(device_names, RIS_BATCH_SIZE), start=1):
        try:
            log(f"RIS lookup batch={batch_number}/{total_batches} devices={len(batch)} sample={batch[:5]}")
            devices.update(lookup_registered_ips_batch(cucm_ip, username, password, batch))
        except Exception as exc:
            failed_batches += 1
            log_exception(f"RIS lookup failed batch={batch_number}/{total_batches}", exc)
    if failed_batches == total_batches:
        log("RIS lookup failed for every batch; not applying UCM phone status/IP changes")
        return None
    log(f"RIS lookup complete devices={len(devices)} sample={list(sorted(devices.items()))[:5]}")
    return devices


def apply_phones(phones):
    conn = db()
    try:
        with conn.cursor() as cur:
            columns = table_columns(cur, ENDPOINT_TABLE)
            missing = {"macaddr", "ipv4", "model", "name", "status", "addedby"} - columns
            if missing:
                log(f"endpoint table is missing expected columns: {sorted(missing)}")
            cur.execute(f"SELECT macaddr, addedby, ipv4, status FROM `{ENDPOINT_TABLE}`")
            existing = {
                normalize_device_name(row.get("macaddr")): row
                for row in cur.fetchall()
                if row.get("macaddr")
            }
            phone_names = set(phones)
            inserted = 0
            updated = 0
            skipped_manual = 0

            if phone_names:
                placeholders = ",".join(["%s"] * len(phone_names))
                cur.execute(
                    f"DELETE FROM `{ENDPOINT_TABLE}` "
                    f"WHERE addedby='UCM' AND macaddr NOT IN ({placeholders})",
                    tuple(sorted(phone_names)),
                )
                deleted = cur.rowcount
            else:
                cur.execute(f"DELETE FROM `{ENDPOINT_TABLE}` WHERE addedby='UCM'")
                deleted = cur.rowcount

            for name, phone in phones.items():
                row = existing.get(name)
                if row and row.get("addedby") != "UCM":
                    skipped_manual += 1
                    continue
                current_status = str(row.get("status") or "") if row else ""
                current_ipv4 = str(row.get("ipv4") or "") if row else ""
                desired_ipv4 = phone.get("ipv4", "")
                if desired_ipv4:
                    desired_status = current_status if row and current_ipv4 == desired_ipv4 and current_status in ("Online", "New") else "New"
                else:
                    desired_status = "Offline"
                if row:
                    assignments = []
                    values = []
                    for column, value in (
                        ("name", phone.get("name", "")),
                        ("ipv4", desired_ipv4),
                        ("status", desired_status),
                        ("model", phone.get("model", "")),
                        ("visual", "Image"),
                        ("addedby", "UCM"),
                    ):
                        if column in columns:
                            assignments.append(f"`{column}`=%s")
                            values.append(value)
                    if assignments:
                        values.append(name)
                        cur.execute(
                            f"UPDATE `{ENDPOINT_TABLE}` SET {', '.join(assignments)} WHERE macaddr=%s",
                            tuple(values),
                        )
                        updated += cur.rowcount
                    continue

                insert_values = {
                    "macaddr": name,
                    "name": phone.get("name", ""),
                    "ipv4": phone.get("ipv4", ""),
                    "status": "New" if phone.get("ipv4") else "Offline",
                    "audio": "Multicast",
                    "model": phone.get("model", ""),
                    "visual": "Image",
                    "addedby": "UCM",
                }
                insert_columns = [column for column in insert_values if column in columns]
                placeholders = ",".join(["%s"] * len(insert_columns))
                cur.execute(
                    f"INSERT INTO `{ENDPOINT_TABLE}` "
                    f"({', '.join(f'`{column}`' for column in insert_columns)}) "
                    f"VALUES ({placeholders})",
                    tuple(insert_values[column] for column in insert_columns),
                )
                inserted += cur.rowcount
        conn.commit()
        log(
            "DB apply complete "
            f"phones={len(phones)} inserted={inserted} updated={updated} "
            f"deleted={deleted} skipped_manual={skipped_manual}"
        )
    finally:
        conn.close()


def check_phone(ip):
    url = f"http://{ip}/CGI/Java/Serviceability?adapter=device.statistics.configuration"
    try:
        response = requests.get(url, timeout=3)
        if response.status_code != 200:
            log(f"immediate phone check ip={ip} status=Offline http_status={response.status_code}")
            return "Offline"

        match = re.search(
            r"Authentication URL</B></TD><td[^>]*></TD><TD><B>(.*?)</B>",
            response.text,
            re.IGNORECASE | re.DOTALL,
        )
        if not match:
            log(f"immediate phone check ip={ip} status=NoAuthURL reason=auth_url_not_found")
            return "NoAuthURL"

        value = html.unescape(match.group(1)).strip()
        log(f"immediate phone check ip={ip} auth_url={value or '<empty>'}")
        if not value:
            return "NoAuthURL"
        if AUTH_URL_MARKER and AUTH_URL_MARKER not in value:
            log(
                f"immediate phone check ip={ip} status=Online "
                f"reason=auth_url_marker_missing marker={AUTH_URL_MARKER} auth_url={value}"
            )
            return "Online"
        return "Online" if value else "NoAuthURL"
    except Exception as exc:
        log_exception(f"immediate phone check failed ip={ip}", exc)
        return "Offline"


def update_phone_status(macaddr, ipv4, status):
    conn = db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE `{ENDPOINT_TABLE}` SET status=%s WHERE macaddr=%s AND ipv4=%s",
                (status, macaddr, ipv4),
            )
        conn.commit()
    finally:
        conn.close()


def check_synced_phones_now(phones):
    checked = 0
    for macaddr, phone in phones.items():
        ipv4 = phone.get("ipv4", "")
        if not ipv4:
            continue
        result = check_phone(ipv4)
        update_phone_status(macaddr, ipv4, result)
        log(f"immediate status check {macaddr} {ipv4} -> {result}")
        checked += 1
    log(f"immediate status checks complete checked={checked}")


def sync_from_server(cucm_server, username, password, settings):
    last_error = None
    for target in cucm_connection_targets(cucm_server):
        try:
            log(f"trying CUCM sync target={target} configured_server={cucm_server}")
            phones = list_cucm_phones(target, username, password, settings)
            supported = supported_models()
            before_filter = len(phones)
            phones = {
                name: phone
                for name, phone in phones.items()
                if phone.get("model") in supported
            }
            log(
                f"filtered CUCM phones by supported model target={target} before={before_filter} "
                f"after={len(phones)} skipped={before_filter - len(phones)}"
            )
            registered_devices = lookup_registered_devices(target, username, password, phones.keys())
            if registered_devices is None:
                raise RuntimeError("RIS lookup failed for every batch")
            for name, phone in phones.items():
                realtime = registered_devices.get(name, {})
                phone["ipv4"] = realtime.get("ipv4", "")
                phone["cucm_status"] = realtime.get("cucm_status", "")
            apply_phones(phones)
            check_synced_phones_now(phones)
            log(f"synced {len(phones)} UCM phones target={target} configured_server={cucm_server}")
            return True
        except Exception as exc:
            last_error = exc
            log_exception(f"CUCM sync target failed target={target} configured_server={cucm_server}", exc)
    if last_error:
        raise last_error
    return False


def sync_once(settings):
    cucm_ip = setting(settings, "ucmsync-ip")
    username = setting(settings, "ucmsync-username")
    password = setting(settings, "ucmsync-password")
    servers = cucm_servers(cucm_ip)
    log(
        "sync_once starting "
        f"cucm_servers={servers or '<missing>'} username={'<set>' if username else '<missing>'} "
        f"password={'<set>' if password else '<missing>'}"
    )
    if not servers or not username or not password:
        log("ucmsync enabled but server/username/password is missing")
        return
    ensure_ucm_columns()
    last_error = None
    for index, cucm_server in enumerate(servers, start=1):
        try:
            log(f"trying CUCM server {index}/{len(servers)} server={cucm_server}")
            if sync_from_server(cucm_server, username, password, settings):
                log(f"CUCM sync succeeded server={cucm_server}")
                return
        except Exception as exc:
            last_error = exc
            log_exception(f"CUCM server failed server={cucm_server}; trying next server", exc)
    if last_error:
        raise last_error
    log("CUCM sync had no usable servers")


def main():
    requests.packages.urllib3.disable_warnings()
    log("Cisco UCM sync process started")
    last_fingerprint = None
    next_run = 0

    while True:
        try:
            settings = load_settings()
            fingerprint = settings_fingerprint(settings)
            interval = sync_interval(settings)
            enabled = truthy(setting(settings, "ucmsync"))
            now = time.time()

            if last_fingerprint is None:
                last_fingerprint = fingerprint
            elif fingerprint != last_fingerprint:
                log("UCM sync settings changed; applying without service restart")
                last_fingerprint = fingerprint
                if enabled:
                    next_run = now

            log(f"UCM sync tick enabled={enabled}")

            if enabled and now >= next_run:
                try:
                    sync_once(settings)
                except Exception as exc:
                    log_exception("sync failed", exc)
                next_run = time.time() + interval
            elif not enabled:
                log("UCM sync disabled")
                next_run = now + interval

        except Exception as exc:
            log_exception("main loop failed", exc)

        time.sleep(SETTINGS_POLL_INTERVAL)


if __name__ == "__main__":
    main()
