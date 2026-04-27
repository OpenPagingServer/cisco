#!/usr/bin/env python3

import os
import json
import random
import socket
import struct
import threading
import time
import urllib.parse
import uuid
import xml.sax.saxutils as saxutils
from datetime import datetime
from pathlib import Path

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
LOG_FILE = BASE_DIR / "cisco_debug.log"
DETAILS_STORE_DIR = BASE_DIR / "details_store"

USERNAME = os.getenv("CISCO_USERNAME", "admin")
PASSWORD = os.getenv("CISCO_PASSWORD", "admin")
PAYLOAD_TYPE = 0
IPC_PORT = 50000
STREAM_IDLE_TIMEOUT = 3.0
WATCHDOG_INTERVAL = 0.1
PRE_AUDIO_GRACE_SECONDS = 6.0

rtp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
rtp_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
rtp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 262144)
try:
    rtp_sock.setsockopt(socket.IPPROTO_IP, socket.IP_TOS, 0xB8)
except OSError:
    pass

active_streams = {}
streams_lock = threading.Lock()
column_cache = {}
column_cache_lock = threading.Lock()


def debug_log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as handle:
        handle.write(f"[{timestamp}] {message}\n")


def xml_document(body):
    return f'<?xml version="1.0" encoding="utf-8"?>{body}'


def db():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor,
    )


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


def send_phone(ip, xml, results, idx):
    try:
        response = requests.post(
            f"http://{ip}/CGI/Execute",
            data={"XML": xml},
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            timeout=5,
        )
        body = response.text or ""
        lowered = body.lower()
        results[idx] = (
            response.status_code == 200
            and "xml error" not in lowered
            and "error[4]" not in lowered
            and "request too large" not in lowered
            and "status=\"4\"" not in lowered
            and "status='4'" not in lowered
        )
        preview = (response.text or "")[:200].replace("\r", " ").replace("\n", " ")
        debug_log(f"POST {ip} status={response.status_code} body={preview}")
    except requests.exceptions.RequestException:
        results[idx] = False
        debug_log(f"POST {ip} request_failed")


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


def send_phone_request(ip, xml, timeout_seconds=5):
    try:
        response = requests.post(
            f"http://{ip}/CGI/Execute",
            data={"XML": xml},
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            timeout=timeout_seconds,
        )
        preview = (response.text or "")[:200].replace("\r", " ").replace("\n", " ")
        lowered = (response.text or "").lower()
        success = (
            response.status_code == 200
            and "xml error" not in lowered
            and "error[4]" not in lowered
            and "request too large" not in lowered
            and "status=\"4\"" not in lowered
            and "status='4'" not in lowered
        )
        debug_log(f"POST {ip} status={response.status_code} body={preview}")
        return success
    except requests.exceptions.RequestException:
        debug_log(f"POST {ip} request_failed")
        return False


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
    body = saxutils.escape("" if text is None else str(text))
    return xml_document(
        "<CiscoIPPhoneText>"
        f"<Title>{title}</Title>"
        "<Prompt>Select an Action</Prompt>"
        f"<Text>{body}</Text>"
        "</CiscoIPPhoneText>"
    )


def xml_text_message_with_back(name, text, back_url):
    title = saxutils.escape("" if name is None else str(name))
    body = saxutils.escape("" if text is None else str(text))
    url = saxutils.escape(back_url)
    return xml_document(
        "<CiscoIPPhoneText>"
        f"<Title>{title}</Title>"
        "<Prompt>Details</Prompt>"
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
    return model.startswith("79") or model.startswith("88")


def xml_image_message(name, short_text, bg_color, symbol, image_url):
    title = saxutils.escape("" if name is None else str(name))
    url = saxutils.escape(image_url)
    return xml_document(
        "<CiscoIPPhoneImageFile>"
        f"<Title>{title}</Title>"
        "<Prompt>Select an action</Prompt>"
        "<LocationX>0</LocationX>"
        "<LocationY>0</LocationY>"
        f"<URL>{url}</URL>"
        "</CiscoIPPhoneImageFile>"
    )


def build_image_url(phone_ip, short_text, bg_color, symbol):
    base_ip = local_ip_for_phone(phone_ip)
    params = {
        "resolution": "600x300",
        "bg": bg_color or "FFFFFF",
        "text": "" if short_text is None else str(short_text),
    }
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


def persist_details_snapshot(phone_ip, message):
    DETAILS_STORE_DIR.mkdir(parents=True, exist_ok=True)
    snapshot_id = uuid.uuid4().hex
    payload = {
        "server_ip": local_ip_for_phone(phone_ip),
        "name": message.get("name", "") or "",
        "shortmessage": message.get("shortmessage", "") or "",
        "longmessage": message.get("longmessage", "") or "",
        "color": (message.get("color") or "").strip() or "FFFFFF",
        "icon": (message.get("icon") or "").strip(),
    }
    with open(DETAILS_STORE_DIR / f"{snapshot_id}.json", "w", encoding="utf-8") as handle:
        json.dump(payload, handle)
    return snapshot_id


def build_visual_payloads(endpoint, message):
    visual_mode = normalize_visual_mode(endpoint.get("visual"))
    if visual_mode == "none":
        return []
    name = message.get("name", "")
    longmessage = message.get("longmessage", "")
    if visual_mode == "image" and model_supports_visual(endpoint.get("model")):
        color = (message.get("color") or "").strip() or "FFFFFF"
        symbol = (message.get("icon") or "").strip()
        short_text = message.get("shortmessage", "") or ""
        image_url = build_image_url(endpoint.get("ipv4"), short_text, color, symbol)
        payloads = []
        if longmessage and str(longmessage).strip():
            snapshot_id = persist_details_snapshot(endpoint.get("ipv4"), message)
            payloads.append(
                (
                    "image_details",
                    xml_execute_url(details_server_url(endpoint.get("ipv4"), "image", snapshot_id)),
                )
            )
        payloads.append(("image", xml_image_message(name, short_text, color, symbol, image_url)))
        if longmessage and str(longmessage).strip():
            payloads.append(("text", xml_text_message(name, longmessage)))
        return payloads
    return [("text", xml_text_message(name, longmessage))]


def send_visual_payload_sequence(ip, payloads):
    for label, xml in payloads:
        debug_log(f"send_visual_payload_sequence ip={ip} mode={label} xml={xml[:240]}")
        if send_phone_request(ip, xml):
            return True
    return False


def send_endpoint_visuals(endpoints, message):
    visual_targets = []
    for endpoint in endpoints:
        ip = endpoint.get("ipv4")
        if not ip:
            continue
        payloads = build_visual_payloads(endpoint, message)
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


def xml_start_multicast(mcast_ip, mcast_port):
    return xml_document(
        "<CiscoIPPhoneExecute>"
        f"<ExecuteItem Priority=\"0\" URL=\"RTPMRx:{mcast_ip}:{mcast_port}\"/>"
        "</CiscoIPPhoneExecute>"
    )


def xml_stop_multicast():
    return xml_document(
        "<CiscoIPPhoneExecute>"
        "<ExecuteItem Priority=\"0\" URL=\"RTPMRx:Stop\"/>"
        "</CiscoIPPhoneExecute>"
    )


def normalize_device_id(value):
    if value is None:
        return ""
    return "".join(ch for ch in str(value).upper() if ch.isalnum())


def fetch_endpoints_and_message(targets, msg_id):
    endpoint_columns = table_columns("endpoints-output-ciscosep")
    message_columns = table_columns("messages")
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
    conn = db()
    try:
        with conn.cursor() as cur:
            use_all = any(target == "all" for target in targets)
            cur.execute(
                f"SELECT {', '.join(endpoint_select)} FROM `endpoints-output-ciscosep` "
                "WHERE ipv4 IS NOT NULL AND ipv4 <> ''"
            )
            endpoints = cur.fetchall()
            if not use_all:
                normalized_targets = {normalize_device_id(target) for target in targets if target}
                endpoints = [
                    endpoint
                    for endpoint in endpoints
                    if normalize_device_id(endpoint.get("macaddr")) in normalized_targets
                ]
            cur.execute(f"SELECT {', '.join(message_select)} FROM messages WHERE messageid=%s", (msg_id,))
            message = cur.fetchone()
    finally:
        conn.close()
    debug_log(f"fetch_endpoints_and_message targets={targets} matched={[(ep.get('macaddr'), ep.get('ipv4'), ep.get('status'), ep.get('audio'), ep.get('model'), ep.get('visual')) for ep in endpoints]} message_found={bool(message)}")
    return endpoints or [], message


def update_endpoint_status(ip, status):
    conn = db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE `endpoints-output-ciscosep` SET status=%s WHERE ipv4=%s",
                (status, ip),
            )
        conn.commit()
    finally:
        conn.close()


def stream_watchdog(stream_id):
    while True:
        time.sleep(WATCHDOG_INTERVAL)
        with streams_lock:
            stream = active_streams.get(stream_id)
            if stream is None:
                break
            if not stream.get("received_audio") and time.time() < stream.get("pre_audio_until", 0):
                continue
            if time.time() - stream["last_seen"] <= STREAM_IDLE_TIMEOUT:
                continue
        stop_stream(stream_id)
        break


def ensure_stream(stream_id, audio_ips):
    with streams_lock:
        stream = active_streams.get(stream_id)
        if stream is None:
            stream = {
                "seq": 0,
                "ts": 0,
                "ssrc": random.randint(1, 0xFFFFFFFF),
                "last_seen": time.time(),
                "mcast_ip": f"239.{random.randint(1,254)}.{random.randint(1,254)}.{random.randint(1,254)}",
                "mcast_port": random.randrange(20480, 32768, 2),
                "phones": set(),
                "received_audio": False,
                "pre_audio_until": time.time() + PRE_AUDIO_GRACE_SECONDS,
            }
            active_streams[stream_id] = stream
            threading.Thread(target=stream_watchdog, args=(stream_id,), daemon=True).start()
        new_ips = [ip for ip in audio_ips if ip not in stream["phones"]]
        stream["phones"].update(audio_ips)
        return stream.copy(), new_ips


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
            send_ready_signal("ciscosep", stream_id)
        return
    debug_log(f"handle_dispatch action={action} stream={stream_id} msg={msg_id} targets={normalized_targets}")
    endpoints, message = fetch_endpoints_and_message(normalized_targets, msg_id)
    if not message:
        if action == "prepare_audio":
            send_ready_signal("ciscosep", stream_id)
        debug_log(f"message_not_found msg={msg_id}")
        return
    msg_type = message.get("type", "text+audio")
    name = message.get("name", "")
    longmessage = message.get("longmessage", "")
    online_endpoints = [
        endpoint
        for endpoint in endpoints
        if endpoint.get("ipv4") and endpoint.get("status") in ("Unchecked", "Online")
    ]
    debug_log(f"online_endpoints={[(ep.get('macaddr'), ep.get('ipv4'), ep.get('audio'), ep.get('model'), ep.get('visual')) for ep in online_endpoints]} msg_type={msg_type}")
    if msg_type in ("text", "text+audio"):
        if online_endpoints:
            send_endpoint_visuals(online_endpoints, message)
        else:
            debug_log("no_text_endpoints")
    if msg_type not in ("audio", "text+audio"):
        if action == "prepare_audio":
            send_ready_signal("ciscosep", stream_id)
        return
    audio_ips = [
        endpoint["ipv4"]
        for endpoint in online_endpoints
        if endpoint.get("audio") == "Multicast"
    ]
    if action != "prepare_audio":
        debug_log("audio message arrived on non-prepare action")
        return
    if not audio_ips:
        debug_log("no_audio_ips")
        send_ready_signal("ciscosep", stream_id)
        return
    stream, new_ips = ensure_stream(stream_id, audio_ips)
    debug_log(f"prepare_audio stream={stream_id} multicast={stream['mcast_ip']}:{stream['mcast_port']} new_ips={new_ips}")
    if new_ips:
        send_parallel_and_wait(new_ips, xml_start_multicast(stream["mcast_ip"], stream["mcast_port"]))
    send_ready_signal("ciscosep", stream_id)


def handle_api(command_string):
    parts = str(command_string).strip().split()
    if len(parts) < 4:
        return
    handle_dispatch(parts[0], parts[2], parts[3], [parts[1]])


def receive_audio(chunk, stream_id):
    with streams_lock:
        stream = active_streams.get(stream_id)
        if stream is None:
            debug_log(f"receive_audio missing_stream stream={stream_id} bytes={len(chunk)}")
            return
        seq = stream["seq"]
        ts = stream["ts"]
        ssrc = stream["ssrc"]
        mcast_ip = stream["mcast_ip"]
        mcast_port = stream["mcast_port"]
        stream["received_audio"] = True
    offset = 0
    while offset < len(chunk):
        frame = chunk[offset:offset + 160]
        if len(frame) < 160:
            frame = frame.ljust(160, b"\xff")
        packet = struct.pack("!BBHII", 0x80, PAYLOAD_TYPE, seq, ts, ssrc) + frame
        try:
            rtp_sock.sendto(packet, (mcast_ip, mcast_port))
        except OSError:
            pass
        seq = (seq + 1) % 65536
        ts = (ts + 160) % 4294967296
        offset += 160
    with streams_lock:
        if stream_id in active_streams:
            active_streams[stream_id]["seq"] = seq
            active_streams[stream_id]["ts"] = ts
            active_streams[stream_id]["last_seen"] = time.time()
    debug_log(f"receive_audio stream={stream_id} bytes={len(chunk)} seq={seq} ts={ts}")


def stop_stream(stream_id):
    with streams_lock:
        stream = active_streams.pop(stream_id, None)
    if not stream:
        debug_log(f"stop_stream missing stream={stream_id}")
        return
    phones = sorted(stream["phones"])
    debug_log(f"stop_stream stream={stream_id} phones={phones}")
    if phones:
        send_parallel_and_wait(phones, xml_stop_multicast())


def end_stream(stream_id):
    stop_stream(stream_id)
