#!/usr/bin/env python3

import json
import os
import shutil
import threading
import urllib.parse
import urllib.request
import xml.sax.saxutils as saxutils
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
STORE_DIR = BASE_DIR / "details_store"
server = None
thread = None


def xml_document(body):
    return f'<?xml version="1.0" encoding="utf-8"?>{body}'


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


def thumb_source_url(server_ip, snapshot):
    params = {
        "resolution": "600x300",
        "bg": snapshot.get("color") or "FFFFFF",
        "text": snapshot.get("shortmessage") or "",
    }
    icon = snapshot.get("icon") or ""
    if icon:
        params["symbol"] = icon
    return f"http://{server_ip}:6975/thumb?{urllib.parse.urlencode(params)}"


def image_url(server_ip, snapshot_id):
    return f"http://{server_ip}:6967/thumb?id={urllib.parse.quote(snapshot_id)}"


def image_page(server_ip, snapshot_id, snapshot):
    title = saxutils.escape(snapshot.get("name") or "")
    image = saxutils.escape(image_url(server_ip, snapshot_id))
    has_details = bool((snapshot.get("longmessage") or "").strip())
    parts = [
        "<CiscoIPPhoneImageFile>",
        f"<Title>{title}</Title>",
        "<Prompt>Select an action</Prompt>",
        "<LocationX>0</LocationX>",
        "<LocationY>0</LocationY>",
        f"<URL>{image}</URL>",
    ]
    if has_details:
        detail_url = saxutils.escape(f"http://{server_ip}:6967/details?id={urllib.parse.quote(snapshot_id)}")
        parts.extend(
            [
                "<SoftKeyItem>",
                "<Name>Exit</Name>",
                "<URL>SoftKey:Exit</URL>",
                "<Position>1</Position>",
                "</SoftKeyItem>",
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
    title = saxutils.escape(snapshot.get("name") or "")
    body = saxutils.escape(snapshot.get("longmessage") or "")
    back_url = saxutils.escape(f"http://{server_ip}:6967/image?id={urllib.parse.quote(snapshot_id)}")
    return xml_document(
        "<CiscoIPPhoneText>"
        f"<Title>{title}</Title>"
        "<Prompt>Details</Prompt>"
        f"<Text>{body}</Text>"
        "<SoftKeyItem>"
        "<Name>Back</Name>"
        f"<URL>{back_url}</URL>"
        "<Position>1</Position>"
        "</SoftKeyItem>"
        "</CiscoIPPhoneText>"
    )


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path not in ("/image", "/details", "/thumb"):
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
