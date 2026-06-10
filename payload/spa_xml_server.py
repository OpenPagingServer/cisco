
import os
import re
import threading
import urllib.parse
import uuid
import xml.sax.saxutils as saxutils
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
server = None
thread = None
messages = {}
messages_lock = threading.Lock()


def normalize_mac(value):
    if value is None:
        return ""
    return "".join(ch for ch in str(value).upper() if ch.isalnum())


def xml_document(body):
    return f'<?xml version="1.0" encoding="UTF-8"?>{body}'


def xml_text_content(value):
    text = "" if value is None else str(value)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return saxutils.escape(text).replace("\n", "&#10;")


def text_page(title, prompt, text):
    safe_title = saxutils.escape("" if title is None else str(title))
    safe_prompt = saxutils.escape("" if prompt is None else str(prompt))
    safe_text = xml_text_content(text)
    return xml_document(
        "<CiscoIPPhoneText>"
        f"<Title>{safe_title}</Title>"
        f"<Prompt>{safe_prompt}</Prompt>"
        f"<Text>{safe_text}</Text>"
        "</CiscoIPPhoneText>"
    )


def expired_page():
    return text_page(
        "Message expired",
        "Message expired",
        "This message has expired and is no longer in affect.",
    )


def user_agent_mac(user_agent):
    match = re.search(r"\(([0-9A-Fa-f:.\-]+)\)\s*$", user_agent or "")
    if not match:
        return ""
    return normalize_mac(match.group(1))


def store_text_message(title, prompt, text, allowed_macs):
    message_id = uuid.uuid4().hex
    allowed = {normalize_mac(mac) for mac in allowed_macs if normalize_mac(mac)}
    payload = text_page(title, prompt, text)
    with messages_lock:
        messages[message_id] = {
            "body": payload,
            "allowed_macs": allowed,
        }
    return message_id


def clear_message(message_id):
    if not message_id:
        return
    with messages_lock:
        messages.pop(message_id, None)


def clear_all():
    with messages_lock:
        messages.clear()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if not parsed.path.endswith(".xml"):
            self.send_404(expired_page())
            return
        message_id = parsed.path.strip("/").rsplit("/", 1)[-1][:-4]
        with messages_lock:
            record = messages.get(message_id)
        if record is None:
            self.send_404(expired_page())
            return
        mac = user_agent_mac(self.headers.get("User-Agent", ""))
        if not mac:
            self.send_empty_403()
            return
        if mac not in record.get("allowed_macs", set()):
            self.send_empty_403()
            return
        self.send_xml(200, record["body"])

    def send_xml(self, status, body):
        encoded = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/xml; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def send_404(self, body):
        self.send_xml(404, body)

    def send_empty_403(self):
        self.send_response(403)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format, *args):
        return


def start():
    global server, thread
    if server is not None:
        return
    clear_all()
    port = int(os.getenv("CISCO_SPA_XML_PORT", "6989"))
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
    clear_all()
