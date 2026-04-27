#!/usr/bin/env python3

import importlib
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


def load_message_send():
    return importlib.import_module("ciscosep_message_send_runtime")


message_send = load_message_send()
ACTIVATION_RETRY_WINDOW = 5.0
ACTIVATION_RETRY_INTERVAL = 1.0


def handle_dispatch(action, stream_id, group_id, targets, metadata=None):
    if action != "prepare_livepage":
        return
    normalized_targets = []
    for target in targets:
        token = str(target).strip()
        if token and token not in normalized_targets:
            normalized_targets.append(token)
    if not normalized_targets:
        message_send.send_ready_signal("ciscosep", stream_id)
        return
    endpoints, _ = message_send.fetch_endpoints_and_message(normalized_targets, "-1")
    online_endpoints = [
        endpoint
        for endpoint in endpoints
        if endpoint.get("ipv4") and endpoint.get("status") in ("Unchecked", "Online")
    ]
    audio_ips = [
        endpoint["ipv4"]
        for endpoint in online_endpoints
        if endpoint.get("audio") == "Multicast"
    ]
    if not audio_ips:
        message_send.send_ready_signal("ciscosep", stream_id)
        return
    stream, new_ips = message_send.ensure_stream(stream_id, audio_ips)
    if new_ips:
        xml = message_send.xml_start_multicast(stream["mcast_ip"], stream["mcast_port"])
        remaining = list(new_ips)
        deadline = time.time() + ACTIVATION_RETRY_WINDOW
        while remaining and time.time() < deadline:
            next_remaining = []
            for ip in remaining:
                if message_send.send_phone_request(ip, xml, timeout_seconds=1.0):
                    continue
                next_remaining.append(ip)
            remaining = next_remaining
            if remaining and time.time() < deadline:
                time.sleep(ACTIVATION_RETRY_INTERVAL)
        for ip in remaining:
            message_send.update_endpoint_status(ip, "Offline")
            message_send.remove_stream_phone(stream_id, ip)
    message_send.send_ready_signal("ciscosep", stream_id)


def receive_audio(chunk, stream_id):
    message_send.receive_audio(chunk, stream_id)


def end_stream(stream_id):
    message_send.end_stream(stream_id)
