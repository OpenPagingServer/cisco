#!/usr/bin/env python3

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DEBUG = os.getenv("DEBUG", "").strip().lower() == "true"
message_send = None


def page_debug(message):
    if DEBUG and message_send is not None:
        message_send.debug_log(f"page_handler {message}")


def init(message_send_module):
    global message_send
    message_send = message_send_module


def handle_dispatch(action, stream_id, group_id, targets, metadata=None):
    page_debug(f"handle_dispatch_start action={action} stream={stream_id} group={group_id} targets={targets} metadata={metadata}")
    if action != "prepare_livepage":
        return
    if message_send is None:
        page_debug(f"handle_dispatch_no_message_send stream={stream_id}")
        return
    normalized_targets = []
    for target in targets:
        token = str(target).strip()
        if token and token not in normalized_targets:
            normalized_targets.append(token)
    if not normalized_targets:
        page_debug(f"handle_dispatch_no_targets stream={stream_id}")
        message_send.send_ready_signal("cisco", stream_id)
        return
    message_send.debug_log(f"livepage handle_dispatch action={action} stream={stream_id} group={group_id} targets={normalized_targets}")
    target_info = message_send.parse_targets(normalized_targets)
    spa_multicast_targets = message_send.fetch_spa_multicast_targets(target_info)
    endpoints, _ = message_send.fetch_endpoints_and_message(normalized_targets, "-1")
    page_debug(
        f"handle_dispatch_targets stream={stream_id} normalized={normalized_targets} "
        f"target_info={target_info} endpoints={[(ep.get('macaddr'), ep.get('ipv4'), ep.get('status'), ep.get('audio')) for ep in endpoints]} "
        f"spa={[(target.get('id'), target.get('address'), target.get('port')) for target in spa_multicast_targets]}"
    )
    message_key = message_send.message_auth_key(stream_id, group_id)
    message_send.prepare_auth_credentials(endpoints, message_key)
    online_endpoints = [
        endpoint
        for endpoint in endpoints
        if endpoint.get("ipv4") and endpoint.get("status") in ("Unchecked", "Online")
    ]
    message_send.debug_log(
        f"livepage online_endpoints={[(ep.get('macaddr'), ep.get('ipv4'), ep.get('audio'), ep.get('model'), ep.get('visual')) for ep in online_endpoints]}"
    )
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
    if not audio_ips and not unicast_endpoints and not spa_multicast_targets:
        message_send.debug_log("livepage no_audio_ips")
        page_debug(f"handle_dispatch_no_audio stream={stream_id}")
        message_send.send_ready_signal("cisco", stream_id)
        message_send.clear_auth_credentials(message_key)
        return
    stream, new_ips, new_unicast_ips = message_send.ensure_stream(
        stream_id,
        audio_ips,
        message_key,
        spa_multicast_targets,
        unicast_endpoints,
        message_send.LIVE_PAGE_SOURCE_KIND,
    )
    message_send.debug_log(
        f"livepage prepare stream={stream_id} multicast={stream['mcast_ip']}:{stream['mcast_port']} "
        f"new_ips={new_ips} new_unicast_ips={new_unicast_ips} "
        f"spa_multicast_targets={[(target.get('id'), target.get('address'), target.get('port')) for target in spa_multicast_targets]}"
    )
    active_multicast_phone_count = message_send.start_multicast_phone_sessions(
        audio_ips,
        stream_id,
        "livepage",
        message_send.LIVE_PAGE_SOURCE_KIND,
    )
    if audio_ips and active_multicast_phone_count == 0 and not new_unicast_ips and not spa_multicast_targets:
        message_send.debug_log(f"livepage no active Cisco phones after multicast start failures stream={stream_id}")
        page_debug(f"handle_dispatch_no_active_phones stream={stream_id}")
        message_send.stop_stream(stream_id)
        message_send.send_ready_signal("cisco", stream_id)
        return
    for endpoint in unicast_endpoints:
        ip = endpoint.get("ipv4")
        if not ip or ip not in new_unicast_ips:
            continue
        server_ip = message_send.local_ip_for_phone(ip)
        session, created = message_send.add_unicast_source(
            ip,
            server_ip,
            stream_id,
            message_send.LIVE_PAGE_SOURCE_KIND,
        )
        if created:
            result = message_send.send_phone_request_with_result(
                ip,
                message_send.xml_start_unicast(server_ip, session["port"]),
            )
            if not result.get("success"):
                message_send.remove_unicast_sources(stream_id, [ip])
                with message_send.streams_lock:
                    active_stream = message_send.active_streams.get(stream_id)
                    if active_stream is not None:
                        active_stream.get("unicast_phone_ips", set()).discard(ip)
                message_send.debug_log(
                    f"livepage removed failed unicast start ip={ip} status={result.get('status')} device_status_unchanged=true"
                )
        else:
            message_send.debug_log(f"livepage multiplexed unicast ip={ip} stream={stream_id} port={session['port']}")
    page_debug(f"handle_dispatch_ready stream={stream_id}")
    message_send.send_ready_signal("cisco", stream_id)


def receive_audio(chunk, stream_id):
    message_send.receive_audio(chunk, stream_id)


def end_stream(stream_id):
    message_send.end_stream(stream_id)
