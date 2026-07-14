import html
import ipaddress
import re


ENTERPRISE_TABLE = "endpoints-output-cisco"
MULTICAST_TABLE = "endpoints-output-cisco-spamulticast"
EXE_TABLE = "endpoints-output-cisco-spaxmlexe"
SETTINGS_TABLE = "endpoints-modulesettings-cisco"

MODELS = [
    "7811", "7821", "7841", "7861",
    "7925", "7926", "7931", "7940", "7941", "7942", "7945", "7960", "7961", "7962", "7965", "7970", "7971", "7975",
    "8811", "8831", "8841", "8845", "8851", "8861", "8865", "8875", "8961",
    "9811", "9841", "9851", "9861", "9871",
    "9951", "9971",
]
AUDIO_MODES = ["Multicast", "Unicast", "Disabled"]
VISUAL_MODES = ["None", "Text", "Image"]
TEXT_ONLY_MODELS = {"7811", "7821", "7841", "7861", "8831", "9811", "9841", "9851"}
VOLUMES = ["0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100", "asis"]
DEFAULT_SETTINGS = {
    "messageinfo-enabled": "1",
    "messageinfo-showsender": "1",
    "messageinfo-productname": "1",
    "ucmsync": "0",
    "ucmsync-ip": "",
    "ucmsync-username": "",
    "ucmsync-password": "",
    "ucmsync-interval": "300",
    "authrelay": "false",
}


def h(value):
    return html.escape("" if value is None else str(value), quote=True)


def truthy(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def normalize_device_id(value):
    return re.sub(r"[^A-Za-z0-9]", "", str(value or "")).upper()


def normalize_macaddr(value):
    cleaned = re.sub(r"[^A-Fa-f0-9]", "", str(value or "")).upper()
    raw = re.sub(r"[^A-Za-z0-9]", "", str(value or "")).upper()
    if raw.startswith("SEP"):
        cleaned = re.sub(r"[^A-Fa-f0-9]", "", raw[3:]).upper()
    if len(cleaned) != 12:
        raise ValueError("Enter a valid 12-digit hexadecimal MAC address.")
    return "SEP" + cleaned


def normalize_host_or_ip(value):
    host = str(value or "").strip()
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1].strip()
    return host


def validate_host_or_ip(value):
    host = normalize_host_or_ip(value)
    if not host:
        raise ValueError("Hostname or IP is required.")
    try:
        ipaddress.ip_address(host)
        return host
    except ValueError:
        pass
    if len(host) > 255 or any(ch in host for ch in "/\\?#@"):
        raise ValueError("Enter a valid hostname, IPv4 address, or IPv6 address.")
    if any(ch.isspace() for ch in host):
        raise ValueError("Enter a valid hostname, IPv4 address, or IPv6 address.")
    return host


def ensure_model_enum(cur, table_name):
    cur.execute(f"SHOW COLUMNS FROM `{table_name}` LIKE 'model'")
    row = cur.fetchone()
    if not row:
        return
    current_values = re.findall(r"'((?:[^'\\\\]|\\\\.)*)'", str(row.get("Type") or ""))
    if current_values == ["", *MODELS] and str(row.get("Default") or "") == "":
        return
    enum_sql = ",".join(f"'{model}'" for model in ["", *MODELS])
    placeholders = ",".join(["%s"] * len(MODELS))
    cur.execute(
        f"UPDATE `{table_name}` SET `model`='' "
        f"WHERE `model` IS NOT NULL AND `model` NOT IN ({placeholders})",
        tuple(MODELS),
    )
    cur.execute(
        f"ALTER TABLE `{table_name}` "
        f"MODIFY COLUMN `model` ENUM({enum_sql}) NOT NULL DEFAULT ''"
    )


def ensure_varchar_column(cur, table_name, column_name, size):
    cur.execute(f"SHOW COLUMNS FROM `{table_name}` LIKE %s", (column_name,))
    row = cur.fetchone()
    if not row:
        return
    column_type = str(row.get("Type") or "").lower()
    if column_type == f"varchar({size})":
        return
    default = row.get("Default")
    null_sql = "NOT NULL" if row.get("Null") == "NO" else "NULL"
    escaped_default = str(default).replace("'", "''") if default is not None else ""
    default_sql = f" DEFAULT '{escaped_default}'" if default is not None else ""
    cur.execute(
        f"ALTER TABLE `{table_name}` "
        f"MODIFY COLUMN `{column_name}` VARCHAR({size}) {null_sql}{default_sql}"
    )


def forms():
    return {
        "enterprise": {
            "label": "Cisco Enterprise (SEP)",
            "description": "Send audio and visual messages to Cisco UCM phones<br>7900, 8900, 9900 series phones<br>7800, 8800, 9800 series phones running UCM/Enterprise firmware",
        },
        "spa-multicast": {
            "label": "Cisco SPA Multicast",
            "description": "Cisco/Sipura SPA multicast paging target.",
        },
        "spa-exe": {
            "label": "Cisco SPA/MPP EXE",
            "description": "Send visual notifications to Cisco MPP phones<br>SPA series phones<br>7800, 8800, 9800 series phones running MPP/3PCC firmware",
        },
    }


def ensure_schema(conn_factory):
    conn = conn_factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS `{ENTERPRISE_TABLE}` ("
                "`macaddr` VARCHAR(64) NOT NULL, `name` VARCHAR(255) DEFAULT '', `ipv4` VARCHAR(45) DEFAULT '', "
                "`status` VARCHAR(32) NOT NULL DEFAULT 'Unchecked', `audio` VARCHAR(32) NOT NULL DEFAULT 'Multicast', "
                "`model` VARCHAR(32) NOT NULL DEFAULT '', `visual` VARCHAR(32) NOT NULL DEFAULT 'Image', "
                "`volume` VARCHAR(32) NOT NULL DEFAULT 'asis', `addedby` VARCHAR(32) NOT NULL DEFAULT 'MANUAL', "
                "PRIMARY KEY (`macaddr`), KEY `ipv4_idx` (`ipv4`), KEY `status_idx` (`status`)"
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"
            )
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS `{MULTICAST_TABLE}` ("
                "`id` INT NOT NULL AUTO_INCREMENT, `name` VARCHAR(100) NOT NULL DEFAULT '', "
                "`address` VARCHAR(45) NOT NULL DEFAULT '', `port` INT NOT NULL DEFAULT 0, PRIMARY KEY (`id`)"
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"
            )
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS `{EXE_TABLE}` ("
                "`id` INT NOT NULL AUTO_INCREMENT, `ipv4` VARCHAR(255) NOT NULL DEFAULT '', "
                "`username` VARCHAR(255) NOT NULL DEFAULT '', `password` VARCHAR(255) NOT NULL DEFAULT '', "
                "`macaddress` VARCHAR(64) NOT NULL DEFAULT '', `status` VARCHAR(32) NOT NULL DEFAULT 'Unchecked', "
                "PRIMARY KEY (`id`), KEY `macaddress_idx` (`macaddress`), KEY `ipv4_idx` (`ipv4`), KEY `status_idx` (`status`)"
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"
            )
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS `{SETTINGS_TABLE}` ("
                "`parameter` VARCHAR(128) NOT NULL, `value` TEXT, PRIMARY KEY (`parameter`)"
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"
            )
            ensure_model_enum(cur, ENTERPRISE_TABLE)
            ensure_varchar_column(cur, ENTERPRISE_TABLE, "ipv4", 255)
            ensure_varchar_column(cur, EXE_TABLE, "ipv4", 255)
        conn.commit()
    finally:
        conn.close()


def query_all(conn_factory, sql, params=()):
    conn = conn_factory()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


def query_one(conn_factory, sql, params=()):
    rows = query_all(conn_factory, sql, params)
    return rows[0] if rows else None


def execute(conn_factory, sql, params=()):
    conn = conn_factory()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def module_body(content):
    return (
        "<style>body{font-family:Tahoma,sans-serif;margin:0;padding:18px;color:#202124;background:#fff}.grid{display:grid;gap:12px}.row{display:grid;gap:6px}"
        "label{font-weight:500}.check{display:flex;align-items:center;gap:8px;font-weight:400}.model-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(92px,1fr));gap:10px}"
        ".control{padding:10px;border:1px solid #ddd;border-radius:4px;font:inherit}.button,button{background:#1976D2;color:#fff;border:0;border-radius:4px;padding:10px 14px;font:inherit;cursor:pointer;text-decoration:none;display:inline-flex;align-items:center;justify-content:center}.button.secondary{background:#5f6368}"
        ".secondary{background:#5f6368}.danger{background:#c62828}.success{background:#e8f5e9;border:1px solid #a5d6a7;color:#1b5e20;padding:10px;border-radius:6px;margin-bottom:12px}"
        ".error{background:#ffebee;border:1px solid #ef9a9a;color:#b71c1c;padding:10px;border-radius:6px;margin-bottom:12px}.warn{background:#fff8e1;border:1px solid #ffe082;color:#5d4037;padding:12px;border-radius:6px;margin-bottom:12px}"
        ".info{display:flex;gap:10px;align-items:flex-start;background:#e3f2fd;border:1px solid #90caf9;color:#0d47a1;padding:12px;border-radius:6px;margin-bottom:12px}.info .icon{font-weight:700;line-height:1}"
        ".hidden{display:none!important}.note,.meta{color:#5f6368}.section{border-top:1px solid #eee;padding-top:12px;margin-top:4px}.title{font-size:22px;font-weight:600;margin:0 0 14px}.subtitle{margin:0 0 18px;color:#5f6368}.selected-model{background:#E3F2FD;border:1px solid #90CAF9;color:#0D47A1;padding:10px;border-radius:6px;margin-bottom:12px}.topbar{display:flex;align-items:center;gap:10px;margin-bottom:14px}.audio-key{font-size:13px;color:#5f6368;line-height:1.45;margin-top:2px}.audio-key div{margin:4px 0}.audio-key strong{color:#202124}"
        "@media(prefers-color-scheme:dark){body{background:#1e1e1e;color:#e0e0e0}.control{background:#171717;border-color:#333;color:#eee}.button,button{background:#BB86FC;color:#000}.button.secondary{background:#444;color:#eee}.subtitle,.note,.meta,.audio-key{color:#aaa}.selected-model{background:#102334;border-color:#24577d;color:#b8ddff}.audio-key strong{color:#e0e0e0}.info{background:#102334;border-color:#24577d;color:#b8ddff}}</style>"
        + content
    )


def alert(message, error):
    out = ""
    if message:
        out += f'<div class="success">{h(message)}</div>'
    if error:
        out += f'<div class="error">{h(error)}</div>'
    return out


def selected(actual, expected):
    return " selected" if str(actual) == str(expected) else ""


def option_list(options, current):
    return "".join(f"<option{selected(current, opt)}>{h(opt)}</option>" for opt in options)


def validate_port(value):
    port = int(str(value or "0"))
    if port < 1 or port > 65535:
        raise ValueError("Port must be between 1 and 65535.")
    return port


def active_visual_modes(model):
    return ["None", "Text"] if str(model) in TEXT_ONLY_MODELS else VISUAL_MODES


def render_form(form_type, request, conn_factory, page, user):
    ensure_schema(conn_factory)
    if form_type not in forms():
        return page("Endpoint Form", module_body("<h1>Endpoint form not found</h1>"), "endpoints", user, status=404)
    if form_type == "spa-multicast":
        deprecated_body = (
            "<div class='warn'><strong>Deprecated</strong><p>"
            "Starting in Open Paging Server 0.4.0, Multicast RTP is now built-in. Since Cisco SPA &amp; MPP frimware phones accept any Multicast RTP stream, this eliminates the need for this functionally in the module. Existing endpoints will remain functional. However, you should move over since this will be removed before the first stable OPS release."
            "</p></div>"
        )
        return page(forms()[form_type]["label"], module_body(deprecated_body), "endpoints", user)
    message = ""
    error = ""
    selected_model = str(request.form.get("model", "") or "").strip()
    values = {
        "macaddr": "", "name": "", "ipv4": "", "unchecked": "", "audio": "Multicast",
        "model": selected_model, "visual": "Text" if selected_model in TEXT_ONLY_MODELS else "Image", "volume": "asis",
        "address": "", "port": "", "username": "", "password": "", "macaddress": "",
    }
    if request.method == "POST":
        try:
            for key in values:
                values[key] = str(request.form.get(key, values[key]) or "").strip()
            if form_type == "enterprise" and request.form.get("macaddr"):
                macaddr = normalize_macaddr(values["macaddr"])
                if values["model"] not in MODELS:
                    raise ValueError("Phone model is required.")
                visual_modes = active_visual_modes(values["model"])
                audio = values["audio"] if values["audio"] in AUDIO_MODES else "Multicast"
                visual = values["visual"] if values["visual"] in visual_modes else visual_modes[-1]
                volume = values["volume"] if values["volume"] in VOLUMES else "asis"
                if query_one(conn_factory, f"SELECT macaddr FROM `{ENTERPRISE_TABLE}` WHERE macaddr=%s", (macaddr,)):
                    raise ValueError("That Cisco SEP endpoint already exists.")
                host = validate_host_or_ip(values["ipv4"])
                status = "Unchecked" if request.form.get("unchecked") else "New"
                execute(
                    conn_factory,
                    f"INSERT INTO `{ENTERPRISE_TABLE}` (macaddr, name, ipv4, status, audio, model, visual, volume, addedby) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'MANUAL')",
                    (macaddr, values["name"], host, status, audio, values["model"], visual, volume),
                )
                message = "Cisco enterprise endpoint added."
                selected_model = ""
            elif form_type == "spa-multicast":
                if not values["name"] or not values["address"] or not values["port"]:
                    raise ValueError("Name, address, and port are required.")
                execute(conn_factory, f"INSERT INTO `{MULTICAST_TABLE}` (name, address, port) VALUES (%s,%s,%s)", (values["name"], values["address"], validate_port(values["port"])))
                message = "Cisco SPA multicast endpoint added."
            elif form_type == "spa-exe":
                label = str(values["macaddress"] or "").strip()
                label_token = normalize_device_id(label)
                host = validate_host_or_ip(values["ipv4"])
                if not label_token:
                    raise ValueError("Label is required.")
                if any(normalize_device_id(row.get("macaddress")) == label_token for row in query_all(conn_factory, f"SELECT id, macaddress FROM `{EXE_TABLE}`")):
                    raise ValueError("That SPA EXE label already exists.")
                status = "Unchecked" if request.form.get("unchecked") else "New"
                execute(conn_factory, f"INSERT INTO `{EXE_TABLE}` (ipv4, username, password, macaddress, status) VALUES (%s,%s,%s,%s,%s)", (host, values["username"], values["password"], label, status))
                message = "Cisco SPA EXE endpoint added."
        except Exception as exc:
            error = str(exc)

    if form_type == "enterprise" and not selected_model:
        buttons = "".join(f"<button class='button' type='submit' name='model' value='{h(model)}'>{h(model)}</button>" for model in MODELS)
        return page("Cisco Enterprise Endpoint", module_body(f"{alert(message, error)}<h1 class='title'>Device model</h1><p class='subtitle'>(This page will be redone before the final release!, only tested phones are here for now)</p><form method='post' class='model-grid'>{buttons}</form>"), "endpoints", user)
    if form_type == "enterprise":
        visual_modes = active_visual_modes(selected_model)
        audio_key = "<div class='audio-key'><div><strong>Multicast:</strong> Sends a single RTP stream for all phones receiving a page. Uses less server resources, less delay. Requires multicast compatible network infrastructure. High amount of packet loss on weak WLAN. Does not usually transmit over NAT/WAN & VPN tunnels. Enable IGMP on your network switch(es) for the best results.</div><div><strong>Unicast:</strong> Sends RTP streams directly to the phone. Works better over WAN, VPN, and WLAN. Uses more server resources, may cause noticeable delay between speakers. Use Unicast only if Multicast cannot be used on your network.</div><div><strong>Disabled:</strong> Audio will not be sent to this telephone.</div></div>"
        body = (
            f"{alert(message, error)}<div class='topbar'><form method='post'><button class='button secondary' type='submit' name='model' value=''>Back</button></form></div><div class='selected-model'>Selected model: {h(selected_model)}</div><form method='post' class='grid'><input type='hidden' name='model' value='{h(selected_model)}'>"
            f"<div class='row'><label>MAC Address</label><input class='control' id='enterpriseMacAddress' name='macaddr' value='{h(values['macaddr'])}' placeholder='SEP001122334455' maxlength='15' pattern='SEP[A-F0-9]{{12}}' autocomplete='off' spellcheck='false' required><small class='note'>Enter 12 hexadecimal digits. Colons and dashes are removed automatically, letters are converted to uppercase, and SEP is added automatically.</small></div>"
            f"<div class='row'><label>Name</label><input class='control' name='name' value='{h(values['name'])}'></div>"
            f"<div class='row'><label>Hostname or IP Address</label><input class='control' name='ipv4' value='{h(values['ipv4'])}' placeholder='phone.example.local, 192.0.2.10, or 2001:db8::10' required><small class='note'>DNS hostnames, IPv4 addresses, and IPv6 addresses are supported.</small></div>"
            f"<label class='check'><input type='checkbox' name='unchecked' value='1'{' checked' if values.get('unchecked') else ''}> Do not check status of device (may slow sending of broadcasts)</label>"
            f"<div class='row'><label>Audio</label><select class='control' name='audio'>{option_list(AUDIO_MODES, values['audio'])}</select>{audio_key}</div>"
            f"<div class='row'><label>Visual</label><select class='control' name='visual'>{option_list(visual_modes, values['visual'])}</select></div>"
            f"<div class='row'><label>Volume</label><select class='control' name='volume'>{option_list(VOLUMES, values['volume'])}</select></div>"
            "<button class='button' type='submit'>Add Cisco Enterprise Endpoint</button></form>"
            "<script>"
            "const enterpriseMacAddress=document.getElementById('enterpriseMacAddress');"
            "if(enterpriseMacAddress){"
            "const normalizeEnterpriseMac=()=>{"
            "let value=enterpriseMacAddress.value.toUpperCase();"
            "let hasSep=value.replace(/[^A-Z0-9]/g,'').startsWith('SEP');"
            "let digits=hasSep?value.replace(/[^A-Z0-9]/g,'').slice(3):value;"
            "digits=digits.replace(/[^A-F0-9]/g,'').slice(0,12);"
            "enterpriseMacAddress.value=(digits.length||hasSep?'SEP':'')+digits;"
            "};"
            "enterpriseMacAddress.addEventListener('input',normalizeEnterpriseMac);"
            "enterpriseMacAddress.addEventListener('blur',normalizeEnterpriseMac);"
            "normalizeEnterpriseMac();"
            "}"
            "</script>"
        )
    elif form_type == "spa-multicast":
        body = (
            f"{alert(message, error)}<form method='post' class='grid'>"
            f"<div class='row'><label>Name</label><input class='control' name='name' value='{h(values['name'])}' required></div>"
            f"<div class='row'><label>Multicast Address</label><input class='control' name='address' value='{h(values['address'])}' placeholder='224.168.168.168' required></div>"
            f"<div class='row'><label>Port</label><input class='control' type='number' name='port' min='1' max='65535' value='{h(values['port'])}' required></div>"
            "<button class='button' type='submit'>Add Cisco SPA Multicast Endpoint</button></form>"
        )
    else:
        info = "<div class='info'><div class='icon'>i</div><div>To send audio, use a Multicast RTP or SIP endpoint</div></div>"
        body = (
            f"{alert(message, error)}{info}<form method='post' class='grid'>"
            f"<div class='row'><label>Label</label><input class='control' name='macaddress' value='{h(values['macaddress'])}' required></div>"
            f"<div class='row'><label>Hostname or IP</label><input class='control' name='ipv4' value='{h(values['ipv4'])}' placeholder='phone.example.local or 2001:db8::10' required></div>"
            f"<div class='row'><label>Username</label><input class='control' name='username' value='{h(values['username'])}'></div>"
            f"<div class='row'><label>Password</label><input class='control' type='password' name='password' value='{h(values['password'])}'></div>"
            "<label class='check'><input type='checkbox' name='unchecked' value='1'> Do not check status</label><button class='button' type='submit'>Add Cisco SPA EXE Endpoint</button></form>"
        )
    return page(forms()[form_type]["label"], module_body(body), "endpoints", user)


def spa_exe_row(conn_factory, endpoint_id):
    token = normalize_device_id(str(endpoint_id)[8:])
    for row in query_all(conn_factory, f"SELECT id, ipv4, username, password, macaddress, status FROM `{EXE_TABLE}`"):
        if normalize_device_id(row.get("macaddress")) == token:
            return row
    return None


def render_action(action, endpoint_id, request, conn_factory, page, user):
    ensure_schema(conn_factory)
    message = ""
    error = ""
    row = None
    kind = "enterprise"
    label = endpoint_id
    try:
        if str(endpoint_id).startswith("spa-multicast-"):
            kind = "spa-multicast"
            row_id = int(str(endpoint_id)[14:])
            row = query_one(conn_factory, f"SELECT id, name, address, port FROM `{MULTICAST_TABLE}` WHERE id=%s", (row_id,))
            if not row:
                raise ValueError("Endpoint not found.")
            label = f"{row.get('name') or 'Cisco SPA multicast'} ({row.get('address')}:{row.get('port')})"
        elif str(endpoint_id).startswith("spa-exe-"):
            kind = "spa-exe"
            lookup_id = str(request.form.get("_lookup_id", "") or "").strip()
            row = query_one(conn_factory, f"SELECT id, ipv4, username, password, macaddress, status FROM `{EXE_TABLE}` WHERE id=%s", (lookup_id,)) if lookup_id.isdigit() else spa_exe_row(conn_factory, endpoint_id)
            if not row:
                raise ValueError("Endpoint not found.")
            label = f"{row.get('macaddress') or 'Cisco SPA EXE'} ({row.get('ipv4')})"
        else:
            lookup_macaddr = str(request.form.get("_lookup_macaddr", endpoint_id) or "").strip()
            row = query_one(conn_factory, f"SELECT macaddr, name, ipv4, status, audio, model, visual, volume, addedby FROM `{ENTERPRISE_TABLE}` WHERE macaddr=%s", (lookup_macaddr,))
            if not row:
                raise ValueError("Endpoint not found.")
            label = f"{row.get('name') or row.get('macaddr')}{' (' + row.get('ipv4') + ')' if row.get('ipv4') else ''}"

        if request.method == "POST":
            if action == "delete":
                if kind == "spa-multicast":
                    execute(conn_factory, f"DELETE FROM `{MULTICAST_TABLE}` WHERE id=%s", (row["id"],))
                elif kind == "spa-exe":
                    execute(conn_factory, f"DELETE FROM `{EXE_TABLE}` WHERE id=%s", (row["id"],))
                else:
                    execute(conn_factory, f"DELETE FROM `{ENTERPRISE_TABLE}` WHERE macaddr=%s", (row["macaddr"],))
                return page("Endpoint Deleted", module_body("<script>window.top.location.href='/admin/manage-endpoints'</script><div class='success'>Cisco endpoint deleted.</div>"), "endpoints", user)
            if kind == "spa-multicast":
                name = str(request.form.get("name", "") or "").strip()
                address = str(request.form.get("address", "") or "").strip()
                port = validate_port(request.form.get("port"))
                execute(conn_factory, f"UPDATE `{MULTICAST_TABLE}` SET name=%s, address=%s, port=%s WHERE id=%s", (name, address, port, row["id"]))
            elif kind == "spa-exe":
                label = str(request.form.get("macaddress", "") or "").strip()
                label_token = normalize_device_id(label)
                host = validate_host_or_ip(request.form.get("ipv4"))
                if not label_token:
                    raise ValueError("Label is required.")
                existing_rows = query_all(conn_factory, f"SELECT id, macaddress FROM `{EXE_TABLE}` WHERE id<>%s", (row["id"],))
                if any(normalize_device_id(existing.get("macaddress")) == label_token for existing in existing_rows):
                    raise ValueError("That SPA EXE label already exists.")
                execute(conn_factory, f"UPDATE `{EXE_TABLE}` SET ipv4=%s, username=%s, password=%s, macaddress=%s WHERE id=%s", (host, request.form.get("username", ""), request.form.get("password", ""), label, row["id"]))
            else:
                mac = normalize_macaddr(request.form.get("macaddr"))
                model = request.form.get("model", "")
                if model not in MODELS:
                    raise ValueError("Phone model is required.")
                audio = request.form.get("audio") if request.form.get("audio") in AUDIO_MODES else "Multicast"
                visual_modes = active_visual_modes(model)
                visual = request.form.get("visual") if request.form.get("visual") in visual_modes else visual_modes[-1]
                volume = request.form.get("volume") if request.form.get("volume") in VOLUMES else "asis"
                if query_one(conn_factory, f"SELECT macaddr FROM `{ENTERPRISE_TABLE}` WHERE macaddr=%s AND macaddr<>%s", (mac, row["macaddr"])):
                    raise ValueError("That Cisco SEP endpoint already exists.")
                host = validate_host_or_ip(request.form.get("ipv4"))
                execute(conn_factory, f"UPDATE `{ENTERPRISE_TABLE}` SET macaddr=%s, name=%s, ipv4=%s, audio=%s, model=%s, visual=%s, volume=%s WHERE macaddr=%s", (mac, request.form.get("name", ""), host, audio, model, visual, volume, row["macaddr"]))
            return page("Endpoint Saved", module_body("<script>window.top.location.href='/admin/manage-endpoints'</script><div class='success'>Cisco endpoint updated.</div>"), "endpoints", user)
    except Exception as exc:
        error = str(exc)

    if action == "delete":
        body = alert(message, error)
        if row:
            body += f"<div class='warn'>Delete {h(label)}?</div><form method='post'><button class='button danger' type='submit'>Delete Endpoint</button></form>"
        return page("Delete Cisco Endpoint", module_body(body), "endpoints", user)
    if not row:
        return page("Edit Cisco Endpoint", module_body(alert(message, error)), "endpoints", user)
    if kind == "spa-multicast":
        body = f"{alert(message, error)}<form method='post' class='grid'><div class='row'><label>Name</label><input class='control' name='name' value='{h(row.get('name'))}' required></div><div class='row'><label>Multicast Address</label><input class='control' name='address' value='{h(row.get('address'))}' required></div><div class='row'><label>Port</label><input class='control' type='number' name='port' min='1' max='65535' value='{h(row.get('port'))}' required></div><button class='button'>Save Cisco SPA Multicast Endpoint</button></form>"
    elif kind == "spa-exe":
        body = f"{alert(message, error)}<div class='info'><div class='icon'>i</div><div>To send audio, use a Multicast RTP or SIP endpoint</div></div><p class='meta'>Current status: {h(row.get('status'))}</p><form method='post' class='grid'><input type='hidden' name='_lookup_id' value='{h(row.get('id'))}'><div class='row'><label>Label</label><input class='control' name='macaddress' value='{h(row.get('macaddress'))}' required></div><div class='row'><label>Hostname or IP</label><input class='control' name='ipv4' value='{h(row.get('ipv4'))}' placeholder='phone.example.local or 2001:db8::10' required></div><div class='row'><label>Username</label><input class='control' name='username' value='{h(row.get('username'))}'></div><div class='row'><label>Password</label><input class='control' type='password' name='password' value='{h(row.get('password'))}'></div><button class='button'>Save Cisco SPA EXE Endpoint</button></form>"
    else:
        visual_modes = active_visual_modes(row.get("model"))
        body = (
            f"{alert(message, error)}<p class='meta'>Current status: {h(row.get('status'))}</p><form method='post' class='grid'>"
            f"<input type='hidden' name='_lookup_macaddr' value='{h(row.get('macaddr'))}'>"
            f"<div class='row'><label>MAC Address</label><input class='control' id='enterpriseMacAddress' name='macaddr' value='{h(row.get('macaddr'))}' maxlength='15' pattern='SEP[A-F0-9]{{12}}' autocomplete='off' spellcheck='false' required><small class='note'>Enter 12 hexadecimal digits. Colons and dashes are removed automatically, letters are converted to uppercase, and SEP is added automatically.</small></div>"
            f"<div class='row'><label>Name</label><input class='control' name='name' value='{h(row.get('name'))}'></div>"
            f"<div class='row'><label>Hostname or IP Address</label><input class='control' name='ipv4' value='{h(row.get('ipv4'))}' placeholder='phone.example.local, 192.0.2.10, or 2001:db8::10' required><small class='note'>DNS hostnames, IPv4 addresses, and IPv6 addresses are supported.</small></div>"
            f"<div class='row'><label>Model</label><select class='control' name='model'>{option_list(MODELS, row.get('model'))}</select></div>"
            f"<div class='row'><label>Audio</label><select class='control' name='audio'>{option_list(AUDIO_MODES, row.get('audio'))}</select></div>"
            f"<div class='row'><label>Visual</label><select class='control' name='visual'>{option_list(visual_modes, row.get('visual'))}</select></div>"
            f"<div class='row'><label>Volume</label><select class='control' name='volume'>{option_list(VOLUMES, row.get('volume'))}</select></div>"
            "<button class='button'>Save Cisco Enterprise Endpoint</button></form>"
            "<script>"
            "const enterpriseMacAddress=document.getElementById('enterpriseMacAddress');"
            "if(enterpriseMacAddress){"
            "const normalizeEnterpriseMac=()=>{"
            "let value=enterpriseMacAddress.value.toUpperCase();"
            "let hasSep=value.replace(/[^A-Z0-9]/g,'').startsWith('SEP');"
            "let digits=hasSep?value.replace(/[^A-Z0-9]/g,'').slice(3):value;"
            "digits=digits.replace(/[^A-F0-9]/g,'').slice(0,12);"
            "enterpriseMacAddress.value=(digits.length||hasSep?'SEP':'')+digits;"
            "};"
            "enterpriseMacAddress.addEventListener('input',normalizeEnterpriseMac);"
            "enterpriseMacAddress.addEventListener('blur',normalizeEnterpriseMac);"
            "normalizeEnterpriseMac();"
            "}"
            "</script>"
        )
    return page("Edit Cisco Endpoint", module_body(body), "endpoints", user)


def load_settings(conn_factory):
    ensure_schema(conn_factory)
    values = dict(DEFAULT_SETTINGS)
    for row in query_all(conn_factory, f"SELECT parameter, value FROM `{SETTINGS_TABLE}`"):
        key = str(row.get("parameter") or "")
        if key in values:
            values[key] = "" if row.get("value") is None else str(row.get("value"))
    save_settings(conn_factory, values)
    return values


def save_settings(conn_factory, values):
    conn = conn_factory()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM `{SETTINGS_TABLE}`")
            for key, value in values.items():
                cur.execute(f"INSERT INTO `{SETTINGS_TABLE}` (parameter, value) VALUES (%s,%s)", (key, value))
        conn.commit()
    finally:
        conn.close()


def render_settings(request, conn_factory, page, user):
    values = load_settings(conn_factory)
    message = ""
    error = ""
    if request.method == "POST":
        try:
            authrelay_enabled = bool(request.form.get("authrelay-enabled"))
            messageinfo_enabled = bool(request.form.get("messageinfo-enabled"))
            values = {
                "messageinfo-enabled": "1" if messageinfo_enabled else "0",
                "messageinfo-showsender": "1" if messageinfo_enabled and request.form.get("messageinfo-showsender") else "0",
                "messageinfo-productname": "1" if messageinfo_enabled and request.form.get("messageinfo-productname") else "0",
                "ucmsync": "1" if request.form.get("ucmsync") else "0",
                "ucmsync-ip": str(request.form.get("ucmsync-ip", "") or "").strip(),
                "ucmsync-username": str(request.form.get("ucmsync-username", "") or "").strip(),
                "ucmsync-password": str(request.form.get("ucmsync-password", "") or ""),
                "ucmsync-interval": str(request.form.get("ucmsync-interval", "300") or "300").strip(),
                "authrelay": str(request.form.get("authrelay", "") or "").strip() if authrelay_enabled else "false",
            }
            if not values["ucmsync-interval"].isdigit():
                values["ucmsync-interval"] = "300"
            if authrelay_enabled and not values["authrelay"]:
                raise ValueError("Auth Relay URL is required when Auth Relay is enabled.")
            save_settings(conn_factory, values)
            message = "Cisco module settings saved."
        except Exception as exc:
            error = str(exc)
    authrelay_enabled = values.get("authrelay", "").strip().lower() not in {"", "false"}
    checked = lambda key: " checked" if truthy(values.get(key)) else ""
    settings_style = "<style>body{font-family:Tahoma,sans-serif;margin:0;padding:18px;color:#202124;background:#fff}.grid{display:grid;gap:12px}.section{border-top:1px solid #eee;padding-top:12px;margin-top:4px}.nested{display:grid;gap:12px}.row{display:grid;gap:6px}.check{display:flex;align-items:center;gap:8px;font-weight:400}.control{padding:10px;border:1px solid #ddd;border-radius:4px;font:inherit;box-sizing:border-box;width:100%}.button{background:#1976D2;color:#fff;border:0;border-radius:4px;padding:10px 14px;font:inherit;cursor:pointer;justify-self:start}.success{background:#E8F5E9;border:1px solid #A5D6A7;color:#1B5E20;padding:10px;border-radius:6px;margin-bottom:12px}.error{background:#FFEBEE;border:1px solid #EF9A9A;color:#B71C1C;padding:10px;border-radius:6px;margin-bottom:12px}.hidden{display:none!important}@media(prefers-color-scheme:dark){body{background:#1e1e1e;color:#e0e0e0}.section{border-top-color:#333}.control{background:#171717;border-color:#333;color:#eee}.button{background:#BB86FC;color:#000}.success{background:#14351A;border-color:#2E7D32;color:#C8E6C9}.error{background:#3B1515;border-color:#6D2A2A;color:#FFCDD2}}</style>"
    body = settings_style + f"""
{alert(message, error)}
<form method="post" class="grid">
    <label class="check"><input type="checkbox" id="messageInfoToggle" name="messageinfo-enabled" value="1"{checked("messageinfo-enabled")}> Message Info Enabled</label>
    <div class="nested" id="messageInfoSettings">
        <label class="check"><input type="checkbox" name="messageinfo-showsender" value="1"{checked("messageinfo-showsender")}> Show Sender on Info Page</label>
        <label class="check"><input type="checkbox" name="messageinfo-productname" value="1"{checked("messageinfo-productname")}> Show Product Name on Info Page</label>
    </div>

    <div class="section grid">
        <label class="check"><input type="checkbox" id="ucmSyncToggle" name="ucmsync" value="1"{checked("ucmsync")}> Sync with Unified Communications Manager (CUCM)</label>
        <div class="nested" id="ucmSyncSettings">
			<small class="note">Currently, only CUCM 11 or later is supported. Support for older versions of AXL is planned.</small>
            <div class="row"><label>CUCM Server(s)</label><input class="control" name="ucmsync-ip" value="{h(values.get("ucmsync-ip"))}">
			<small class="note">Enter the IP of the Publisher. If you have any Subcribers, you can them add them separated by commas for redundancy. All servers must have the Cisco AXL Web Service activated.</small>
			</div>
            <div class="row"><label>Application User ID</label><input class="control" name="ucmsync-username" value="{h(values.get("ucmsync-username"))}"></div>
            <div class="row"><label>Application User Password</label><input class="control" type="password" name="ucmsync-password" value="{h(values.get("ucmsync-password"))}" autocomplete="new-password"></div>
            <div class="row"><label>CUCM Sync Interval</label><input class="control" type="number" min="1" name="ucmsync-interval" value="{h(values.get("ucmsync-interval"))}"></div>
        </div>
    </div>

    <div class="section grid">
        <label class="check"><input type="checkbox" id="authRelayToggle" name="authrelay-enabled" value="1"{" checked" if authrelay_enabled else ""}> Auth Relay</label>
        <div class="nested" id="authRelaySettings">
            <div class="row"><label>Auth Relay URL</label><input class="control" type="url" name="authrelay" value="{h(values.get("authrelay") if authrelay_enabled else "")}" placeholder="https://example.local/auth"></div>
        </div>
    </div>

    <button class="button" type="submit">Save Cisco Settings</button>
</form>
<script>
function bindToggle(toggleId, targetId) {{
  const toggle = document.getElementById(toggleId);
  const target = document.getElementById(targetId);
  if (!toggle || !target) return;
  function sync() {{
    target.classList.toggle('hidden', !toggle.checked);
  }}
  toggle.addEventListener('change', sync);
  sync();
}}
bindToggle('messageInfoToggle', 'messageInfoSettings');
bindToggle('ucmSyncToggle', 'ucmSyncSettings');
bindToggle('authRelayToggle', 'authRelaySettings');
</script>"""
    return page("Cisco Settings", body, "endpoints", user)
