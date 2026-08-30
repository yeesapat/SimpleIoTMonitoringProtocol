#!/usr/bin/env python3
import socket
import sys
import json
import time
import random
import threading
from collections import deque

SIMP_VERSION  = "SIMP/1.0"
HUB_PORT      = 7070
SERVER_PORT   = 7071
SENSOR_PORT   = 7072
TIMEOUT       = 2.0
MAX_RETRIES   = 3
BUF_SIZE      = 65535

STATUS = {
    200: "OK",
    204: "NO DATA",
    400: "BAD REQUEST",
    404: "SENSOR NOT FOUND",
    408: "TIMEOUT",
    409: "ALREADY REGISTERED",
    503: "HUB BUSY",
}

# Adaptive interval thresholds
# variance suggested poll interval (seconds)
ADAPTIVE_RULES = [
    # (variance threshold, suggested poll interval, label)
    (0.5,  5.0,  "STABLE   ▁"),
    (2.0,  3.0,  "NORMAL   ▃"),
    (5.0,  1.5,  "ACTIVE   ▆"),
    (999,  0.5,  "VOLATILE █"),
]

HISTORY_SIZE = 8   #readings kept per sensor for variance calc

SENSOR_META = {
    "temp":     {"unit": "°C",  "value": 28.0, "min": 15.0,  "max": 55.0,   "drift": 2.5},
    "humidity": {"unit": "%",   "value": 60.0, "min": 20.0,  "max": 95.0,   "drift": 0.3},
    "pressure": {"unit": "hPa", "value": 1013.0,"min": 950.0,"max": 1050.0, "drift": 0.2},
    "co2":      {"unit": "ppm", "value": 400.0, "min": 300.0,"max": 2000.0, "drift": 12.0},
}

# Circular buffer of recent log lines. In server mode (_log_server_mode) every
# log_tx/log_rx/log_info appends here instead of printing, so render_history() can
# redraw them above the live table in a single clear screen.
_log_buf = deque(maxlen=12)
_log_server_mode = False

def log_tx(node, method, extra=""):
    line = f"  ▶  [{SIMP_VERSION}] {node} » {method}  {extra}"
    if _log_server_mode:
        _log_buf.append(line)
    else:
        print(line, flush=True)

def log_rx(node, line, extra=""):
    msg = f"  ◀  [{SIMP_VERSION}] {node} « {line}  {extra}"
    if _log_server_mode:
        _log_buf.append(msg)
    else:
        print(msg, flush=True)

def log_info(msg):
    line = f"  ···  {msg}"
    if _log_server_mode:
        _log_buf.append(line)
    else:
        print(line, flush=True)

def divider(char="─", n=52):
    line = char * n
    if _log_server_mode:
        _log_buf.append(line)
    else:
        print(line, flush=True)

def calc_variance(history):
    # Population variance of the recent reading window — a measure of how "jumpy"
    # a sensor is, used to pick how often it should be polled.
    if len(history) < 2:
        return 0.0
    mean = sum(history) / len(history)
    return round(sum((x - mean) ** 2 for x in history) / len(history), 4)

def suggest_interval(variance):
    for threshold, interval, label in ADAPTIVE_RULES:
        if variance < threshold:
            return interval, label
    return 0.5, "VOLATILE █"

def build_hint(from_id, sensor_type, variance, interval, label):
    lines = [
        f"{SIMP_VERSION} HINT",
        f"MsgID: {int(time.time()*1000) % 99999}",
        f"From: {from_id}",
        f"Sensor: {sensor_type}",
        f"Variance: {variance}",
        f"Suggest-Interval: {interval}",
        f"State: {label}",
        f"Timestamp: {time.time():.3f}",
        "",
    ]
    return ("\r\n".join(lines) + "\r\n").encode()

def parse_hint(data):
    try:
        raw = data.decode(errors="replace")
        hdrs = {}
        for line in raw.split("\r\n"):
            if ": " in line:
                k, v = line.split(": ", 1)
                hdrs[k.strip().lower()] = v.strip()
        return {
            "sensor":   hdrs.get("sensor", ""),
            "variance": float(hdrs.get("variance", 0)),
            "interval": float(hdrs.get("suggest-interval", 3.0)),
            "state":    hdrs.get("state", "NORMAL"),
        }
    except:
        return {}

def build_interval_msg(from_id, intervals):
    """Server → Hub: report current poll intervals"""
    lines = [
        f"{SIMP_VERSION} INTERVAL",
        f"MsgID: {int(time.time()*1000) % 99999}",
        f"From: {from_id}",
        f"Timestamp: {time.time():.3f}",
        "",
    ]
    header = "\r\n".join(lines) + "\r\n"
    body   = json.dumps(intervals).encode()
    return header.encode() + body

def build_msg(method, from_id, payload=None, **kwargs):
    lines = [
        f"{SIMP_VERSION} {method}",
        f"MsgID: {int(time.time()*1000) % 99999}",
        f"From: {from_id}",
        f"Timestamp: {time.time():.3f}",
    ]
    for k, v in kwargs.items():
        lines.append(f"{k}: {v}")
    lines.append("")
    header = "\r\n".join(lines) + "\r\n"
    body   = json.dumps(payload).encode() if payload else b""
    return header.encode() + body

def parse_msg(data):
    try:
        raw = data.decode(errors="replace")
        header_part, body_part = (raw.split("\r\n\r\n", 1)
                                  if "\r\n\r\n" in raw else (raw, ""))
        lines  = header_part.strip().split("\r\n")
        first  = lines[0].split()
        method = first[1] if len(first) >= 2 else ""
        hdrs   = {}
        for line in lines[1:]:
            if ": " in line:
                k, v = line.split(": ", 1)
                hdrs[k.strip().lower()] = v.strip()
        payload = json.loads(body_part) if body_part.strip() else {}
        return method, hdrs, payload
    except:
        return "", {}, {}

def build_response(code, from_id, payload=None):
    phrase = STATUS.get(code, "UNKNOWN")
    lines  = [
        f"{SIMP_VERSION} {code} {phrase}",
        f"From: {from_id}",
        f"Timestamp: {time.time():.3f}",
        "",
    ]
    header = "\r\n".join(lines) + "\r\n"
    body   = json.dumps(payload).encode() if payload else b""
    return header.encode() + body

def parse_status(data):
    try:
        line  = data.decode(errors="replace").split("\r\n")[0]
        parts = line.split()
        return int(parts[1]) if len(parts) >= 2 else 0, line
    except:
        return 0, ""

def send_recv_udp(sock, msg, addr, label, node_id, retries=MAX_RETRIES):
    # App-level reliability: UDP is connectionless, so we retry the send+recv up to
    # MAX_RETRIES times with TIMEOUT between attempts instead of relying on TCP.
    for attempt in range(1, retries + 1):
        try:
            sock.sendto(msg, addr)
            log_tx(node_id, label, f"(retry {attempt})" if attempt > 1 else "")
            data, _ = sock.recvfrom(BUF_SIZE)
            return data
        except socket.timeout:
            log_info(f"Timeout ({attempt}/{retries})")
    log_info(f"408 TIMEOUT — giving up")
    return None

def run_sensor(sensor_id, sensor_types, hub_ip):
    """Mock hardware sensor — registers with hub, answers POLL from hub"""

    for st in sensor_types:
        if st not in SENSOR_META:
            print(f"  Unknown sensor type: {st}. Valid: {list(SENSOR_META.keys())}")
            sys.exit(1)

    state = {st: dict(SENSOR_META[st]) for st in sensor_types}

    def read(st):
        s     = state[st]
        delta = random.uniform(-s["drift"], s["drift"])
        s["value"] = round(max(s["min"], min(s["max"], s["value"] + delta)), 2)
        return s["value"]

    sock     = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", 0)) 
    sock.settimeout(TIMEOUT)
    hub_addr = (hub_ip, HUB_PORT)

    my_port = sock.getsockname()[1]
    print(f"\n╔══════════════════════════════════════════╗")
    print(f"║   SIMP/1.0  Mock Sensor                  ║")
    print(f"╚══════════════════════════════════════════╝")
    print(f"  Sensor  : {sensor_id}")
    print(f"  Types   : {', '.join(sensor_types)}")
    print(f"  Port    : {my_port}")
    print(f"  Hub     : {hub_ip}:{HUB_PORT}\n")

    #  REGISTER 
    reg_payload = {st: {"unit": SENSOR_META[st]["unit"]} for st in sensor_types}
    msg  = build_msg("REGISTER", sensor_id, reg_payload)
    resp = send_recv_udp(sock, msg, hub_addr, "REGISTER", sensor_id)
    if resp is None:
        print("  Could not reach hub — exiting")
        sys.exit(1)
    code, line = parse_status(resp)
    log_rx(sensor_id, line)
    if code not in (200, 409):
        print(f"  Registration failed: {line}")
        sys.exit(1)

    log_info(f"Registered — waiting for POLL / THRESH / PING from hub\n")

    thresholds = {}

    while True:
        try:
            data, addr = sock.recvfrom(BUF_SIZE)
        except socket.timeout:
            continue

        method, hdrs, payload = parse_msg(data)
        divider()
        log_rx(sensor_id, f"{method}  (From: {hdrs.get('from','?')}  MsgID:{hdrs.get('msgid','?')})")

        if method == "PING":
            sock.sendto(build_msg("PONG", sensor_id), addr)
            log_tx(sensor_id, "PONG")

        elif method == "POLL":
            requested = hdrs.get("sensors", ",".join(sensor_types)).split(",")
            requested = [s.strip() for s in requested if s.strip()]
            readings  = {}
            missing   = []
            for st in requested:
                if st in state:
                    val = read(st)
                    readings[st] = {"value": val, "unit": state[st]["unit"]}
                else:
                    missing.append(st)
            resp_payload = {"readings": readings, "missing": missing}
            sock.sendto(build_response(200, sensor_id, resp_payload), addr)
            log_tx(sensor_id, f"REPORT  {list(readings.keys())}")

            # check thresholds → ALERT hub
            for st, info in readings.items():
                if st in thresholds and info["value"] > thresholds[st]:
                    alert = build_msg("ALERT", sensor_id, {
                        "sensor": st, "value": info["value"],
                        "threshold": thresholds[st]
                    })
                    sock.sendto(alert, addr)
                    log_tx(sensor_id, f"ALERT  {st}={info['value']} > {thresholds[st]}")

        elif method == "THRESH":
            thresholds.update(payload)
            sock.sendto(build_response(200, sensor_id, {"thresholds": thresholds}), addr)
            log_tx(sensor_id, f"200 OK  thresholds={thresholds}")

        else:
            sock.sendto(build_response(400, sensor_id), addr)
            log_tx(sensor_id, "400 BAD REQUEST")

def run_hub():
    sock    = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", HUB_PORT))
    node_id = "HUB-01"

    registry    = {}
    reg_lock    = threading.Lock()
    server_addr = [None]

    from collections import deque
    history  = {}
    hist_lock = threading.Lock()

    my_ip = socket.gethostbyname(socket.gethostname())
    print(f"\n╔══════════════════════════════════════════╗")
    print(f"║   SIMP/1.0  Mock Hub  Ready              ║")
    print(f"╚══════════════════════════════════════════╝")
    print(f"  Node    : {node_id}")
    print(f"  Address : {my_ip}:{HUB_PORT}")
    print(f"  Waiting for REGISTER / POLL / THRESH / PING...\n")

    def poll_sensor(sensor_id, sensor_addr, types, timeout=1.5):
        """Hub polls one sensor synchronously, returns readings dict"""
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(timeout)
        msg = build_msg("POLL", node_id, sensors=",".join(types))
        try:
            s.sendto(msg, sensor_addr)
            log_tx(node_id, f"POLL→{sensor_id}  types={types}")
            data, _ = s.recvfrom(BUF_SIZE)
            _, _, payload = parse_msg(data)
            log_rx(node_id, f"REPORT←{sensor_id}  {list(payload.get('readings',{}).keys())}")
            return payload.get("readings", {})
        except socket.timeout:
            log_info(f"Timeout waiting for {sensor_id}")
            return {}
        finally:
            s.close()

    def forward_thresh(sensor_addr, payload, sensor_id):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(1.5)
        msg = build_msg("THRESH", node_id, payload)
        try:
            s.sendto(msg, sensor_addr)
            log_tx(node_id, f"THRESH→{sensor_id}  {payload}")
            data, _ = s.recvfrom(BUF_SIZE)
            code, line = parse_status(data)
            log_rx(node_id, f"{line}←{sensor_id}")
        except:
            pass
        finally:
            s.close()

    while True:
        data, addr = sock.recvfrom(BUF_SIZE)
        method, hdrs, payload = parse_msg(data)

        divider()
        log_rx(node_id, f"{method}  (From:{hdrs.get('from','?')}  MsgID:{hdrs.get('msgid','?')})")

        # REGISTER (from sensor)
        if method == "REGISTER":
            sid = hdrs.get("from", "UNKNOWN")
            with reg_lock:
                if sid in registry:
                    sock.sendto(build_response(409, node_id), addr)
                    log_tx(node_id, f"409 ALREADY REGISTERED  {sid}")
                else:
                    types = list(payload.keys()) if payload else []
                    registry[sid] = {"addr": addr, "types": types, "meta": payload}
                    sock.sendto(build_response(200, node_id, {"registered": sid, "types": types}), addr)
                    log_tx(node_id, f"200 OK  registered={sid}  types={types}")
                    log_info(f"Registry: {list(registry.keys())}")

        # PING (from server) 
        elif method == "PING":
            server_addr[0] = addr
            sock.sendto(build_msg("PONG", node_id), addr)
            log_tx(node_id, "PONG")

        # POLL(from server)
        elif method == "POLL":
            server_addr[0] = addr
            requested = hdrs.get("sensors", "").split(",")
            requested = [s.strip() for s in requested if s.strip()]

            all_readings = {}
            with reg_lock:
                snap = dict(registry)

            # Poll every registered sensor concurrently so one slow/unreachable
            # sensor doesn't delay the others; results are merged afterwards.
            results = {}
            threads = []

            def do_poll(sid, info):
                types = info["types"]
                if requested:
                    types = [t for t in types if t in requested]
                if types:
                    r = poll_sensor(sid, info["addr"], types)
                    results[sid] = r

            for sid, info in snap.items():
                t = threading.Thread(target=do_poll, args=(sid, info))
                threads.append(t)
                t.start()
            for t in threads:
                t.join(timeout=2.0)

            for sid_readings in results.values():
                all_readings.update(sid_readings)

            missing = [r for r in requested if r not in all_readings] if requested else []
            resp_payload = {"readings": all_readings, "missing": missing,
                            "sources": list(results.keys())}
            sock.sendto(build_response(200, node_id, resp_payload), addr)
            log_tx(node_id, f"200 OK  total_sensors={len(all_readings)}  sources={list(results.keys())}")

            # After each poll, update the rolling reading history per sensor type and
            # send the server a HINT carrying the variance and suggested poll interval.
            with hist_lock:
                for stype, info in all_readings.items():
                    if stype not in history:
                        history[stype] = deque(maxlen=HISTORY_SIZE)
                    history[stype].append(info["value"])

                    if len(history[stype]) >= 3:
                        var      = calc_variance(list(history[stype]))
                        interval, label = suggest_interval(var)
                        hint = build_hint(node_id, stype, var, interval, label)
                        sock.sendto(hint, addr)
                        log_tx(node_id, f"HINT  sensor={stype}  var={var}  "
                                        f"interval={interval}s  [{label.strip()}]")

        # THRESH (from server) forward to all sensors
        elif method == "THRESH":
            with reg_lock:
                snap = dict(registry)
            for sid, info in snap.items():
                relevant = {k: v for k, v in payload.items() if k in info["types"]}
                if relevant:
                    threading.Thread(
                        target=forward_thresh,
                        args=(info["addr"], relevant, sid),
                        daemon=True
                    ).start()
            sock.sendto(build_response(200, node_id, {"forwarded_to": list(snap.keys())}), addr)
            log_tx(node_id, f"200 OK  forwarded THRESH to {list(snap.keys())}")

        # ALERT (from sensor) forward to server 
        elif method == "ALERT":
            if server_addr[0]:
                sock.sendto(data, server_addr[0])   # forward raw
                log_tx(node_id, f"ALERT→SERVER  {payload}")
            else:
                log_info("ALERT received but no server registered yet")

        # INTERVAL (from server) —just log it
        elif method == "INTERVAL":
            log_rx(node_id, f"INTERVAL  {payload}")

        else:
            sock.sendto(build_response(400, node_id), addr)
            log_tx(node_id, "400 BAD REQUEST")


def run_server(hub_ip):
    global _log_server_mode
    _log_server_mode = True   # route all logs through _log_buf for the buffered UI
    sock     = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", SERVER_PORT))
    sock.settimeout(TIMEOUT)
    node_id  = "SERVER-01"
    hub_addr = (hub_ip, HUB_PORT)

    print(f"\n╔══════════════════════════════════════════╗")
    print(f"║   SIMP/1.0  Monitoring Server            ║")
    print(f"╚══════════════════════════════════════════╝")
    print(f"  Node    : {node_id}")
    print(f"  Hub     : {hub_ip}:{HUB_PORT}")
    print(f"  Timeout : {TIMEOUT}s  Retries: {MAX_RETRIES}\n")

    def send_recv(msg, label):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                sock.sendto(msg, hub_addr)
                log_tx(node_id, label, f"(retry {attempt})" if attempt > 1 else "")
                data, _ = sock.recvfrom(BUF_SIZE)
                return data
            except socket.timeout:
                log_info(f"Timeout ({attempt}/{MAX_RETRIES})")
        return None

    # PING 
    divider()
    resp = send_recv(build_msg("PING", node_id), "PING")
    if resp:
        m, _, _ = parse_msg(resp)
        log_rx(node_id, m)

    #THRESH 
    divider()
    thresh = {"temp": 38.0, "co2": 900.0, "humidity": 85.0}
    resp   = send_recv(build_msg("THRESH", node_id, thresh), f"THRESH  {thresh}")
    if resp:
        code, line = parse_status(resp)
        log_rx(node_id, line)

    intervals   = {}
    default_ivl = 3.0
    poll_count  = 0

    def get_interval():
        # The most volatile sensor dictates the cadence: poll at the SHORTEST
        # suggested interval so no sensor is checked less often than it needs.
        if not intervals:
            return default_ivl
        return min(intervals.values())

    def drain_hints():
        # Briefly switch the socket to non-blocking (50 ms) so we can scoop up every
        # pending HINT/ALERT datagram without stalling the main poll loop.
        sock.settimeout(0.05)
        changed = False
        try:
            while True:
                data, _ = sock.recvfrom(BUF_SIZE)
                first_line = data.decode(errors="replace").split("\r\n")[0]

                if "HINT" in first_line:
                    h = parse_hint(data)
                    if h and h["sensor"]:
                        old = intervals.get(h["sensor"], default_ivl)
                        intervals[h["sensor"]] = h["interval"]
                        if old != h["interval"]:
                            changed = True
                            log_info(f"⚡  HINT  sensor={h['sensor']}  "
                                     f"var={h['variance']}  "
                                     f"interval {old}s → {h['interval']}s  "
                                     f"[{h['state'].strip()}]")
                elif "ALERT" in first_line:
                    _, hdrs, ap = parse_msg(data)
                    log_info(f"🚨  ALERT!  from={hdrs.get('from','?')}  "
                             f"sensor={ap.get('sensor')}  "
                             f"value={ap.get('value')}  "
                             f"threshold={ap.get('threshold')}")
        except socket.timeout:
            pass
        finally:
            sock.settimeout(TIMEOUT)

        if changed:
            msg = build_interval_msg(node_id, intervals)
            sock.sendto(msg, hub_addr)
            log_tx(node_id, f"INTERVAL  {intervals}")

    poll_history = deque(maxlen=5)

    def render_history():
        # ESC[2J ESC[H clears the whole screen and moves the cursor home, so each
        # poll cycle redraws the last-5 table + buffered logs instead of scrolling.
        print("\033[2J\033[H", end="")
        print(f"  Adaptive POLL — last 5 records\n")
        if _log_buf:
            for line in _log_buf:
                print(line)
            print()
        print(f"  {'Poll':<8} {'Time':<10} {'RTT':>8}  {'Sensor':<12} {'Value':>8}  {'Unit':<6}  {'Interval':<8}")
        print(f"  {'────':<8} {'────':<10} {'───':>8}  {'──────':<12} {'─────':>8}  {'────':<6}  {'────────':<8}")
        for rec in poll_history:
            for i, (stype, info) in enumerate(rec["readings"].items()):
                ivl = intervals.get(stype, default_ivl)
                poll_label = f"#{rec['poll']}" if i == 0 else ""
                time_label = rec["time"] if i == 0 else ""
                rtt_label  = f"{rec['rtt']:.1f}ms" if i == 0 else ""
                print(f"  {poll_label:<8} {time_label:<10} {rtt_label:>8}  "
                      f"{stype:<12} {info['value']:>8}  "
                      f"{info['unit']:<6}  {ivl}s")
        print()

    print(f"\n  Adaptive POLL — starting at {default_ivl}s, adjusts by sensor variance\n")

    while True:
        poll_count += 1
        current_ivl = get_interval()

        t0   = time.time()
        resp = send_recv(build_msg("POLL", node_id), "POLL")
        rtt  = (time.time() - t0) * 1000

        if resp is None:
            log_info("No response from hub")
            time.sleep(current_ivl)
            continue

        code, status_line_ = parse_status(resp)
        _, _, payload = parse_msg(resp)
        log_rx(node_id, status_line_, f"RTT={rtt:.1f}ms")

        if code == 200:
            readings = payload.get("readings", {})
            sources  = payload.get("sources", [])
            missing  = payload.get("missing", [])
            poll_history.append({
                "poll": poll_count,
                "time": time.strftime('%H:%M:%S'),
                "rtt": rtt,
                "readings": readings,
                "sources": sources,
                "missing": missing,
            })
            render_history()

        drain_hints()

        time.sleep(get_interval())

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    mode = sys.argv[1].lower()

    if mode == "hub":
        run_hub()

    elif mode == "sensor":
        if len(sys.argv) < 4:
            print("Usage: python3 simp2.py sensor <sensor_id> <type1> [type2 ...]")
            print(f"Types: {list(SENSOR_META.keys())}")
            sys.exit(1)
        sensor_id    = sys.argv[2]
        sensor_types = sys.argv[3:]
        hub_ip       = "127.0.0.1"
        run_sensor(sensor_id, sensor_types, hub_ip)

    elif mode == "server":
        if len(sys.argv) < 3:
            print("Usage: python3 simp2.py server <hub_ip>")
            sys.exit(1)
        run_server(sys.argv[2])

    else:
        print(__doc__)
        sys.exit(1)