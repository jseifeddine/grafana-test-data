#!/usr/bin/env python3
"""
Prometheus / VictoriaMetrics historical backfill generator.

Generates N days of timestamped HAProxy and Kubernetes metrics and imports
them via VictoriaMetrics' /api/v1/import/prometheus endpoint.

Signal functions mirror haproxy_generator.py and k8s_generator.py exactly
so historical data is visually consistent with the live-pushed data.
"""

import argparse
import math
import random
import sys
import time

import requests

_TTY = sys.stdout.isatty()

VM_URL  = "http://prometheus:8428"
DAYS    = 7
STEP_S  = 60
BATCH   = 8_000

INSTANCE = "haproxy:9101"
ORIGIN   = "cluster-prod"

# ─── shared signal layer (same logic as the live generators) ─────────────────

def _det_rng(ts: float, slot_s: float, seed: int) -> random.Random:
    return random.Random(int(ts / slot_s) * 2654435761 + seed)


def _load(ts: float) -> float:
    """Multi-scale traffic load; weekly + diurnal + fractal noise + events."""
    hour = (ts % 86400) / 3600
    am   = math.exp(-0.5 * ((hour - 10.5) / 1.8) ** 2)
    pm   = math.exp(-0.5 * ((hour - 14.5) / 1.6) ** 2)
    base = max(0.06, am * 0.90 + pm * 0.72)

    dow = int(ts // 86400 + 4) % 7
    if dow >= 5:
        base *= 0.35
    elif dow == 4:
        base *= max(0.72, 1.0 - max(0, hour - 16) * 0.10)

    base *= 1 + (
        0.10 * math.sin(ts / 157   + 1.1) +
        0.07 * math.sin(ts / 523   + 2.7) +
        0.05 * math.sin(ts / 1801  + 0.4) +
        0.04 * math.sin(ts / 7207  + 3.9) +
        0.02 * math.sin(ts / 28800 + 1.5)
    )

    # Flash-traffic spike
    r = _det_rng(ts, 21600, 1)
    if r.random() < 0.18:
        w0      = int(ts / 21600) * 21600
        s_start = w0 + r.uniform(1800, 19800)
        s_dur   = r.uniform(180, 1200)
        if s_start <= ts < s_start + s_dur:
            base *= r.uniform(2.0, 4.5)

    # Maintenance window
    r2 = _det_rng(ts, 86400 * 14, 2)
    if r2.random() < 0.40:
        w0      = int(ts / (86400 * 14)) * 86400 * 14
        m_start = w0 + r2.uniform(0, 86400 * 13)
        m_dur   = r2.uniform(900, 3600)
        if m_start <= ts < m_start + m_dur:
            base *= 0.03

    return max(0.01, base)


def _error_surge(ts: float) -> float:
    """1.0 normally; 10-45× during error storms."""
    r = _det_rng(ts, 10800, 3)
    if r.random() < 0.14:
        w0      = int(ts / 10800) * 10800
        s_start = w0 + r.uniform(300, 9900)
        s_dur   = r.uniform(120, 600)
        if s_start <= ts < s_start + s_dur:
            return r.uniform(10.0, 45.0)
    return 1.0


def _latency_mult(ts: float, load: float) -> float:
    """Queuing-theory latency + occasional DB slow-query events."""
    util   = min(0.92, load * 0.70)
    q_mult = 1.0 / max(0.08, 1.0 - util)
    r = _det_rng(ts, 7200, 4)
    db_mult = 1.0
    if r.random() < 0.15:
        w0      = int(ts / 7200) * 7200
        s_start = w0 + r.uniform(0, 6600)
        s_dur   = r.uniform(60, 600)
        if s_start <= ts < s_start + s_dur:
            db_mult = r.uniform(5.0, 22.0)
    return q_mult * db_mult


def _ddos_mult(ts: float) -> float:
    r = _det_rng(ts, 86400 * 3, 5)
    if r.random() < 0.35:
        w0      = int(ts / (86400 * 3)) * 86400 * 3
        s_start = w0 + r.uniform(0, 86400 * 2.5)
        s_dur   = r.uniform(300, 3600)
        if s_start <= ts < s_start + s_dur:
            return r.uniform(20.0, 100.0)
    return 1.0


def _workload_cpu_ratio(ts: float, pod: str, wl_type: str) -> float:
    """CPU usage ratio (0-1) for a pod, by workload type."""
    load = _load(ts)
    seed = hash(pod) & 0x7FFFFFFF
    noise = (
        0.08 * math.sin(ts / 97  + seed) +
        0.05 * math.sin(ts / 307 + seed * 0.3)
    )
    if wl_type == "api":
        ratio = load * 0.70 * (1 + noise)
        r = _det_rng(ts, 1800, seed)
        if r.random() < 0.08:
            w0 = int(ts / 1800) * 1800
            s  = w0 + r.uniform(0, 1600)
            if s <= ts < s + r.uniform(30, 90):
                ratio = min(0.98, ratio * r.uniform(2, 4))

    elif wl_type == "batch":
        r = _det_rng(ts, 7200, seed)
        if r.random() < 0.65:
            w0    = int(ts / 7200) * 7200
            start = w0 + r.uniform(600, 6600)
            dur   = r.uniform(300, 900)
            ratio = r.uniform(0.75, 0.95) if start <= ts < start + dur else 0.02
        else:
            ratio = 0.02

    elif wl_type == "db":
        ratio = 0.35 + load * 0.30 * (1 + noise * 0.5)
        r = _det_rng(ts, 3600 * 6, seed)
        if r.random() < 0.20:
            w0 = int(ts / (3600 * 6)) * 3600 * 6
            s  = w0 + r.uniform(0, 3600 * 5)
            if s <= ts < s + r.uniform(120, 600):
                ratio = min(0.90, ratio * r.uniform(2.0, 3.5))

    elif wl_type == "queue":
        ratio = 0.25 + load * 0.45 * (1 + noise)

    elif wl_type == "monitor":
        scrape_cycle = ts % 60
        ratio = (0.55 + noise * 0.10) if scrape_cycle < 5 else (0.08 + noise * 0.03)

    elif wl_type == "dns":
        ratio = load * 0.25 * (1 + noise * 0.8)

    elif wl_type == "cache":
        ratio = 0.06 + noise * 0.02

    else:  # daemon
        ratio = 0.03 + noise * 0.01

    return max(0.001, min(0.99, ratio))


def _workload_mem_ratio(ts: float, pod: str, mem_pattern: str) -> float:
    """Memory usage ratio (0-1) for a pod, by pattern."""
    seed = hash(f"{pod}/mem") & 0x7FFFFFFF

    if mem_pattern == "stable":
        return min(0.90, max(0.10, 0.42 + 0.12 * math.sin(ts / (3600 * 3) + seed)))

    elif mem_pattern == "stable_high":
        return min(0.90, max(0.50, 0.68 + 0.08 * math.sin(ts / (3600 * 6) + seed)))

    elif mem_pattern == "sawtooth":
        period = 3600 * 2 + seed % 3600
        phase_offset = seed * 0.37 * period
        pos = ((ts + phase_offset) % period) / period
        if pos < 0.88:
            growth = (math.exp(pos / 0.88 * 2.2) - 1) / (math.e ** 2.2 - 1)
            ratio  = 0.30 + 0.55 * growth
        else:
            gc_pos = (pos - 0.88) / 0.12
            ratio  = 0.30 + 0.55 * (1 - gc_pos) ** 3
        # Prometheus TSDB growth over days
        if "prometheus" in pod:
            week_pos = (ts % (86400 * 7)) / (86400 * 7)
            ratio += 0.12 * week_pos
        return min(0.96, max(0.15, ratio))

    elif mem_pattern == "growing":
        day_pos = (ts % (86400 * 30)) / (86400 * 30)
        ratio   = 0.45 + 0.40 * day_pos + 0.05 * math.sin(ts / (3600 * 4) + seed)
        return min(0.96, max(0.15, ratio))

    return 0.45


def jitter(v: float, pct: float = 0.06) -> float:
    return v * (1 + random.uniform(-pct, pct))


# ─── HAProxy topology ────────────────────────────────────────────────────────

HA_FRONTENDS = ["http_in", "https_in", "stats"]
HA_BACKENDS  = {
    "backend_web":    ["web01", "web02", "web03"],
    "backend_api":    ["api01", "api02"],
    "backend_static": ["static01", "static02"],
    "backend_auth":   ["auth01", "auth02"],
}
HA_BASE_RPS  = {"http_in": 600, "https_in": 1800, "stats": 2}
HA_BE_BASE   = {"backend_web": 1600, "backend_api": 600, "backend_static": 400, "backend_auth": 200}
HA_BE_BPS    = {"backend_web": 2e6, "backend_api": 800e3, "backend_static": 5e6, "backend_auth": 200e3}
HA_BASE_RT   = {"backend_web": 0.045, "backend_api": 0.140, "backend_static": 0.008, "backend_auth": 0.065}
HA_SRV_W     = {
    "backend_web/web01": 1.05, "backend_web/web02": 1.00, "backend_web/web03": 0.80,
    "backend_api/api01": 1.00, "backend_api/api02": 1.20,
    "backend_static/static01": 1.0, "backend_static/static02": 1.0,
    "backend_auth/auth01": 1.0, "backend_auth/auth02": 1.0,
}
HTTP_CODES = {
    "200": 0.883, "206": 0.030, "301": 0.018, "304": 0.030,
    "400": 0.010, "404": 0.016, "429": 0.004, "500": 0.005, "503": 0.004,
}


def haproxy_samples(ts: float, step: float, acc: dict) -> list[str]:
    ms   = int(ts * 1000)
    load = _load(ts)
    esrg = _error_surge(ts)
    lmul = _latency_mult(ts, load)
    ddos = _ddos_mult(ts)
    inst = INSTANCE
    lines: list[str] = []

    # Adjust HTTP code distribution during error storms
    codes: dict[str, float] = {}
    for c, share in HTTP_CODES.items():
        if c in ("500", "503"):
            codes[c] = share * esrg
        elif c in ("400", "404", "429"):
            codes[c] = share * max(1.0, esrg * 0.3)
        else:
            codes[c] = share
    tot = sum(codes.values())
    codes = {c: v / tot for c, v in codes.items()}

    def bump(key: str, delta: float) -> float:
        acc[key] = acc.get(key, 0.0) + max(0.0, delta)
        return acc[key]

    uptime = ts - acc.get("start_time", ts - 86400)
    pool_b = 45e6 + uptime * 12
    lines.append(f'haproxy_process_nbproc{{instance="{inst}"}} 1 {ms}')
    lines.append(f'haproxy_process_start_time_seconds{{instance="{inst}"}} {acc.get("start_time", ts - 86400):.3f} {ms}')
    lines.append(f'haproxy_process_uptime_seconds{{instance="{inst}"}} {uptime:.1f} {ms}')
    lines.append(f'haproxy_process_current_connections{{instance="{inst}"}} {int(load * jitter(1400))} {ms}')
    lines.append(f'haproxy_process_pool_allocated_bytes{{instance="{inst}"}} {int(pool_b)} {ms}')
    lines.append(f'haproxy_process_pool_used_bytes{{instance="{inst}"}} {int(pool_b * jitter(0.80, 0.12))} {ms}')

    # Frontends
    for fe in HA_FRONTENDS:
        rps = HA_BASE_RPS.get(fe, 100) * load * jitter(1.0, 0.06)
        lines.append(f'haproxy_frontend_http_requests_total{{instance="{inst}",proxy="{fe}"}} {bump(f"fe_req/{fe}", rps * step):.0f} {ms}')
        lines.append(f'haproxy_frontend_request_errors_total{{instance="{inst}",proxy="{fe}"}} {bump(f"fe_err/{fe}", rps * 0.002 * max(1, esrg * 0.1) * step):.0f} {ms}')
        lines.append(f'haproxy_frontend_requests_denied_total{{instance="{inst}",proxy="{fe}"}} {bump(f"fe_deny/{fe}", rps * 0.001 * ddos * step):.0f} {ms}')
        lines.append(f'haproxy_frontend_sessions_total{{instance="{inst}",proxy="{fe}"}} {bump(f"fe_sess/{fe}", rps * 0.05 * step):.0f} {ms}')
        lines.append(f'haproxy_frontend_connections_total{{instance="{inst}",proxy="{fe}"}} {bump(f"fe_conn/{fe}", rps * 0.065 * step):.0f} {ms}')
        lines.append(f'haproxy_frontend_bytes_in_total{{instance="{inst}",proxy="{fe}"}} {bump(f"fe_bin/{fe}", rps * 580 * step):.0f} {ms}')
        lines.append(f'haproxy_frontend_bytes_out_total{{instance="{inst}",proxy="{fe}"}} {bump(f"fe_bout/{fe}", rps * 2500 * step):.0f} {ms}')
        for code, share in codes.items():
            lines.append(f'haproxy_frontend_http_responses_total{{instance="{inst}",proxy="{fe}",code="{code}"}} {bump(f"fe_resp/{fe}/{code}", rps * share * step):.0f} {ms}')
        lines.append(f'haproxy_frontend_status{{instance="{inst}",proxy="{fe}",state="UP"}} 1 {ms}')
        lines.append(f'haproxy_frontend_status{{instance="{inst}",proxy="{fe}",state="DOWN"}} 0 {ms}')
        fe_cur = int(load * jitter({"http_in": 80, "https_in": 350, "stats": 1}.get(fe, 10)))
        lines.append(f'haproxy_frontend_current_sessions{{instance="{inst}",proxy="{fe}"}} {fe_cur} {ms}')

    # Backends
    for be, servers in HA_BACKENDS.items():
        rps    = HA_BE_BASE.get(be, 100) * load * jitter(1.0, 0.06)
        bps_o  = HA_BE_BPS.get(be, 500e3) * load * jitter(1.0, 0.08)
        bps_i  = bps_o * jitter(0.22, 0.10)
        rt_s   = HA_BASE_RT.get(be, 0.05) * lmul * jitter(1.0, 0.08)
        qt_s   = max(0.0, (load - 0.6) * 0.025) if load > 0.6 else jitter(0.002, 0.30)
        # queue grows non-linearly at high load
        queue_d = max(0, int((load - 0.7) * 50 * jitter(1.0, 0.40))) if load > 0.7 else 0

        lines.append(f'haproxy_backend_http_requests_total{{instance="{inst}",proxy="{be}"}} {bump(f"be_req/{be}", rps * step):.0f} {ms}')
        lines.append(f'haproxy_backend_sessions_total{{instance="{inst}",proxy="{be}"}} {bump(f"be_sess/{be}", rps * 0.05 * step):.0f} {ms}')
        lines.append(f'haproxy_backend_connection_attempts_total{{instance="{inst}",proxy="{be}"}} {bump(f"be_conn/{be}", rps * 0.06 * step):.0f} {ms}')
        lines.append(f'haproxy_backend_bytes_in_total{{instance="{inst}",proxy="{be}"}} {bump(f"be_bin/{be}", bps_i * step):.0f} {ms}')
        lines.append(f'haproxy_backend_bytes_out_total{{instance="{inst}",proxy="{be}"}} {bump(f"be_bout/{be}", bps_o * step):.0f} {ms}')
        lines.append(f'haproxy_backend_connection_errors_total{{instance="{inst}",proxy="{be}"}} {bump(f"be_cerr/{be}", rps * 0.0006 * step):.0f} {ms}')
        lines.append(f'haproxy_backend_status{{instance="{inst}",proxy="{be}",state="UP"}} 1 {ms}')
        lines.append(f'haproxy_backend_status{{instance="{inst}",proxy="{be}",state="DOWN"}} 0 {ms}')
        lines.append(f'haproxy_backend_current_sessions{{instance="{inst}",proxy="{be}"}} {int(load * jitter(rps * 0.03))} {ms}')
        lines.append(f'haproxy_backend_current_queue{{instance="{inst}",proxy="{be}"}} {queue_d} {ms}')
        lines.append(f'haproxy_backend_response_time_average_seconds{{instance="{inst}",proxy="{be}"}} {rt_s:.6f} {ms}')
        lines.append(f'haproxy_backend_queue_time_average_seconds{{instance="{inst}",proxy="{be}"}} {qt_s:.6f} {ms}')
        for code, share in codes.items():
            lines.append(f'haproxy_backend_http_responses_total{{instance="{inst}",proxy="{be}",code="{code}"}} {bump(f"be_resp/{be}/{code}", rps * share * step):.0f} {ms}')

        total_w = sum(HA_SRV_W.get(f"{be}/{s}", 1.0) for s in servers)
        for srv in servers:
            w = HA_SRV_W.get(f"{be}/{srv}", 1.0)
            for code, share in codes.items():
                srv_rps = rps * share * w / total_w
                lines.append(f'haproxy_server_http_responses_total{{instance="{inst}",proxy="{be}",server="{srv}",code="{code}"}} {bump(f"srv_resp/{be}/{srv}/{code}", srv_rps * step):.0f} {ms}')
            srv_bps_o = bps_o * w / total_w
            lines.append(f'haproxy_server_bytes_out_total{{instance="{inst}",proxy="{be}",server="{srv}"}} {bump(f"srv_bout/{be}/{srv}", srv_bps_o * step):.0f} {ms}')
            lines.append(f'haproxy_server_bytes_in_total{{instance="{inst}",proxy="{be}",server="{srv}"}} {bump(f"srv_bin/{be}/{srv}", srv_bps_o * 0.22 * step):.0f} {ms}')
            lines.append(f'haproxy_server_status{{instance="{inst}",proxy="{be}",server="{srv}",state="UP"}} 1 {ms}')
            lines.append(f'haproxy_server_status{{instance="{inst}",proxy="{be}",server="{srv}",state="DOWN"}} 0 {ms}')
            lines.append(f'haproxy_server_current_sessions{{instance="{inst}",proxy="{be}",server="{srv}"}} {int(load * jitter(rps * 0.03 * w / total_w))} {ms}')
            lines.append(f'haproxy_server_weight{{instance="{inst}",proxy="{be}",server="{srv}"}} {w:.2f} {ms}')

    return lines


# ─── Kubernetes topology ─────────────────────────────────────────────────────
# Pod list is generated from the same WORKLOADS table as the live generator so
# pod names are identical in historical and live data.

K8S_NODES = [
    {"name": "node-01", "cpu": 8,  "mem": 32 * 1024**3},
    {"name": "node-02", "cpu": 8,  "mem": 32 * 1024**3},
    {"name": "node-03", "cpu": 16, "mem": 64 * 1024**3},
]

# fmt: off
# (namespace, kind, name, replicas, cpu_req, cpu_lim, mem_req_mb, mem_lim_mb, image, wl_type, mem_pattern)
_WORKLOADS = [
    ("prod",       "Deployment",  "api-server",     3, 0.25, 1.0,  256,  512,  "myapp/api-server:2.1.4",            "api",     "sawtooth"),
    ("prod",       "Deployment",  "web-frontend",   2, 0.10, 0.5,  128,  256,  "myapp/web-frontend:3.0.1",          "api",     "stable"),
    ("prod",       "Deployment",  "auth-service",   2, 0.20, 0.8,  192,  384,  "myapp/auth-service:1.8.2",          "api",     "stable"),
    ("prod",       "Deployment",  "billing-worker", 1, 0.50, 2.0,  512, 1024,  "myapp/billing-worker:1.2.0",        "batch",   "sawtooth"),
    ("prod",       "StatefulSet", "postgres",       1, 1.00, 4.0, 2048, 4096,  "postgres:15.4",                     "db",      "growing"),
    ("prod",       "StatefulSet", "redis",          2, 0.25, 1.0,  512, 1024,  "redis:7.2",                         "cache",   "stable_high"),
    ("staging",    "Deployment",  "api-server",     1, 0.10, 0.5,  128,  256,  "myapp/api-server:2.2.0-rc1",        "api",     "stable"),
    ("staging",    "Deployment",  "web-frontend",   1, 0.05, 0.25,  64,  128,  "myapp/web-frontend:3.1.0-rc2",      "api",     "stable"),
    ("monitoring", "Deployment",  "prometheus",     1, 0.50, 2.0, 2048, 4096,  "prom/prometheus:v2.48.0",           "monitor", "sawtooth"),
    ("monitoring", "Deployment",  "grafana",        1, 0.10, 0.5,  256,  512,  "grafana/grafana:10.2.2",            "api",     "stable"),
    ("monitoring", "Deployment",  "alertmanager",   1, 0.05, 0.25,  64,  128,  "prom/alertmanager:v0.26.0",         "daemon",  "stable"),
    ("monitoring", "DaemonSet",   "node-exporter",  3, 0.05, 0.20,  32,   64,  "prom/node-exporter:v1.7.0",         "daemon",  "stable"),
    ("data",       "StatefulSet", "kafka",          3, 1.00, 4.0, 4096, 8192,  "confluentinc/cp-kafka:7.5.0",       "queue",   "growing"),
    ("data",       "StatefulSet", "zookeeper",      3, 0.25, 1.0,  512, 1024,  "confluentinc/cp-zookeeper:7.5.0",  "db",      "stable"),
    ("data",       "Deployment",  "schema-registry",1, 0.10, 0.5,  256,  512,  "confluentinc/cp-schema-registry:7.5.0","api", "stable"),
    ("kube-system","DaemonSet",   "kube-proxy",     3, 0.02, 0.10,  32,   64,  "registry.k8s.io/kube-proxy:v1.28.3","daemon",  "stable"),
    ("kube-system","Deployment",  "coredns",        2, 0.05, 0.20,  64,  128,  "registry.k8s.io/coredns/coredns:v1.11.1","dns","stable"),
    ("kube-system","Deployment",  "metrics-server", 1, 0.03, 0.10,  64,  128,  "registry.k8s.io/metrics-server:v0.7.0","daemon","stable"),
]
# fmt: on


def _pod_name(ns: str, name: str, idx: int, kind: str) -> str:
    """Must match k8s_generator.py exactly."""
    if kind == "StatefulSet":
        return f"{name}-{idx}"
    if kind == "DaemonSet":
        return f"{name}-{K8S_NODES[idx % len(K8S_NODES)]['name'].replace('-', '')}"
    ns_tag = f"{abs(hash(ns)) % 99999:05d}"
    return f"{name}-{ns_tag}-{idx}"


# Flat pod list: (namespace, pod, container, node, cpu_lim, mem_lim_bytes, wl_type, mem_pattern)
K8S_PODS: list[tuple] = []
for (_ns, _kind, _name, _replicas, _cpu_req, _cpu_lim,
     _mem_req_mb, _mem_lim_mb, _image, _wl_type, _mem_pattern) in _WORKLOADS:
    for _i in range(_replicas):
        _pname     = _pod_name(_ns, _name, _i, _kind)
        _node_idx  = hash(_pname) % len(K8S_NODES)
        _node_name = K8S_NODES[_node_idx]["name"]
        K8S_PODS.append((_ns, _pname, _name, _node_name,
                         _cpu_lim, _mem_lim_mb * 1024**2, _wl_type, _mem_pattern))


def _lset(**kv) -> str:
    parts = [f'origin_prometheus="{ORIGIN}"']
    parts += [f'{k}="{v}"' for k, v in kv.items()]
    return "{" + ",".join(parts) + "}"


def k8s_static_samples(ts: float) -> list[str]:
    ms    = int(ts * 1000)
    lines: list[str] = []

    for node in K8S_NODES:
        n = node["name"]
        lines.append(f'kube_node_info{_lset(node=n, kernel_version="6.1.0-26-cloud-amd64", os_image="Debian GNU/Linux 12", kubelet_version="v1.29.0")} 1 {ms}')
        lines.append(f'kube_node_status_allocatable{_lset(node=n, resource="cpu", unit="core")} {node["cpu"] * 0.94:.3f} {ms}')
        lines.append(f'kube_node_status_allocatable{_lset(node=n, resource="memory", unit="byte")} {int(node["mem"] * 0.95)} {ms}')
        lines.append(f'kube_node_status_allocatable{_lset(node=n, resource="pods", unit="integer")} 110 {ms}')
        lines.append(f'kube_node_status_condition{_lset(node=n, condition="Ready", status="true")} 1 {ms}')
        lines.append(f'kube_node_status_condition{_lset(node=n, condition="Ready", status="false")} 0 {ms}')

    for ns, pod, container, node, cpu_lim, mem_lim, wl_type, mem_pattern in K8S_PODS:
        cpu_req = cpu_lim * 0.25
        mem_req = int(mem_lim * 0.50)
        lines.append(f'kube_pod_info{_lset(namespace=ns, pod=pod, node=node, created_by_kind="Deployment", created_by_name=container)} 1 {ms}')
        lines.append(f'kube_pod_container_info{_lset(namespace=ns, pod=pod, container=container, node=node)} 1 {ms}')
        lines.append(f'kube_pod_container_resource_requests{_lset(namespace=ns, pod=pod, node=node, container=container, resource="cpu", unit="core")} {cpu_req} {ms}')
        lines.append(f'kube_pod_container_resource_requests{_lset(namespace=ns, pod=pod, node=node, container=container, resource="memory", unit="byte")} {mem_req} {ms}')
        lines.append(f'kube_pod_container_resource_limits{_lset(namespace=ns, pod=pod, node=node, container=container, resource="cpu", unit="core")} {cpu_lim} {ms}')
        lines.append(f'kube_pod_container_resource_limits{_lset(namespace=ns, pod=pod, node=node, container=container, resource="memory", unit="byte")} {mem_lim} {ms}')
        lines.append(f'kube_pod_status_ready{_lset(namespace=ns, pod=pod, condition="Ready")} 1 {ms}')
        lines.append(f'kube_pod_status_phase{_lset(namespace=ns, pod=pod, phase="Running")} 1 {ms}')

    for ns in ["default", "kube-system", "monitoring", "prod", "staging", "data"]:
        lines.append(f'kube_namespace_created{_lset(namespace=ns)} {int(ts - 86400 * 200)} {ms}')

    pvcs = [
        ("prod",  "data-postgres-0", 10*1024**3), ("prod",  "data-redis-0",   2*1024**3),
        ("data",  "data-kafka-0",    50*1024**3), ("data",  "data-kafka-1",   50*1024**3),
        ("data",  "data-zookeeper-0",10*1024**3),
    ]
    for ns, pvc, cap in pvcs:
        used  = int(cap * jitter(0.40, 0.05))
        lines.append(f'kubelet_volume_stats_used_bytes{_lset(namespace=ns, persistentvolumeclaim=pvc)} {used} {ms}')
        lines.append(f'kubelet_volume_stats_available_bytes{_lset(namespace=ns, persistentvolumeclaim=pvc)} {cap - used} {ms}')

    return lines


def k8s_dynamic_samples(ts: float, step: float, acc: dict) -> list[str]:
    ms    = int(ts * 1000)
    load  = _load(ts)
    lines: list[str] = []

    for ns, pod, container, node, cpu_lim, mem_lim, wl_type, mem_pattern in K8S_PODS:
        cpu_ratio = _workload_cpu_ratio(ts, pod, wl_type)
        cpu_rate  = cpu_lim * cpu_ratio

        key       = f"cpu/{pod}"
        cpu_acc   = acc.get(key, random.uniform(0, 3600))
        cpu_acc  += cpu_rate * step
        acc[key]  = cpu_acc

        mem_ratio = _workload_mem_ratio(ts, pod, mem_pattern)
        wss       = int(mem_lim * mem_ratio)
        rss       = int(wss * 0.85)

        # Throttling: increases when CPU ratio nears 1.0
        throttle = min(0.65, max(0.0, (cpu_ratio - 0.70) * 2.0))

        bps_base = {"prod": 5e6, "data": 20e6, "monitoring": 1e6}.get(ns, 1e6)
        rx_rate  = bps_base * load * jitter(0.15, 0.15)
        tx_rate  = rx_rate  * jitter(1.10, 0.15)
        rx_key   = f"rx/{pod}"; tx_key = f"tx/{pod}"
        rx_acc   = acc.get(rx_key, random.uniform(0, 1e9))
        tx_acc   = acc.get(tx_key, random.uniform(0, 1e9))
        rx_acc  += rx_rate * step
        tx_acc  += tx_rate * step
        acc[rx_key] = rx_acc
        acc[tx_key] = tx_acc

        img = "myapp/api-server:2.1.4"
        id_ = f"/kubepods/{pod}"

        lines.append(f'container_cpu_usage_seconds_total{_lset(namespace=ns, pod=pod, node=node, container=container, id=id_, image=img)} {cpu_acc:.3f} {ms}')
        lines.append(f'container_cpu_cfs_throttled_seconds_total{_lset(namespace=ns, pod=pod, node=node, container=container, id=id_)} {throttle * cpu_acc:.3f} {ms}')
        lines.append(f'container_cpu_cfs_periods_total{_lset(namespace=ns, pod=pod, node=node, container=container, id=id_)} {int(ts / 0.1)} {ms}')
        lines.append(f'container_cpu_cfs_throttled_periods_total{_lset(namespace=ns, pod=pod, node=node, container=container, id=id_)} {int(throttle * ts / 0.1)} {ms}')
        lines.append(f'container_memory_working_set_bytes{_lset(namespace=ns, pod=pod, node=node, container=container, id=id_, image=img)} {wss} {ms}')
        lines.append(f'container_memory_rss{_lset(namespace=ns, pod=pod, node=node, container=container, id=id_, image=img)} {rss} {ms}')
        lines.append(f'container_memory_cache{_lset(namespace=ns, pod=pod, node=node, container=container, id=id_)} {int(wss * 0.10)} {ms}')
        lines.append(f'container_network_receive_bytes_total{_lset(namespace=ns, pod=pod, node=node, interface="eth0")} {rx_acc:.0f} {ms}')
        lines.append(f'container_network_transmit_bytes_total{_lset(namespace=ns, pod=pod, node=node, interface="eth0")} {tx_acc:.0f} {ms}')
        lines.append(f'container_fs_usage_bytes{_lset(namespace=ns, pod=pod, node=node, container=container, id=id_, device="/dev/sda1")} {int(5 * 1024**3 * jitter(0.20))} {ms}')
        lines.append(f'container_fs_limit_bytes{_lset(namespace=ns, pod=pod, node=node, container=container, id=id_, device="/dev/sda1")} {20 * 1024**3} {ms}')
        lines.append(f'container_spec_cpu_quota{_lset(namespace=ns, pod=pod, node=node, container=container, id=id_)} {int(cpu_lim * 100_000)} {ms}')
        lines.append(f'container_spec_memory_limit_bytes{_lset(namespace=ns, pod=pod, node=node, container=container, id=id_)} {mem_lim} {ms}')

    return lines


# ─── HTTP push ────────────────────────────────────────────────────────────────

def post_batch(lines: list[str], vm_url: str) -> None:
    body = "\n".join(lines) + "\n"
    resp = requests.post(
        f"{vm_url}/api/v1/import/prometheus",
        data=body.encode("utf-8"),
        headers={"Content-Type": "text/plain"},
        timeout=60,
    )
    resp.raise_for_status()


def _wait_for_vm(url: str, timeout: int = 120) -> None:
    deadline = time.time() + timeout
    print(f"[prometheus_backfill] Waiting for VictoriaMetrics at {url} …", flush=True)
    while time.time() < deadline:
        try:
            if requests.get(f"{url}/health", timeout=3).ok:
                return
        except Exception:
            pass
        time.sleep(3)
    raise SystemExit(f"[prometheus_backfill] VictoriaMetrics not ready after {timeout}s")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--vm-url",  default=VM_URL)
    parser.add_argument("--days",    type=float, default=DAYS)
    parser.add_argument("--step-s",  type=int,   default=STEP_S)
    parser.add_argument("--batch",   type=int,   default=BATCH)
    args = parser.parse_args()

    _wait_for_vm(args.vm_url)

    now         = time.time()
    start_ts    = now - args.days * 86400
    total_steps = int((now - start_ts) / args.step_s)

    print(f"[prometheus_backfill] {args.days:.1f}d  ({total_steps:,} steps × {args.step_s}s)"
          f"  pods={len(K8S_PODS)}  frontends={len(HA_FRONTENDS)}  backends={len(HA_BACKENDS)}"
          f"  → {args.vm_url}", flush=True)

    ha_acc:  dict = {"start_time": start_ts - 3600}
    k8s_acc: dict = {}
    batch:   list[str] = []
    total_lines = 0
    t0    = time.time()
    WIDTH = 38

    def redraw(step: int) -> None:
        pct     = step / total_steps
        elapsed = time.time() - t0 or 1e-9
        rate    = total_lines / elapsed
        eta     = int((total_steps - step) * elapsed / step) if step > 0 else 0
        if _TTY:
            filled = int(WIDTH * pct)
            bar    = "█" * filled + "░" * (WIDTH - filled)
            print(f"\r  [{bar}] {pct*100:5.1f}%  {total_lines/1e6:.2f}M  {rate/1e3:.0f}k/s  ETA {eta}s   ",
                  end="", flush=True)
        else:
            prev = int((step - 1) / total_steps * 10)
            curr = int(step / total_steps * 10)
            if curr > prev:
                print(f"  {curr*10:3d}%  {total_lines/1e6:.2f}M lines  {rate/1e3:.0f}k/s  ETA {eta}s",
                      flush=True)

    for i, ts_int in enumerate(range(int(start_ts), int(now), args.step_s)):
        ts = float(ts_int)
        batch.extend(haproxy_samples(ts, args.step_s, ha_acc))
        batch.extend(k8s_dynamic_samples(ts, args.step_s, k8s_acc))
        if i % 5 == 0:
            batch.extend(k8s_static_samples(ts))

        if len(batch) >= args.batch:
            try:
                post_batch(batch, args.vm_url)
            except Exception as exc:
                print(f"\n[prometheus_backfill] POST error: {exc}", file=sys.stderr, flush=True)
            total_lines += len(batch)
            batch = []
            redraw(i + 1)

    if batch:
        try:
            post_batch(batch, args.vm_url)
        except Exception as exc:
            print(f"\n[prometheus_backfill] POST error: {exc}", file=sys.stderr, flush=True)
        total_lines += len(batch)

    elapsed = time.time() - t0
    if _TTY:
        print(f"\r  [{'█' * WIDTH}] 100.0%  {total_lines/1e6:.2f}M lines  done in {elapsed:.1f}s          ")
    else:
        print(f"  100%  {total_lines/1e6:.2f}M lines  done in {elapsed:.1f}s", flush=True)


if __name__ == "__main__":
    main()
