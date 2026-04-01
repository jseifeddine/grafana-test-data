#!/usr/bin/env python3
"""
HAProxy metrics generator.

Topology: 3 frontends → 4 backends → 10 servers

Signal model (all functions are deterministic given ts so backfill and live
mode are consistent):

  _load(ts)          — traffic multiplier with weekly seasonality, multi-scale
                       fractal noise, flash-traffic spikes, and maintenance dips
  _error_surge(ts)   — baseline ~0.5 %; occasional error storms (10-30x)
  _latency_mult(ts)  — correlates with load; DB-slow-query events
  _ddos_mult(ts)     — rarely, requests_denied spikes 20-100x
  _server_health()   — flaps individual servers DOWN for realistic durations
"""

import argparse
import math
import random
import sys
import time

import requests

PUSHGATEWAY_URL = "http://pushgateway:9091"
INSTANCE        = "haproxy:9101"
PUSH_INTERVAL   = 15

# ─── topology ────────────────────────────────────────────────────────────────

FRONTENDS = ["http_in", "https_in", "stats"]

BACKENDS = {
    "backend_web":    ["web01", "web02", "web03"],
    "backend_api":    ["api01", "api02"],
    "backend_static": ["static01", "static02"],
    "backend_auth":   ["auth01", "auth02"],
}

# Relative weights — web03 is older/slower, api02 carries heavier load
SERVER_WEIGHTS = {
    "backend_web/web01":    1.05,
    "backend_web/web02":    1.00,
    "backend_web/web03":    0.80,   # older, slower box
    "backend_api/api01":    1.00,
    "backend_api/api02":    1.20,   # handles more traffic
    "backend_static/static01": 1.0,
    "backend_static/static02": 1.0,
    "backend_auth/auth01":  1.0,
    "backend_auth/auth02":  1.0,
}

# Baseline response times per backend (seconds)
BASE_RT = {
    "backend_web":    0.045,
    "backend_api":    0.140,
    "backend_static": 0.008,
    "backend_auth":   0.065,
}

HTTP_CODES = {
    "200": 0.883, "206": 0.030, "301": 0.018, "304": 0.030,
    "400": 0.010, "404": 0.016, "429": 0.004, "500": 0.005, "503": 0.004,
}

_start_time = time.time()

# ─── signal layer (deterministic given ts) ───────────────────────────────────

def _det_rng(ts: float, slot_s: float, seed: int) -> random.Random:
    """Return a seeded RNG for the time-slot containing ts."""
    return random.Random(int(ts / slot_s) * 2654435761 + seed)


def _load(ts: float) -> float:
    """
    Traffic load multiplier, 0.02 – ~4.0.

    Components:
      1. Diurnal bell curve (business hours peaks)
      2. Weekend depression (-65 %)
      3. Multi-scale fractal oscillations (2-min → 2-hour)
      4. Flash-traffic spikes  (rare, 2-5× for 3-20 min, ~3 per day)
      5. Maintenance windows   (rare, -97 %, ~1 per fortnight, off-hours)
    """
    hour = (ts % 86400) / 3600
    am   = math.exp(-0.5 * ((hour - 10.5) / 1.8) ** 2)
    pm   = math.exp(-0.5 * ((hour - 14.5) / 1.6) ** 2)
    base = max(0.06, am * 0.90 + pm * 0.72)

    # Epoch day 0 = Thu; +4 makes 0=Mon
    dow = int(ts // 86400 + 4) % 7
    if dow >= 5:          # Sat / Sun
        base *= 0.35
    elif dow == 4:        # Friday afternoon taper
        base *= max(0.70, 1.0 - max(0, hour - 16) * 0.12)

    # Multi-scale fractal noise — irrational ratios prevent harmonic lock-in
    base *= 1 + (
        0.10 * math.sin(ts / 157   + 1.1) +
        0.07 * math.sin(ts / 523   + 2.7) +
        0.05 * math.sin(ts / 1801  + 0.4) +
        0.04 * math.sin(ts / 7207  + 3.9) +
        0.02 * math.sin(ts / 28800 + 1.5)
    )

    # Flash-traffic spike — ~18 % chance per 6-hour window
    r = _det_rng(ts, 21600, 1)
    if r.random() < 0.18:
        w0    = int(ts / 21600) * 21600
        s_start = w0 + r.uniform(1800, 19800)
        s_dur   = r.uniform(180, 1200)
        if s_start <= ts < s_start + s_dur:
            base *= r.uniform(2.0, 4.5)

    # Maintenance window — ~40 % chance per 2-week window, off-peak hours
    r2 = _det_rng(ts, 86400 * 14, 2)
    if r2.random() < 0.40:
        w0      = int(ts / (86400 * 14)) * 86400 * 14
        m_start = w0 + r2.uniform(0, 86400 * 13)
        if int(m_start // 3600) % 24 not in range(2, 6):
            m_start += 3600 * r2.randint(1, 4)   # push to off-hours
        m_dur = r2.uniform(900, 3600)
        if m_start <= ts < m_start + m_dur:
            base *= 0.03

    return max(0.01, base)


def _error_surge(ts: float) -> float:
    """
    Multiplicative boost to 5xx error share.
    Normal: 1.0.  During an error storm (upstream timeout / deploy gone wrong):
    10 – 40×, lasting 2-10 minutes, ~2 per day.
    """
    r = _det_rng(ts, 10800, 3)   # 3-hour windows
    if r.random() < 0.14:
        w0      = int(ts / 10800) * 10800
        s_start = w0 + r.uniform(300, 9900)
        s_dur   = r.uniform(120, 600)
        if s_start <= ts < s_start + s_dur:
            return r.uniform(10.0, 45.0)
    return 1.0


def _latency_mult(ts: float, load: float) -> float:
    """
    Response-time multiplier.  Load pushes latency up (queuing theory),
    and occasional DB slow-query events spike it 5-20×.
    """
    # Little's law approximation: latency ∝ 1/(1-utilisation)
    util   = min(0.92, load * 0.70)
    q_mult = 1.0 / max(0.08, 1.0 - util)

    # DB slow query / GC pause events — ~15 % per 2h window
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
    """Rare DDoS / scraper burst — requests_denied jumps 20-100×."""
    r = _det_rng(ts, 86400 * 3, 5)  # ~1 event per 3 days
    if r.random() < 0.35:
        w0      = int(ts / (86400 * 3)) * 86400 * 3
        s_start = w0 + r.uniform(0, 86400 * 2.5)
        s_dur   = r.uniform(300, 3600)
        if s_start <= ts < s_start + s_dur:
            return r.uniform(20.0, 100.0)
    return 1.0


# ─── server health state (live mode) ─────────────────────────────────────────

_flapped_server: str | None = None
_flap_until:     float      = 0.0


def _tick_server_health() -> None:
    global _flapped_server, _flap_until
    now = time.time()
    if _flapped_server and now > _flap_until:
        _flapped_server = None
    if not _flapped_server and random.random() < 0.0015:   # ~0.15 % per tick
        be  = random.choice(list(BACKENDS))
        srv = random.choice(BACKENDS[be])
        _flapped_server = f"{be}/{srv}"
        _flap_until     = now + random.uniform(45, 180)


def _server_up(be: str, srv: str) -> bool:
    return _flapped_server != f"{be}/{srv}"


# ─── counter accumulator ──────────────────────────────────────────────────────

_acc: dict[str, float] = {}


def _bump(key: str, delta: float) -> float:
    _acc[key] = _acc.get(key, 0.0) + max(0.0, delta)
    return _acc[key]


# ─── metric builder ───────────────────────────────────────────────────────────

def build_metrics(step: float) -> str:
    ts   = time.time()
    load = _load(ts)
    esrg = _error_surge(ts)
    lmul = _latency_mult(ts, load)
    ddos = _ddos_mult(ts)

    # Derive an effective error distribution
    err_boost  = esrg
    codes: dict[str, float] = {}
    for c, share in HTTP_CODES.items():
        if c in ("500", "503"):
            codes[c] = share * err_boost
        elif c in ("400", "404", "429"):
            codes[c] = share * max(1.0, err_boost * 0.3)
        else:
            codes[c] = share
    total_share = sum(codes.values())
    codes = {c: v / total_share for c, v in codes.items()}

    lines: list[str] = []

    # ── process ──────────────────────────────────────────────────────────────
    uptime = ts - _start_time
    pool_base = 45e6 + uptime * 12   # memory grows slowly over time
    pool_used = pool_base * random.uniform(0.65, 0.90)
    lines += [
        "# HELP haproxy_process_nbproc Current worker processes.",
        "# TYPE haproxy_process_nbproc gauge",
        f'haproxy_process_nbproc{{instance="{INSTANCE}"}} 1',
        "# HELP haproxy_process_start_time_seconds Process start time.",
        "# TYPE haproxy_process_start_time_seconds gauge",
        f'haproxy_process_start_time_seconds{{instance="{INSTANCE}"}} {_start_time:.3f}',
        "# HELP haproxy_process_uptime_seconds Process uptime.",
        "# TYPE haproxy_process_uptime_seconds gauge",
        f'haproxy_process_uptime_seconds{{instance="{INSTANCE}"}} {uptime:.1f}',
        "# HELP haproxy_process_max_connections Maximum simultaneous connections.",
        "# TYPE haproxy_process_max_connections gauge",
        f'haproxy_process_max_connections{{instance="{INSTANCE}"}} 50000',
        "# HELP haproxy_process_current_connections Active sessions.",
        "# TYPE haproxy_process_current_connections gauge",
        f'haproxy_process_current_connections{{instance="{INSTANCE}"}} {int(load * random.uniform(600, 2800))}',
        "# HELP haproxy_process_current_connection_rate Connections/s.",
        "# TYPE haproxy_process_current_connection_rate gauge",
        f'haproxy_process_current_connection_rate{{instance="{INSTANCE}"}} {load * random.uniform(15, 160):.1f}',
        "# HELP haproxy_process_pool_allocated_bytes Memory pool allocated.",
        "# TYPE haproxy_process_pool_allocated_bytes gauge",
        f'haproxy_process_pool_allocated_bytes{{instance="{INSTANCE}"}} {int(pool_base)}',
        "# HELP haproxy_process_pool_used_bytes Memory pool used.",
        "# TYPE haproxy_process_pool_used_bytes gauge",
        f'haproxy_process_pool_used_bytes{{instance="{INSTANCE}"}} {int(pool_used)}',
    ]

    # ── frontends ─────────────────────────────────────────────────────────────
    FE_BASE_RPS = {"http_in": 600, "https_in": 1800, "stats": 2}

    lines += ["# HELP haproxy_frontend_http_requests_total Total HTTP requests received.",
              "# TYPE haproxy_frontend_http_requests_total counter"]
    for fe in FRONTENDS:
        rps   = FE_BASE_RPS.get(fe, 100) * load * random.uniform(0.92, 1.08)
        total = _bump(f"fe_req/{fe}", rps * step)
        lines.append(f'haproxy_frontend_http_requests_total{{instance="{INSTANCE}",proxy="{fe}"}} {total:.0f}')

    lines += ["# HELP haproxy_frontend_http_responses_total Total HTTP responses.",
              "# TYPE haproxy_frontend_http_responses_total counter"]
    for fe in FRONTENDS:
        rps = FE_BASE_RPS.get(fe, 100) * load
        for code, share in codes.items():
            noise = random.uniform(0.88, 1.12)
            total = _bump(f"fe_resp/{fe}/{code}", rps * share * noise * step)
            lines.append(f'haproxy_frontend_http_responses_total{{instance="{INSTANCE}",proxy="{fe}",code="{code}"}} {total:.0f}')

    lines += ["# HELP haproxy_frontend_bytes_in_total Bytes received by frontends.",
              "# TYPE haproxy_frontend_bytes_in_total counter",
              "# HELP haproxy_frontend_bytes_out_total Bytes sent by frontends.",
              "# TYPE haproxy_frontend_bytes_out_total counter"]
    FE_BPS = {"http_in": 500e3, "https_in": 2e6, "stats": 1e3}
    for fe in FRONTENDS:
        bps_in  = FE_BPS.get(fe, 1e3) * load * random.uniform(0.9, 1.1)
        bps_out = bps_in * random.uniform(3.5, 6.0)
        ti = _bump(f"fe_bin/{fe}",  bps_in  * step)
        to = _bump(f"fe_bout/{fe}", bps_out * step)
        lines.append(f'haproxy_frontend_bytes_in_total{{instance="{INSTANCE}",proxy="{fe}"}} {ti:.0f}')
        lines.append(f'haproxy_frontend_bytes_out_total{{instance="{INSTANCE}",proxy="{fe}"}} {to:.0f}')

    lines += ["# HELP haproxy_frontend_sessions_total Total sessions created.",
              "# TYPE haproxy_frontend_sessions_total counter"]
    for fe in FRONTENDS:
        rps   = FE_BASE_RPS.get(fe, 100) * load * 0.05
        total = _bump(f"fe_sess/{fe}", rps * step)
        lines.append(f'haproxy_frontend_sessions_total{{instance="{INSTANCE}",proxy="{fe}"}} {total:.0f}')

    lines += ["# HELP haproxy_frontend_connections_total Total connections.",
              "# TYPE haproxy_frontend_connections_total counter"]
    for fe in FRONTENDS:
        rps   = FE_BASE_RPS.get(fe, 100) * load * 0.065
        total = _bump(f"fe_conn/{fe}", rps * step)
        lines.append(f'haproxy_frontend_connections_total{{instance="{INSTANCE}",proxy="{fe}"}} {total:.0f}')

    lines += ["# HELP haproxy_frontend_request_errors_total Request errors on frontends.",
              "# TYPE haproxy_frontend_request_errors_total counter"]
    for fe in FRONTENDS:
        rps   = FE_BASE_RPS.get(fe, 100) * load * 0.002 * max(1.0, esrg * 0.1)
        total = _bump(f"fe_err/{fe}", rps * step)
        lines.append(f'haproxy_frontend_request_errors_total{{instance="{INSTANCE}",proxy="{fe}"}} {total:.0f}')

    lines += ["# HELP haproxy_frontend_requests_denied_total Denied requests.",
              "# TYPE haproxy_frontend_requests_denied_total counter"]
    for fe in FRONTENDS:
        rps   = FE_BASE_RPS.get(fe, 100) * load * 0.001 * ddos
        total = _bump(f"fe_deny/{fe}", rps * step)
        lines.append(f'haproxy_frontend_requests_denied_total{{instance="{INSTANCE}",proxy="{fe}"}} {total:.0f}')

    lines += ["# HELP haproxy_frontend_status Current frontend status.",
              "# TYPE haproxy_frontend_status gauge"]
    for fe in FRONTENDS:
        for state in ("UP", "DOWN", "NOLB", "MAINT"):
            lines.append(f'haproxy_frontend_status{{instance="{INSTANCE}",proxy="{fe}",state="{state}"}} {1 if state == "UP" else 0}')

    lines += ["# HELP haproxy_frontend_current_sessions Active frontend sessions.",
              "# TYPE haproxy_frontend_current_sessions gauge"]
    for fe in FRONTENDS:
        FE_SESS_BASE = {"http_in": 80, "https_in": 350, "stats": 1}
        cur = int(FE_SESS_BASE.get(fe, 10) * load * random.uniform(0.7, 1.4))
        lines.append(f'haproxy_frontend_current_sessions{{instance="{INSTANCE}",proxy="{fe}"}} {cur}')

    # ── backends ──────────────────────────────────────────────────────────────
    BE_BASE_RPS = {"backend_web": 1600, "backend_api": 600, "backend_static": 400, "backend_auth": 200}
    BE_BPS_OUT  = {"backend_web": 2e6,  "backend_api": 800e3, "backend_static": 5e6, "backend_auth": 200e3}

    lines += ["# HELP haproxy_backend_http_requests_total Total backend requests.",
              "# TYPE haproxy_backend_http_requests_total counter"]
    for be in BACKENDS:
        rps   = BE_BASE_RPS.get(be, 100) * load * random.uniform(0.92, 1.08)
        total = _bump(f"be_req/{be}", rps * step)
        lines.append(f'haproxy_backend_http_requests_total{{instance="{INSTANCE}",proxy="{be}"}} {total:.0f}')

    lines += ["# HELP haproxy_backend_http_responses_total Total backend responses.",
              "# TYPE haproxy_backend_http_responses_total counter"]
    for be in BACKENDS:
        rps = BE_BASE_RPS.get(be, 100) * load
        for code, share in codes.items():
            total = _bump(f"be_resp/{be}/{code}", rps * share * random.uniform(0.88, 1.12) * step)
            lines.append(f'haproxy_backend_http_responses_total{{instance="{INSTANCE}",proxy="{be}",code="{code}"}} {total:.0f}')

    lines += ["# HELP haproxy_backend_bytes_in_total Bytes received by backends.",
              "# TYPE haproxy_backend_bytes_in_total counter",
              "# HELP haproxy_backend_bytes_out_total Bytes sent by backends.",
              "# TYPE haproxy_backend_bytes_out_total counter"]
    for be in BACKENDS:
        bps_out = BE_BPS_OUT.get(be, 500e3) * load * random.uniform(0.9, 1.1)
        bps_in  = bps_out * random.uniform(0.15, 0.35)
        ti = _bump(f"be_bin/{be}",  bps_in  * step)
        to = _bump(f"be_bout/{be}", bps_out * step)
        lines.append(f'haproxy_backend_bytes_in_total{{instance="{INSTANCE}",proxy="{be}"}} {ti:.0f}')
        lines.append(f'haproxy_backend_bytes_out_total{{instance="{INSTANCE}",proxy="{be}"}} {to:.0f}')

    lines += ["# HELP haproxy_backend_sessions_total Total backend sessions.",
              "# TYPE haproxy_backend_sessions_total counter"]
    for be in BACKENDS:
        rps   = BE_BASE_RPS.get(be, 100) * load * 0.05
        total = _bump(f"be_sess/{be}", rps * step)
        lines.append(f'haproxy_backend_sessions_total{{instance="{INSTANCE}",proxy="{be}"}} {total:.0f}')

    lines += ["# HELP haproxy_backend_connection_attempts_total Backend connection attempts.",
              "# TYPE haproxy_backend_connection_attempts_total counter"]
    for be in BACKENDS:
        rps   = BE_BASE_RPS.get(be, 100) * load * 0.06
        total = _bump(f"be_conn/{be}", rps * step)
        lines.append(f'haproxy_backend_connection_attempts_total{{instance="{INSTANCE}",proxy="{be}"}} {total:.0f}')

    lines += ["# HELP haproxy_backend_connection_errors_total Backend connection errors.",
              "# TYPE haproxy_backend_connection_errors_total counter"]
    for be in BACKENDS:
        # Errors spike when a server is flapped
        flap_mult = 5.0 if any(not _server_up(be, s) for s in BACKENDS[be]) else 1.0
        rps   = BE_BASE_RPS.get(be, 100) * load * 0.0006 * flap_mult
        total = _bump(f"be_cerr/{be}", rps * step)
        lines.append(f'haproxy_backend_connection_errors_total{{instance="{INSTANCE}",proxy="{be}"}} {total:.0f}')

    lines += ["# HELP haproxy_backend_status Backend status.",
              "# TYPE haproxy_backend_status gauge"]
    for be in BACKENDS:
        is_up = all(_server_up(be, s) for s in BACKENDS[be])
        for state in ("UP", "DOWN", "NOLB", "MAINT"):
            val = 1 if (state == "UP" and is_up) or (state == "DOWN" and not is_up) else 0
            lines.append(f'haproxy_backend_status{{instance="{INSTANCE}",proxy="{be}",state="{state}"}} {val}')

    lines += ["# HELP haproxy_backend_current_sessions Active backend sessions.",
              "# TYPE haproxy_backend_current_sessions gauge",
              "# HELP haproxy_backend_current_queue Queued requests.",
              "# TYPE haproxy_backend_current_queue gauge",
              "# HELP haproxy_backend_response_time_average_seconds Avg response time.",
              "# TYPE haproxy_backend_response_time_average_seconds gauge",
              "# HELP haproxy_backend_queue_time_average_seconds Avg queue time.",
              "# TYPE haproxy_backend_queue_time_average_seconds gauge"]
    for be in BACKENDS:
        cur  = int(BE_BASE_RPS.get(be, 100) * load * 0.03 * random.uniform(0.8, 1.3))
        # Queue depth grows non-linearly with load
        queue_depth = max(0, int((load - 0.7) * 50 * random.uniform(0.5, 2.0))) if load > 0.7 else 0
        rt   = BASE_RT.get(be, 0.05) * lmul * random.uniform(0.85, 1.20)
        qt   = max(0, (load - 0.6) * 0.025) if load > 0.6 else random.uniform(0, 0.003)
        lines.append(f'haproxy_backend_current_sessions{{instance="{INSTANCE}",proxy="{be}"}} {cur}')
        lines.append(f'haproxy_backend_current_queue{{instance="{INSTANCE}",proxy="{be}"}} {queue_depth}')
        lines.append(f'haproxy_backend_response_time_average_seconds{{instance="{INSTANCE}",proxy="{be}"}} {rt:.6f}')
        lines.append(f'haproxy_backend_queue_time_average_seconds{{instance="{INSTANCE}",proxy="{be}"}} {qt:.6f}')

    # ── servers ───────────────────────────────────────────────────────────────
    lines += ["# HELP haproxy_server_http_responses_total Server HTTP responses.",
              "# TYPE haproxy_server_http_responses_total counter"]
    for be, servers in BACKENDS.items():
        total_w = sum(SERVER_WEIGHTS.get(f"{be}/{s}", 1.0) for s in servers if _server_up(be, s))
        rps_be  = BE_BASE_RPS.get(be, 100) * load
        for srv in servers:
            up = _server_up(be, srv)
            w  = SERVER_WEIGHTS.get(f"{be}/{srv}", 1.0) if up else 0.0
            for code, share in codes.items():
                srv_rps = (rps_be * share * w / total_w) if total_w > 0 else 0
                total   = _bump(f"srv_resp/{be}/{srv}/{code}", srv_rps * random.uniform(0.9, 1.1) * step)
                lines.append(f'haproxy_server_http_responses_total{{instance="{INSTANCE}",proxy="{be}",server="{srv}",code="{code}"}} {total:.0f}')

    lines += ["# HELP haproxy_server_bytes_in_total Server bytes received.",
              "# TYPE haproxy_server_bytes_in_total counter",
              "# HELP haproxy_server_bytes_out_total Server bytes sent.",
              "# TYPE haproxy_server_bytes_out_total counter"]
    for be, servers in BACKENDS.items():
        bps_total = BE_BPS_OUT.get(be, 500e3) * load
        total_w   = sum(SERVER_WEIGHTS.get(f"{be}/{s}", 1.0) for s in servers if _server_up(be, s))
        for srv in servers:
            up = _server_up(be, srv)
            w  = SERVER_WEIGHTS.get(f"{be}/{srv}", 1.0) if up else 0.0
            bps_out = (bps_total * w / total_w * random.uniform(0.9, 1.1)) if total_w > 0 else 0
            bps_in  = bps_out * random.uniform(0.15, 0.30)
            ti = _bump(f"srv_bin/{be}/{srv}",  bps_in  * step)
            to = _bump(f"srv_bout/{be}/{srv}", bps_out * step)
            lines.append(f'haproxy_server_bytes_in_total{{instance="{INSTANCE}",proxy="{be}",server="{srv}"}} {ti:.0f}')
            lines.append(f'haproxy_server_bytes_out_total{{instance="{INSTANCE}",proxy="{be}",server="{srv}"}} {to:.0f}')

    lines += ["# HELP haproxy_server_status Server status.",
              "# TYPE haproxy_server_status gauge",
              "# HELP haproxy_server_current_sessions Active server sessions.",
              "# TYPE haproxy_server_current_sessions gauge",
              "# HELP haproxy_server_weight Server weight.",
              "# TYPE haproxy_server_weight gauge",
              "# HELP haproxy_server_check_failures_total Failed health checks.",
              "# TYPE haproxy_server_check_failures_total counter"]
    for be, servers in BACKENDS.items():
        active = [s for s in servers if _server_up(be, s)]
        for srv in servers:
            up  = _server_up(be, srv)
            w   = SERVER_WEIGHTS.get(f"{be}/{srv}", 1.0)
            cur = int(BE_BASE_RPS.get(be, 100) * load * 0.03 * w / max(1, len(active)) * random.uniform(0.7, 1.4)) if up else 0
            chk_delta = random.uniform(0.02, 0.15) if not up else 0.0
            fails = _bump(f"srv_chkfail/{be}/{srv}", chk_delta)
            for state in ("UP", "DOWN", "NOLB", "MAINT", "DRAIN"):
                val = 1 if (state == "UP" and up) or (state == "DOWN" and not up) else 0
                lines.append(f'haproxy_server_status{{instance="{INSTANCE}",proxy="{be}",server="{srv}",state="{state}"}} {val}')
            lines.append(f'haproxy_server_current_sessions{{instance="{INSTANCE}",proxy="{be}",server="{srv}"}} {cur}')
            lines.append(f'haproxy_server_weight{{instance="{INSTANCE}",proxy="{be}",server="{srv}"}} {w:.2f}')
            lines.append(f'haproxy_server_check_failures_total{{instance="{INSTANCE}",proxy="{be}",server="{srv}"}} {fails:.0f}')

    return "\n".join(lines) + "\n"


def push(text: str, url: str) -> None:
    r = requests.post(
        f"{url}/metrics/job/haproxy/instance/{INSTANCE}",
        data=text,
        headers={"Content-Type": "text/plain; charset=utf-8"},
        timeout=5,
    )
    r.raise_for_status()


def _wait_for(url: str, timeout: int = 120) -> None:
    deadline = time.time() + timeout
    print(f"[haproxy_generator] Waiting for {url} …", flush=True)
    while time.time() < deadline:
        try:
            if requests.get(url, timeout=3).ok:
                return
        except Exception:
            pass
        time.sleep(3)
    raise SystemExit(f"[haproxy_generator] {url} not ready after {timeout}s")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pushgateway", default=PUSHGATEWAY_URL)
    parser.add_argument("--interval",    type=float, default=PUSH_INTERVAL)
    parser.add_argument("--once",        action="store_true")
    args = parser.parse_args()

    _wait_for(f"{args.pushgateway}/-/healthy")
    print(f"[haproxy_generator] Pushing every {args.interval}s …", flush=True)

    while True:
        _tick_server_health()
        try:
            metrics = build_metrics(args.interval)
            push(metrics, args.pushgateway)
            load_now = _load(time.time())
            flap_info = f"  (server DOWN: {_flapped_server})" if _flapped_server else ""
            print(f"[haproxy_generator] pushed  load={load_now:.2f}{flap_info}", flush=True)
        except Exception as exc:
            print(f"[haproxy_generator] ERROR: {exc}", file=sys.stderr, flush=True)
        if args.once:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
