#!/usr/bin/env python3
"""
PowerDNS Authoritative metrics generator for Graphite.

Simulates two authoritative nameservers (ns1, ns2) and writes metrics via
the Carbon plaintext protocol (TCP 2003) to a Graphite backend.

Metric paths follow the standard PowerDNS → Graphite export scheme:
    pdns.<host>.auth.<metric>

The dashboard's $host template variable queries pdns.* from Graphite, which
returns ns1 and ns2 once data is present.

Signal model (all functions deterministic given ts):
  _dns_load(ts, seed)     — muted diurnal + weekly + fractal noise;
                            DNS traffic is much flatter than web traffic
                            because recursive resolvers cache responses
  _latency_us(ts, load)   — µs; rises with load; spikes during DB slow events
  _servfail_mult(ts)      — normally 1×; backend issue storms: 20–100×
  _ddns_event(ts, seed)   — rare DDNS bursts (TSIG-authenticated updates)
  _notify_event(ts, seed) — incoming NOTIFY messages from hidden primaries
"""

import argparse
import math
import random
import socket
import sys
import time

_TTY = sys.stdout.isatty()

CARBON_HOST  = "graphite"
CARBON_PORT  = 2003
HOSTS        = ["ns1", "ns2"]
PUSH_INTERVAL = 60      # seconds — matches the 60s Whisper retention bucket
BACKFILL_DAYS = 7
STEP_S        = 60      # one sample per minute in the backfill

# Per-host seeds so the two servers don't produce identical patterns
_HOST_SEEDS = {"ns1": 1, "ns2": 2}

# ─── signal helpers ───────────────────────────────────────────────────────────

def _det_rng(ts: float, slot_s: float, seed: int) -> random.Random:
    return random.Random(int(ts / slot_s) * 2654435761 + seed)


def _dns_load(ts: float, seed: int = 0) -> float:
    """
    DNS query load, 0.15 – ~1.0.

    DNS traffic at an authoritative server is much more stable than web
    traffic: recursive resolvers cache answers, so the diurnal curve is
    shallower (~3× peak-to-trough versus 10–20× for web).

    Components:
      1. Shallow diurnal (business hours see ~20 % more queries)
      2. Weekend reduction (~20 % drop — corp DNS uses still decline)
      3. Multi-scale fractal noise (2-min → 2-hour)
      4. Rare flash events (DDoS / amplification attacks, scan storms)
    """
    hour = (ts % 86400) / 3600
    # Flatter Gaussian than web traffic
    am   = math.exp(-0.5 * ((hour - 10.0) / 3.5) ** 2)
    pm   = math.exp(-0.5 * ((hour - 15.0) / 3.0) ** 2)
    base = 0.55 + 0.25 * (am * 0.70 + pm * 0.55)  # stays between 0.55–0.80

    dow = int(ts // 86400 + 4) % 7
    if dow >= 5:
        base *= 0.82   # weekend: modest drop

    # Fractal noise — irrational period ratios avoid harmonic alignment
    base *= 1 + (
        0.06 * math.sin(ts / 127   + 1.1 + seed) +
        0.05 * math.sin(ts / 503   + 2.7 + seed) +
        0.04 * math.sin(ts / 1801  + 0.4 + seed) +
        0.03 * math.sin(ts / 7213  + 3.9 + seed)
    )

    # Rare amplification / scan storm (2–5× load for 5–30 min, ~1/day)
    r = _det_rng(ts, 86400, 100 + seed)
    if r.random() < 0.40:   # ~40 % chance per 24 h window
        w0      = int(ts / 86400) * 86400
        s_start = w0 + r.uniform(0, 86400 * 0.9)
        s_dur   = r.uniform(300, 1800)
        if s_start <= ts < s_start + s_dur:
            base *= r.uniform(2.0, 5.0)

    return max(0.10, min(5.0, base))   # allow >1.0 during storms


def _latency_us(ts: float, load: float, seed: int = 0) -> float:
    """
    Response latency in microseconds.
    Baseline: 80–250 µs (fast authoritative with hot caches).
    Under load: rises via queuing theory.
    DB slow-query events: 2000–8000 µs for minutes at a time.
    """
    util   = min(0.92, load * 0.60)
    q_mult = 1.0 / max(0.08, 1.0 - util)
    base   = (100 + 60 * math.sin(ts / 3600 + seed)) * q_mult

    # DB backend slow event
    r = _det_rng(ts, 5400, 200 + seed)
    if r.random() < 0.10:
        w0 = int(ts / 5400) * 5400
        s  = w0 + r.uniform(0, 4800)
        if s <= ts < s + r.uniform(60, 600):
            base *= r.uniform(8, 35)

    return max(20.0, base * (1 + random.uniform(-0.08, 0.08)))


def _servfail_mult(ts: float, seed: int = 0) -> float:
    """SERVFAIL rate multiplier. Usually 1×; storms 20–100×."""
    r = _det_rng(ts, 14400, 300 + seed)   # 4-hour windows
    if r.random() < 0.08:
        w0 = int(ts / 14400) * 14400
        s  = w0 + r.uniform(300, 13200)
        if s <= ts < s + r.uniform(120, 900):
            return r.uniform(20.0, 100.0)
    return 1.0


def _ddns_rate(ts: float, seed: int = 0) -> float:
    """DDNS update queries per second — near zero, occasional bursts."""
    # Rare DDNS batch (e.g., Kea DHCP server updating 100s of records)
    r = _det_rng(ts, 3600 * 12, 400 + seed)
    if r.random() < 0.15:
        w0 = int(ts / (3600 * 12)) * 3600 * 12
        s  = w0 + r.uniform(0, 3600 * 11)
        if s <= ts < s + r.uniform(60, 600):
            return r.uniform(20.0, 200.0)
    return random.uniform(0, 0.5)


def _notify_rate(ts: float, seed: int = 0) -> float:
    """Incoming NOTIFY rate — occasional zone-change bursts."""
    r = _det_rng(ts, 3600 * 6, 500 + seed)
    if r.random() < 0.25:
        w0 = int(ts / (3600 * 6)) * 3600 * 6
        s  = w0 + r.uniform(0, 3600 * 5)
        if s <= ts < s + r.uniform(30, 300):
            return r.uniform(2.0, 20.0)
    return 0.0


def jitter(v: float, pct: float = 0.05) -> float:
    return v * (1 + random.uniform(-pct, pct))


# ─── per-host metric snapshot ─────────────────────────────────────────────────

def snapshot(ts: float, host: str, step: float, acc: dict) -> list[tuple[str, float]]:
    """
    Returns a list of (metric_path, value) for a single host at timestamp ts.

    Counter metrics are cumulative (monotonically increasing).
    Gauge metrics are instantaneous.
    Timestamps are aligned to the minute boundary for Whisper compatibility.
    """
    seed = _HOST_SEEDS.get(host, 0)
    load = _dns_load(ts, seed)
    sfm  = _servfail_mult(ts, seed)

    # ── query rates (queries/s) ───────────────────────────────────────────────
    # Medium-traffic authoritative server: 8000–40000 qps peak
    qps_total = jitter(18000 * load)
    # TCP is ~1–2 % of UDP for an auth server (mostly AXFR / large responses)
    tcp_frac  = jitter(0.015, 0.30)
    udp_qps   = qps_total * (1 - tcp_frac)
    tcp_qps   = qps_total * tcp_frac

    # IPv4 vs IPv6 split — typically ~65 % IPv4, varies by network
    ipv4_frac = jitter(0.65, 0.08)
    udp4_qps  = udp_qps * ipv4_frac
    udp6_qps  = udp_qps * (1 - ipv4_frac)
    tcp4_qps  = tcp_qps * ipv4_frac
    tcp6_qps  = tcp_qps * (1 - ipv4_frac)

    # ── cache tiers ───────────────────────────────────────────────────────────
    # Packet cache: serves identical queries; hit rate 92–97 %
    pc_hit_rate  = min(0.97, jitter(0.94, 0.02))
    pc_hit_qps   = qps_total * pc_hit_rate
    pc_miss_qps  = qps_total * (1 - pc_hit_rate)

    # Query cache: answers from DB cache; hit rate 75–88 %
    qc_hit_rate  = min(0.88, jitter(0.82, 0.04))
    qc_hit_qps   = pc_miss_qps * qc_hit_rate
    qc_miss_qps  = pc_miss_qps * (1 - qc_hit_rate)

    # Answers ≈ queries (auth server almost always answers)
    udp_ans_qps  = udp_qps * jitter(0.999)
    tcp_ans_qps  = tcp_qps * jitter(0.999)

    # ── error / edge cases ────────────────────────────────────────────────────
    base_sf_rate = 0.0004   # 0.04 % SERVFAIL baseline
    sf_qps       = qps_total * base_sf_rate * sfm
    overload_qps = max(0.0, (load - 1.2) * 500 * jitter(1.0, 0.40))
    rd_qps       = qps_total * jitter(0.00008, 0.40)  # recursion desired (rare)
    udp_err_qps  = qps_total * jitter(0.00015, 0.30)

    # ── recursion metrics (tiny for an auth server) ───────────────────────────
    rec_q_qps    = rd_qps * jitter(0.90)
    rec_a_qps    = rec_q_qps * jitter(0.80)
    rec_un_qps   = rec_q_qps - rec_a_qps

    # ── DDNS ─────────────────────────────────────────────────────────────────
    ddns_r  = _ddns_rate(ts, seed)
    ddns_q  = jitter(ddns_r)
    ddns_a  = ddns_q * jitter(0.95)
    ddns_c  = ddns_a * jitter(0.70)   # changes = successful updates
    ddns_rf = ddns_q - ddns_a

    # ── NOTIFY ────────────────────────────────────────────────────────────────
    notify_qps = _notify_rate(ts, seed)

    # ── Latency (µs) ─────────────────────────────────────────────────────────
    lat = _latency_us(ts, load, seed)

    # ── gauges: queue depth ───────────────────────────────────────────────────
    qsize = max(0, int((load - 0.8) * 120 * jitter(1.0, 0.50))) if load > 0.8 else random.randint(0, 3)

    # ── gauges: cache sizes ───────────────────────────────────────────────────
    # Packet cache fills up over time, stabilises at TTL-driven equilibrium
    # Uses slow sinusoidal variation (cache entries expire and are repopulated)
    pc_size  = int(jitter(280000 + 60000 * math.sin(ts / (3600 * 6) + seed)))
    qc_size  = int(jitter(120000 + 25000 * math.sin(ts / (3600 * 8) + seed + 1.5)))
    # DNSSEC caches
    key_size = int(jitter(840  + 120 * math.sin(ts / (3600 * 24) + seed)))
    meta_size= int(jitter(1800 + 200 * math.sin(ts / (3600 * 12) + seed + 0.8)))
    # Signature cache cycles every ~TTL_SIG (typically 10–20 min)
    sig_size = int(jitter(5000 + 2000 * math.sin(ts / (3600 * 0.25) + seed)))

    # ── accumulate counters ───────────────────────────────────────────────────
    def bump(key: str, rate: float) -> float:
        acc[key] = acc.get(key, 0.0) + max(0.0, rate) * step
        return acc[key]

    prefix = f"pdns.{host}.auth"
    metrics: list[tuple[str, float]] = [
        # Counters
        (f"{prefix}.udp-queries",            bump(f"{host}.udq",    udp_qps)),
        (f"{prefix}.tcp-queries",            bump(f"{host}.tcpq",   tcp_qps)),
        (f"{prefix}.udp-answers",            bump(f"{host}.uda",    udp_ans_qps)),
        (f"{prefix}.tcp-answers",            bump(f"{host}.tcpa",   tcp_ans_qps)),
        (f"{prefix}.udp4-queries",           bump(f"{host}.udq4",   udp4_qps)),
        (f"{prefix}.udp6-queries",           bump(f"{host}.udq6",   udp6_qps)),
        (f"{prefix}.tcp4-queries",           bump(f"{host}.tcpq4",  tcp4_qps)),
        (f"{prefix}.tcp6-queries",           bump(f"{host}.tcpq6",  tcp6_qps)),
        (f"{prefix}.packetcache-hit",        bump(f"{host}.pch",    pc_hit_qps)),
        (f"{prefix}.packetcache-miss",       bump(f"{host}.pcm",    pc_miss_qps)),
        (f"{prefix}.query-cache-hit",        bump(f"{host}.qch",    qc_hit_qps)),
        (f"{prefix}.query-cache-miss",       bump(f"{host}.qcm",    qc_miss_qps)),
        (f"{prefix}.servfail-packets",       bump(f"{host}.sf",     sf_qps)),
        (f"{prefix}.overload-drops",         bump(f"{host}.ol",     overload_qps)),
        (f"{prefix}.incoming-notifications", bump(f"{host}.notif",  notify_qps)),
        (f"{prefix}.rd-queries",             bump(f"{host}.rd",     rd_qps)),
        (f"{prefix}.recursing-questions",    bump(f"{host}.recq",   rec_q_qps)),
        (f"{prefix}.recursing-answers",      bump(f"{host}.reca",   rec_a_qps)),
        (f"{prefix}.recursion-unanswered",   bump(f"{host}.recu",   max(0, rec_un_qps))),
        (f"{prefix}.dnsupdate-queries",      bump(f"{host}.duq",    ddns_q)),
        (f"{prefix}.dnsupdate-answers",      bump(f"{host}.dua",    ddns_a)),
        (f"{prefix}.dnsupdate-changes",      bump(f"{host}.duc",    ddns_c)),
        (f"{prefix}.dnsupdate-refused",      bump(f"{host}.dur",    max(0, ddns_rf))),
        (f"{prefix}.udp-in-errors",          bump(f"{host}.udpie",  udp_err_qps)),
        # Gauges
        (f"{prefix}.latency",                round(lat, 1)),
        (f"{prefix}.qsize-q",                qsize),
        (f"{prefix}.packetcache-size",       pc_size),
        (f"{prefix}.query-cache-size",       qc_size),
        (f"{prefix}.key-cache-size",         key_size),
        (f"{prefix}.meta-cache-size",        meta_size),
        (f"{prefix}.signature-cache-size",   sig_size),
    ]
    return metrics


# ─── Carbon TCP writer ────────────────────────────────────────────────────────

def carbon_send(host: str, port: int, lines: list[str]) -> None:
    """Send a batch of Carbon plaintext lines over a single TCP connection."""
    with socket.create_connection((host, port), timeout=15) as s:
        payload = "\n".join(lines) + "\n"
        s.sendall(payload.encode("ascii"))


def _wait_for_carbon(host: str, port: int, timeout: int = 120) -> None:
    deadline = time.time() + timeout
    print(f"[powerdns_generator] Waiting for Carbon at {host}:{port} …", flush=True)
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=3):
                return
        except OSError:
            pass
        time.sleep(3)
    raise SystemExit(f"[powerdns_generator] Carbon not ready after {timeout}s")


# ─── progress ─────────────────────────────────────────────────────────────────

def _progress(done: int, total: int, t0: float, width: int = 38) -> None:
    pct     = done / total
    elapsed = time.time() - t0 or 1e-9
    rate    = done / elapsed
    eta     = int((total - done) / rate) if rate > 0 else 0
    if _TTY:
        filled = int(width * pct)
        bar    = "█" * filled + "░" * (width - filled)
        print(f"\r  [{bar}] {pct*100:5.1f}%  {rate:,.0f} steps/s  ETA {eta}s   ",
              end="", flush=True)
    else:
        prev = int((done - 1) / total * 10)
        curr = int(done / total * 10)
        if curr > prev:
            print(f"  {curr*10:3d}%  {rate:,.0f} steps/s  ETA {eta}s", flush=True)


# ─── backfill ─────────────────────────────────────────────────────────────────

def backfill(carbon_host: str, carbon_port: int, days: int) -> None:
    now      = time.time()
    start_ts = now - days * 86400
    # Align to minute boundary
    start_ts = (int(start_ts) // STEP_S) * STEP_S
    total    = int((now - start_ts) / STEP_S)
    BATCH    = 2000   # Carbon lines per TCP connection

    print(f"[powerdns_generator] Backfilling {days}d ({total} steps × {len(HOSTS)} hosts) …",
          flush=True)

    acc:   dict = {}
    batch: list[str] = []
    done = 0
    t0   = time.time()

    ts = float(start_ts)
    while ts <= now:
        ts_int = int(ts)
        for host in HOSTS:
            for path, value in snapshot(ts, host, STEP_S, acc):
                batch.append(f"{path} {value:.2f} {ts_int}")

        done += 1
        _progress(done, total, t0)

        if len(batch) >= BATCH:
            try:
                carbon_send(carbon_host, carbon_port, batch)
            except Exception as exc:
                print(f"\n[powerdns_generator] Carbon error: {exc}", file=sys.stderr, flush=True)
            batch = []

        ts += STEP_S

    if batch:
        try:
            carbon_send(carbon_host, carbon_port, batch)
        except Exception as exc:
            print(f"\n[powerdns_generator] Carbon error: {exc}", file=sys.stderr, flush=True)

    if _TTY:
        print()
    print(f"[powerdns_generator] Backfill complete — {done * len(HOSTS)} snapshots written.",
          flush=True)


# ─── live loop ────────────────────────────────────────────────────────────────

def live_loop(carbon_host: str, carbon_port: int, interval: float) -> None:
    print(f"[powerdns_generator] Live mode — pushing every {interval}s …", flush=True)
    acc: dict = {}
    while True:
        # Align timestamp to interval boundary (Whisper expects aligned timestamps)
        ts  = (int(time.time()) // int(interval)) * int(interval)
        lines: list[str] = []
        for host in HOSTS:
            for path, value in snapshot(float(ts), host, interval, acc):
                lines.append(f"{path} {value:.2f} {ts}")
        try:
            carbon_send(carbon_host, carbon_port, lines)
            load_ns1 = _dns_load(float(ts), _HOST_SEEDS["ns1"])
            print(f"[powerdns_generator] pushed {len(lines)} metrics  load(ns1)={load_ns1:.2f}", flush=True)
        except Exception as exc:
            print(f"[powerdns_generator] ERROR: {exc}", file=sys.stderr, flush=True)
        time.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="PowerDNS Graphite metrics generator")
    parser.add_argument("--carbon-host", default=CARBON_HOST)
    parser.add_argument("--carbon-port", type=int, default=CARBON_PORT)
    parser.add_argument("--days",        type=int, default=BACKFILL_DAYS)
    parser.add_argument("--interval",    type=float, default=PUSH_INTERVAL)
    parser.add_argument("--no-backfill", action="store_true")
    args = parser.parse_args()

    _wait_for_carbon(args.carbon_host, args.carbon_port)

    if not args.no_backfill:
        backfill(args.carbon_host, args.carbon_port, args.days)

    live_loop(args.carbon_host, args.carbon_port, args.interval)


if __name__ == "__main__":
    main()
