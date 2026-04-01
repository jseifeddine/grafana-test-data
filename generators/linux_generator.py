#!/usr/bin/env python3
"""
Linux system metrics generator for InfluxDB (Telegraf schema).

Signal model — all functions are deterministic given ts:

  _load(ts)              — diurnal + weekly + fractal noise + incident events
  _cron_factor(ts)       — CPU / I-O multiplier from scheduled jobs (backup,
                           log-rotation, metrics-collection, weekly full-backup)
  _mem_sawtooth(ts)      — realistic heap grow → sharp GC-drop cycle
  _disk_fill(ts, host)   — slow monotonic fill with occasional scan spikes
  _net_burst(ts)         — bursty network with occasional large-transfer events
"""

import argparse
import math
import random
import sys
import time

import requests

_TTY = sys.stdout.isatty()

INFLUXDB_URL    = "http://influxdb:8086"
DATABASE        = "telegraf"
BACKFILL_DAYS   = 7
BACKFILL_STEP_S = 60
LIVE_INTERVAL_S = 30

HOSTS = [
    {
        "name":       "web-server-01",
        "cpu_count":  8,
        "mem_total":  16 * 1024**3,
        "disks": [
            {"path": "/",     "device": "sda", "total": 100 * 1024**3},
            {"path": "/data", "device": "sdb", "total": 500 * 1024**3},
        ],
        "interfaces": ["eth0", "eth1"],
        "role":       "web",
        "seed":       1,
    },
    {
        "name":       "web-server-02",
        "cpu_count":  4,
        "mem_total":   8 * 1024**3,
        "disks": [
            {"path": "/", "device": "sda", "total": 100 * 1024**3},
        ],
        "interfaces": ["eth0"],
        "role":       "db",
        "seed":       2,
    },
]

# ─── signal helpers ───────────────────────────────────────────────────────────

def _det_rng(ts: float, slot_s: float, seed: int) -> random.Random:
    return random.Random(int(ts / slot_s) * 2654435761 + seed)


def _load(ts: float, host_seed: int = 0) -> float:
    """
    Base load 0.04 – ~1.0.
    Includes diurnal curve, weekly pattern, fractal noise, and rare incidents.
    """
    hour = (ts % 86400) / 3600
    am   = math.exp(-0.5 * ((hour - 10.5) / 1.8) ** 2)
    pm   = math.exp(-0.5 * ((hour - 14.5) / 1.6) ** 2)
    base = max(0.06, am * 0.88 + pm * 0.70)

    dow = int(ts // 86400 + 4) % 7
    if dow >= 5:
        base *= 0.40
    elif dow == 4:
        base *= max(0.72, 1.0 - max(0, hour - 16) * 0.10)

    base *= 1 + (
        0.09 * math.sin(ts / 157   + 1.1 + host_seed) +
        0.07 * math.sin(ts / 523   + 2.7 + host_seed) +
        0.05 * math.sin(ts / 1801  + 0.4 + host_seed) +
        0.03 * math.sin(ts / 7207  + 3.9 + host_seed)
    )
    return max(0.03, min(1.0, base))


def _cron_factor(ts: float, host_seed: int = 0) -> dict:
    """
    Returns per-resource multipliers driven by scheduled jobs.
    Keys: cpu, iowait, disk_read, disk_write, net
    """
    hour   = (ts % 86400) / 3600
    minute = (ts % 3600)  / 60
    dow    = int(ts // 86400 + 4) % 7   # 0=Mon, 6=Sun

    cpu = iow = dr = dw = net = 1.0

    # Every 5 min: lightweight metrics collection (small CPU blip)
    if minute % 5 < 0.5:
        cpu *= 1.25
        net *= 1.10

    # 00:02 – 00:08 — log rotation (disk write + brief CPU spike)
    if 0.033 < hour < 0.133:
        cpu *= 2.0
        dw  *= 4.0
        iow *= 2.5

    # 02:00 – 03:30 — nightly incremental backup (heavy I/O, elevated CPU)
    if 2.0 < hour < 3.5:
        progress = (hour - 2.0) / 1.5   # 0→1 across the window
        # peaks mid-window then subsides
        intensity = 4.0 - 2.0 * abs(progress - 0.5) * 2
        cpu *= intensity
        dr  *= intensity * 1.5
        dw  *= intensity * 0.8
        iow *= intensity * 2.0
        net *= 1.5

    # Sunday 01:00 – 05:00 — weekly full backup (very heavy)
    if dow == 6 and 1.0 < hour < 5.0:
        progress = (hour - 1.0) / 4.0
        intensity = 5.5 - 3.0 * abs(progress - 0.5) * 2
        cpu *= intensity
        dr  *= intensity * 2.0
        dw  *= intensity * 1.2
        iow *= intensity * 3.0
        net *= 2.0

    # Random cron bursts — ~1 per hour, lasting 1-3 min
    r = _det_rng(ts, 3600, 10 + host_seed)
    if r.random() < 0.60:
        b_start = int(ts / 3600) * 3600 + r.uniform(0, 3540)
        b_dur   = r.uniform(60, 180)
        if b_start <= ts < b_start + b_dur:
            burst = r.uniform(1.4, 3.0)
            cpu  *= burst
            iow  *= burst * 0.5

    return {"cpu": cpu, "iowait": iow, "disk_read": dr, "disk_write": dw, "net": net}


def _mem_sawtooth(ts: float, period_s: float, base_ratio: float,
                  max_ratio: float, host_seed: int = 0) -> float:
    """
    Heap allocation → GC drop pattern.
      • Grows exponentially during 90 % of the cycle (mimics Java/Python heap)
      • Drops sharply in the last 10 % (GC / memory release)
      • A long-term slow leak adds up over days, reset weekly
    """
    # Slight per-host phase offset so GC events don't align
    phase_offset = host_seed * period_s * 0.37
    pos = ((ts + phase_offset) % period_s) / period_s

    if pos < 0.90:
        # Exponential growth: slow start, accelerates near GC trigger
        growth = (math.exp(pos / 0.90 * 2) - 1) / (math.e ** 2 - 1)
        ratio  = base_ratio + (max_ratio - base_ratio) * growth
    else:
        # Sharp drop: GC completed, memory released back toward base
        gc_pos = (pos - 0.90) / 0.10
        ratio  = base_ratio + (max_ratio - base_ratio) * (1 - gc_pos) ** 3

    # Slow week-long leak (+0 to +8 %) reset each Monday midnight
    week_pos   = (ts % (86400 * 7)) / (86400 * 7)
    leak_trend = 0.08 * week_pos
    return min(0.96, ratio + leak_trend)


def _disk_fill(ts: float, initial_pct: float, host_seed: int = 0) -> float:
    """
    Disk usage that:
      • Grows slowly over weeks
      • Has occasional rapid-fill events (large log dumps, data imports)
      • Is partially released by log-rotation each midnight
    """
    days_elapsed = (ts % (86400 * 30)) / 86400   # reset monthly
    base = initial_pct + days_elapsed * 0.005     # ~0.5 % per day growth

    # Periodic log purges at midnight: release ~3-5 %
    hour = (ts % 86400) / 3600
    if 0.0 < hour < 0.15:
        base -= 0.03

    # Occasional large data import (~1 per week, +5-15 %)
    r = _det_rng(ts, 86400 * 7, 20 + host_seed)
    if r.random() < 0.55:
        w0    = int(ts / (86400 * 7)) * 86400 * 7
        start = w0 + r.uniform(0, 86400 * 6)
        dur   = r.uniform(3600, 14400)
        if start <= ts < start + dur:
            progress = (ts - start) / dur
            base += 0.10 * math.sin(progress * math.pi)

    return min(0.92, max(0.08, base))


def _net_burst(ts: float, base_bps: float, host_seed: int = 0) -> float:
    """Network throughput with organic burstiness."""
    load  = _load(ts, host_seed)
    bps   = base_bps * load

    # Bursty multi-scale noise
    bps *= 1 + (
        0.15 * math.sin(ts / 37   + host_seed) +
        0.10 * math.sin(ts / 151  + host_seed * 2) +
        0.07 * math.sin(ts / 601  + host_seed * 3)
    )

    # Occasional large transfer (log ship / backup sync)
    r = _det_rng(ts, 3600, 30 + host_seed)
    if r.random() < 0.08:
        w0    = int(ts / 3600) * 3600
        start = w0 + r.uniform(0, 3500)
        dur   = r.uniform(30, 300)
        if start <= ts < start + dur:
            bps *= r.uniform(3.0, 8.0)

    return max(0.0, bps)


def jitter(v: float, pct: float = 0.05) -> float:
    return v * (1 + random.uniform(-pct, pct))


# ─── line-protocol helpers ────────────────────────────────────────────────────

def lp(measurement: str, tags: dict, fields: dict, ts_ns: int) -> str:
    tag_str   = ",".join(f"{k}={v}" for k, v in sorted(tags.items()))
    field_str = ",".join(
        f"{k}={v}i" if isinstance(v, int) else f"{k}={v:.6f}"
        for k, v in fields.items()
    )
    return f"{measurement},{tag_str} {field_str} {ts_ns}"


# ─── per-measurement generators ───────────────────────────────────────────────

def gen_system(host: dict, ts: float) -> list[str]:
    load = _load(ts, host["seed"])
    n    = host["cpu_count"]
    cron = _cron_factor(ts, host["seed"])
    return [lp("system", {"host": host["name"]}, {
        "uptime":  int(ts % (86400 * 180)),   # reset every ~6 months
        "load1":   jitter(load * n * 0.65 * cron["cpu"]),
        "load5":   jitter(load * n * 0.58 * cron["cpu"]),
        "load15":  jitter(load * n * 0.52 * cron["cpu"]),
        "n_cpus":  n,
        "n_users": random.randint(1, 5),
    }, int(ts * 1e9))]


def gen_cpu(host: dict, ts: float) -> list[str]:
    load = _load(ts, host["seed"])
    cron = _cron_factor(ts, host["seed"])
    n    = host["cpu_count"]
    cpu_demand = min(0.97, load * 0.75 * cron["cpu"])
    iow_demand = min(0.30, load * 0.04 * cron["iowait"])

    # CPU steal: usually 0, occasional hypervisor noise
    steal = 0.0
    r = _det_rng(ts, 1800, 40 + host["seed"])
    if r.random() < 0.05:
        steal = r.uniform(0.5, 4.0)

    points = []
    totals = {k: 0.0 for k in ("usage_user","usage_system","usage_iowait",
                                 "usage_irq","usage_softirq","usage_steal",
                                 "usage_nice","usage_guest","usage_guest_nice",
                                 "usage_idle")}
    for i in range(n):
        # Each core has its own variance; some cores hotter than others
        core_mult = 1.0 + 0.20 * math.sin(i * 1.618 + ts / 900)
        user   = jitter(cpu_demand * core_mult * 0.78, 0.10)
        system = jitter(cpu_demand * core_mult * 0.12, 0.08)
        iow    = jitter(iow_demand, 0.12)
        irq    = jitter(0.003)
        soft   = jitter(0.006)
        st     = jitter(steal / n, 0.20)
        nice   = jitter(0.002)
        idle   = max(0.0, 100.0 - user - system - iow - irq - soft - st - nice)
        f = {"usage_user": user, "usage_system": system, "usage_iowait": iow,
             "usage_irq": irq, "usage_softirq": soft, "usage_steal": st,
             "usage_nice": nice, "usage_guest": 0.0, "usage_guest_nice": 0.0,
             "usage_idle": idle}
        for k in totals:
            totals[k] += f[k]
        points.append(lp("cpu", {"host": host["name"], "cpu": f"cpu{i}"}, f, int(ts * 1e9)))

    total_f = {k: v / n for k, v in totals.items()}
    points.append(lp("cpu", {"host": host["name"], "cpu": "cpu-total"}, total_f, int(ts * 1e9)))
    return points


def gen_mem(host: dict, ts: float) -> list[str]:
    total = host["mem_total"]
    # Different memory profiles per role
    if host["role"] == "db":
        # Database: large buffer pool, stable high usage
        base_ratio = 0.55
        max_ratio  = 0.88
        period     = 3600 * 4   # GC every 4 hours
    else:
        # Web: typical heap pattern
        base_ratio = 0.38
        max_ratio  = 0.72
        period     = 3600 * 2   # GC every 2 hours

    used_ratio = _mem_sawtooth(ts, period, base_ratio, max_ratio, host["seed"])
    used      = int(total * used_ratio)
    cached    = int(total * jitter(0.14, 0.08))
    buffered  = int(total * jitter(0.04, 0.12))
    free      = max(0, total - used - cached - buffered)
    active    = int(used * random.uniform(0.52, 0.78))
    slab      = int(total * jitter(0.03, 0.10))

    return [lp("mem", {"host": host["name"]}, {
        "total":        total,
        "used":         used,
        "cached":       cached,
        "buffered":     buffered,
        "free":         free,
        "active":       active,
        "inactive":     max(0, used - active),
        "slab":         slab,
        "available":    max(0, free + cached),
        "used_percent": used_ratio * 100,
    }, int(ts * 1e9))]


def gen_swap(host: dict, ts: float) -> list[str]:
    total = 4 * 1024**3
    # Swap usage correlates with memory pressure
    mem_ratio = _mem_sawtooth(ts, 3600 * 2, 0.38, 0.72, host["seed"])
    # Swap starts being used when memory > 80 %
    swap_ratio = max(0, (mem_ratio - 0.80) / 0.20 * 0.15) if mem_ratio > 0.80 else 0
    used = int(total * jitter(swap_ratio, 0.15))
    # Swap I/O rate: high when swapping is occurring
    swap_io_rate = max(0.0, swap_ratio * 500)
    return [lp("swap", {"host": host["name"]}, {
        "total": total,
        "used":  used,
        "free":  total - used,
        "in":    int(jitter(swap_io_rate)),
        "out":   int(jitter(swap_io_rate * 0.4)),
    }, int(ts * 1e9))]


def gen_processes(host: dict, ts: float) -> list[str]:
    load = _load(ts, host["seed"])
    cron = _cron_factor(ts, host["seed"])
    n_cpu = host["cpu_count"]
    return [lp("processes", {"host": host["name"]}, {
        "total":    int(jitter(280 + load * 120 * cron["cpu"])),
        "running":  int(jitter(max(1, load * n_cpu * 0.35 * cron["cpu"]))),
        "sleeping": int(jitter(250 + load * 80)),
        "blocked":  int(jitter(max(0, (load - 0.6) * 12 * cron["iowait"]))),
        "stopped":  random.randint(0, 1),
        "zombies":  random.choices([0, 1, 2], weights=[0.85, 0.12, 0.03])[0],
        "paging":   1 if _mem_sawtooth(ts, 3600*2, 0.38, 0.72, host["seed"]) > 0.85 else 0,
        "unknown":  0,
    }, int(ts * 1e9))]


def gen_kernel(host: dict, ts: float) -> list[str]:
    load = _load(ts, host["seed"])
    cron = _cron_factor(ts, host["seed"])
    # Context switches spike under load and during cron bursts
    ctx_base = 40000 + load * 180000 * cron["cpu"]
    # Interrupt storms during network activity
    irq_base = 25000 + load * 70000
    return [lp("kernel", {"host": host["name"]}, {
        "context_switches": int(jitter(ctx_base, 0.12)),
        "interrupts":       int(jitter(irq_base, 0.10)),
        "processes_forked": int(jitter(800 + load * 6000 * cron["cpu"])),
        "disk_pages_in":    int(jitter(150 + load * 2500 * cron["disk_read"])),
        "disk_pages_out":   int(jitter(80  + load * 1500 * cron["disk_write"])),
    }, int(ts * 1e9))]


def gen_disk(host: dict, ts: float) -> list[str]:
    points = []
    for disk in host["disks"]:
        initial = 0.22 if disk["path"] == "/" else 0.35
        fill    = _disk_fill(ts, initial, host["seed"])
        total   = disk["total"]
        used    = int(total * fill)
        free    = total - used
        inodes_total = 6553600
        inodes_used  = int(inodes_total * jitter(fill * 0.28, 0.06))
        points.append(lp("disk",
            {"host": host["name"], "path": disk["path"],
             "device": disk["device"], "fstype": "ext4"},
            {"total": total, "used": used, "free": max(0, free),
             "used_percent": fill * 100,
             "inodes_total": inodes_total, "inodes_used": inodes_used,
             "inodes_free": inodes_total - inodes_used},
            int(ts * 1e9)))
    return points


def gen_diskio(host: dict, ts: float) -> list[str]:
    cron  = _cron_factor(ts, host["seed"])
    load  = _load(ts, host["seed"])
    points = []
    for disk in host["disks"]:
        dev = disk["device"]
        # db role does heavier sustained I/O on the data disk
        role_mult = 3.5 if (host["role"] == "db" and dev == "sda") else 1.0
        reads  = int(jitter(load * 180  * cron["disk_read"]  * role_mult))
        writes = int(jitter(load * 130  * cron["disk_write"] * role_mult))
        rb     = int(jitter(load * 7e6  * cron["disk_read"]  * role_mult))
        wb     = int(jitter(load * 5e6  * cron["disk_write"] * role_mult))
        rt     = int(jitter(load * 450  * cron["iowait"]     * role_mult))
        wt     = int(jitter(load * 280  * cron["iowait"]     * role_mult))
        iops   = int(jitter(max(0, load * 8 * cron["iowait"] * role_mult)))
        points.append(lp("diskio",
            {"host": host["name"], "name": dev},
            {"reads": reads, "writes": writes,
             "read_bytes": rb, "write_bytes": wb,
             "read_time": rt, "write_time": wt,
             "io_time": rt + wt,
             "iops_in_progress": iops},
            int(ts * 1e9)))
    return points


def gen_net(host: dict, ts: float) -> list[str]:
    cron    = _cron_factor(ts, host["seed"])
    bps_base = {"web": 80e6, "db": 40e6}.get(host["role"], 40e6)
    points  = []
    for iface in host["interfaces"]:
        factor  = 0.85 if iface == "eth0" else 0.15
        bps_in  = _net_burst(ts, bps_base * factor * cron["net"], host["seed"])
        bps_out = _net_burst(ts, bps_base * factor * 0.75 * cron["net"], host["seed"] + 100)
        load    = _load(ts, host["seed"])
        # Errors and drops spike during network events
        err_rate = jitter(load * 2.5 * (1 + 0.5 * math.sin(ts / 3600)))
        drop_rate = max(0.0, (load - 0.80) * 10)
        points.append(lp("net",
            {"host": host["name"], "interface": iface},
            {"bytes_recv":   int(bps_in),
             "bytes_sent":   int(bps_out),
             "packets_recv": int(bps_in  / 1400),
             "packets_sent": int(bps_out / 1400),
             "err_in":       int(err_rate),
             "err_out":      int(err_rate * 0.3),
             "drop_in":      int(drop_rate),
             "drop_out":     0,
             "tcp_estabresets": int(jitter(load * 6))},
            int(ts * 1e9)))
    return points


def gen_netstat(host: dict, ts: float) -> list[str]:
    load = _load(ts, host["seed"])
    # ESTABLISHED count is higher for db role (connection pool)
    base_est = 350 if host["role"] == "db" else 180
    est      = int(jitter(base_est + load * 1200, 0.08))
    tw       = int(jitter(load * 120 + 30))  # TIME_WAIT spikes with load
    return [lp("netstat", {"host": host["name"]}, {
        "tcp_established": est,
        "tcp_syn_sent":    int(jitter(load * 22)),
        "tcp_syn_recv":    int(jitter(load * 35)),
        "tcp_fin_wait1":   int(jitter(load * 6)),
        "tcp_fin_wait2":   int(jitter(load * 12)),
        "tcp_time_wait":   tw,
        "tcp_close":       int(jitter(load * 3)),
        "tcp_close_wait":  int(jitter(load * 18 + 5)),
        "tcp_last_ack":    int(jitter(load * 4)),
        "tcp_listen":      12,
        "tcp_closing":     int(jitter(load * 1.5)),
        "tcp_none":        0,
        "udp_socket":      int(jitter(22)),
    }, int(ts * 1e9))]


def gen_linux_sysctl_fs(host: dict, ts: float) -> list[str]:
    load = _load(ts, host["seed"])
    cron = _cron_factor(ts, host["seed"])
    file_max = 1048576
    file_nr  = int(jitter(4000 + load * 55000 * cron["cpu"]))
    return [lp("linux_sysctl_fs", {"host": host["name"]}, {
        "file-max":           file_max,
        "file-nr":            file_nr,
        "file-allocated":     file_nr,
        "inode-nr":           int(jitter(180000 + load * 120000)),
        "inode-free-nr":      int(jitter(130000)),
        "inode-preshrink-nr": 0,
        "dentry-nr":          int(jitter(450000)),
        "dentry-unused-nr":   int(jitter(80000)),
        "dentry-age-limit":   45,
        "dentry-want-pages":  0,
    }, int(ts * 1e9))]


def gen_conntrack(host: dict, ts: float) -> list[str]:
    load = _load(ts, host["seed"])
    cron = _cron_factor(ts, host["seed"])
    base = 1800 if host["role"] == "db" else 1200
    return [lp("conntrack", {"host": host["name"]}, {
        "ip_conntrack_count": int(jitter(base + load * 12000 * cron["net"])),
        "ip_conntrack_max":   65536,
    }, int(ts * 1e9))]


ALL_GENERATORS = [
    gen_system, gen_cpu, gen_mem, gen_swap, gen_processes,
    gen_kernel, gen_disk, gen_diskio, gen_net, gen_netstat,
    gen_linux_sysctl_fs, gen_conntrack,
]


def generate_all(host: dict, ts: float) -> list[str]:
    pts: list[str] = []
    for g in ALL_GENERATORS:
        pts.extend(g(host, ts))
    return pts


# ─── InfluxDB I/O ─────────────────────────────────────────────────────────────

def write_points(points: list[str], url: str, db: str) -> None:
    if not points:
        return
    body = "\n".join(points).encode("utf-8")
    r = requests.post(f"{url}/write?db={db}&precision=ns",
                      data=body,
                      headers={"Content-Type": "application/octet-stream"},
                      timeout=30)
    r.raise_for_status()


def _wait_for_influxdb(url: str, timeout: int = 120) -> None:
    deadline = time.time() + timeout
    print(f"[linux_generator] Waiting for InfluxDB at {url} …", flush=True)
    while time.time() < deadline:
        try:
            if requests.get(f"{url}/ping", timeout=3).ok:
                return
        except Exception:
            pass
        time.sleep(3)
    raise SystemExit(f"[linux_generator] InfluxDB not ready after {timeout}s")


def ensure_database(url: str, db: str) -> None:
    _wait_for_influxdb(url)
    r = requests.post(f"{url}/query", data={"q": f"CREATE DATABASE {db}"}, timeout=10)
    r.raise_for_status()


# ─── progress ─────────────────────────────────────────────────────────────────

def _progress(done: int, total: int, start: float, width: int = 38) -> None:
    pct     = done / total
    elapsed = time.time() - start or 1e-9
    rate    = done / elapsed
    eta     = int((total - done) / rate) if rate > 0 else 0
    if _TTY:
        filled = int(width * pct)
        bar    = "█" * filled + "░" * (width - filled)
        print(f"\r  [{bar}] {pct*100:5.1f}%  {rate:,.0f} pts/s  ETA {eta}s   ",
              end="", flush=True)
    else:
        prev = int((done - 1) / total * 10)
        curr = int(done / total * 10)
        if curr > prev:
            print(f"  {curr*10:3d}%  {rate:,.0f} pts/s  ETA {eta}s", flush=True)


# ─── backfill ─────────────────────────────────────────────────────────────────

def backfill(url: str, db: str, days: int) -> None:
    now      = time.time()
    start_ts = now - days * 86400
    total    = int((now - start_ts) / BACKFILL_STEP_S)
    BATCH    = 600

    print(f"[linux_generator] Backfilling {days}d ({total} steps × {len(HOSTS)} hosts) …",
          flush=True)

    batch: list[str] = []
    written = 0
    t0      = time.time()
    done    = 0

    ts = start_ts
    while ts <= now:
        for host in HOSTS:
            batch.extend(generate_all(host, ts))
        done += 1
        _progress(done, total, t0)

        if len(batch) >= BATCH:
            write_points(batch, url, db)
            written += len(batch)
            batch = []
        ts += BACKFILL_STEP_S

    if batch:
        write_points(batch, url, db)
        written += len(batch)

    if _TTY:
        print()
    print(f"[linux_generator] Backfill complete — {written:,} points written.", flush=True)


# ─── live loop ────────────────────────────────────────────────────────────────

def live_loop(url: str, db: str, interval: float) -> None:
    print(f"[linux_generator] Live mode — writing every {interval}s …", flush=True)
    while True:
        ts     = time.time()
        points = []
        for host in HOSTS:
            points.extend(generate_all(host, ts))
        try:
            write_points(points, url, db)
            print(f"[linux_generator] wrote {len(points)} pts  load={_load(ts, 1):.2f}", flush=True)
        except Exception as exc:
            print(f"[linux_generator] ERROR: {exc}", file=sys.stderr, flush=True)
        time.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--influxdb",    default=INFLUXDB_URL)
    parser.add_argument("--database",    default=DATABASE)
    parser.add_argument("--days",        type=int,   default=BACKFILL_DAYS)
    parser.add_argument("--interval",    type=float, default=LIVE_INTERVAL_S)
    parser.add_argument("--no-backfill", action="store_true")
    args = parser.parse_args()

    print(f"[linux_generator] target={args.influxdb}/{args.database}", flush=True)
    ensure_database(args.influxdb, args.database)

    if not args.no_backfill:
        backfill(args.influxdb, args.database, args.days)

    live_loop(args.influxdb, args.database, args.interval)


if __name__ == "__main__":
    main()
