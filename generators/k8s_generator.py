#!/usr/bin/env python3
"""
Kubernetes metrics generator.

Simulates a realistic multi-node K8s cluster pushing kube-state-metrics
and cAdvisor-style Prometheus metrics to Pushgateway.

Signal model:
  _load(ts)              — shared traffic load for the whole cluster
  _workload_cpu(ts, wl)  — per-workload CPU pattern (api/batch/db/cache/monitor)
  _workload_mem(ts, wl)  — per-workload memory pattern (stable/sawtooth/growing)
  _oom_event(ts, wl)     — occasional OOM → pod restarts
  _rolling_update(ts,wl) — periodic deploys: brief pod-count dip then recovery
  _node_pressure(ts, n)  — per-node memory/CPU pressure events
"""

import argparse
import math
import random
import sys
import time
from dataclasses import dataclass, field

import requests

PUSHGATEWAY_URL = "http://pushgateway:9091"
PUSH_INTERVAL   = 15
ORIGIN          = "cluster-prod"

# ─── cluster topology ────────────────────────────────────────────────────────

NODES = [
    {"name": "node-01", "cpu_cores": 8,  "memory_bytes": 32 * 1024**3, "zone": "us-east-1a"},
    {"name": "node-02", "cpu_cores": 8,  "memory_bytes": 32 * 1024**3, "zone": "us-east-1b"},
    {"name": "node-03", "cpu_cores": 16, "memory_bytes": 64 * 1024**3, "zone": "us-east-1c"},
]

# fmt: off
# (namespace, kind, name, replicas, cpu_req, cpu_lim, mem_req_mb, mem_lim_mb, image, wl_type, mem_pattern)
WORKLOADS = [
    # prod — user-facing services
    ("prod",       "Deployment",  "api-server",     3, 0.25, 1.0,  256,  512,  "myapp/api-server:2.1.4",          "api",     "sawtooth"),
    ("prod",       "Deployment",  "web-frontend",   2, 0.10, 0.5,  128,  256,  "myapp/web-frontend:3.0.1",        "api",     "stable"),
    ("prod",       "Deployment",  "auth-service",   2, 0.20, 0.8,  192,  384,  "myapp/auth-service:1.8.2",        "api",     "stable"),
    ("prod",       "Deployment",  "billing-worker", 1, 0.50, 2.0,  512, 1024,  "myapp/billing-worker:1.2.0",      "batch",   "sawtooth"),
    ("prod",       "StatefulSet", "postgres",       1, 1.00, 4.0, 2048, 4096,  "postgres:15.4",                   "db",      "growing"),
    ("prod",       "StatefulSet", "redis",          2, 0.25, 1.0,  512, 1024,  "redis:7.2",                       "cache",   "stable_high"),
    # staging — mirrors prod but smaller
    ("staging",    "Deployment",  "api-server",     1, 0.10, 0.5,  128,  256,  "myapp/api-server:2.2.0-rc1",      "api",     "stable"),
    ("staging",    "Deployment",  "web-frontend",   1, 0.05, 0.25,  64,  128,  "myapp/web-frontend:3.1.0-rc2",    "api",     "stable"),
    # monitoring
    ("monitoring", "Deployment",  "prometheus",     1, 0.50, 2.0, 2048, 4096,  "prom/prometheus:v2.48.0",         "monitor", "sawtooth"),
    ("monitoring", "Deployment",  "grafana",        1, 0.10, 0.5,  256,  512,  "grafana/grafana:10.2.2",          "api",     "stable"),
    ("monitoring", "Deployment",  "alertmanager",   1, 0.05, 0.25,  64,  128,  "prom/alertmanager:v0.26.0",       "daemon",  "stable"),
    ("monitoring", "DaemonSet",   "node-exporter",  3, 0.05, 0.20,  32,   64,  "prom/node-exporter:v1.7.0",       "daemon",  "stable"),
    # data plane
    ("data",       "StatefulSet", "kafka",          3, 1.00, 4.0, 4096, 8192,  "confluentinc/cp-kafka:7.5.0",     "queue",   "growing"),
    ("data",       "StatefulSet", "zookeeper",      3, 0.25, 1.0,  512, 1024,  "confluentinc/cp-zookeeper:7.5.0", "db",      "stable"),
    ("data",       "Deployment",  "schema-registry",1, 0.10, 0.5,  256,  512,  "confluentinc/cp-schema-registry:7.5.0", "api", "stable"),
    # kube-system
    ("kube-system","DaemonSet",   "kube-proxy",     3, 0.02, 0.10,  32,   64,  "registry.k8s.io/kube-proxy:v1.28.3",   "daemon", "stable"),
    ("kube-system","Deployment",  "coredns",        2, 0.05, 0.20,  64,  128,  "registry.k8s.io/coredns/coredns:v1.11.1", "dns",  "stable"),
    ("kube-system","Deployment",  "metrics-server", 1, 0.03, 0.10,  64,  128,  "registry.k8s.io/metrics-server:v0.7.0",   "daemon","stable"),
]
# fmt: on

# ─── signal helpers ───────────────────────────────────────────────────────────

def _det_rng(ts: float, slot_s: float, seed: int) -> random.Random:
    return random.Random(int(ts / slot_s) * 2654435761 + seed)


def _load(ts: float) -> float:
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
        0.10 * math.sin(ts / 157  + 1.1) +
        0.07 * math.sin(ts / 523  + 2.7) +
        0.05 * math.sin(ts / 1801 + 0.4) +
        0.03 * math.sin(ts / 7207 + 3.9)
    )
    return max(0.03, min(1.0, base))


def _workload_cpu(ts: float, ns: str, name: str, wl_type: str,
                  cpu_lim: float, replicas: int) -> float:
    """
    CPU usage (cores).  Returns total across all replicas.
    Pattern differs by workload type:
      api    — scales linearly with traffic, high variance
      batch  — idle most of the time, periodic burst to 80-95 % of limit
      db     — relatively steady, correlates weakly with traffic
      cache  — very steady, low CPU
      queue  — scales with throughput, bursty
      monitor— step function (scrape cycles)
      daemon — near-constant low usage
      dns    — correlates with traffic, brief query spikes
    """
    load  = _load(ts)
    seed  = hash(f"{ns}/{name}") & 0x7FFFFFFF
    noise = (
        0.08 * math.sin(ts / 97  + seed) +
        0.05 * math.sin(ts / 307 + seed * 0.3)
    )

    if wl_type == "api":
        # Scales with traffic; high variance; occasional CPU spikes (GC, JIT)
        usage_ratio = load * 0.70 * (1 + noise)
        # Occasional GC pause spike
        r = _det_rng(ts, 1800, seed)
        if r.random() < 0.08:
            w0 = int(ts / 1800) * 1800
            s  = w0 + r.uniform(0, 1600)
            if s <= ts < s + r.uniform(30, 90):
                usage_ratio = min(0.98, usage_ratio * r.uniform(2, 4))

    elif wl_type == "batch":
        # Idle → burst → idle pattern (~every 2h a job runs for 5-15 min)
        r = _det_rng(ts, 7200, seed)
        if r.random() < 0.65:
            w0    = int(ts / 7200) * 7200
            start = w0 + r.uniform(600, 6600)
            dur   = r.uniform(300, 900)
            if start <= ts < start + dur:
                usage_ratio = r.uniform(0.75, 0.95)
            else:
                usage_ratio = random.uniform(0.01, 0.04)
        else:
            usage_ratio = random.uniform(0.01, 0.04)

    elif wl_type == "db":
        # Steady but rises with query load (correlates with traffic)
        usage_ratio = 0.35 + load * 0.30 * (1 + noise * 0.5)
        # Occasional vacuum/analyze
        r = _det_rng(ts, 3600 * 6, seed)
        if r.random() < 0.20:
            w0 = int(ts / (3600 * 6)) * 3600 * 6
            s  = w0 + r.uniform(0, 3600 * 5)
            if s <= ts < s + r.uniform(120, 600):
                usage_ratio = min(0.90, usage_ratio * r.uniform(2.0, 3.5))

    elif wl_type == "queue":
        usage_ratio = 0.25 + load * 0.45 * (1 + noise)

    elif wl_type == "monitor":
        # Prometheus-style: periodic scrape spikes
        scrape_cycle = ts % 60   # 60s scrape interval
        if scrape_cycle < 5:     # busy for ~5 s each scrape
            usage_ratio = 0.55 + noise * 0.10
        else:
            usage_ratio = 0.08 + noise * 0.03

    elif wl_type == "dns":
        usage_ratio = load * 0.25 * (1 + noise * 0.8)

    elif wl_type == "cache":
        usage_ratio = 0.06 + noise * 0.02

    else:  # daemon
        usage_ratio = 0.03 + noise * 0.01

    return max(0.001, usage_ratio * cpu_lim * replicas)


def _workload_mem(ts: float, ns: str, name: str,
                  mem_pattern: str, mem_lim_mb: float, replicas: int) -> float:
    """
    Memory usage in bytes, total across replicas.
    Patterns: stable, stable_high, sawtooth, growing
    """
    seed = hash(f"{ns}/{name}/mem") & 0x7FFFFFFF
    lim  = mem_lim_mb * 1024 ** 2

    if mem_pattern == "stable":
        ratio = 0.42 + 0.12 * math.sin(ts / (3600 * 3) + seed)
        ratio += random.uniform(-0.03, 0.03)

    elif mem_pattern == "stable_high":
        # Redis: fills up, then stays there
        ratio = 0.68 + 0.08 * math.sin(ts / (3600 * 6) + seed)
        ratio += random.uniform(-0.02, 0.02)

    elif mem_pattern == "sawtooth":
        # Heap allocate → GC → repeat
        period = 3600 * 2 + seed % 3600   # 2-3h period, per-workload
        phase_offset = seed * 0.37 * period
        pos = ((ts + phase_offset) % period) / period
        if pos < 0.88:
            growth = (math.exp(pos / 0.88 * 2.2) - 1) / (math.e ** 2.2 - 1)
            ratio  = 0.30 + 0.55 * growth
        else:
            gc_pos = (pos - 0.88) / 0.12
            ratio  = 0.30 + 0.55 * (1 - gc_pos) ** 3
        ratio += random.uniform(-0.02, 0.02)

        # Prometheus specifically grows over weeks (TSDB blocks)
        if name == "prometheus":
            week_pos = (ts % (86400 * 7)) / (86400 * 7)
            ratio += 0.12 * week_pos

    elif mem_pattern == "growing":
        # Slow leak: grows over a 30-day window, then drops (restart/release)
        day_pos = (ts % (86400 * 30)) / (86400 * 30)
        ratio   = 0.45 + 0.40 * day_pos
        ratio  += 0.05 * math.sin(ts / (3600 * 4) + seed)
        ratio  += random.uniform(-0.02, 0.02)

    else:
        ratio = 0.40

    return max(0.05, min(0.97, ratio)) * lim * replicas


def _oom_event(ts: float, ns: str, name: str) -> bool:
    """Returns True if this workload is in an OOM-restart state right now."""
    seed  = hash(f"{ns}/{name}/oom") & 0x7FFFFFFF
    r     = _det_rng(ts, 86400 * 3, seed)     # 3-day windows
    if r.random() > 0.20:                      # 20 % chance of OOM in window
        return False
    w0    = int(ts / (86400 * 3)) * 86400 * 3
    start = w0 + r.uniform(0, 86400 * 2.8)
    dur   = r.uniform(30, 120)                 # restarting for 30-120 s
    return start <= ts < start + dur


def _rolling_update(ts: float, ns: str, name: str, replicas: int) -> int:
    """
    Returns current ready-replica count, which dips during deployments.
    ~1 deploy per 3 days per workload.
    """
    if replicas <= 1:
        return replicas
    seed  = hash(f"{ns}/{name}/deploy") & 0x7FFFFFFF
    r     = _det_rng(ts, 86400 * 3, seed)
    if r.random() > 0.55:
        return replicas
    w0    = int(ts / (86400 * 3)) * 86400 * 3
    start = w0 + r.uniform(0, 86400 * 2.5)
    dur   = r.uniform(60, 300)  # rolling update takes 1-5 min
    if start <= ts < start + dur:
        progress = (ts - start) / dur
        # Dips to 50 % midway through, recovers to full at end
        dip_factor = 1.0 - 0.5 * math.sin(progress * math.pi)
        return max(1, round(replicas * dip_factor))
    return replicas


def _node_pressure_mult(ts: float, node_name: str) -> float:
    """
    Memory pressure events on individual nodes (1.0 = normal, >1 = stressed).
    Used to exaggerate per-node usage metrics.
    """
    seed = hash(node_name) & 0x7FFFFFFF
    r    = _det_rng(ts, 3600 * 8, seed)
    if r.random() < 0.12:
        w0    = int(ts / (3600 * 8)) * 3600 * 8
        start = w0 + r.uniform(0, 3600 * 7)
        dur   = r.uniform(600, 3600)
        if start <= ts < start + dur:
            return r.uniform(1.15, 1.40)
    return 1.0


def _net_rate(ts: float, ns: str, name: str, wl_type: str) -> tuple[float, float]:
    """Returns (rx_bytes/s, tx_bytes/s) for the workload."""
    load = _load(ts)
    seed = hash(f"{ns}/{name}/net") & 0x7FFFFFFF
    noise = 1 + 0.15 * math.sin(ts / 73 + seed) + 0.08 * math.sin(ts / 311 + seed)

    base = {
        "api":     (200e3, 800e3),
        "db":      (300e3, 200e3),
        "cache":   (150e3, 300e3),
        "queue":   (400e3, 400e3),
        "batch":   (50e3,  50e3),
        "monitor": (80e3,  20e3),
        "daemon":  (5e3,   5e3),
        "dns":     (20e3,  20e3),
    }.get(wl_type, (50e3, 50e3))

    return (base[0] * load * noise, base[1] * load * noise)


# ─── pod naming (must match prometheus_backfill.py exactly) ─────────────────

def _pod_name(ns: str, name: str, idx: int, kind: str) -> str:
    """
    Returns a globally unique pod name that is consistent across the live
    generator and the backfill.

    StatefulSets keep their ordinal names (postgres-0, redis-0, …) because
    they are naturally unique.  DaemonSet pods embed the node name.
    Deployment pods embed a 5-digit namespace hash so staging/api-server-0
    and prod/api-server-0 never share the same pod label value.
    """
    if kind == "StatefulSet":
        return f"{name}-{idx}"
    if kind == "DaemonSet":
        return f"{name}-{NODES[idx % len(NODES)]['name'].replace('-', '')}"
    ns_tag = f"{abs(hash(ns)) % 99999:05d}"
    return f"{name}-{ns_tag}-{idx}"


# ─── state accumulators ───────────────────────────────────────────────────────

_net_rx_acc:     dict[str, float] = {}
_net_tx_acc:     dict[str, float] = {}
_cpu_acc:        dict[str, float] = {}   # container_cpu_usage_seconds_total
_throttle_acc:   dict[str, float] = {}   # container_cpu_cfs_throttled_seconds_total
_node_cpu_acc:   dict[str, float] = {}   # node_cpu_usage_seconds_total
_restart_counts: dict[str, int]   = {}


def _net_acc(key: str, delta_rx: float, delta_tx: float, step: float) -> None:
    _net_rx_acc[key] = _net_rx_acc.get(key, 0.0) + delta_rx * step
    _net_tx_acc[key] = _net_tx_acc.get(key, 0.0) + delta_tx * step


_start_time = time.time()


# ─── metric builder ───────────────────────────────────────────────────────────

def build_metrics(step: float) -> str:
    ts   = time.time()
    load = _load(ts)
    lines: list[str] = []

    lbl = f'origin_prometheus="{ORIGIN}"'   # cluster-wide label

    # ── node metrics ─────────────────────────────────────────────────────────

    for node in NODES:
        nn   = node["name"]
        zone = node["zone"]
        nm   = f'{lbl},node="{nn}",zone="{zone}"'

        pressure = _node_pressure_mult(ts, nn)
        # Allocatable is slightly less than capacity (system reserved)
        alloc_cpu = node["cpu_cores"] * 0.94
        alloc_mem = node["memory_bytes"] * 0.92

        # Used = sum of all pods' requests on this node (approximate)
        node_idx    = NODES.index(node)
        pods_on_node = [w for i, w in enumerate(WORKLOADS) if i % len(NODES) == node_idx]
        cpu_used  = sum(_workload_cpu(ts, w[0], w[2], w[9], w[5], w[3]) for w in pods_on_node)
        mem_used  = sum(_workload_mem(ts, w[0], w[2], w[10], w[7], w[3]) for w in pods_on_node)
        cpu_used *= pressure
        mem_used *= pressure

        lines += [
            f'kube_node_info{{{nm},kernel_version="5.15.0-88-generic",os_image="Ubuntu 22.04.3 LTS",container_runtime_version="containerd://1.7.3",kubelet_version="v1.28.3"}} 1',
            f'kube_node_status_capacity{{node="{nn}",{lbl},resource="cpu",unit="core"}} {node["cpu_cores"]}',
            f'kube_node_status_capacity{{node="{nn}",{lbl},resource="memory",unit="byte"}} {node["memory_bytes"]}',
            f'kube_node_status_capacity{{node="{nn}",{lbl},resource="pods",unit="integer"}} 110',
            f'kube_node_status_allocatable{{node="{nn}",{lbl},resource="cpu",unit="core"}} {alloc_cpu:.2f}',
            f'kube_node_status_allocatable{{node="{nn}",{lbl},resource="memory",unit="byte"}} {int(alloc_mem)}',
            f'kube_node_status_allocatable{{node="{nn}",{lbl},resource="pods",unit="integer"}} 110',
            f'kube_node_status_condition{{node="{nn}",{lbl},condition="Ready",status="true"}} 1',
            f'kube_node_status_condition{{node="{nn}",{lbl},condition="MemoryPressure",status="true"}} {1 if pressure > 1.20 else 0}',
            f'kube_node_status_condition{{node="{nn}",{lbl},condition="DiskPressure",status="false"}} 0',
            f'kube_node_status_condition{{node="{nn}",{lbl},condition="PIDPressure",status="false"}} 0',
        ]

        # cAdvisor node-level resource usage
        cpu_usage  = min(alloc_cpu * 0.97, cpu_used)
        mem_usage  = min(int(alloc_mem * 0.97), int(mem_used))
        # Accumulate CPU seconds (never use ts directly — that causes a
        # massive apparent rate-spike where backfill and live data join).
        _node_cpu_acc[nn] = _node_cpu_acc.get(nn, 0.0) + cpu_usage * step
        lines += [
            f'node_cpu_usage_seconds_total{{node="{nn}",{lbl}}} {_node_cpu_acc[nn]:.1f}',
            f'node_memory_working_set_bytes{{node="{nn}",{lbl}}} {mem_usage}',
            f'node_memory_available_bytes{{node="{nn}",{lbl}}} {max(0, int(alloc_mem) - mem_usage)}',
        ]

    # ── workload / pod metrics ────────────────────────────────────────────────

    for (ns, kind, name, replicas, cpu_req, cpu_lim,
         mem_req_mb, mem_lim_mb, image, wl_type, mem_pattern) in WORKLOADS:

        ready_reps  = _rolling_update(ts, ns, name, replicas)
        is_oom      = _oom_event(ts, ns, name)
        if is_oom:
            k = f"{ns}/{name}"
            _restart_counts[k] = _restart_counts.get(k, 0) + 1
            ready_reps = max(0, ready_reps - 1)

        cpu_total   = _workload_cpu(ts, ns, name, wl_type, cpu_lim, ready_reps)
        mem_total   = _workload_mem(ts, ns, name, mem_pattern, mem_lim_mb, ready_reps)
        rx, tx      = _net_rate(ts, ns, name, wl_type)

        wl_labels  = f'namespace="{ns}",{lbl}'
        rep0_pname = _pod_name(ns, name, 0, kind)
        pod_labels = f'{wl_labels},pod="{rep0_pname}"'

        lines += [
            # kube-state-metrics: workload info
            f'kube_{kind.lower()}_created{{{wl_labels},{kind.lower()}="{name}"}} {_start_time - 86400 * 30:.0f}',
            f'kube_{kind.lower()}_status_replicas{{{wl_labels},{kind.lower()}="{name}"}} {replicas}',
            f'kube_{kind.lower()}_status_replicas_available{{{wl_labels},{kind.lower()}="{name}"}} {ready_reps}',
            f'kube_{kind.lower()}_status_replicas_ready{{{wl_labels},{kind.lower()}="{name}"}} {ready_reps}',
            f'kube_{kind.lower()}_status_replicas_updated{{{wl_labels},{kind.lower()}="{name}"}} {replicas}',
            f'kube_{kind.lower()}_spec_replicas{{{wl_labels},{kind.lower()}="{name}"}} {replicas}',
            # resource requests/limits against the first replica's pod name
            f'kube_pod_container_resource_requests{{{pod_labels},container="{name}",resource="cpu",unit="core"}} {cpu_req * replicas}',
            f'kube_pod_container_resource_requests{{{pod_labels},container="{name}",resource="memory",unit="byte"}} {mem_req_mb * 1024**2 * replicas}',
            f'kube_pod_container_resource_limits{{{pod_labels},container="{name}",resource="cpu",unit="core"}} {cpu_lim * replicas}',
            f'kube_pod_container_resource_limits{{{pod_labels},container="{name}",resource="memory",unit="byte"}} {mem_lim_mb * 1024**2 * replicas}',
        ]

        # Pod-level status
        for i in range(replicas):
            pname     = _pod_name(ns, name, i, kind)
            is_ready  = i < ready_reps and not (is_oom and i == 0)
            restarts  = _restart_counts.get(f"{ns}/{name}", 0) + (1 if is_oom and i == 0 else 0)
            node_idx  = hash(pname) % len(NODES)
            node_name = NODES[node_idx]["name"]

            lines += [
                f'kube_pod_info{{{wl_labels},pod="{pname}",node="{node_name}",created_by_kind="{kind}",created_by_name="{name}"}} 1',
                f'kube_pod_status_ready{{{wl_labels},pod="{pname}",condition="Ready"}} {1 if is_ready else 0}',
                f'kube_pod_status_phase{{{wl_labels},pod="{pname}",phase="Running"}} {1 if is_ready else 0}',
                f'kube_pod_status_phase{{{wl_labels},pod="{pname}",phase="Pending"}} {1 if not is_ready else 0}',
                f'kube_pod_container_status_running{{{wl_labels},pod="{pname}",container="{name}"}} {1 if is_ready else 0}',
                f'kube_pod_container_status_restarts_total{{{wl_labels},pod="{pname}",container="{name}"}} {restarts}',
                f'kube_pod_container_status_ready{{{wl_labels},pod="{pname}",container="{name}"}} {1 if is_ready else 0}',
            ]

        # cAdvisor container resource usage (per pod, aggregated to representative pod)
        per_pod_cpu = cpu_total / max(1, ready_reps)
        per_pod_mem = mem_total / max(1, ready_reps)
        uptime      = ts - _start_time
        for i in range(replicas):
            pname    = _pod_name(ns, name, i, kind)
            variance = 1 + 0.08 * math.sin(ts / 301 + i * 2.7)
            p_cpu    = per_pod_cpu * variance
            p_mem    = int(per_pod_mem * variance)
            throttle_ratio = min(0.60, max(0.0, (p_cpu / cpu_lim - 0.70) * 2.0)) if cpu_lim > 0 else 0

            acc_key = f"{ns}/{name}/{i}"
            _cpu_acc[acc_key]      = _cpu_acc.get(acc_key, 0.0)      + p_cpu              * step
            _throttle_acc[acc_key] = _throttle_acc.get(acc_key, 0.0) + throttle_ratio * p_cpu * step
            _net_acc(acc_key, rx / max(1, replicas), tx / max(1, replicas), step)

            cfs_periods   = int(uptime / 0.1)
            cfs_throttled = int(throttle_ratio * cfs_periods)

            lines += [
                f'container_cpu_usage_seconds_total{{{wl_labels},pod="{pname}",container="{name}",image="{image}"}} {_cpu_acc[acc_key]:.2f}',
                f'container_cpu_cfs_throttled_seconds_total{{{wl_labels},pod="{pname}",container="{name}"}} {_throttle_acc[acc_key]:.3f}',
                f'container_cpu_cfs_periods_total{{{wl_labels},pod="{pname}",container="{name}"}} {cfs_periods}',
                f'container_cpu_cfs_throttled_periods_total{{{wl_labels},pod="{pname}",container="{name}"}} {cfs_throttled}',
                f'container_memory_working_set_bytes{{{wl_labels},pod="{pname}",container="{name}",image="{image}"}} {p_mem}',
                f'container_memory_usage_bytes{{{wl_labels},pod="{pname}",container="{name}"}} {int(p_mem * 1.05)}',
                f'container_memory_cache{{{wl_labels},pod="{pname}",container="{name}"}} {int(p_mem * 0.10)}',
                f'container_memory_rss{{{wl_labels},pod="{pname}",container="{name}"}} {int(p_mem * 0.88)}',
                f'container_network_receive_bytes_total{{{wl_labels},pod="{pname}",interface="eth0"}} {_net_rx_acc.get(acc_key, 0):.0f}',
                f'container_network_transmit_bytes_total{{{wl_labels},pod="{pname}",interface="eth0"}} {_net_tx_acc.get(acc_key, 0):.0f}',
                f'container_network_receive_errors_total{{{wl_labels},pod="{pname}",interface="eth0"}} {int(load * 0.5)}',
                f'container_network_transmit_errors_total{{{wl_labels},pod="{pname}",interface="eth0"}} {int(load * 0.2)}',
                f'container_fs_usage_bytes{{{wl_labels},pod="{pname}",container="{name}"}} {int(50e6 + p_mem * 0.3)}',
                f'container_fs_limit_bytes{{{wl_labels},pod="{pname}",container="{name}"}} {int(10e9)}',
            ]

    # ── namespace resource quotas ─────────────────────────────────────────────

    for ns in ["prod", "staging", "monitoring", "data", "kube-system"]:
        limits = {"prod": (16, 32768), "staging": (4, 8192), "monitoring": (8, 16384),
                  "data": (24, 65536), "kube-system": (8, 16384)}.get(ns, (8, 16384))
        ns_wls = [w for w in WORKLOADS if w[0] == ns]
        used_cpu = sum(_workload_cpu(ts, w[0], w[2], w[9], w[5], w[3]) for w in ns_wls)
        used_mem = sum(_workload_mem(ts, w[0], w[2], w[10], w[7], w[3]) for w in ns_wls)
        lines += [
            f'kube_resourcequota{{namespace="{ns}",{lbl},resource="cpu",type="hard"}} {limits[0]}',
            f'kube_resourcequota{{namespace="{ns}",{lbl},resource="cpu",type="used"}} {used_cpu:.3f}',
            f'kube_resourcequota{{namespace="{ns}",{lbl},resource="memory",type="hard"}} {limits[1] * 1024**2}',
            f'kube_resourcequota{{namespace="{ns}",{lbl},resource="memory",type="used"}} {int(used_mem)}',
            f'kube_namespace_status_phase{{namespace="{ns}",{lbl},phase="Active"}} 1',
        ]

    # ── PersistentVolumes ─────────────────────────────────────────────────────

    for pv_name, pv_ns, pv_bytes, pv_used_ratio in [
        ("postgres-pv",    "prod",    100 * 1024**3, 0.45 + load * 0.05),
        ("redis-pv-0",     "prod",    20  * 1024**3, 0.60 + load * 0.03),
        ("redis-pv-1",     "prod",    20  * 1024**3, 0.58 + load * 0.03),
        ("kafka-pv-0",     "data",    200 * 1024**3, 0.38 + load * 0.04),
        ("kafka-pv-1",     "data",    200 * 1024**3, 0.36 + load * 0.04),
        ("kafka-pv-2",     "data",    200 * 1024**3, 0.40 + load * 0.04),
        ("prometheus-pv",  "monitoring", 50 * 1024**3, 0.28 + load * 0.02),
    ]:
        pv_used = int(pv_bytes * min(0.97, pv_used_ratio))
        lines += [
            f'kube_persistentvolume_capacity_bytes{{persistentvolume="{pv_name}",{lbl}}} {pv_bytes}',
            f'kube_persistentvolumeclaim_status_capacity_bytes{{namespace="{pv_ns}",persistentvolumeclaim="{pv_name}",{lbl}}} {pv_used}',
        ]

    # ── HPA ───────────────────────────────────────────────────────────────────

    for hpa_ns, hpa_name, min_r, max_r, base_wl in [
        ("prod", "api-server-hpa",    2, 10, ("prod", "api-server")),
        ("prod", "web-frontend-hpa",  1,  5, ("prod", "web-frontend")),
        ("data", "kafka-hpa",         3,  9, ("data", "kafka")),
    ]:
        wl_match = next((w for w in WORKLOADS if w[0] == base_wl[0] and w[2] == base_wl[1]), None)
        if wl_match:
            desired = _rolling_update(ts, hpa_ns, hpa_name, wl_match[3])
        else:
            desired = min_r
        lines += [
            f'kube_horizontalpodautoscaler_spec_min_replicas{{namespace="{hpa_ns}",horizontalpodautoscaler="{hpa_name}",{lbl}}} {min_r}',
            f'kube_horizontalpodautoscaler_spec_max_replicas{{namespace="{hpa_ns}",horizontalpodautoscaler="{hpa_name}",{lbl}}} {max_r}',
            f'kube_horizontalpodautoscaler_status_current_replicas{{namespace="{hpa_ns}",horizontalpodautoscaler="{hpa_name}",{lbl}}} {desired}',
            f'kube_horizontalpodautoscaler_status_desired_replicas{{namespace="{hpa_ns}",horizontalpodautoscaler="{hpa_name}",{lbl}}} {min(max_r, max(min_r, round(desired * (1 + load * 0.5))))}',
        ]

    return "\n".join(lines) + "\n"


def push(text: str, url: str) -> None:
    r = requests.post(
        f"{url}/metrics/job/kubernetes/instance/cluster-prod",
        data=text,
        headers={"Content-Type": "text/plain; charset=utf-8"},
        timeout=10,
    )
    r.raise_for_status()


def _wait_for(url: str, timeout: int = 120) -> None:
    deadline = time.time() + timeout
    print(f"[k8s_generator] Waiting for {url} …", flush=True)
    while time.time() < deadline:
        try:
            if requests.get(url, timeout=3).ok:
                return
        except Exception:
            pass
        time.sleep(3)
    raise SystemExit(f"[k8s_generator] {url} not ready after {timeout}s")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pushgateway", default=PUSHGATEWAY_URL)
    parser.add_argument("--interval",    type=float, default=PUSH_INTERVAL)
    parser.add_argument("--once",        action="store_true")
    args = parser.parse_args()

    _wait_for(f"{args.pushgateway}/-/healthy")
    print(f"[k8s_generator] Pushing every {args.interval}s …", flush=True)

    while True:
        try:
            metrics = build_metrics(args.interval)
            push(metrics, args.pushgateway)
            load_now = _load(time.time())
            oom_wls  = [f"{w[0]}/{w[2]}" for w in WORKLOADS if _oom_event(time.time(), w[0], w[2])]
            oom_str  = f"  OOM: {oom_wls}" if oom_wls else ""
            print(f"[k8s_generator] pushed  load={load_now:.2f}{oom_str}", flush=True)
        except Exception as exc:
            print(f"[k8s_generator] ERROR: {exc}", file=sys.stderr, flush=True)
        if args.once:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
