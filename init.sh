#!/usr/bin/env bash
# init.sh — build and start the Grafana test-data stack.
#
# Usage:
#   ./init.sh              — 7 days of history (default)
#   ./init.sh --days 30    — 30 days of history
#   ./init.sh --days 1     — quick smoke-test
#
# Stop:
#   docker compose down

set -euo pipefail

# ─── args ─────────────────────────────────────────────────────────────────────
DAYS=7
while [[ $# -gt 0 ]]; do
    case "$1" in
        --days)    DAYS="$2";      shift 2 ;;
        --days=*)  DAYS="${1#*=}"; shift   ;;
        -h|--help) echo "Usage: $0 [--days N]"; exit 0 ;;
        *)         echo "Unknown flag: $1" >&2; exit 1  ;;
    esac
done

# ─── colours ──────────────────────────────────────────────────────────────────
GRN='\033[0;32m'; CYN='\033[0;36m'; YLW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
step() { echo -e "\n${CYN}▶  $*${NC}"; }
ok()   { echo -e "${GRN}✔  $*${NC}";   }
die()  { echo -e "${RED}✖  $*${NC}" >&2; exit 1; }

cd "$(dirname "${BASH_SOURCE[0]}")"

echo -e "\n${CYN}━━━  Grafana test-data stack — ${DAYS} day(s) of history  ━━━${NC}\n"

# ─── preflight ────────────────────────────────────────────────────────────────
command -v docker >/dev/null 2>&1 || die "docker not found"
docker compose version >/dev/null 2>&1 || die "docker compose plugin not found"

# ─── build + start ────────────────────────────────────────────────────────────
step "Building images and starting stack"
export BACKFILL_DAYS="$DAYS"
docker compose up -d --build --remove-orphans
ok "Stack started"

# ─── follow backfill ──────────────────────────────────────────────────────────
step "Running historical backfill (${DAYS} day(s)) — following logs …"
echo -e "  ${YLW}Tip: Ctrl+C detaches from logs; backfill continues in the background.${NC}\n"

# docker logs -f exits automatically once the container stops.
docker logs -f prometheus-backfill 2>&1 || true

ok "Prometheus backfill done"
echo -e "  (InfluxDB backfill runs in the linux-generator container — check with:"
echo -e "   docker logs -f linux-generator)\n"

# ─── summary ──────────────────────────────────────────────────────────────────
echo -e "${GRN}━━━  Done  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n"
printf "  %-20s ${CYN}%s${NC}\n" "Grafana"         "http://localhost:3000  (admin / admin321)"
printf "  %-20s ${CYN}%s${NC}\n" "VictoriaMetrics" "http://localhost:9090"
printf "  %-20s ${CYN}%s${NC}\n" "Pushgateway"     "http://localhost:9091"
printf "  %-20s ${CYN}%s${NC}\n" "InfluxDB"        "http://localhost:8086"
echo ""
echo -e "  Stop:  ${YLW}docker compose down${NC}\n"
