# all-in-one/Dockerfile
# Single image that bundles every service from compose.yml:
#   Grafana Enterprise, VictoriaMetrics, Pushgateway, InfluxDB 1.8,
#   Graphite (carbon-cache + graphite-web + StatsD + nginx), and all Python
#   data generators.  All processes are managed by supervisord.
#
# Build context: repo root
#   docker build -f all-in-one/Dockerfile -t grafana-test-all-in-one .
# Or via compose.single.yml (recommended).

FROM ubuntu:22.04
ARG DEBIAN_FRONTEND=noninteractive

# ── version pins (override with --build-arg) ─────────────────────────────────
ARG GRAFANA_VERSION=12.4.2
ARG VM_VERSION=1.139.0
ARG PG_VERSION=1.11.2
ARG INFLUX_VERSION=1.12.3
ARG TARGETARCH

# ── 1. base system packages ──────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl wget gnupg2 \
    supervisor \
    nginx \
    python3 python3-pip \
    sqlite3 \
    musl \
    libcairo2 \
    adduser libfontconfig1 \
    && rm -rf /var/lib/apt/lists/*

# ── 2. Node.js LTS (for StatsD) ──────────────────────────────────────────────
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
 && apt-get install -y --no-install-recommends nodejs \
 && rm -rf /var/lib/apt/lists/*

# ── 3. Grafana Enterprise (.deb) ─────────────────────────────────────────────
RUN wget -q "https://dl.grafana.com/enterprise/release/grafana-enterprise_${GRAFANA_VERSION}_${TARGETARCH}.deb" \
         -O /tmp/grafana.deb \
 && dpkg -i /tmp/grafana.deb \
 && rm /tmp/grafana.deb

# ── 4. InfluxDB 1.8 (static tarball — avoids focal/jammy repo issues) ────────
RUN wget -q "https://dl.influxdata.com/influxdb/releases/v${INFLUX_VERSION}/influxdb-${INFLUX_VERSION}_linux_${TARGETARCH}.tar.gz" \
         -O /tmp/influxdb.tar.gz \
 && tar -xzf /tmp/influxdb.tar.gz -C /tmp \
 && find /tmp -name 'influxd'     -type f -exec install -m 755 {} /usr/local/bin/influxd \; \
 && find /tmp -name 'influxdb.conf' -type f -exec install -m 644 {} /etc/influxdb.conf \; \
 && mkdir -p /var/lib/influxdb \
 && rm -rf /tmp/influxdb.tar.gz /tmp/influxdb-*

# ── 5. VictoriaMetrics single-node binary ────────────────────────────────────
# Use the single-node tarball (vmutils is the cluster utilities bundle and
# does NOT contain the victoria-metrics server binary).
# The binary inside the tarball is named victoria-metrics-prod.
RUN wget -q "https://github.com/VictoriaMetrics/VictoriaMetrics/releases/download/v${VM_VERSION}/victoria-metrics-linux-${TARGETARCH}-v${VM_VERSION}.tar.gz" \
         -O /tmp/vm.tar.gz \
 && tar -xzf /tmp/vm.tar.gz -C /tmp \
 && install -m 755 /tmp/victoria-metrics-prod /usr/local/bin/victoria-metrics \
 && rm /tmp/vm.tar.gz

# ── 6. Prometheus Pushgateway ─────────────────────────────────────────────────
RUN wget -q "https://github.com/prometheus/pushgateway/releases/download/v${PG_VERSION}/pushgateway-${PG_VERSION}.linux-${TARGETARCH}.tar.gz" \
         -O /tmp/pg.tar.gz \
 && tar -xzf /tmp/pg.tar.gz -C /tmp \
 && install -m 755 "/tmp/pushgateway-${PG_VERSION}.linux-${TARGETARCH}/pushgateway" /usr/local/bin/pushgateway \
 && rm -rf /tmp/pg.tar.gz "/tmp/pushgateway-${PG_VERSION}.linux-${TARGETARCH}"

# ── 7. Graphite stack: graphite-web + carbon + whisper + gunicorn ─────────────
RUN pip3 install --no-cache-dir \
    django \
    graphite-web \
    carbon \
    whisper \
    gunicorn

# Graphite's custom setup.py drops scripts into opt/graphite/bin/ rather than
# /usr/local/bin/.  Symlink so supervisord can find them by name.
RUN GBIN=$(find /usr/local/lib/python3*/dist-packages/opt/graphite/bin -name 'carbon-cache*' | head -1) \
 && [ -n "$GBIN" ] && ln -sf "$GBIN" /usr/local/bin/carbon-cache || true

# ── 8. StatsD ─────────────────────────────────────────────────────────────────
RUN npm install -g statsd

# ── 9. Python generator requirements ─────────────────────────────────────────
COPY generators/requirements.txt /tmp/gen-requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/gen-requirements.txt \
 && rm /tmp/gen-requirements.txt

# ── 10. Directory skeleton ────────────────────────────────────────────────────
RUN mkdir -p \
    /opt/graphite/storage/whisper \
    /opt/graphite/storage/log/webapp \
    /opt/graphite/storage/log/carbon \
    /opt/graphite/conf \
    /etc/statsd \
    /etc/prometheus \
    /prometheus \
    /var/lib/grafana/dashboards \
    /app

# ── 11. Graphite config files ─────────────────────────────────────────────────
COPY all-in-one/graphite/carbon.conf               /opt/graphite/conf/carbon.conf
COPY graphite/storage-schemas.conf                  /opt/graphite/conf/storage-schemas.conf
COPY graphite/storage-aggregation.conf              /opt/graphite/conf/storage-aggregation.conf
COPY all-in-one/graphite/statsd.config.js           /etc/statsd/config.js
COPY all-in-one/graphite/nginx-graphite.conf        /etc/nginx/sites-available/graphite
RUN ln -sf /etc/nginx/sites-available/graphite /etc/nginx/sites-enabled/graphite \
 && rm -f /etc/nginx/sites-enabled/default

# local_settings.py lives inside the graphite Python package directory
COPY all-in-one/graphite/local_settings.py /tmp/graphite_local_settings.py
RUN cp /tmp/graphite_local_settings.py \
      /usr/local/lib/python3.10/dist-packages/opt/graphite/webapp/graphite/local_settings.py \
 && DJANGO_SETTINGS_MODULE=graphite.settings \
    PYTHONPATH=/usr/local/lib/python3.10/dist-packages/opt/graphite/webapp \
    django-admin migrate --run-syncdb --no-input

# ── 12. VictoriaMetrics scrape config (all targets are localhost) ─────────────
COPY all-in-one/prometheus.yml /etc/prometheus/prometheus.yml

# ── 13. Grafana provisioning (datasource URLs repointed to localhost) ─────────
COPY all-in-one/provisioning /etc/grafana/provisioning

# ── 14. Dashboards (baked in; overridable at runtime via volume mount) ────────
COPY dashboards /var/lib/grafana/dashboards

# ── 15. Data generators ───────────────────────────────────────────────────────
COPY generators/*.py /app/

# ── 16. supervisord ──────────────────────────────────────────────────────────
COPY all-in-one/supervisord.conf /etc/supervisor/supervisord.conf

# Expose every port the original stack used
EXPOSE 3000
EXPOSE 8428
EXPOSE 9091
EXPOSE 8086
EXPOSE 2003
EXPOSE 80

# Make both graphite-web (webapp) and carbon (lib) importable everywhere.
# supervisord child programs that override environment= must repeat these explicitly.
ENV PYTHONPATH=/usr/local/lib/python3.10/dist-packages/opt/graphite/webapp:/usr/local/lib/python3.10/dist-packages/opt/graphite/lib \
    GRAPHITE_ROOT=/opt/graphite \
    DJANGO_SETTINGS_MODULE=graphite.settings \
    BACKFILL_DAYS=7

CMD ["/usr/bin/supervisord", "-n", "-c", "/etc/supervisor/supervisord.conf"]
