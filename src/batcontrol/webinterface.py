"""Lightweight built-in web dashboard for batcontrol."""

import datetime
import json
import logging
import os
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


def align_timestamp(timestamp: float, interval_minutes: int) -> int:
    """Align a Unix timestamp to the start of its interval."""
    interval_seconds = interval_minutes * 60
    return int(timestamp - (timestamp % interval_seconds))


class DashboardHistory:
    """Persist a compact interval history for dashboard charts."""

    def __init__(
            self,
            filepath: str,
            interval_minutes: int,
            retention_days: int = 7) -> None:
        self.filepath = filepath
        self.interval_minutes = interval_minutes
        self.retention_days = retention_days
        self._lock = threading.RLock()
        self._entries = []  # type: List[Dict]
        self._load()

    def _load(self) -> None:
        with self._lock:
            self._entries = []
            if not self.filepath or not os.path.isfile(self.filepath):
                return

            try:
                with open(self.filepath, 'r', encoding='utf-8') as handle:
                    for line in handle:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                        except json.JSONDecodeError:
                            logger.warning(
                                'Skipping invalid dashboard history row in %s',
                                self.filepath)
                            continue
                        if isinstance(entry, dict) and 'timestamp' in entry:
                            self._entries.append(entry)
            except OSError as exc:
                logger.warning(
                    'Unable to read dashboard history from %s: %s',
                    self.filepath,
                    exc)
                return

            self._prune_locked()

    def _prune_locked(self) -> None:
        if not self._entries:
            return

        newest_timestamp = max(
            int(entry.get('timestamp', 0)) for entry in self._entries)
        cutoff = newest_timestamp - self.retention_days * 24 * 3600
        self._entries = [
            entry for entry in self._entries
            if int(entry.get('timestamp', 0)) >= cutoff
        ]

    def _write_locked(self) -> None:
        if not self.filepath:
            return

        directory = os.path.dirname(self.filepath)
        if directory:
            os.makedirs(directory, exist_ok=True)

        with open(self.filepath, 'w', encoding='utf-8') as handle:
            for entry in self._entries:
                handle.write(json.dumps(entry, sort_keys=True) + '\n')

    def record(
            self,
            timestamp: float,
            soc: float,
            production: float,
            consumption: float) -> None:
        """Store one interval snapshot, overwriting duplicates in-place."""
        aligned_ts = align_timestamp(timestamp, self.interval_minutes)
        entry = {
            'timestamp': aligned_ts,
            'soc': round(float(soc), 3),
            'production': round(float(production), 3),
            'consumption': round(float(consumption), 3),
        }

        with self._lock:
            replaced = False
            for index, existing in enumerate(self._entries):
                if int(existing.get('timestamp', -1)) == aligned_ts:
                    self._entries[index] = entry
                    replaced = True
                    break
            if not replaced:
                self._entries.append(entry)
                self._entries.sort(key=lambda item: int(item['timestamp']))

            self._prune_locked()
            self._write_locked()

    def get_entries(self, since_timestamp: Optional[int] = None) -> List[Dict]:
        """Return a copy of the stored history, optionally filtered."""
        with self._lock:
            entries = list(self._entries)

        if since_timestamp is None:
            return entries

        return [
            entry for entry in entries
            if int(entry.get('timestamp', 0)) >= since_timestamp
        ]


class DashboardServer:
    """Serve the batcontrol dashboard and a JSON snapshot endpoint."""

    def __init__(
            self,
            host: str,
            port: int,
            snapshot_provider: Callable[[], Dict],
            title: str = 'batcontrol dashboard') -> None:
        self.host = host
        self.port = port
        self.snapshot_provider = snapshot_provider
        self.title = title
        self._server = None  # type: Optional[ThreadingHTTPServer]
        self._thread = None  # type: Optional[threading.Thread]

    def start(self) -> None:
        """Start the HTTP server in a daemon thread."""
        if self._server is not None:
            return

        snapshot_provider = self.snapshot_provider
        page_title = self.title

        class DashboardHandler(BaseHTTPRequestHandler):
            def do_GET(self):  # pylint: disable=invalid-name
                if self.path in ['/', '/index.html']:
                    body = _build_dashboard_html(page_title).encode('utf-8')
                    self.send_response(HTTPStatus.OK)
                    self.send_header('Content-Type', 'text/html; charset=utf-8')
                    self.send_header('Content-Length', str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                if self.path == '/api/dashboard':
                    payload = json.dumps(snapshot_provider()).encode('utf-8')
                    self.send_response(HTTPStatus.OK)
                    self.send_header(
                        'Content-Type', 'application/json; charset=utf-8')
                    self.send_header('Cache-Control', 'no-store')
                    self.send_header('Content-Length', str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return

                self.send_error(HTTPStatus.NOT_FOUND, 'Not found')

            def log_message(self, format_string, *args):  # noqa: A003
                logger.debug("Dashboard HTTP: " + format_string, *args)

        self._server = ThreadingHTTPServer((self.host, self.port), DashboardHandler)
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            daemon=True,
            name='DashboardServer')
        self._thread.start()
        logger.info(
            'Web dashboard available at http://%s:%d',
            self.host,
            self.port)

    def stop(self) -> None:
        """Stop the HTTP server and wait briefly for shutdown."""
        if self._server is None:
            return

        self._server.shutdown()
        self._server.server_close()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)

        self._server = None
        self._thread = None


def build_forecast_series(
        values,
        timestamp: float,
        interval_minutes: int,
        timezone) -> List[Dict]:
    """Convert forecast arrays into timestamped data points."""
    if values is None or timestamp is None:
        return []

    start = align_timestamp(timestamp, interval_minutes)
    interval_seconds = interval_minutes * 60
    points = []

    for index, value in enumerate(values):
        point_time = datetime.datetime.fromtimestamp(
            start + index * interval_seconds,
            tz=timezone,
        )
        points.append({
            'timestamp': int(point_time.timestamp()),
            'iso': point_time.isoformat(),
            'value': round(float(value), 3),
        })

    return points


def _build_dashboard_html(title: str) -> str:
    escaped_title = title.replace('"', '&quot;')
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escaped_title}</title>
  <style>
    :root {{
      --bg: #f5efe4;
      --panel: rgba(255, 251, 244, 0.84);
      --ink: #1f2a2d;
      --muted: #5e6b68;
      --grid: rgba(31, 42, 45, 0.12);
      --accent-load: #14532d;
      --accent-pv: #d97706;
      --accent-price: #0f766e;
      --accent-soc: #1d4ed8;
      --accent-prod: #b45309;
      --accent-cons: #166534;
      --danger: #991b1b;
      --shadow: 0 18px 48px rgba(49, 41, 29, 0.12);
      --radius: 24px;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(245, 158, 11, 0.18), transparent 28%),
        radial-gradient(circle at top right, rgba(20, 83, 45, 0.14), transparent 24%),
        linear-gradient(180deg, #f8f3ea 0%, #efe5d4 100%);
      font-family: Georgia, "Times New Roman", serif;
    }}
    .shell {{
      max-width: 1280px;
      margin: 0 auto;
      padding: 24px;
    }}
    .hero {{
      display: grid;
      grid-template-columns: 1.2fr 1fr;
      gap: 20px;
      margin-bottom: 20px;
    }}
    .panel {{
      background: var(--panel);
      backdrop-filter: blur(10px);
      border: 1px solid rgba(255, 255, 255, 0.6);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      padding: 22px;
    }}
    h1, h2, h3, p {{ margin: 0; }}
    h1 {{
      font-size: clamp(2.2rem, 4vw, 4.4rem);
      line-height: 0.94;
      letter-spacing: -0.04em;
      margin-bottom: 10px;
    }}
    .subtle {{
      color: var(--muted);
      font-size: 1rem;
      line-height: 1.5;
    }}
    .stats {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      margin-top: 18px;
    }}
    .stat {{
      padding: 14px 16px;
      background: rgba(255, 255, 255, 0.58);
      border-radius: 16px;
    }}
    .stat-label {{
      color: var(--muted);
      font-size: 0.9rem;
      margin-bottom: 6px;
    }}
    .stat-value {{
      font-size: clamp(1.4rem, 2vw, 2rem);
      font-weight: 700;
      letter-spacing: -0.04em;
    }}
    .chart-grid {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 18px;
      margin-bottom: 18px;
    }}
    .full-width {{ margin-bottom: 18px; }}
    .chart-card h2 {{
      font-size: 1.15rem;
      margin-bottom: 14px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .legend {{
      display: flex;
      flex-wrap: wrap;
      gap: 14px;
      margin-bottom: 12px;
      color: var(--muted);
      font-size: 0.92rem;
    }}
    .legend span {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }}
    .legend i {{
      width: 14px;
      height: 14px;
      border-radius: 999px;
      display: inline-block;
    }}
    svg {{
      width: 100%;
      height: 260px;
      display: block;
      overflow: visible;
    }}
    .footer {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
      color: var(--muted);
      font-size: 0.92rem;
      margin-top: 10px;
    }}
    .empty {{
      color: var(--muted);
      font-style: italic;
      padding: 42px 0;
      text-align: center;
    }}
    @media (max-width: 980px) {{
      .hero, .chart-grid {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <div class="panel">
        <p class="subtle">Battery control dashboard</p>
        <h1>Daily load, PV forecast, prices, and recent battery history.</h1>
        <p class="subtle" id="summary">Waiting for batcontrol data…</p>
      </div>
      <div class="panel">
        <h2 style="font-size:1rem; letter-spacing:0.1em; text-transform:uppercase; margin-bottom:14px;">Current Status</h2>
        <div class="stats" id="stats"></div>
      </div>
    </section>

    <section class="chart-grid">
      <div class="panel chart-card">
        <h2>Load Profile</h2>
        <div id="load-chart"></div>
      </div>
      <div class="panel chart-card">
        <h2>PV Forecast</h2>
        <div id="pv-chart"></div>
      </div>
      <div class="panel chart-card">
        <h2>Prices</h2>
        <div id="price-chart"></div>
      </div>
    </section>

    <section class="panel chart-card full-width">
      <h2>Past SOC, Production, Consumption</h2>
      <div class="legend">
        <span><i style="background: var(--accent-soc);"></i>SOC %</span>
        <span><i style="background: var(--accent-prod);"></i>Production W</span>
        <span><i style="background: var(--accent-cons);"></i>Consumption W</span>
      </div>
      <div id="history-chart"></div>
      <div class="footer">
        <span id="refresh-info">Not loaded yet</span>
        <span id="history-note">Historical production/consumption starts when the dashboard history recorder is enabled.</span>
      </div>
    </section>
  </div>

  <script>
    const COLORS = {{
      load: getCss('--accent-load'),
      pv: getCss('--accent-pv'),
      price: getCss('--accent-price'),
      soc: getCss('--accent-soc'),
      production: getCss('--accent-prod'),
      consumption: getCss('--accent-cons'),
      grid: getCss('--grid'),
      ink: getCss('--ink'),
      muted: getCss('--muted'),
      danger: getCss('--danger'),
    }};

    function getCss(name) {{
      return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    }}

    function fmtDate(iso) {{
      return new Date(iso).toLocaleString([], {{
        hour: '2-digit',
        minute: '2-digit',
        day: '2-digit',
        month: 'short'
      }});
    }}

    function fmtNumber(value, digits = 0) {{
      if (value === null || value === undefined || Number.isNaN(value)) {{
        return 'n/a';
      }}
      return Number(value).toFixed(digits);
    }}

    function buildStats(status) {{
      const stats = [
        ['SOC', status.soc_percent, '%', 1],
        ['Mode', status.mode_label, '', null],
        ['Charge rate', status.charge_rate_w, 'W', 0],
        ['Stored energy', status.stored_energy_wh, 'Wh', 0],
        ['Reserved energy', status.reserved_energy_wh, 'Wh', 0],
        ['Resolution', status.interval_minutes, 'min', 0],
      ];
      const container = document.getElementById('stats');
      container.innerHTML = stats.map(([label, value, unit, digits]) => {{
        const display = digits === null ? (value || 'n/a') : `${{fmtNumber(value, digits)}}${{unit ? ' ' + unit : ''}}`;
        return `<div class="stat"><div class="stat-label">${{label}}</div><div class="stat-value">${{display}}</div></div>`;
      }}).join('');
    }}

    function renderChart(targetId, series, options = {{}}) {{
      const target = document.getElementById(targetId);
      if (!series.length || !series.some(item => item.points && item.points.length)) {{
        target.innerHTML = '<div class="empty">No data available yet.</div>';
        return;
      }}

      const width = 880;
      const height = options.height || 260;
      const pad = {{ top: 12, right: 16, bottom: 34, left: 54 }};
      const points = series.flatMap(item => item.points);
      const values = points.map(point => point.value);
      let minY = Math.min(...values);
      let maxY = Math.max(...values);
      if (minY === maxY) {{
        minY -= 1;
        maxY += 1;
      }}
      if (options.minY !== undefined) minY = Math.min(minY, options.minY);
      if (options.maxY !== undefined) maxY = Math.max(maxY, options.maxY);

      const times = points.map(point => point.timestamp);
      const minX = Math.min(...times);
      const maxX = Math.max(...times);
      const xSpan = Math.max(maxX - minX, 1);
      const ySpan = Math.max(maxY - minY, 1);

      function xScale(ts) {{
        return pad.left + ((ts - minX) / xSpan) * (width - pad.left - pad.right);
      }}

      function yScale(value) {{
        return height - pad.bottom - ((value - minY) / ySpan) * (height - pad.top - pad.bottom);
      }}

      const gridLines = 4;
      let svg = `<svg viewBox="0 0 ${{width}} ${{height}}" role="img" aria-label="chart">`;
      for (let i = 0; i <= gridLines; i += 1) {{
        const yValue = minY + (ySpan / gridLines) * i;
        const y = yScale(yValue);
        svg += `<line x1="${{pad.left}}" y1="${{y}}" x2="${{width - pad.right}}" y2="${{y}}" stroke="${{COLORS.grid}}" stroke-width="1" />`;
        svg += `<text x="${{pad.left - 8}}" y="${{y + 4}}" text-anchor="end" font-size="12" fill="${{COLORS.muted}}">${{fmtNumber(yValue, options.yDigits ?? 0)}}</text>`;
      }}

      const tickCount = Math.min(6, points.length);
      for (let i = 0; i < tickCount; i += 1) {{
        const index = Math.round((points.length - 1) * (i / Math.max(tickCount - 1, 1)));
        const point = points[index];
        const x = xScale(point.timestamp);
        svg += `<line x1="${{x}}" y1="${{pad.top}}" x2="${{x}}" y2="${{height - pad.bottom}}" stroke="${{COLORS.grid}}" stroke-width="1" />`;
        svg += `<text x="${{x}}" y="${{height - 10}}" text-anchor="middle" font-size="12" fill="${{COLORS.muted}}">${{new Date(point.iso).toLocaleTimeString([], {{ hour: '2-digit', minute: '2-digit' }})}}</text>`;
      }}

      series.forEach(item => {{
        if (!item.points.length) return;
        const path = item.points.map((point, index) => `${{index === 0 ? 'M' : 'L'}} ${{xScale(point.timestamp)}} ${{yScale(point.value)}}`).join(' ');
        svg += `<path d="${{path}}" fill="none" stroke="${{item.color}}" stroke-width="3" stroke-linejoin="round" stroke-linecap="round"/>`;
      }});

      svg += `</svg>`;
      target.innerHTML = svg;
    }}

    function toSingleSeries(points, color) {{
      return [{{
        color,
        points,
      }}];
    }}

    async function refresh() {{
      try {{
        const response = await fetch('/api/dashboard', {{ cache: 'no-store' }});
        const data = await response.json();

        buildStats(data.status);
        document.getElementById('summary').textContent =
          `Latest evaluation ${fmtDate(data.generated_at)} in ${data.timezone}.`;
        document.getElementById('refresh-info').textContent =
          `Last refresh ${fmtDate(data.generated_at)}.`;
        document.getElementById('history-note').textContent =
          data.history_note || '';

        renderChart('load-chart', toSingleSeries(data.today.load_profile, COLORS.load));
        renderChart('pv-chart', toSingleSeries(data.today.pv_forecast, COLORS.pv));
        renderChart('price-chart', toSingleSeries(data.today.prices, COLORS.price), {{ yDigits: 3 }});
        renderChart('history-chart', [
          {{ color: COLORS.soc, points: data.history.soc }},
          {{ color: COLORS.production, points: data.history.production }},
          {{ color: COLORS.consumption, points: data.history.consumption }},
        ], {{ yDigits: 0 }});
      }} catch (error) {{
        document.getElementById('summary').textContent =
          `Dashboard fetch failed: ${{error.message}}`;
      }}
    }}

    refresh();
    setInterval(refresh, 60000);
  </script>
</body>
</html>
"""
