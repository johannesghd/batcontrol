"""Lightweight built-in web dashboard for batcontrol."""

import datetime
import json
import logging
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

logger = logging.getLogger(__name__)


def align_timestamp(timestamp: float, interval_minutes: int) -> int:
    """Align a Unix timestamp to the start of its interval."""
    interval_seconds = interval_minutes * 60
    return int(timestamp - (timestamp % interval_seconds))


class DashboardServer:
    """Serve the batcontrol dashboard and JSON endpoints."""

    def __init__(
            self,
            host: str,
            port: int,
            snapshot_provider: Callable[[Optional[float]], Dict],
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
                parsed_url = urlparse(self.path)
                query = parse_qs(parsed_url.query)

                if parsed_url.path in ['/', '/index.html']:
                    body = _build_dashboard_html(page_title).encode('utf-8')
                    self.send_response(HTTPStatus.OK)
                    self.send_header('Content-Type', 'text/html; charset=utf-8')
                    self.send_header('Content-Length', str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                if parsed_url.path == '/api/dashboard':
                    at_ts = _parse_float(query.get('at', [None])[0])
                    payload = json.dumps(snapshot_provider(at_ts)).encode('utf-8')
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


def format_timepoint(timestamp: float, timezone) -> Dict:
    """Convert a timestamp into dashboard-friendly datetime fields."""
    point_time = datetime.datetime.fromtimestamp(timestamp, tz=timezone)
    return {
        'timestamp': int(timestamp),
        'iso': point_time.isoformat(),
        'label': point_time.strftime('%Y-%m-%d %H:%M'),
    }


def _parse_float(value):
    if value in [None, '']:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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
      --bg: #f4f7fb;
      --panel: rgba(255, 255, 255, 0.78);
      --panel-strong: rgba(255, 255, 255, 0.92);
      --ink: #15202b;
      --muted: #5d6b7a;
      --grid: rgba(21, 32, 43, 0.12);
      --accent-load: #2f6fed;
      --accent-pv: #f59e0b;
      --accent-net: #14b8a6;
      --accent-price: #ef4444;
      --accent-soc-forecast: #22c55e;
      --accent-soc: #8b5cf6;
      --accent-prod: #f97316;
      --accent-cons: #10b981;
      --accent-prod-pred: #fdba74;
      --accent-cons-pred: #86efac;
      --shadow: 0 20px 60px rgba(23, 34, 56, 0.14);
      --radius: 24px;
    }}
    @media (prefers-color-scheme: dark) {{
      :root {{
        --bg: #0b1220;
        --panel: rgba(17, 24, 39, 0.72);
        --panel-strong: rgba(17, 24, 39, 0.9);
        --ink: #e5edf6;
        --muted: #9fb0c4;
        --grid: rgba(159, 176, 196, 0.16);
        --accent-load: #7fb0ff;
        --accent-pv: #fbbf24;
        --accent-net: #2dd4bf;
        --accent-price: #fb7185;
        --accent-soc-forecast: #4ade80;
        --accent-soc: #a78bfa;
        --accent-prod: #fb923c;
        --accent-cons: #34d399;
        --accent-prod-pred: #fcd34d;
        --accent-cons-pred: #6ee7b7;
        --shadow: 0 24px 80px rgba(0, 0, 0, 0.34);
      }}
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(47, 111, 237, 0.18), transparent 26%),
        radial-gradient(circle at top right, rgba(245, 158, 11, 0.16), transparent 24%),
        linear-gradient(180deg, var(--bg) 0%, color-mix(in srgb, var(--bg) 85%, #dbe7f5) 100%);
      font-family: "Inter", "Segoe UI", sans-serif;
    }}
    .shell {{
      max-width: 1400px;
      margin: 0 auto;
      padding: 24px;
    }}
    .hero {{
      display: grid;
      grid-template-columns: 1.3fr 1fr;
      gap: 20px;
      margin-bottom: 20px;
    }}
    .panel {{
      background: var(--panel);
      backdrop-filter: blur(16px);
      border: 1px solid color-mix(in srgb, var(--ink) 10%, transparent);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      padding: 22px;
    }}
    h1, h2, p {{ margin: 0; }}
    h1 {{
      font-size: clamp(2.2rem, 4vw, 4.4rem);
      line-height: 0.94;
      letter-spacing: -0.04em;
      margin-bottom: 10px;
      font-weight: 800;
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
      background: var(--panel-strong);
      border-radius: 16px;
    }}
    .stat-label {{
      color: var(--muted);
      font-size: 0.9rem;
      margin-bottom: 6px;
    }}
    .stat-value {{
      font-size: clamp(1.2rem, 2vw, 2rem);
      font-weight: 700;
      letter-spacing: -0.04em;
    }}
    .full-width {{ margin-bottom: 18px; }}
    .chart-card h2 {{
      font-size: 1.05rem;
      margin-bottom: 14px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .chart-card.lead {{
      padding: 26px;
    }}
    .chart-card.lead svg {{
      height: 380px;
    }}
    .slider-row {{
      display: grid;
      gap: 10px;
      margin-top: 18px;
    }}
    .timeline-wrap {{
      position: relative;
      padding: 0 58px;
      margin-top: 18px;
    }}
    .timeline-axis {{
      position: relative;
      height: 28px;
    }}
    .timeline-line {{
      position: absolute;
      left: 0;
      right: 0;
      top: 8px;
      height: 2px;
      background: var(--grid);
      border-radius: 999px;
    }}
    .timeline-tick {{
      position: absolute;
      top: 0;
      transform: translateX(-50%);
      width: 14px;
      height: 18px;
      border: 0;
      background: transparent;
      cursor: pointer;
      padding: 0;
    }}
    .timeline-tick::before {{
      content: "";
      position: absolute;
      left: 50%;
      top: 7px;
      transform: translateX(-50%);
      width: 2px;
      height: 11px;
      border-radius: 999px;
      background: var(--muted);
    }}
    .timeline-tick.active::before {{
      background: var(--accent-load);
      box-shadow: 0 0 0 4px color-mix(in srgb, var(--accent-load) 18%, transparent);
    }}
    .timeline-slider {{
      margin-top: 4px;
      width: 100%;
      margin-left: 0;
      margin-right: 0;
      -webkit-appearance: none;
      appearance: none;
      background: transparent;
    }}
    .timeline-slider::-webkit-slider-runnable-track {{
      height: 4px;
      border-radius: 999px;
      background: color-mix(in srgb, var(--muted) 35%, transparent);
    }}
    .timeline-slider::-webkit-slider-thumb {{
      -webkit-appearance: none;
      appearance: none;
      width: 14px;
      height: 14px;
      border-radius: 999px;
      margin-top: -5px;
      background: var(--accent-load);
      border: 2px solid color-mix(in srgb, var(--panel-strong) 90%, transparent);
      box-shadow: 0 0 0 4px color-mix(in srgb, var(--accent-load) 18%, transparent);
    }}
    .timeline-slider::-moz-range-track {{
      height: 4px;
      border-radius: 999px;
      background: color-mix(in srgb, var(--muted) 35%, transparent);
    }}
    .timeline-slider::-moz-range-thumb {{
      width: 14px;
      height: 14px;
      border-radius: 999px;
      background: var(--accent-load);
      border: 2px solid color-mix(in srgb, var(--panel-strong) 90%, transparent);
      box-shadow: 0 0 0 4px color-mix(in srgb, var(--accent-load) 18%, transparent);
    }}
    .timeline-slider:disabled {{
      opacity: 0.45;
    }}
    .slider-meta {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
      color: var(--muted);
      font-size: 0.92rem;
    }}
    input[type="range"] {{
      width: 100%;
      accent-color: var(--accent-load);
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
      .hero {{
        grid-template-columns: 1fr;
      }}
      .chart-card.lead svg {{
        height: 300px;
      }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <div class="panel">
        <p class="subtle">Battery control dashboard</p>
        <h1>Forecasts, prices, and net demand in one view.</h1>
        <p class="subtle" id="summary">Waiting for batcontrol data…</p>
        <div class="slider-meta" style="margin-top:18px;">
          <span id="selected-run">No historical runs available.</span>
          <span id="source-updates"></span>
        </div>
      </div>
      <div class="panel">
        <h2 style="font-size:1rem; letter-spacing:0.1em; text-transform:uppercase; margin-bottom:14px;">Selected Run</h2>
        <div class="stats" id="stats"></div>
      </div>
    </section>

    <section class="panel chart-card lead full-width">
      <h2>Combined Forecast Window</h2>
      <div class="legend">
        <span><i style="background: var(--accent-load);"></i>Consumption</span>
        <span><i style="background: var(--accent-pv);"></i>PV forecast</span>
        <span><i style="background: var(--accent-net);"></i>Net consumption</span>
        <span><i style="background: var(--accent-price);"></i>Price</span>
        <span><i style="background: var(--accent-soc-forecast);"></i>Predicted SOC</span>
      </div>
      <div id="combined-chart"></div>
      <div class="footer">
        <span>Left axis: W</span>
        <span>Inner right axis: ct/kWh</span>
        <span>Outer right axis: SOC %</span>
      </div>
    </section>

    <section class="panel chart-card full-width">
      <h2>Recent Run History</h2>
      <div class="legend">
        <span><i style="background: var(--accent-soc);"></i>SOC %</span>
        <span><i style="background: var(--accent-prod);"></i>Actual production W</span>
        <span><i style="background: var(--accent-prod-pred);"></i>Predicted production W</span>
        <span><i style="background: var(--accent-cons);"></i>Actual consumption W</span>
        <span><i style="background: var(--accent-cons-pred);"></i>Predicted consumption W</span>
      </div>
      <div id="history-chart"></div>
      <div class="timeline-wrap">
        <div class="timeline-axis">
          <div class="timeline-line"></div>
          <div id="timeline-ticks"></div>
        </div>
        <input class="timeline-slider" id="timeline-slider" type="range" min="0" max="0" value="0" step="1" disabled>
      </div>
      <div class="footer">
        <span id="refresh-info">Not loaded yet</span>
        <span id="history-note">Move the slider to inspect how stored forecasts changed over time.</span>
      </div>
    </section>
  </div>

  <script>
    const COLORS = {{
      load: getCss('--accent-load'),
      pv: getCss('--accent-pv'),
      net: getCss('--accent-net'),
      price: getCss('--accent-price'),
      socForecast: getCss('--accent-soc-forecast'),
      soc: getCss('--accent-soc'),
      production: getCss('--accent-prod'),
      consumption: getCss('--accent-cons'),
      productionPred: getCss('--accent-prod-pred'),
      consumptionPred: getCss('--accent-cons-pred'),
      grid: getCss('--grid'),
      muted: getCss('--muted'),
    }};

    let timeline = [];
    let selectedIndex = null;

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

      const width = 1080;
      const height = options.height || 260;
      const hasSocAxis = series.some(item => item.axis === 'soc');
      const pad = {{
        top: 16,
        right: hasSocAxis ? 96 : (options.rightAxis ? 58 : 18),
        bottom: 36,
        left: 58
      }};
      const points = series.flatMap(item => item.points);
      const times = points.map(point => point.timestamp);
      const minX = Math.min(...times);
      const maxX = Math.max(...times);
      const xSpan = Math.max(maxX - minX, 1);

      const leftSeries = series.filter(item => item.axis !== 'right');
      const rightSeries = series.filter(item => item.axis === 'right');
      const socSeries = series.filter(item => item.axis === 'soc');
      const primaryLeftSeries = series.filter(item => !item.axis);
      const leftValues = leftSeries.flatMap(item => item.points.map(point => point.value));
      const rightValues = rightSeries.flatMap(item => item.points.map(point => point.value));
      const socValues = socSeries.flatMap(item => item.points.map(point => point.value));

      function getBounds(values, includeZero = false) {{
        if (!values.length) return {{ min: 0, max: 1 }};
        let min = Math.min(...values);
        let max = Math.max(...values);
        if (includeZero) {{
          min = Math.min(min, 0);
          max = Math.max(max, 0);
        }}
        if (min === max) {{
          min -= 1;
          max += 1;
        }}
        return {{ min, max }};
      }}

      const leftBounds = options.leftBounds || getBounds(primaryLeftSeries.flatMap(item => item.points.map(point => point.value)), !!options.leftIncludeZero);
      const rightBounds = options.rightBounds || getBounds(rightValues);
      const socBounds = options.socBounds || {{ min: 0, max: 100 }};
      const leftSpan = Math.max(leftBounds.max - leftBounds.min, 1);
      const rightSpan = Math.max(rightBounds.max - rightBounds.min, 1);
      const socSpan = Math.max(socBounds.max - socBounds.min, 1);

      function xScale(ts) {{
        return pad.left + ((ts - minX) / xSpan) * (width - pad.left - pad.right);
      }}

      function yScaleLeft(value) {{
        return height - pad.bottom - ((value - leftBounds.min) / leftSpan) * (height - pad.top - pad.bottom);
      }}

      function yScaleRight(value) {{
        return height - pad.bottom - ((value - rightBounds.min) / rightSpan) * (height - pad.top - pad.bottom);
      }}

      function yScaleSoc(value) {{
        return height - pad.bottom - ((value - socBounds.min) / socSpan) * (height - pad.top - pad.bottom);
      }}

      let svg = `<svg viewBox="0 0 ${{width}} ${{height}}" role="img" aria-label="chart">`;
      for (let i = 0; i <= 4; i += 1) {{
        const yValue = leftBounds.min + (leftSpan / 4) * i;
        const y = yScaleLeft(yValue);
        svg += `<line x1="${{pad.left}}" y1="${{y}}" x2="${{width - pad.right}}" y2="${{y}}" stroke="${{COLORS.grid}}" stroke-width="1" />`;
        svg += `<text x="${{pad.left - 8}}" y="${{y + 4}}" text-anchor="end" font-size="12" fill="${{COLORS.muted}}">${{fmtNumber(yValue, options.yDigits ?? 0)}}</text>`;
        if (options.rightAxis && rightValues.length) {{
          const rightValue = rightBounds.min + (rightSpan / 4) * i;
          svg += `<text x="${{width - pad.right + 8}}" y="${{y + 4}}" text-anchor="start" font-size="12" fill="${{COLORS.muted}}">${{fmtNumber(rightValue, options.rightYDigits ?? 3)}}</text>`;
        }}
        if (hasSocAxis && socValues.length) {{
          const socValue = socBounds.min + (socSpan / 4) * i;
          svg += `<text x="${{width - 4}}" y="${{y + 4}}" text-anchor="end" font-size="12" fill="${{COLORS.muted}}">${{fmtNumber(socValue, options.socYDigits ?? 0)}}</text>`;
        }}
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
        let yScale = yScaleLeft;
        if (item.axis === 'right') yScale = yScaleRight;
        if (item.axis === 'soc') yScale = yScaleSoc;
        const path = item.step
          ? buildStepPath(item.points, xScale, yScale)
          : item.points.map((point, index) => `${{index === 0 ? 'M' : 'L'}} ${{xScale(point.timestamp)}} ${{yScale(point.value)}}`).join(' ');
        const dash = item.dash ? ` stroke-dasharray="${{item.dash}}"` : '';
        svg += `<path d="${{path}}" fill="none" stroke="${{item.color}}" stroke-width="3"${{dash}} stroke-linejoin="round" stroke-linecap="round"/>`;
      }});

      if (options.selectedTimestamp) {{
        const x = xScale(options.selectedTimestamp);
        svg += `<line x1="${{x}}" y1="${{pad.top}}" x2="${{x}}" y2="${{height - pad.bottom}}" stroke="${{COLORS.price}}" stroke-width="2" stroke-dasharray="5 5" />`;
      }}

      svg += `</svg>`;
      target.innerHTML = svg;
    }}

    function series(points, color) {{
      return [{{ color, points }}];
    }}

    function transformSeries(points, factor = 1) {{
      return (points || []).map((point) => ({{
        ...point,
        value: point.value * factor,
      }}));
    }}

    function buildStepPath(points, xScale, yScale) {{
      if (!points.length) return '';
      let path = `M ${{xScale(points[0].timestamp)}} ${{yScale(points[0].value)}}`;
      for (let i = 1; i < points.length; i += 1) {{
        const prev = points[i - 1];
        const current = points[i];
        path += ` L ${{xScale(current.timestamp)}} ${{yScale(prev.value)}}`;
        path += ` L ${{xScale(current.timestamp)}} ${{yScale(current.value)}}`;
      }}
      return path;
    }}

    function updateSlider() {{
      const slider = document.getElementById('timeline-slider');
      const ticks = document.getElementById('timeline-ticks');
      if (!timeline.length) {{
        slider.disabled = true;
        slider.min = 0;
        slider.max = 0;
        slider.value = 0;
        ticks.innerHTML = '';
        return;
      }}

      slider.disabled = false;
      slider.min = 0;
      slider.max = timeline.length - 1;
      slider.value = selectedIndex ?? timeline.length - 1;
      const denominator = Math.max(timeline.length - 1, 1);
      ticks.innerHTML = timeline.map((item, index) => {{
        const left = (index / denominator) * 100;
        const active = index === Number(slider.value);
        return `
          <button
            type="button"
            class="timeline-tick${{active ? ' active' : ''}}"
            data-index="${{index}}"
            title="${{item.label}}"
            aria-label="Select run ${{item.label}}"
            style="left:${{left}}%;"></button>
        `;
      }}).join('');
      ticks.querySelectorAll('button').forEach((button) => {{
        button.addEventListener('click', async () => {{
          const index = Number(button.dataset.index);
          if (!timeline[index]) return;
          await render(timeline[index].timestamp);
        }});
      }});
    }}

    function formatSourceUpdates(sources) {{
      const parts = [];
      if (sources.prices && sources.prices.updated_at) {{
        parts.push(`prices updated ${{fmtDate(sources.prices.updated_at)}}`);
      }}
      if (sources.solar_forecast && sources.solar_forecast.updated_at) {{
        parts.push(`solar updated ${{fmtDate(sources.solar_forecast.updated_at)}}`);
      }}
      return parts.join(' | ');
    }}

    async function loadSnapshot(atTimestamp = null) {{
      const url = atTimestamp ? `/api/dashboard?at=${{encodeURIComponent(atTimestamp)}}` : '/api/dashboard';
      const response = await fetch(url, {{ cache: 'no-store' }});
      return response.json();
    }}

    async function render(atTimestamp = null) {{
      const data = await loadSnapshot(atTimestamp);
      timeline = data.timeline || [];

      if (timeline.length) {{
        const selectedTs = data.selected_run ? data.selected_run.timestamp : timeline[timeline.length - 1].timestamp;
        selectedIndex = timeline.findIndex(item => item.timestamp === selectedTs);
        if (selectedIndex < 0) selectedIndex = timeline.length - 1;
      }} else {{
        selectedIndex = null;
      }}

      updateSlider();
      buildStats(data.status);
      document.getElementById('summary').textContent =
        `Showing stored run ${{data.selected_run ? fmtDate(data.selected_run.iso) : 'n/a'}} from ${{data.timezone}}.`;
      document.getElementById('selected-run').textContent =
        data.selected_run ? `Selected run: ${{fmtDate(data.selected_run.iso)}}` : 'No stored run available.';
      document.getElementById('source-updates').textContent =
        formatSourceUpdates(data.sources);
      document.getElementById('refresh-info').textContent =
        `Dashboard generated ${{fmtDate(data.generated_at)}}. Timeline entries: ${{timeline.length}}.`;
      document.getElementById('history-note').textContent =
        data.history_note || '';

      renderChart('combined-chart', [
        {{ color: COLORS.load, points: data.today.load_profile }},
        {{ color: COLORS.pv, points: data.today.pv_forecast }},
        {{ color: COLORS.net, points: data.today.net_consumption, dash: '8 6' }},
        {{ color: COLORS.price, points: transformSeries(data.today.prices, 100), axis: 'right', step: true }},
        {{ color: COLORS.socForecast, points: data.today.predicted_soc, axis: 'soc', dash: '7 5' }},
      ], {{
        height: 380,
        leftIncludeZero: true,
        rightAxis: true,
        yDigits: 0,
        rightYDigits: 2,
        socBounds: {{ min: 0, max: 100 }},
        socYDigits: 0,
      }});
      renderChart('history-chart', [
        {{ color: COLORS.soc, points: data.history.soc, axis: 'right' }},
        {{ color: COLORS.production, points: data.history.actual_production }},
        {{ color: COLORS.productionPred, points: data.history.predicted_production, dash: '6 5' }},
        {{ color: COLORS.consumption, points: data.history.actual_consumption }},
        {{ color: COLORS.consumptionPred, points: data.history.predicted_consumption, dash: '6 5' }},
      ], {{
        selectedTimestamp: data.selected_run ? data.selected_run.timestamp : null,
        yDigits: 0,
        rightAxis: true,
        rightYDigits: 0,
        rightBounds: {{ min: 0, max: 100 }},
      }});
    }}

    document.getElementById('timeline-slider').addEventListener('input', async (event) => {{
      const index = Number(event.target.value);
      if (!timeline[index]) return;
      await render(timeline[index].timestamp);
    }});

    render().catch((error) => {{
      document.getElementById('summary').textContent = `Dashboard fetch failed: ${{error.message}}`;
    }});
  </script>
</body>
</html>
"""
