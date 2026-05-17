"""
monitoring.metrics_exporter
============================
Prometheus-format metrics HTTP server for Grafana dashboards.

Exposes port 8000 (default) with:
  /metrics   — Prometheus scrape endpoint
  /health    — JSON health check
  /api/v1/*  — Grafana API probe responses (prevents 405 errors)
"""
from __future__ import annotations

import asyncio
import threading
import urllib.parse
from datetime import datetime
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, Optional

from loguru import logger
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    REGISTRY,
    Counter,
    Gauge,
    Histogram,
    Summary,
    generate_latest,
)

from monitoring.performance_tracker import get_performance_tracker
from execution.risk_engine import get_risk_engine
from execution.execution_engine import get_execution_engine


class MetricsHandler(BaseHTTPRequestHandler):
    """HTTP handler for Prometheus metrics and Grafana API probes."""

    exporter: Optional["GrafanaMetricsExporter"] = None

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path in ("/", ""):
            self._respond(200, "text/html", b"""
            <html><head><title>Polymarket Bot Metrics</title></head><body>
            <h1>Polymarket Trading Bot Metrics</h1>
            <p>Metrics: <a href="/metrics">/metrics</a></p>
            <p>Health: <a href="/health">/health</a></p>
            </body></html>""")
        elif parsed.path == "/health":
            self._respond(200, "application/json", b'{"status":"healthy"}')
        elif parsed.path == "/metrics":
            try:
                data = generate_latest(REGISTRY)
                self._respond(200, CONTENT_TYPE_LATEST, data, cors=True)
            except Exception as e:
                logger.error(f"Error generating metrics: {e}")
                self._respond(500, "text/plain", f"Error: {e}".encode())
        elif parsed.path.startswith("/api/v1/"):
            body = (
                b'{"status":"success","data":[]}'
                if "labels" in parsed.path
                else (
                    b'{"status":"success","data":{"resultType":"vector","result":[]}}'
                    if "query" in parsed.path
                    else b'{"status":"success"}'
                )
            )
            self._respond(200, "application/json", body, cors=True)
        else:
            self._respond(404, "text/plain", b"Not Found")

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path.startswith("/api/v1/") or parsed.path == "/metrics":
            self.do_GET()
        else:
            self._respond(404, "text/plain", b"Not Found")

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Accept, Content-Type")
        self.send_header("Access-Control-Max-Age", "86400")
        self.end_headers()

    def _respond(
        self,
        code: int,
        content_type: str,
        body: bytes,
        cors: bool = False,
    ) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        if cors:
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        try:
            if len(args) >= 2:
                status_code = int(args[1]) if str(args[1]).isdigit() else 0
                if status_code >= 400:
                    logger.debug(f"Metrics server: {format % args}")
        except Exception:
            pass


class GrafanaMetricsExporter:
    """Prometheus metrics exporter for Grafana dashboards."""

    def __init__(self, port: int = 8000, update_interval: int = 5):
        self.port = port
        self.update_interval = update_interval

        self.performance = get_performance_tracker()
        self.risk = get_risk_engine()
        self.execution = get_execution_engine()

        self._setup_metrics()
        self._is_running = False
        self._server: Optional[HTTPServer] = None
        self._thread: Optional[threading.Thread] = None

        logger.info(f"Initialized Grafana Metrics Exporter (port {port})")

    def _setup_metrics(self) -> None:
        self.total_pnl         = Gauge("trading_total_pnl",             "Total P&L in USD")
        self.roi               = Gauge("trading_roi",                    "ROI as percentage")
        self.win_rate          = Gauge("trading_win_rate",               "Win rate percentage")
        self.sharpe_ratio      = Gauge("trading_sharpe_ratio",           "Sharpe ratio")
        self.max_drawdown      = Gauge("trading_max_drawdown",           "Max drawdown percentage")
        self.open_positions    = Gauge("trading_open_positions",         "Open positions")
        self.total_exposure    = Gauge("trading_total_exposure",         "Total exposure USD")
        self.avg_signal_score  = Gauge("trading_avg_signal_score",       "Avg signal score 0-100")
        self.avg_signal_confidence = Gauge("trading_avg_signal_confidence", "Avg signal confidence")
        self.profit_factor     = Gauge("trading_profit_factor",          "Profit factor")
        self.expectancy_usd    = Gauge("trading_expectancy_usd",         "Mean P&L per trade USD")
        self.avg_hold_seconds  = Gauge("trading_avg_hold_seconds",       "Avg hold duration seconds")
        self.risk_utilization  = Gauge("trading_risk_utilization",       "Risk limits utilization pct")
        self.current_capital   = Gauge("trading_current_capital",        "Current capital USD")
        self.total_trades      = Counter("trading_trades_closed",        "Total closed trades")
        self.winning_trades    = Counter("trading_winning_trades",       "Winning trades")
        self.losing_trades     = Counter("trading_losing_trades",        "Losing trades")
        self.orders_placed     = Counter("trading_orders_placed",        "Orders placed")
        self.orders_filled     = Counter("trading_orders_filled",        "Orders filled")
        self.orders_rejected   = Counter("trading_orders_rejected",      "Orders rejected")
        self.trade_duration    = Histogram(
            "trading_trade_duration_seconds",
            "Trade duration seconds",
            buckets=[60, 300, 900, 1800, 3600, 7200, 14400, 28800],
        )
        logger.info("Prometheus metrics initialized")

    def update_metrics(self) -> None:
        try:
            perf = self.performance.calculate_metrics()
            self.total_pnl.set(float(perf.total_pnl))
            self.roi.set(perf.roi * 100)
            self.win_rate.set(perf.win_rate * 100)
            self.sharpe_ratio.set(perf.sharpe_ratio)
            self.max_drawdown.set(perf.max_drawdown * 100)
            self.open_positions.set(perf.open_positions)
            self.total_exposure.set(float(perf.total_exposure))
            self.avg_signal_score.set(perf.avg_signal_score)
            self.avg_signal_confidence.set(perf.avg_signal_confidence)
            self.avg_hold_seconds.set(perf.avg_hold_time)
            self.current_capital.set(float(self.performance.current_capital))

            dist = self.performance.get_win_loss_distribution()
            self.profit_factor.set(float(dist.get("profit_factor") or 0.0))
            if perf.total_trades > 0:
                self.expectancy_usd.set(float(perf.total_pnl / perf.total_trades))

            risk_summary = self.risk.get_risk_summary()
            if risk_summary:
                self.risk_utilization.set(
                    risk_summary["exposure"]["utilization_pct"]
                )

            logger.debug("Metrics updated")
        except Exception as e:
            logger.error(f"Error updating metrics: {e}")

    async def start(self) -> None:
        if self._is_running:
            logger.warning("Metrics exporter already running")
            return
        try:
            MetricsHandler.exporter = self
            self._server = HTTPServer(("0.0.0.0", self.port), MetricsHandler)
            self._thread = threading.Thread(
                target=self._server.serve_forever, daemon=True
            )
            self._thread.start()
            self._is_running = True
            logger.info(f"Metrics server started on port {self.port}")
            asyncio.create_task(self._update_loop())
        except Exception as e:
            logger.error(f"Failed to start metrics server: {e}")

    async def _update_loop(self) -> None:
        while self._is_running:
            try:
                self.update_metrics()
                await asyncio.sleep(self.update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Metrics update loop error: {e}")
                await asyncio.sleep(self.update_interval)

    async def stop(self) -> None:
        self._is_running = False
        if self._server:
            self._server.shutdown()
            self._server.server_close()
        logger.info("Metrics exporter stopped")

    def increment_trade_counter(self, won: bool) -> None:
        self.total_trades.inc()
        if won:
            self.winning_trades.inc()
        else:
            self.losing_trades.inc()

    def record_trade_duration(self, duration_seconds: float) -> None:
        self.trade_duration.observe(duration_seconds)

    def increment_order_counter(self, status: str) -> None:
        if status == "placed":
            self.orders_placed.inc()
        elif status == "filled":
            self.orders_filled.inc()
        elif status == "rejected":
            self.orders_rejected.inc()


_grafana_exporter_instance: Optional[GrafanaMetricsExporter] = None


def get_grafana_exporter() -> GrafanaMetricsExporter:
    global _grafana_exporter_instance
    if _grafana_exporter_instance is None:
        _grafana_exporter_instance = GrafanaMetricsExporter()
    return _grafana_exporter_instance
