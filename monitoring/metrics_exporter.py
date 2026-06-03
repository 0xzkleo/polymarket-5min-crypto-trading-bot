"""
monitoring.metrics_exporter
============================
Prometheus-format metrics HTTP server for Grafana dashboards.

Exposes port 8000 (default) with:
  /metrics   — Prometheus scrape endpoint
  /health    — JSON health check
  /api/v1/*  — Grafana API probe responses (prevents 405 errors)

Metrics covered
---------------
Portfolio / P&L
  trading_current_capital, trading_total_pnl, trading_roi,
  trading_daily_roi, trading_weekly_roi, trading_monthly_roi,
  trading_unrealized_pnl

Risk-adjusted returns
  trading_sharpe_ratio, trading_sortino_ratio, trading_calmar_ratio,
  trading_kelly_fraction, trading_recovery_factor

Drawdown / capital
  trading_max_drawdown, trading_max_drawdown_usd, trading_peak_capital

Trade statistics
  trading_win_rate, trading_profit_factor, trading_expectancy_usd,
  trading_avg_win_usd, trading_avg_loss_usd, trading_best_trade_usd,
  trading_worst_trade_usd, trading_avg_hold_seconds,
  trading_consecutive_wins, trading_consecutive_losses,
  trading_avg_trades_per_day, trading_pnl_variance

Signal / ML
  trading_avg_signal_score, trading_avg_signal_confidence,
  trading_fusion_score, trading_fusion_confidence, trading_fusion_num_signals,
  trading_ml_edge_score, trading_ml_prediction

Execution
  trading_open_positions, trading_total_exposure, trading_risk_utilization,
  trading_orders_placed_total, trading_orders_filled_total,
  trading_orders_rejected_total, trading_trades_closed_total,
  trading_winning_trades_total, trading_losing_trades_total,
  trading_trade_duration_seconds (histogram)

Per-processor metrics (label: processor=<name>)
  signal_processor_score, signal_processor_confidence, signal_processor_direction,
  signal_processor_fires_total

Processor-specific metadata gauges
  signal_ohlcv_rsi, signal_ohlcv_macd_histogram,
  signal_tick_velocity_30s, signal_tick_velocity_60s,
  signal_cvd_delta, signal_orderbook_bid_ask_ratio,
  signal_liquidation_cascade_volume, signal_spike_magnitude,
  signal_divergence_score, signal_funding_rate, signal_oi_change_pct,
  signal_pcr_value, signal_fear_greed_index

Environment
-----------
METRICS_UPDATE_INTERVAL  Seconds between portfolio gauge refreshes (default 1, range 1–60).
                         Match Prometheus scrape_interval and Grafana dashboard refresh for
                         lowest lag (e.g. all set to 1 or 2).
"""
from __future__ import annotations

import asyncio
import os
import sys
import threading
import urllib.parse
from datetime import datetime
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, Optional

# ── Windows guard for prometheus_client ──────────────────────────────────────
# Something in the runtime dep tree installs a META-PATH FINDER that
# synthesises an empty `resource` module; that module lacks `getpagesize`,
# causing an AttributeError inside prometheus_client's process_collector.
# Pre-install a stub before importing prometheus_client to avoid this.
if sys.platform == "win32":
    _existing = sys.modules.get("resource")
    if _existing is None or not hasattr(_existing, "getpagesize"):
        import types as _types

        _stub = _types.ModuleType("resource")
        _stub.getpagesize = lambda: 4096
        sys.modules["resource"] = _stub
        del _types, _stub

from loguru import logger
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Info,
    REGISTRY,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

from monitoring.performance_tracker import get_performance_tracker
from execution.risk_engine import get_risk_engine
from execution.execution_engine import get_execution_engine

# All 10 signal processor names (canonical labels used in Prometheus)
PROCESSOR_NAMES = [
    "OHLCVMomentum",
    "TickVelocity",
    "CVDOrderBook",
    "OrderBookImbalance",
    "Liquidations",
    "SpikeDetection",
    "PriceDivergence",
    "FundingRateOI",
    "DeribitPCR",
    "SentimentAnalysis",
]


class MetricsHandler(BaseHTTPRequestHandler):
    """HTTP handler for Prometheus metrics and Grafana API probes."""

    exporter: Optional["GrafanaMetricsExporter"] = None

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path in ("/", ""):
            self._respond(200, "text/html", b"""
            <html><head><title>Polymarket Bot Metrics</title>
            <style>body{font-family:sans-serif;margin:2em;background:#111;color:#eee;}
            a{color:#58a6ff;}h1{color:#f0b429;}</style></head><body>
            <h1>&#128200; Polymarket AI Trading Bot</h1>
            <p><a href="/metrics">/metrics</a> &mdash; Prometheus scrape target</p>
            <p><a href="/health">/health</a> &mdash; JSON liveness probe</p>
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
    """
    Prometheus metrics exporter for Grafana dashboards.

    Call ``update_signal_processor(name, score, confidence, direction, metadata)``
    from your strategy whenever a processor fires to keep per-processor gauges live.
    Call ``update_fusion_metrics(score, confidence, num_signals, consensus)`` after
    each fusion pass, and ``update_ml_metrics(edge, prediction)`` after each ML
    inference.
    """

    def __init__(self, port: int = 8000, update_interval: int = 1):
        self.port = port
        self.update_interval = update_interval

        self.performance = get_performance_tracker()
        self.risk = get_risk_engine()
        self.execution = get_execution_engine()

        self._setup_metrics()
        self._is_running = False
        self._server: Optional[HTTPServer] = None
        self._thread: Optional[threading.Thread] = None

        logger.info(
            f"Initialized Grafana Metrics Exporter "
            f"(port {port}, update_interval={update_interval}s)"
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Metric registration
    # ──────────────────────────────────────────────────────────────────────────

    def _setup_metrics(self) -> None:
        # ── Portfolio / P&L ──────────────────────────────────────────────────
        self.total_pnl          = Gauge("trading_total_pnl",             "Total realized P&L USD")
        self.unrealized_pnl     = Gauge("trading_unrealized_pnl",        "Unrealized P&L USD")
        self.roi                = Gauge("trading_roi",                    "Total ROI percent")
        self.daily_roi          = Gauge("trading_daily_roi",              "Today ROI percent")
        self.weekly_roi         = Gauge("trading_weekly_roi",             "7-day ROI percent")
        self.monthly_roi        = Gauge("trading_monthly_roi",            "30-day ROI percent")
        self.current_capital    = Gauge("trading_current_capital",        "Current capital USD")
        self.peak_capital       = Gauge("trading_peak_capital",           "Peak capital USD")

        # ── Risk-adjusted returns ────────────────────────────────────────────
        self.sharpe_ratio       = Gauge("trading_sharpe_ratio",           "Sharpe ratio (annualised)")
        self.sortino_ratio      = Gauge("trading_sortino_ratio",          "Sortino ratio (annualised)")
        self.calmar_ratio       = Gauge("trading_calmar_ratio",           "Calmar ratio (ROI / max drawdown)")
        self.kelly_fraction     = Gauge("trading_kelly_fraction",         "Kelly criterion optimal fraction")
        self.recovery_factor    = Gauge("trading_recovery_factor",        "Net profit / max drawdown USD")

        # ── Drawdown ─────────────────────────────────────────────────────────
        self.max_drawdown       = Gauge("trading_max_drawdown",           "Max drawdown percent")
        self.max_drawdown_usd   = Gauge("trading_max_drawdown_usd",       "Max drawdown USD")

        # ── Trade statistics ─────────────────────────────────────────────────
        self.win_rate           = Gauge("trading_win_rate",               "Win rate percent")
        self.profit_factor      = Gauge("trading_profit_factor",          "Gross profit / gross loss")
        self.expectancy_usd     = Gauge("trading_expectancy_usd",         "Expected P&L per trade USD")
        self.avg_win_usd        = Gauge("trading_avg_win_usd",            "Average winning trade USD")
        self.avg_loss_usd       = Gauge("trading_avg_loss_usd",           "Average losing trade USD (abs)")
        self.best_trade_usd     = Gauge("trading_best_trade_usd",         "Best single trade USD")
        self.worst_trade_usd    = Gauge("trading_worst_trade_usd",        "Worst single trade USD")
        self.avg_hold_seconds   = Gauge("trading_avg_hold_seconds",       "Average hold duration seconds")
        self.consecutive_wins   = Gauge("trading_consecutive_wins",       "Current consecutive winning trades")
        self.consecutive_losses = Gauge("trading_consecutive_losses",     "Current consecutive losing trades")
        self.avg_trades_per_day = Gauge("trading_avg_trades_per_day",     "Average trades per day")
        self.pnl_variance       = Gauge("trading_pnl_variance",           "P&L variance (trade-level)")

        # ── Execution counters ───────────────────────────────────────────────
        self.total_trades       = Counter("trading_trades_closed",        "Total closed trades")
        self.winning_trades     = Counter("trading_winning_trades",       "Winning trades")
        self.losing_trades      = Counter("trading_losing_trades",        "Losing trades")
        self.orders_placed      = Counter("trading_orders_placed",        "Orders placed")
        self.orders_filled      = Counter("trading_orders_filled",        "Orders filled")
        self.orders_rejected    = Counter("trading_orders_rejected",      "Orders rejected")
        self.trade_duration     = Histogram(
            "trading_trade_duration_seconds",
            "Trade duration seconds",
            buckets=[60, 300, 600, 900, 1800, 3600, 7200, 14400],
        )

        # ── Position / risk ───────────────────────────────────────────────────
        self.open_positions     = Gauge("trading_open_positions",         "Open positions count")
        self.total_exposure     = Gauge("trading_total_exposure",         "Total exposure USD")
        self.risk_utilization   = Gauge("trading_risk_utilization",       "Risk utilisation percent")

        # ── Signal / fusion / ML ─────────────────────────────────────────────
        self.avg_signal_score       = Gauge("trading_avg_signal_score",       "Average signal score 0-100")
        self.avg_signal_confidence  = Gauge("trading_avg_signal_confidence",  "Average signal confidence 0-1")
        self.fusion_score           = Gauge("trading_fusion_score",           "Latest fusion composite score 0-100")
        self.fusion_confidence      = Gauge("trading_fusion_confidence",      "Latest fusion confidence 0-1")
        self.fusion_num_signals     = Gauge("trading_fusion_num_signals",     "Signals contributing to last fusion")
        self.ml_edge_score          = Gauge("trading_ml_edge_score",          "Latest ML edge vs market price")
        self.ml_prediction          = Gauge("trading_ml_prediction",          "Latest ML p(UP) prediction 0-1")

        # ── Per-processor gauges (labeled) ────────────────────────────────────
        self.proc_score         = Gauge(
            "signal_processor_score",
            "Latest signal score 0-100 per processor",
            ["processor"],
        )
        self.proc_confidence    = Gauge(
            "signal_processor_confidence",
            "Latest signal confidence 0-1 per processor",
            ["processor"],
        )
        self.proc_direction     = Gauge(
            "signal_processor_direction",
            "Latest signal direction: 1=bullish, -1=bearish, 0=neutral",
            ["processor"],
        )
        self.proc_fires         = Counter(
            "signal_processor_fires",
            "Total signals fired per processor",
            ["processor"],
        )

        # ── Processor-specific metadata gauges ───────────────────────────────
        self.ohlcv_rsi              = Gauge("signal_ohlcv_rsi",                 "OHLCV RSI value 0-100")
        self.ohlcv_macd_histogram   = Gauge("signal_ohlcv_macd_histogram",      "OHLCV MACD histogram value")
        self.tick_velocity_30s      = Gauge("signal_tick_velocity_30s",         "Tick velocity 30-second window")
        self.tick_velocity_60s      = Gauge("signal_tick_velocity_60s",         "Tick velocity 60-second window")
        self.cvd_delta              = Gauge("signal_cvd_delta",                 "Cumulative volume delta (CVD)")
        self.orderbook_bid_ask      = Gauge("signal_orderbook_bid_ask_ratio",   "Polymarket CLOB bid/ask imbalance ratio")
        self.liquidation_volume     = Gauge("signal_liquidation_cascade_volume","Liquidation cascade volume USD")
        self.spike_magnitude        = Gauge("signal_spike_magnitude",           "Spike detection deviation magnitude")
        self.divergence_score_g     = Gauge("signal_divergence_score",          "Price divergence score")
        self.funding_rate           = Gauge("signal_funding_rate",              "Binance perp funding rate")
        self.oi_change_pct          = Gauge("signal_oi_change_pct",             "Open interest change percent")
        self.pcr_value              = Gauge("signal_pcr_value",                 "Deribit put/call ratio")
        self.fear_greed_index       = Gauge("signal_fear_greed_index",          "Fear & Greed index 0-100")

        # ── Latest order (updated immediately before each submission) ─────────
        self.last_order_direction   = Gauge(
            "trading_last_order_direction",
            "Last order direction: 1=UP/YES (long), -1=DOWN/NO (short), 0=none",
        )
        self.last_order_size_usd    = Gauge("trading_last_order_size_usd",    "Last order notional USD")
        self.last_order_entry_price = Gauge("trading_last_order_entry_price", "Held-token entry price")
        self.last_order_poly_yes    = Gauge("trading_last_order_poly_yes_price", "Polymarket YES price at entry")
        self.last_order_qty_tokens  = Gauge("trading_last_order_qty_tokens",  "Token quantity")
        self.last_order_bid         = Gauge("trading_last_order_bid_price",   "Best bid at entry")
        self.last_order_ask         = Gauge("trading_last_order_ask_price",   "Best ask at entry")
        self.last_order_spread_pct  = Gauge("trading_last_order_spread_pct",  "Bid-ask spread percent at entry")
        self.last_order_signal_score = Gauge("trading_last_order_signal_score", "Signal score at entry 0-100")
        self.last_order_signal_conf  = Gauge("trading_last_order_signal_confidence", "Signal confidence 0-1")
        self.last_order_ml_edge      = Gauge("trading_last_order_ml_edge",     "ML edge at entry")
        self.last_order_ml_p_up      = Gauge("trading_last_order_ml_p_up",     "ML p(UP) at entry")
        self.last_order_btc_spot     = Gauge("trading_last_order_btc_spot_usd", "BTC spot USD at entry")
        self.last_order_secs_settle  = Gauge("trading_last_order_seconds_to_settle", "Seconds until settlement")
        self.last_order_is_sim       = Gauge("trading_last_order_is_simulation", "1=simulation 0=live")
        self.last_order_fusion_score = Gauge("trading_last_order_fusion_score", "Fusion score at entry")
        self.last_order_info         = Info(
            "trading_last_order",
            "Metadata for the most recently submitted order",
        )
        self.orders_submitted        = Counter(
            "trading_orders_submitted_total",
            "Orders submitted by direction and mode",
            ["direction", "mode"],
        )

        # ── Initialise all labeled time-series so they appear immediately ─────
        for name in PROCESSOR_NAMES:
            self.proc_score.labels(processor=name).set(0)
            self.proc_confidence.labels(processor=name).set(0)
            self.proc_direction.labels(processor=name).set(0)

        logger.info("Prometheus metrics initialised — %d metric families registered", 65)

    def update_order_metrics(
        self,
        *,
        direction: str,
        size_usd: float,
        entry_price: float,
        poly_yes_price: float,
        qty_tokens: float,
        signal_score: float = 0.0,
        signal_confidence: float = 0.0,
        ml_edge: float = 0.0,
        ml_p_up: Optional[float] = None,
        fusion_score: float = 0.0,
        btc_spot: float = 0.0,
        bid_price: Optional[float] = None,
        ask_price: Optional[float] = None,
        seconds_to_settle: Optional[float] = None,
        market_slug: str = "",
        is_simulation: bool = True,
    ) -> None:
        """Publish order details to Prometheus before submission (Grafana UI)."""
        try:
            is_long = direction == "long"
            dir_val = 1.0 if is_long else -1.0
            side = "YES" if is_long else "NO"
            outcome = "UP" if is_long else "DOWN"
            mode = "simulation" if is_simulation else "live"

            self.last_order_direction.set(dir_val)
            self.last_order_size_usd.set(size_usd)
            self.last_order_entry_price.set(entry_price)
            self.last_order_poly_yes.set(poly_yes_price)
            self.last_order_qty_tokens.set(qty_tokens)
            self.last_order_signal_score.set(signal_score)
            self.last_order_signal_conf.set(signal_confidence)
            self.last_order_ml_edge.set(ml_edge)
            if ml_p_up is not None:
                self.last_order_ml_p_up.set(float(ml_p_up))
            self.last_order_fusion_score.set(fusion_score)
            if btc_spot > 0:
                self.last_order_btc_spot.set(btc_spot)
            self.last_order_is_sim.set(1.0 if is_simulation else 0.0)

            if bid_price is not None:
                self.last_order_bid.set(bid_price)
            if ask_price is not None:
                self.last_order_ask.set(ask_price)
            if bid_price is not None and ask_price is not None:
                mid = (bid_price + ask_price) / 2
                if mid > 0:
                    self.last_order_spread_pct.set((ask_price - bid_price) / mid * 100)

            if seconds_to_settle is not None:
                self.last_order_secs_settle.set(max(0.0, seconds_to_settle))

            self.last_order_info.info({
                "direction": outcome,
                "side": side,
                "mode": mode,
                "market": market_slug or "unknown",
            })
            self.orders_submitted.labels(direction=outcome.lower(), mode=mode).inc()
            self.increment_order_counter("placed")

            logger.debug(
                f"Order metrics: {outcome} {side} ${size_usd:.2f} @ {entry_price:.4f} "
                f"qty={qty_tokens:.4f} market={market_slug}"
            )
        except Exception as e:
            logger.debug(f"update_order_metrics error: {e}")

    # ──────────────────────────────────────────────────────────────────────────
    # Periodic update (called from _update_loop every update_interval seconds)
    # ──────────────────────────────────────────────────────────────────────────

    def update_metrics(self) -> None:
        try:
            perf = self.performance.calculate_metrics()

            # Portfolio
            self.total_pnl.set(float(perf.total_pnl))
            self.unrealized_pnl.set(float(perf.unrealized_pnl))
            self.roi.set(perf.roi * 100)
            self.current_capital.set(float(self.performance.current_capital))
            self.peak_capital.set(float(self.performance._peak_capital))

            # Rolling ROI
            self.daily_roi.set(self.performance.get_rolling_roi(days=1) * 100)
            self.weekly_roi.set(self.performance.get_rolling_roi(days=7) * 100)
            self.monthly_roi.set(self.performance.get_rolling_roi(days=30) * 100)

            # Risk-adjusted
            self.sharpe_ratio.set(perf.sharpe_ratio)
            self.sortino_ratio.set(self.performance.calculate_sortino_ratio())
            self.calmar_ratio.set(self.performance.calculate_calmar_ratio())
            self.kelly_fraction.set(self.performance.calculate_kelly_fraction())
            self.recovery_factor.set(self.performance.calculate_recovery_factor())

            # Drawdown
            self.max_drawdown.set(perf.max_drawdown * 100)
            self.max_drawdown_usd.set(
                float(self.performance._peak_capital - self.performance.current_capital)
            )

            # Trade stats
            self.win_rate.set(perf.win_rate * 100)
            self.avg_hold_seconds.set(perf.avg_hold_time)
            self.avg_signal_score.set(perf.avg_signal_score)
            self.avg_signal_confidence.set(perf.avg_signal_confidence)

            dist = self.performance.get_win_loss_distribution()
            self.profit_factor.set(float(dist.get("profit_factor") or 0.0))
            self.avg_win_usd.set(float(dist["wins"]["avg"]))
            self.avg_loss_usd.set(abs(float(dist["losses"]["avg"])))
            self.best_trade_usd.set(float(dist["wins"]["max"]))
            self.worst_trade_usd.set(float(dist["losses"]["max"]))

            if perf.total_trades > 0:
                self.expectancy_usd.set(float(perf.total_pnl / perf.total_trades))

            # Streaks and variance
            streaks = self.performance.get_streak_info()
            self.consecutive_wins.set(streaks["current_wins"])
            self.consecutive_losses.set(streaks["current_losses"])
            self.pnl_variance.set(self.performance.calculate_pnl_variance())
            self.avg_trades_per_day.set(self.performance.calculate_avg_trades_per_day())

            # Position / risk
            self.open_positions.set(perf.open_positions)
            self.total_exposure.set(float(perf.total_exposure))
            risk_summary = self.risk.get_risk_summary()
            if risk_summary:
                self.risk_utilization.set(
                    risk_summary["exposure"]["utilization_pct"]
                )

            logger.debug("Portfolio metrics updated")
        except Exception as e:
            logger.error(f"Error updating portfolio metrics: {e}")

    # ──────────────────────────────────────────────────────────────────────────
    # Per-processor update API (called by strategy on every signal fire)
    # ──────────────────────────────────────────────────────────────────────────

    def update_signal_processor(
        self,
        name: str,
        score: float,
        confidence: float,
        direction: str,  # "bullish" | "bearish" | "neutral"
        metadata: Dict[str, Any] = None,
    ) -> None:
        """Update per-processor Prometheus gauges.

        Call this every time a signal processor fires (even if the signal was
        filtered out by risk/ML) so Grafana always has fresh per-processor data.
        """
        try:
            self.proc_score.labels(processor=name).set(score)
            self.proc_confidence.labels(processor=name).set(confidence)
            dir_val = 1.0 if direction == "bullish" else (-1.0 if direction == "bearish" else 0.0)
            self.proc_direction.labels(processor=name).set(dir_val)
            self.proc_fires.labels(processor=name).inc()

            if metadata:
                self._apply_processor_metadata(name, metadata)
        except Exception as e:
            logger.debug(f"update_signal_processor({name}) error: {e}")

    def _apply_processor_metadata(self, name: str, md: Dict[str, Any]) -> None:
        """Route processor-specific metadata fields to dedicated gauges."""
        try:
            n = name.lower()
            if "ohlcv" in n or "momentum" in n:
                if "rsi" in md:
                    self.ohlcv_rsi.set(float(md["rsi"]))
                if "macd_histogram" in md:
                    self.ohlcv_macd_histogram.set(float(md["macd_histogram"]))
            elif "tick" in n or "velocity" in n:
                if "velocity_30s" in md:
                    self.tick_velocity_30s.set(float(md["velocity_30s"]))
                if "velocity_60s" in md:
                    self.tick_velocity_60s.set(float(md["velocity_60s"]))
            elif "cvd" in n:
                if "cvd_delta" in md:
                    self.cvd_delta.set(float(md["cvd_delta"]))
            elif "orderbook" in n or "imbalance" in n:
                if "bid_ask_ratio" in md:
                    self.orderbook_bid_ask.set(float(md["bid_ask_ratio"]))
            elif "liquidat" in n:
                if "cascade_volume" in md:
                    self.liquidation_volume.set(float(md["cascade_volume"]))
                elif "volume" in md:
                    self.liquidation_volume.set(float(md["volume"]))
            elif "spike" in n:
                if "magnitude" in md:
                    self.spike_magnitude.set(float(md["magnitude"]))
                elif "spike_magnitude" in md:
                    self.spike_magnitude.set(float(md["spike_magnitude"]))
            elif "divergence" in n:
                if "divergence_score" in md:
                    self.divergence_score_g.set(float(md["divergence_score"]))
                elif "score" in md:
                    self.divergence_score_g.set(float(md["score"]))
            elif "funding" in n or "oi" in n:
                if "funding_rate" in md:
                    self.funding_rate.set(float(md["funding_rate"]))
                if "oi_change_pct" in md:
                    self.oi_change_pct.set(float(md["oi_change_pct"]))
            elif "pcr" in n or "deribit" in n:
                if "pcr" in md:
                    self.pcr_value.set(float(md["pcr"]))
                elif "put_call_ratio" in md:
                    self.pcr_value.set(float(md["put_call_ratio"]))
            elif "sentiment" in n:
                if "fear_greed_index" in md:
                    self.fear_greed_index.set(float(md["fear_greed_index"]))
                elif "sentiment_score" in md:
                    self.fear_greed_index.set(float(md["sentiment_score"]))
        except Exception as e:
            logger.debug(f"_apply_processor_metadata({name}) error: {e}")

    # ──────────────────────────────────────────────────────────────────────────
    # Fusion / ML update API
    # ──────────────────────────────────────────────────────────────────────────

    def update_fusion_metrics(
        self,
        score: float,
        confidence: float,
        num_signals: int,
        direction: str = "neutral",
    ) -> None:
        """Call after each SignalFusionEngine.fuse_signals() pass."""
        try:
            self.fusion_score.set(score)
            self.fusion_confidence.set(confidence)
            self.fusion_num_signals.set(num_signals)
        except Exception as e:
            logger.debug(f"update_fusion_metrics error: {e}")

    def update_ml_metrics(self, edge: float, prediction: float) -> None:
        """Call after each MLEngine inference."""
        try:
            self.ml_edge_score.set(edge)
            self.ml_prediction.set(prediction)
        except Exception as e:
            logger.debug(f"update_ml_metrics error: {e}")

    # ──────────────────────────────────────────────────────────────────────────
    # Event counters (called by strategy on order/trade events)
    # ──────────────────────────────────────────────────────────────────────────

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

    # ──────────────────────────────────────────────────────────────────────────
    # Server lifecycle
    # ──────────────────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Start the HTTP metrics server (update loop driven separately)."""
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


_grafana_exporter_instance: Optional[GrafanaMetricsExporter] = None


def _metrics_update_interval_from_env(default: int = 1) -> int:
    """Read METRICS_UPDATE_INTERVAL from env; clamp to 1–60 seconds."""
    raw = os.getenv("METRICS_UPDATE_INTERVAL", str(default))
    try:
        value = int(raw)
    except ValueError:
        logger.warning(
            f"Invalid METRICS_UPDATE_INTERVAL={raw!r}; using {default}s"
        )
        return default
    if value < 1 or value > 60:
        clamped = max(1, min(60, value))
        logger.warning(
            f"METRICS_UPDATE_INTERVAL={value} out of range; using {clamped}s"
        )
        return clamped
    return value


def get_grafana_exporter() -> GrafanaMetricsExporter:
    global _grafana_exporter_instance
    if _grafana_exporter_instance is None:
        _grafana_exporter_instance = GrafanaMetricsExporter(
            update_interval=_metrics_update_interval_from_env(),
        )
    return _grafana_exporter_instance
