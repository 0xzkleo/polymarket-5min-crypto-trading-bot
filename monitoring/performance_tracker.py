"""
monitoring.performance_tracker
================================
In-memory trade history with full ROI and risk analytics.

Methods exposed to GrafanaMetricsExporter
------------------------------------------
calculate_metrics()           → PerformanceMetrics (cached, invalidated on new trade)
calculate_sortino_ratio()     → float
calculate_calmar_ratio()      → float
calculate_kelly_fraction()    → float
calculate_recovery_factor()   → float
get_rolling_roi(days)         → float  (e.g. days=1/7/30)
get_streak_info()             → dict   {current_wins, current_losses, max_wins, max_losses}
calculate_pnl_variance()      → float
calculate_avg_trades_per_day()→ float
get_win_loss_distribution()   → dict
get_trade_history()           → List[Trade]
get_equity_curve()            → List[dict]
get_daily_pnl(days)           → List[dict]
export_for_grafana()          → dict
"""
import asyncio
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from collections import deque
from loguru import logger


@dataclass
class Trade:
    """Individual trade record."""
    trade_id: str
    timestamp: datetime
    direction: str          # "long" or "short"
    entry_price: Decimal
    exit_price: Decimal
    size: Decimal
    pnl: Decimal
    pnl_pct: float
    duration_seconds: float
    signal_score: float
    signal_confidence: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PerformanceMetrics:
    """Performance metrics snapshot."""
    timestamp: datetime

    # P&L metrics
    total_pnl: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal

    # Trade statistics
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float

    # Return metrics
    roi: float
    sharpe_ratio: float
    max_drawdown: float

    # Position metrics
    open_positions: int
    avg_position_size: Decimal
    avg_hold_time: float        # seconds

    # Risk metrics
    total_exposure: Decimal
    risk_utilization: float

    # Signal performance
    avg_signal_score: float
    avg_signal_confidence: float


class PerformanceTracker:
    """
    Tracks and analyses trading performance.

    All monetary amounts are in USD (Decimal for precision).
    """

    def __init__(self, initial_capital: Decimal = Decimal("1000.0")):
        self.initial_capital = initial_capital
        self.current_capital = initial_capital

        self._trades: List[Trade] = []
        self._max_trades_history = 1000

        # Metrics history for Grafana time-series
        self._metrics_history: deque = deque(maxlen=10000)

        # Cache
        self._last_metrics: Optional[PerformanceMetrics] = None
        self._metrics_dirty = True

        # Peak tracking for drawdown
        self._peak_capital = initial_capital

        self._start_time = datetime.now(timezone.utc)
        logger.info(f"Initialized Performance Tracker (capital=${initial_capital})")

    # ──────────────────────────────────────────────────────────────────────────
    # Trade recording
    # ──────────────────────────────────────────────────────────────────────────

    def record_trade(
        self,
        trade_id: str,
        direction: str,
        entry_price: Decimal,
        exit_price: Decimal,
        size: Decimal,
        entry_time: datetime,
        exit_time: datetime,
        signal_score: float = 0.0,
        signal_confidence: float = 0.0,
        metadata: Dict[str, Any] = None,
    ) -> Trade:
        if direction == "long":
            pnl_pct = (exit_price - entry_price) / entry_price
        else:
            pnl_pct = (entry_price - exit_price) / entry_price

        pnl = size * pnl_pct
        duration = (exit_time - entry_time).total_seconds()

        trade = Trade(
            trade_id=trade_id,
            timestamp=exit_time,
            direction=direction,
            entry_price=entry_price,
            exit_price=exit_price,
            size=size,
            pnl=pnl,
            pnl_pct=float(pnl_pct),
            duration_seconds=duration,
            signal_score=signal_score,
            signal_confidence=signal_confidence,
            metadata=metadata or {},
        )

        self._trades.append(trade)
        if len(self._trades) > self._max_trades_history:
            self._trades.pop(0)

        self.current_capital += pnl
        if self.current_capital > self._peak_capital:
            self._peak_capital = self.current_capital

        self._metrics_dirty = True
        logger.info(
            f"Recorded trade: {trade_id} {direction.upper()} "
            f"P&L=${pnl:+.2f} ({pnl_pct:+.2%})"
        )
        return trade

    # ──────────────────────────────────────────────────────────────────────────
    # Core metrics (cached)
    # ──────────────────────────────────────────────────────────────────────────

    def calculate_metrics(self, force: bool = False) -> PerformanceMetrics:
        if not force and not self._metrics_dirty and self._last_metrics:
            return self._last_metrics

        total_pnl = self.current_capital - self.initial_capital
        total_trades = len(self._trades)
        winning_trades = len([t for t in self._trades if t.pnl > 0])
        losing_trades = len([t for t in self._trades if t.pnl < 0])
        win_rate = winning_trades / total_trades if total_trades > 0 else 0.0
        roi = float(total_pnl / self.initial_capital) if self.initial_capital else 0.0
        sharpe = self._calculate_sharpe_ratio()
        max_dd = (
            float((self._peak_capital - self.current_capital) / self._peak_capital)
            if self._peak_capital > 0
            else 0.0
        )

        if total_trades > 0:
            avg_size = sum(t.size for t in self._trades) / total_trades
            avg_hold = sum(t.duration_seconds for t in self._trades) / total_trades
            avg_score = sum(t.signal_score for t in self._trades) / total_trades
            avg_conf = sum(t.signal_confidence for t in self._trades) / total_trades
        else:
            avg_size = Decimal("0")
            avg_hold = avg_score = avg_conf = 0.0

        metrics = PerformanceMetrics(
            timestamp=datetime.now(timezone.utc),
            total_pnl=total_pnl,
            realized_pnl=total_pnl,
            unrealized_pnl=Decimal("0"),
            total_trades=total_trades,
            winning_trades=winning_trades,
            losing_trades=losing_trades,
            win_rate=win_rate,
            roi=roi,
            sharpe_ratio=sharpe,
            max_drawdown=max_dd,
            open_positions=0,
            avg_position_size=avg_size,
            avg_hold_time=avg_hold,
            total_exposure=Decimal("0"),
            risk_utilization=0.0,
            avg_signal_score=avg_score,
            avg_signal_confidence=avg_conf,
        )

        self._last_metrics = metrics
        self._metrics_dirty = False
        self._metrics_history.append(metrics)
        return metrics

    # ──────────────────────────────────────────────────────────────────────────
    # Risk-adjusted return calculations
    # ──────────────────────────────────────────────────────────────────────────

    def _calculate_sharpe_ratio(self, risk_free_rate: float = 0.02) -> float:
        if len(self._trades) < 2:
            return 0.0
        returns = [float(t.pnl / t.size) for t in self._trades if t.size > 0]
        if not returns:
            return 0.0
        mean_r = sum(returns) / len(returns)
        variance = sum((r - mean_r) ** 2 for r in returns) / len(returns)
        std_r = variance ** 0.5
        if std_r == 0:
            return 0.0
        return (mean_r - risk_free_rate / 252) / std_r * (252 ** 0.5)

    def calculate_sortino_ratio(self, risk_free_rate: float = 0.02) -> float:
        """Sortino ratio — penalises only downside deviation."""
        if len(self._trades) < 2:
            return 0.0
        returns = [float(t.pnl / t.size) for t in self._trades if t.size > 0]
        if not returns:
            return 0.0
        mean_r = sum(returns) / len(returns)
        neg_returns = [r for r in returns if r < 0]
        if not neg_returns:
            return 10.0  # no losses → very good, cap at 10
        downside_var = sum(r ** 2 for r in neg_returns) / len(neg_returns)
        downside_std = downside_var ** 0.5
        if downside_std == 0:
            return 10.0
        target = risk_free_rate / 252
        return (mean_r - target) / downside_std * (252 ** 0.5)

    def calculate_calmar_ratio(self) -> float:
        """Calmar ratio = annualised ROI / max drawdown."""
        if not self._trades:
            return 0.0
        metrics = self.calculate_metrics()
        if metrics.max_drawdown == 0:
            return 0.0 if metrics.roi <= 0 else 10.0
        # Annualise ROI by trading days elapsed
        days = self._trading_days_elapsed()
        annualised_roi = (metrics.roi / max(days, 1)) * 252
        return annualised_roi / metrics.max_drawdown

    def calculate_kelly_fraction(self) -> float:
        """Kelly criterion: W - (1-W)/R, clamped to [0, 0.5]."""
        dist = self.get_win_loss_distribution()
        wins = dist["wins"]
        losses = dist["losses"]
        if not wins["count"] or not losses["count"]:
            return 0.0
        win_rate = wins["count"] / (wins["count"] + losses["count"])
        if losses["avg"] == 0:
            return 0.5
        reward_risk = abs(wins["avg"] / losses["avg"])
        kelly = win_rate - (1.0 - win_rate) / reward_risk
        return max(0.0, min(0.5, kelly))

    def calculate_recovery_factor(self) -> float:
        """Recovery factor = net P&L / max drawdown USD."""
        net_pnl = float(self.current_capital - self.initial_capital)
        max_dd_usd = float(self._peak_capital - self.current_capital)
        if max_dd_usd <= 0:
            return 0.0 if net_pnl <= 0 else 10.0
        return net_pnl / max_dd_usd

    # ──────────────────────────────────────────────────────────────────────────
    # Rolling ROI
    # ──────────────────────────────────────────────────────────────────────────

    def get_rolling_roi(self, days: int = 1) -> float:
        """Return on investment over the last *days* calendar days."""
        if not self._trades:
            return 0.0
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        def _ts_aware(ts: datetime) -> datetime:
            return ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)
        recent = [t for t in self._trades if _ts_aware(t.timestamp) >= cutoff]
        if not recent:
            return 0.0
        period_pnl = sum(t.pnl for t in recent)
        # Use the capital at the start of the window as denominator
        capital_at_start = self.current_capital - period_pnl
        if capital_at_start <= 0:
            return 0.0
        return float(period_pnl / capital_at_start)

    # ──────────────────────────────────────────────────────────────────────────
    # Streak tracking
    # ──────────────────────────────────────────────────────────────────────────

    def get_streak_info(self) -> Dict[str, int]:
        """Current and maximum consecutive win/loss streaks."""
        if not self._trades:
            return {"current_wins": 0, "current_losses": 0,
                    "max_wins": 0, "max_losses": 0}

        current_wins = current_losses = 0
        max_wins = max_losses = 0
        cur_w = cur_l = 0

        for trade in self._trades:
            if trade.pnl > 0:
                cur_w += 1
                cur_l = 0
            elif trade.pnl < 0:
                cur_l += 1
                cur_w = 0
            else:
                cur_w = cur_l = 0
            max_wins = max(max_wins, cur_w)
            max_losses = max(max_losses, cur_l)

        # "current" = the streak at the tail of the trade list
        current_wins = cur_w
        current_losses = cur_l

        return {
            "current_wins": current_wins,
            "current_losses": current_losses,
            "max_wins": max_wins,
            "max_losses": max_losses,
        }

    # ──────────────────────────────────────────────────────────────────────────
    # Variance and cadence
    # ──────────────────────────────────────────────────────────────────────────

    def calculate_pnl_variance(self) -> float:
        """Population variance of per-trade P&L in USD."""
        if len(self._trades) < 2:
            return 0.0
        pnls = [float(t.pnl) for t in self._trades]
        mean = sum(pnls) / len(pnls)
        return sum((p - mean) ** 2 for p in pnls) / len(pnls)

    def calculate_avg_trades_per_day(self) -> float:
        """Average number of trades per calendar day since first trade."""
        if not self._trades:
            return 0.0
        days = max(self._trading_days_elapsed(), 1)
        return len(self._trades) / days

    def _trading_days_elapsed(self) -> float:
        if not self._trades:
            return 1.0
        first = min(t.timestamp for t in self._trades)
        now = datetime.now(timezone.utc)
        # Make first tz-aware if it isn't (defensive — Nautilus uses aware datetimes)
        if first.tzinfo is None:
            first = first.replace(tzinfo=timezone.utc)
        return max((now - first).total_seconds() / 86400, 1.0)

    # ──────────────────────────────────────────────────────────────────────────
    # Distribution
    # ──────────────────────────────────────────────────────────────────────────

    def get_win_loss_distribution(self) -> Dict[str, Any]:
        wins = [t.pnl for t in self._trades if t.pnl > 0]
        losses = [t.pnl for t in self._trades if t.pnl < 0]
        return {
            "total_trades": len(self._trades),
            "wins": {
                "count": len(wins),
                "total": float(sum(wins)) if wins else 0.0,
                "avg": float(sum(wins) / len(wins)) if wins else 0.0,
                "max": float(max(wins)) if wins else 0.0,
            },
            "losses": {
                "count": len(losses),
                "total": float(sum(losses)) if losses else 0.0,
                "avg": float(sum(losses) / len(losses)) if losses else 0.0,
                "max": float(min(losses)) if losses else 0.0,
            },
            "profit_factor": (
                abs(float(sum(wins) / sum(losses)))
                if losses and sum(losses) != 0
                else 0.0
            ),
        }

    # ──────────────────────────────────────────────────────────────────────────
    # Historical queries
    # ──────────────────────────────────────────────────────────────────────────

    def get_trade_history(
        self,
        limit: int = 100,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[Trade]:
        def _aw(ts: datetime) -> datetime:
            return ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)
        trades = self._trades
        if start_date:
            sd = start_date if start_date.tzinfo else start_date.replace(tzinfo=timezone.utc)
            trades = [t for t in trades if _aw(t.timestamp) >= sd]
        if end_date:
            ed = end_date if end_date.tzinfo else end_date.replace(tzinfo=timezone.utc)
            trades = [t for t in trades if _aw(t.timestamp) <= ed]
        return trades[-limit:]

    def get_equity_curve(self) -> List[Dict[str, Any]]:
        curve = [{
            "timestamp": self._trades[0].timestamp if self._trades else datetime.now(timezone.utc),
            "equity": float(self.initial_capital),
        }]
        running = self.initial_capital
        for trade in self._trades:
            running += trade.pnl
            curve.append({"timestamp": trade.timestamp, "equity": float(running)})
        return curve

    def get_daily_pnl(self, days: int = 30) -> List[Dict[str, Any]]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        def _aw(ts: datetime) -> datetime:
            return ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)
        recent = [t for t in self._trades if _aw(t.timestamp) >= cutoff]
        daily: Dict[str, Decimal] = {}
        for trade in recent:
            key = trade.timestamp.strftime("%Y-%m-%d")
            daily[key] = daily.get(key, Decimal("0")) + trade.pnl
        return [
            {"date": day, "pnl": float(pnl)}
            for day, pnl in sorted(daily.items())
        ]

    # ──────────────────────────────────────────────────────────────────────────
    # Grafana export helper
    # ──────────────────────────────────────────────────────────────────────────

    def export_for_grafana(self) -> Dict[str, Any]:
        metrics = self.calculate_metrics()
        dist = self.get_win_loss_distribution()
        streaks = self.get_streak_info()
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "metrics": {
                "total_pnl": float(metrics.total_pnl),
                "roi": metrics.roi * 100,
                "daily_roi": self.get_rolling_roi(1) * 100,
                "weekly_roi": self.get_rolling_roi(7) * 100,
                "monthly_roi": self.get_rolling_roi(30) * 100,
                "win_rate": metrics.win_rate * 100,
                "sharpe_ratio": metrics.sharpe_ratio,
                "sortino_ratio": self.calculate_sortino_ratio(),
                "calmar_ratio": self.calculate_calmar_ratio(),
                "kelly_fraction": self.calculate_kelly_fraction(),
                "max_drawdown": metrics.max_drawdown * 100,
                "total_trades": metrics.total_trades,
                "current_capital": float(self.current_capital),
                "avg_win": dist["wins"]["avg"],
                "avg_loss": dist["losses"]["avg"],
                "profit_factor": dist["profit_factor"],
                "consecutive_wins": streaks["current_wins"],
                "consecutive_losses": streaks["current_losses"],
            },
            "equity_curve": self.get_equity_curve(),
            "daily_pnl": self.get_daily_pnl(30),
        }


# Singleton instance
_performance_tracker_instance = None


def get_performance_tracker() -> PerformanceTracker:
    global _performance_tracker_instance
    if _performance_tracker_instance is None:
        _performance_tracker_instance = PerformanceTracker()
    return _performance_tracker_instance
