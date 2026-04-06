"""
ML Prediction Engine
=====================
XGBoost model that consumes all 8 features and outputs a calibrated probability
that BTC will be HIGHER at the next Chainlink settlement.

FEATURE VECTOR (assembled from all processors):
  From OHLCV:          rsi, macd_line, macd_signal, pct_b, ret1, ret3, ret5, ret15, vol_regime_encoded
  From CVD+OB:         cvd_delta_norm, ob_imbalance
  From Funding+OI:     funding_rate, oi_change
  From Liquidations:   liq_imbalance, liq_total_norm
  From TickVelocity:   tick_vel_60s, tick_vel_30s
  From OrderBook:      poly_ob_imbalance
  From Divergence:     spot_momentum, poly_prob
  From TimeOfDay:      hour_sin, hour_cos, is_ny_open, is_asia_open, is_dead_zone

OUTPUT:
  p_up: float [0, 1] — calibrated probability BTC price will be higher at settlement

USAGE FLOW:
  1. Every 15 min: collect feature vector → model.predict_proba(features) → p_up
  2. Compare p_up vs Polymarket YES price → if edge > threshold → bet
  3. After settlement: record (features, outcome) to training DB
  4. Weekly: retrain model on accumulated outcomes

TRAINING:
  - Minimum 200 samples before model activates (falls back to signal fusion before then)
  - Features saved to SQLite: feature_store.db (trades table)
  - Weekly retraining triggered automatically or via retrain_now()
  - Calibration via Platt scaling (CalibratedClassifierCV)

BETTING EDGE FILTER:
  Only bet when: abs(p_up - polymarket_price) > MIN_EDGE
  MIN_EDGE = 0.07 (7%) — if model says 65% but market says 60%, that's only 5% edge → skip
  The edge needs to exceed the ~2% Polymarket fee + noise margin.
"""

import os
import json
import pickle
import sqlite3
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple
import numpy as np
from loguru import logger

# XGBoost + sklearn — installed via: pip install xgboost scikit-learn
try:
    import xgboost as xgb
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import brier_score_loss, roc_auc_score
    ML_AVAILABLE = True
except ImportError:
    ML_AVAILABLE = False
    logger.warning("XGBoost/sklearn not installed — ML model disabled. Run: pip install xgboost scikit-learn")


MODEL_PATH   = "ml_model.pkl"
DB_PATH      = "feature_store.db"
MIN_SAMPLES  = 200    # minimum records before model activates
MIN_EDGE     = 0.07   # minimum model vs market probability gap to bet
RETRAIN_DAYS = 7      # retrain weekly


FEATURE_NAMES = [
    # OHLCV / Momentum
    "rsi", "macd_line", "macd_signal", "pct_b",
    "ret1", "ret3", "ret5", "ret15",
    "vol_regime",          # 0=LOW, 1=NORMAL, 2=HIGH
    # CVD + OB
    "cvd_delta_norm",      # CVD delta / 1M
    "ob_imbalance",        # Binance spot OB imbalance
    # Funding + OI
    "funding_rate",
    "oi_change",
    # Liquidations
    "liq_imbalance",       # (long_liq - short_liq) / total
    "liq_total_norm",      # total liq USD / 1M
    # Tick Velocity (Polymarket)
    "tick_vel_60s",
    "tick_vel_30s",
    # Polymarket OB
    "poly_ob_imbalance",
    # Price divergence
    "spot_momentum",
    "poly_prob",           # current Polymarket YES price
    # Time of day
    "hour_sin",
    "hour_cos",
    "is_ny_open",
    "is_asia_open",
    "is_dead_zone",
]


def _vol_regime_to_int(regime: str) -> int:
    return {"LOW": 0, "NORMAL": 1, "HIGH": 2}.get(regime, 1)


def _time_features(dt: datetime) -> Dict[str, float]:
    hour = dt.hour + dt.minute / 60.0
    return {
        "hour_sin": np.sin(2 * np.pi * hour / 24),
        "hour_cos": np.cos(2 * np.pi * hour / 24),
        "is_ny_open":   float(13 <= dt.hour < 16),
        "is_asia_open": float(dt.hour in {0, 1}),
        "is_dead_zone": float(6 <= dt.hour < 10),
    }


class MLPredictionEngine:
    """
    XGBoost-based probability model for Polymarket BTC 15-min markets.
    Thread-safe — model reads can happen concurrently with background retraining.
    """

    def __init__(
        self,
        model_path: str = MODEL_PATH,
        db_path: str = DB_PATH,
        min_edge: float = MIN_EDGE,
        min_samples: int = MIN_SAMPLES,
        retrain_days: int = RETRAIN_DAYS,
    ):
        self.model_path = model_path
        self.db_path = db_path
        self.min_edge = min_edge
        self.min_samples = min_samples
        self.retrain_days = retrain_days

        self._model = None
        self._model_lock = threading.RLock()
        self._last_retrain: Optional[datetime] = None
        self._sample_count = 0

        self._init_db()
        self._load_model()
        self._refresh_sample_count()

        logger.info(
            f"Initialized ML Prediction Engine: "
            f"samples={self._sample_count}, "
            f"model_active={self.is_active}, "
            f"min_edge={min_edge:.0%}"
        )

    # ── Database ─────────────────────────────────────────────────────────────

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS trades (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp     TEXT NOT NULL,
                    market_slug   TEXT,
                    poly_price    REAL NOT NULL,
                    {', '.join(f'{f} REAL' for f in FEATURE_NAMES)},
                    outcome       INTEGER,          -- 1=UP won, 0=DOWN won, NULL=pending
                    chainlink_entry REAL,
                    chainlink_exit  REAL,
                    created_at    TEXT DEFAULT (datetime('now'))
                )
            """)
            conn.commit()
        logger.debug(f"Feature store DB ready: {self.db_path}")

    def _refresh_sample_count(self) -> None:
        try:
            with sqlite3.connect(self.db_path) as conn:
                row = conn.execute(
                    "SELECT COUNT(*) FROM trades WHERE outcome IS NOT NULL"
                ).fetchone()
                self._sample_count = row[0] if row else 0
        except Exception:
            self._sample_count = 0

    # ── Model persistence ─────────────────────────────────────────────────────

    def _load_model(self) -> None:
        if not ML_AVAILABLE:
            return
        if os.path.exists(self.model_path):
            try:
                with open(self.model_path, "rb") as f:
                    self._model = pickle.load(f)
                logger.info(f"Loaded ML model from {self.model_path}")
            except Exception as e:
                logger.warning(f"Could not load ML model: {e}")

    def _save_model(self) -> None:
        if self._model is None:
            return
        try:
            with open(self.model_path, "wb") as f:
                pickle.dump(self._model, f)
            logger.info(f"ML model saved to {self.model_path}")
        except Exception as e:
            logger.warning(f"Could not save ML model: {e}")

    # ── Feature collection ────────────────────────────────────────────────────

    def build_feature_vector(self, metadata: Dict[str, Any], poly_price: float) -> Optional[np.ndarray]:
        """
        Build the feature vector from the assembled metadata dict.
        Returns None if critical features are missing.
        """
        now = datetime.now(timezone.utc)
        time_feats = _time_features(now)

        def g(key, default=0.0):
            val = metadata.get(key, default)
            return float(val) if val is not None else default

        try:
            features = {
                "rsi":             g("rsi", 50.0),
                "macd_line":       g("macd_line", 0.0),
                "macd_signal":     g("macd_signal", 0.0),
                "pct_b":           g("pct_b", 0.5),
                "ret1":            g("ret1", 0.0),
                "ret3":            g("ret3", 0.0),
                "ret5":            g("ret5", 0.0),
                "ret15":           g("ret15", g("momentum", 0.0)),
                "vol_regime":      float(_vol_regime_to_int(metadata.get("vol_regime", "NORMAL"))),
                "cvd_delta_norm":  g("cvd_delta_usd", 0.0) / 1_000_000,
                "ob_imbalance":    g("ob_imbalance", 0.0),
                "funding_rate":    g("funding_rate", 0.0),
                "oi_change":       g("oi_change", 0.0),
                "liq_imbalance":   g("liq_imbalance", 0.0),
                "liq_total_norm":  g("liq_total_usd", 0.0) / 1_000_000,
                "tick_vel_60s":    g("velocity_60s", 0.0),
                "tick_vel_30s":    g("velocity_30s", 0.0),
                "poly_ob_imbalance": g("poly_ob_imbalance", 0.0),
                "spot_momentum":   g("spot_momentum", g("momentum", 0.0)),
                "poly_prob":       poly_price,
                **time_feats,
            }

            vec = np.array([features[f] for f in FEATURE_NAMES], dtype=np.float32)
            return vec

        except Exception as e:
            logger.warning(f"Feature vector build failed: {e}")
            return None

    # ── Prediction ────────────────────────────────────────────────────────────

    @property
    def is_active(self) -> bool:
        return ML_AVAILABLE and self._model is not None and self._sample_count >= self.min_samples

    def predict(self, feature_vector: np.ndarray) -> Optional[float]:
        """Return p(BTC goes UP) or None if model not ready."""
        if not self.is_active:
            return None
        with self._model_lock:
            try:
                proba = self._model.predict_proba(feature_vector.reshape(1, -1))[0]
                p_up = float(proba[1])
                return p_up
            except Exception as e:
                logger.warning(f"ML prediction failed: {e}")
                return None

    def should_bet(
        self,
        p_up: float,
        poly_price: float,
    ) -> Tuple[bool, str, float]:
        """
        Determine whether to bet and in which direction.

        Returns:
            (should_bet, direction, edge)
            direction: "long" (YES) or "short" (NO) or ""
            edge: model_prob - market_price (signed)
        """
        edge_up   = p_up - poly_price          # positive = model more bullish than market
        edge_down = (1 - p_up) - (1 - poly_price)  # = -(edge_up)

        if edge_up > self.min_edge:
            return True, "long", edge_up
        elif edge_down > self.min_edge:
            return True, "short", edge_down
        return False, "", 0.0

    # ── Recording outcomes ────────────────────────────────────────────────────

    def record_trade(
        self,
        market_slug: str,
        poly_price: float,
        feature_vector: np.ndarray,
        chainlink_entry: Optional[float] = None,
    ) -> Optional[int]:
        """
        Save a trade record (outcome=NULL until settlement).
        Returns the row ID for later outcome update.
        """
        try:
            feat_dict = dict(zip(FEATURE_NAMES, feature_vector.tolist()))
            cols = ["timestamp", "market_slug", "poly_price", "chainlink_entry"] + FEATURE_NAMES
            vals = [
                datetime.now(timezone.utc).isoformat(),
                market_slug,
                poly_price,
                chainlink_entry,
            ] + [feat_dict[f] for f in FEATURE_NAMES]

            placeholders = ", ".join(["?"] * len(cols))
            col_str = ", ".join(cols)

            with sqlite3.connect(self.db_path) as conn:
                cur = conn.execute(
                    f"INSERT INTO trades ({col_str}) VALUES ({placeholders})",
                    vals,
                )
                conn.commit()
                return cur.lastrowid
        except Exception as e:
            logger.warning(f"Failed to record trade: {e}")
            return None

    def record_outcome(
        self,
        trade_id: int,
        chainlink_entry: float,
        chainlink_exit: float,
        outcome: int,   # 1 = BTC went up, 0 = BTC went down
    ) -> None:
        """
        Update a previously recorded trade with settlement outcome.
        Call this after Chainlink settles the market.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "UPDATE trades SET outcome=?, chainlink_entry=?, chainlink_exit=? WHERE id=?",
                    (outcome, chainlink_entry, chainlink_exit, trade_id),
                )
                conn.commit()
            self._sample_count += 1
            logger.info(
                f"Recorded outcome for trade {trade_id}: "
                f"{'UP' if outcome == 1 else 'DOWN'} "
                f"(entry=${chainlink_entry:.2f}, exit=${chainlink_exit:.2f})"
            )
        except Exception as e:
            logger.warning(f"Failed to record outcome: {e}")

    # ── Training ──────────────────────────────────────────────────────────────

    def retrain_now(self) -> bool:
        """
        Retrain the XGBoost model on all recorded outcomes.
        Returns True if training succeeded.
        Should be called weekly (or manually).
        """
        if not ML_AVAILABLE:
            logger.warning("XGBoost not installed — cannot retrain")
            return False

        self._refresh_sample_count()
        if self._sample_count < self.min_samples:
            logger.warning(
                f"Not enough samples to retrain: {self._sample_count} < {self.min_samples}"
            )
            return False

        try:
            # Load all labelled samples
            with sqlite3.connect(self.db_path) as conn:
                rows = conn.execute(
                    f"SELECT {', '.join(FEATURE_NAMES)}, outcome FROM trades "
                    f"WHERE outcome IS NOT NULL ORDER BY id"
                ).fetchall()

            if len(rows) < self.min_samples:
                return False

            X = np.array([r[:-1] for r in rows], dtype=np.float32)
            y = np.array([r[-1] for r in rows], dtype=np.int32)

            # Time-based train/test split (no random shuffle — preserve time order)
            split = int(len(X) * 0.80)
            X_train, X_test = X[:split], X[split:]
            y_train, y_test = y[:split], y[split:]

            logger.info(f"Training XGBoost on {len(X_train)} samples, testing on {len(X_test)}")

            base_model = xgb.XGBClassifier(
                n_estimators=300,
                max_depth=4,
                learning_rate=0.05,
                subsample=0.80,
                colsample_bytree=0.80,
                use_label_encoder=False,
                eval_metric="logloss",
                random_state=42,
                n_jobs=-1,
            )

            # Calibrate probabilities with Platt scaling
            calibrated = CalibratedClassifierCV(base_model, method="sigmoid", cv=3)
            calibrated.fit(X_train, y_train)

            # Evaluate
            y_pred_proba = calibrated.predict_proba(X_test)[:, 1]
            brier = brier_score_loss(y_test, y_pred_proba)
            auc = roc_auc_score(y_test, y_pred_proba)

            logger.info(f"ML model metrics — Brier={brier:.4f}, AUC={auc:.4f}")

            # Feature importance
            try:
                importances = base_model.estimators_[0].feature_importances_ if hasattr(base_model, 'estimators_') else calibrated.estimator.feature_importances_
                top_features = sorted(
                    zip(FEATURE_NAMES, importances),
                    key=lambda x: x[1],
                    reverse=True,
                )[:5]
                logger.info(f"Top features: {[(f, round(i, 3)) for f, i in top_features]}")
            except Exception:
                pass

            with self._model_lock:
                self._model = calibrated
            self._save_model()
            self._last_retrain = datetime.now(timezone.utc)

            logger.info(f"✓ ML model retrained on {len(X_train)} samples. AUC={auc:.4f}")
            return True

        except Exception as e:
            logger.error(f"ML retraining failed: {e}")
            import traceback
            traceback.print_exc()
            return False

    def maybe_retrain(self) -> None:
        """Retrain if it's been more than retrain_days since last training."""
        if self._last_retrain is None:
            self.retrain_now()
        elif (datetime.now(timezone.utc) - self._last_retrain).days >= self.retrain_days:
            logger.info(f"Scheduled weekly retrain triggered")
            self.retrain_now()

    def get_stats(self) -> Dict[str, Any]:
        return {
            "is_active": self.is_active,
            "sample_count": self._sample_count,
            "min_samples": self.min_samples,
            "last_retrain": self._last_retrain.isoformat() if self._last_retrain else None,
            "model_path": self.model_path,
        }


# Singleton
_ml_engine_instance: Optional[MLPredictionEngine] = None

def get_ml_engine() -> MLPredictionEngine:
    global _ml_engine_instance
    if _ml_engine_instance is None:
        _ml_engine_instance = MLPredictionEngine()
    return _ml_engine_instance