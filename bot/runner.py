"""
bot.runner — Node construction and top-level ``run_integrated_bot`` entry point.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv()

from loguru import logger
import redis

# Apply patches BEFORE importing Nautilus
from patches.gamma_markets import apply_gamma_markets_patch, verify_patch

_patch_applied = apply_gamma_markets_patch()
if _patch_applied:
    verify_patch()
else:
    print("ERROR: Failed to apply gamma_markets patch")
    sys.exit(1)

from nautilus_trader.config import (
    InstrumentProviderConfig,
    LiveDataEngineConfig,
    LiveExecEngineConfig,
    LiveRiskEngineConfig,
    LoggingConfig,
    TradingNodeConfig,
)
from nautilus_trader.live.node import TradingNode
from nautilus_trader.adapters.polymarket import POLYMARKET
from nautilus_trader.adapters.polymarket import (
    PolymarketDataClientConfig,
    PolymarketExecClientConfig,
)
from nautilus_trader.adapters.polymarket.factories import (
    PolymarketLiveDataClientFactory,
    PolymarketLiveExecClientFactory,
)

from patches.market_orders import apply_market_order_patch

_mo_patch = apply_market_order_patch()
if _mo_patch:
    logger.info("Market order patch applied successfully")
else:
    logger.warning("Market order patch failed — orders may be rejected")


def init_redis():
    """Initialise Redis connection for live simulation-mode control."""
    try:
        client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            db=int(os.getenv("REDIS_DB", 2)),
            decode_responses=True,
            socket_connect_timeout=5,
            socket_keepalive=True,
        )
        client.ping()
        logger.info("Redis connection established")
        return client
    except Exception as e:
        logger.warning(f"Redis connection failed: {e}")
        logger.warning("Simulation mode will be static (from .env)")
        return None


def run_integrated_bot(
    simulation: bool = False,
    enable_grafana: bool = True,
    test_mode: bool = False,
) -> None:
    """
    Build and run the integrated BTC 15-min Polymarket trading bot.

    Parameters
    ----------
    simulation:
        When *True* no real orders are placed; trades are paper-logged.
    enable_grafana:
        Start the Prometheus metrics exporter thread on port 8000.
    test_mode:
        Accelerated simulation (trade every minute, 5-min learning cycle).
    """
    print("=" * 80)
    print("INTEGRATED POLYMARKET BTC 15-MIN TRADING BOT")
    print("Nautilus + 7-Phase System + Redis Control")
    print("=" * 80)

    redis_client = init_redis()

    if redis_client:
        try:
            mode_value = "1" if simulation else "0"
            redis_client.set("btc_trading:simulation_mode", mode_value)
            mode_label = "SIMULATION" if simulation else "LIVE"
            logger.info(f"Redis simulation_mode forced to: {mode_label} ({mode_value})")
        except Exception as e:
            logger.warning(f"Could not set Redis simulation mode: {e}")

    print(f"\nConfiguration:")
    print(f"  Initial Mode: {'SIMULATION' if simulation else 'LIVE TRADING'}")
    print(f"  Redis Control: {'Enabled' if redis_client else 'Disabled'}")
    print(f"  Grafana: {'Enabled' if enable_grafana else 'Disabled'}")
    print(f"  Max Trade Size: ${os.getenv('MARKET_BUY_USD', '1.00')}")
    print()

    now = datetime.now(timezone.utc)
    unix_interval_start = (int(now.timestamp()) // 900) * 900

    btc_slugs = []
    for i in range(-1, 97):
        timestamp = unix_interval_start + (i * 900)
        btc_slugs.append(f"btc-updown-15m-{timestamp}")

    filters = {
        "active": True,
        "closed": False,
        "archived": False,
        "slug": tuple(btc_slugs),
        "limit": 100,
    }

    logger.info("=" * 80)
    logger.info("LOADING BTC 15-MIN MARKETS BY SLUG")
    logger.info(f"  Interval start: {unix_interval_start} | Count: {len(btc_slugs)}")
    logger.info(f"  First: {btc_slugs[0]}  Last: {btc_slugs[-1]}")
    logger.info("=" * 80)

    instrument_cfg = InstrumentProviderConfig(
        load_all=True,
        filters=filters,
        use_gamma_markets=True,
    )

    poly_data_cfg = PolymarketDataClientConfig(
        private_key=os.getenv("POLYMARKET_PK"),
        api_key=os.getenv("POLYMARKET_API_KEY"),
        api_secret=os.getenv("POLYMARKET_API_SECRET"),
        passphrase=os.getenv("POLYMARKET_PASSPHRASE"),
        signature_type=1,
        instrument_provider=instrument_cfg,
    )

    poly_exec_cfg = PolymarketExecClientConfig(
        private_key=os.getenv("POLYMARKET_PK"),
        api_key=os.getenv("POLYMARKET_API_KEY"),
        api_secret=os.getenv("POLYMARKET_API_SECRET"),
        passphrase=os.getenv("POLYMARKET_PASSPHRASE"),
        signature_type=1,
        instrument_provider=instrument_cfg,
    )

    config = TradingNodeConfig(
        environment="live",
        trader_id="BTC-15MIN-INTEGRATED-001",
        logging=LoggingConfig(
            log_level="INFO",
            log_directory="./logs/nautilus",
        ),
        data_engine=LiveDataEngineConfig(qsize=6000),
        exec_engine=LiveExecEngineConfig(qsize=6000),
        risk_engine=LiveRiskEngineConfig(bypass=simulation),
        data_clients={POLYMARKET: poly_data_cfg},
        exec_clients={POLYMARKET: poly_exec_cfg},
    )

    from bot.strategy import IntegratedBTCStrategy

    strategy = IntegratedBTCStrategy(
        redis_client=redis_client,
        enable_grafana=enable_grafana,
        test_mode=test_mode,
    )

    print("\nBuilding Nautilus node...")
    node = TradingNode(config=config)
    node.add_data_client_factory(POLYMARKET, PolymarketLiveDataClientFactory)
    node.add_exec_client_factory(POLYMARKET, PolymarketLiveExecClientFactory)
    node.trader.add_strategy(strategy)
    node.build()
    logger.info("Nautilus node built successfully")

    print()
    print("=" * 80)
    print("BOT STARTING")
    print("=" * 80)

    try:
        node.run()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        node.dispose()
        logger.info("Bot stopped")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Integrated BTC 15-Min Trading Bot")
    parser.add_argument(
        "--live", action="store_true",
        help="Run in LIVE mode (real money at risk!). Default is simulation.",
    )
    parser.add_argument("--no-grafana", action="store_true", help="Disable Grafana metrics")
    parser.add_argument(
        "--test-mode", action="store_true",
        help="Run in TEST MODE (trade every minute for faster testing)",
    )

    args = parser.parse_args()
    enable_grafana = not args.no_grafana
    test_mode = args.test_mode
    simulation = True if args.test_mode else (not args.live)

    if not simulation:
        logger.warning("=" * 80)
        logger.warning("LIVE TRADING MODE — REAL MONEY AT RISK!")
        logger.warning("=" * 80)
    else:
        logger.info("=" * 80)
        logger.info(
            f"SIMULATION MODE — {'TEST MODE (fast clock)' if test_mode else 'paper trading only'}"
        )
        logger.info("No real orders will be placed.")
        logger.info("=" * 80)

    run_integrated_bot(
        simulation=simulation,
        enable_grafana=enable_grafana,
        test_mode=test_mode,
    )


if __name__ == "__main__":
    main()
