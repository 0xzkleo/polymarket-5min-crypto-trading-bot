"""core.nautilus.instruments — BTC instrument definitions for NautilusTrader."""
from decimal import Decimal
from typing import Optional

from loguru import logger
from nautilus_trader.model.currencies import BTC, USDC
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.objects import Price, Quantity


def create_btc_polymarket_instrument() -> CryptoPerpetual:
    """BTC prediction market instrument for Polymarket (modelled as a perpetual)."""
    instrument = CryptoPerpetual(
        instrument_id=InstrumentId(Symbol("BTC-POLYMARKET"), Venue("POLYMARKET")),
        raw_symbol=Symbol("BTC-POLYMARKET"),
        base_currency=BTC,
        quote_currency=USDC,
        settlement_currency=USDC,
        is_inverse=False,
        price_precision=2,
        size_precision=4,
        price_increment=Price.from_str("0.01"),
        size_increment=Quantity.from_str("0.0001"),
        max_quantity=Quantity.from_str("1000000"),
        min_quantity=Quantity.from_str("0.01"),
        max_price=Price.from_str("1.00"),
        min_price=Price.from_str("0.00"),
        margin_init=Decimal("0.05"),
        margin_maint=Decimal("0.03"),
        maker_fee=Decimal("0.001"),
        taker_fee=Decimal("0.002"),
        ts_event=0,
        ts_init=0,
    )
    logger.info(f"Created Polymarket BTC instrument: {instrument.id}")
    return instrument


def create_btc_spot_instrument() -> CryptoPerpetual:
    """BTC-USD spot reference instrument (Coinbase)."""
    instrument = CryptoPerpetual(
        instrument_id=InstrumentId(Symbol("BTC-USD"), Venue("COINBASE")),
        raw_symbol=Symbol("BTC-USD"),
        base_currency=BTC,
        quote_currency=USDC,
        settlement_currency=USDC,
        is_inverse=False,
        price_precision=2,
        size_precision=8,
        price_increment=Price.from_str("0.01"),
        size_increment=Quantity.from_str("0.00000001"),
        max_quantity=Quantity.from_str("1000"),
        min_quantity=Quantity.from_str("0.001"),
        max_price=Price.from_str("1000000.00"),
        min_price=Price.from_str("1.00"),
        margin_init=Decimal("0.05"),
        margin_maint=Decimal("0.03"),
        maker_fee=Decimal("0.005"),
        taker_fee=Decimal("0.005"),
        ts_event=0,
        ts_init=0,
    )
    logger.info(f"Created Coinbase BTC spot instrument: {instrument.id}")
    return instrument


def create_btc_binance_instrument() -> CryptoPerpetual:
    """BTCUSDT reference instrument (Binance)."""
    instrument = CryptoPerpetual(
        instrument_id=InstrumentId(Symbol("BTCUSDT"), Venue("BINANCE")),
        raw_symbol=Symbol("BTCUSDT"),
        base_currency=BTC,
        quote_currency=USDC,
        settlement_currency=USDC,
        is_inverse=False,
        price_precision=2,
        size_precision=8,
        price_increment=Price.from_str("0.01"),
        size_increment=Quantity.from_str("0.00000001"),
        max_quantity=Quantity.from_str("9000"),
        min_quantity=Quantity.from_str("0.00001"),
        max_price=Price.from_str("1000000.00"),
        min_price=Price.from_str("1.00"),
        margin_init=Decimal("0.01"),
        margin_maint=Decimal("0.005"),
        maker_fee=Decimal("0.001"),
        taker_fee=Decimal("0.001"),
        ts_event=0,
        ts_init=0,
    )
    logger.info(f"Created Binance BTCUSDT instrument: {instrument.id}")
    return instrument


class InstrumentRegistry:
    """Registry for all trading instruments."""

    def __init__(self):
        self.instruments: dict = {}
        polymarket = create_btc_polymarket_instrument()
        coinbase   = create_btc_spot_instrument()
        binance    = create_btc_binance_instrument()
        for inst in (polymarket, coinbase, binance):
            self.instruments[str(inst.id)] = inst
        logger.info(f"Instrument registry initialised ({len(self.instruments)} instruments)")

    def get(self, instrument_id: str) -> Optional[CryptoPerpetual]:
        return self.instruments.get(instrument_id)

    def get_polymarket(self) -> Optional[CryptoPerpetual]:
        return self.get("BTC-POLYMARKET.POLYMARKET")

    def get_coinbase(self) -> Optional[CryptoPerpetual]:
        return self.get("BTC-USD.COINBASE")

    def get_binance(self) -> Optional[CryptoPerpetual]:
        return self.get("BTCUSDT.BINANCE")

    def get_all(self) -> list:
        return list(self.instruments.values())


_registry_instance: Optional[InstrumentRegistry] = None


def get_instrument_registry() -> InstrumentRegistry:
    global _registry_instance
    if _registry_instance is None:
        _registry_instance = InstrumentRegistry()
    return _registry_instance
