"""core.nautilus.dispatcher — Central pub/sub event dispatcher."""
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from loguru import logger


class EventType(Enum):
    PRICE_UPDATE      = "price_update"
    QUOTE_TICK        = "quote_tick"
    TRADE_TICK        = "trade_tick"
    SENTIMENT_UPDATE  = "sentiment_update"
    ANOMALY_DETECTED  = "anomaly_detected"
    SIGNAL_GENERATED  = "signal_generated"
    ORDER_FILLED      = "order_filled"
    POSITION_OPENED   = "position_opened"
    POSITION_CLOSED   = "position_closed"
    RISK_ALERT        = "risk_alert"


@dataclass
class Event:
    type: EventType
    timestamp: datetime
    source: str
    data: Dict[str, Any]


class EventDispatcher:
    """Central event dispatcher implementing a simple pub/sub pattern."""

    def __init__(self):
        self._subscribers: Dict[EventType, List[Callable]] = {
            et: [] for et in EventType
        }
        self._event_history: List[Event] = []
        self._max_history = 1000
        self._event_counts: Dict[EventType, int] = {et: 0 for et in EventType}
        logger.info("Initialized Event Dispatcher")

    def subscribe(self, event_type: EventType, callback: Callable) -> None:
        if callback not in self._subscribers[event_type]:
            self._subscribers[event_type].append(callback)

    def unsubscribe(self, event_type: EventType, callback: Callable) -> None:
        if callback in self._subscribers[event_type]:
            self._subscribers[event_type].remove(callback)

    def dispatch(self, event: Event) -> None:
        self._event_counts[event.type] += 1
        self._event_history.append(event)
        if len(self._event_history) > self._max_history:
            self._event_history.pop(0)
        for cb in self._subscribers.get(event.type, []):
            try:
                cb(event)
            except Exception as e:
                logger.error(f"Event subscriber error ({event.type.value}): {e}")

    def dispatch_price_update(
        self, source: str, price: float, metadata: Dict[str, Any] = None
    ) -> None:
        self.dispatch(Event(
            type=EventType.PRICE_UPDATE,
            timestamp=datetime.now(),
            source=source,
            data={"price": price, **(metadata or {})},
        ))

    def get_statistics(self) -> Dict[str, Any]:
        return {
            "total_events": sum(self._event_counts.values()),
            "events_by_type": {et.value: c for et, c in self._event_counts.items()},
        }

    def get_recent_events(
        self, event_type: Optional[EventType] = None, limit: int = 10
    ) -> List[Event]:
        events = (
            [e for e in self._event_history if e.type == event_type]
            if event_type
            else self._event_history
        )
        return events[-limit:]


_dispatcher_instance: Optional[EventDispatcher] = None


def get_event_dispatcher() -> EventDispatcher:
    global _dispatcher_instance
    if _dispatcher_instance is None:
        _dispatcher_instance = EventDispatcher()
    return _dispatcher_instance
