"""
Core data models for the trade surveillance system.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional
import uuid


class Side(str, Enum):
    BUY  = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    LIMIT  = "LIMIT"
    MARKET = "MARKET"


class TraderType(str, Enum):
    NORMAL       = "NORMAL"
    SPOOFER      = "SPOOFER"
    WASH_TRADER  = "WASH_TRADER"


@dataclass
class TradeEvent:
    trader_id:        str
    symbol:           str
    side:             Side
    quantity:         int
    price:            float
    order_type:       OrderType        = OrderType.LIMIT
    trade_id:         str              = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp:        datetime         = field(default_factory=datetime.utcnow)
    cancelled:        bool             = False
    cancel_timestamp: Optional[datetime] = None

    def to_dict(self) -> dict:
        return {
            "trade_id":         self.trade_id,
            "trader_id":        self.trader_id,
            "symbol":           self.symbol,
            "side":             self.side.value,
            "quantity":         self.quantity,
            "price":            round(self.price, 2),
            "order_type":       self.order_type.value,
            "timestamp":        self.timestamp.isoformat(),
            "cancelled":        self.cancelled,
            "cancel_timestamp": self.cancel_timestamp.isoformat() if self.cancel_timestamp else None,
        }
