# order.py
from enum import Enum, auto


class OrderState(Enum):
    NEW = auto()
    ACKED = auto()
    FILLED = auto()
    CANCELED = auto()
    REJECTED = auto()


class Order:
    def __init__(self, symbol: str, qty: int, side: str):
        """
        side: FIX convention
          '1' = Buy
          '2' = Sell
        """
        self.symbol = symbol
        self.qty = qty
        self.side = side
        self.state = OrderState.NEW

    def transition(self, new_state: OrderState):
        """
        Change state if allowed, otherwise print a message
        and keep the current state.
        """
        allowed = {
            OrderState.NEW: {OrderState.ACKED, OrderState.REJECTED},
            OrderState.ACKED: {OrderState.FILLED, OrderState.CANCELED},
            # FILLED, CANCELED, REJECTED are terminal here
        }

        current = self.state
        if current in allowed and new_state in allowed[current]:
            self.state = new_state
            print(f"Order {self.symbol} is now {self.state.name}")
        else:
            print(
                f"Invalid state transition from {current.name} "
                f"to {new_state.name} for order {self.symbol}"
            )
