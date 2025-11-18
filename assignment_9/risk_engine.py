# risk_engine.py

class RiskEngine:
    """
    Very simple, symbol-level position risk engine.
    - max_order_size: maximum absolute size of any single order
    - max_position: maximum absolute position per symbol after the fill
    """

    def __init__(self, max_order_size=1000, max_position=2000):
        self.max_order_size = max_order_size
        self.max_position = max_position
        # positions[symbol] = current net position
        self.positions = {}

    def _side_sign(self, side: str) -> int:
        # FIX: '1' = Buy, '2' = Sell
        if side == "1":
            return 1
        elif side == "2":
            return -1
        else:
            raise ValueError(f"Unknown side: {side}")

    def check(self, order) -> bool:
        """
        Validate an order.
        Raises ValueError if the order fails risk checks.
        Returns True if the order passes.
        """
        if order.qty <= 0:
            raise ValueError("Order quantity must be positive")

        if abs(order.qty) > self.max_order_size:
            raise ValueError(
                f"Order size {order.qty} exceeds max_order_size "
                f"{self.max_order_size}"
            )

        sign = self._side_sign(order.side)
        current_pos = self.positions.get(order.symbol, 0)
        projected_pos = current_pos + sign * order.qty

        if abs(projected_pos) > self.max_position:
            raise ValueError(
                f"Projected position {projected_pos} for {order.symbol} "
                f"exceeds max_position {self.max_position}"
            )

        return True

    def update_position(self, order):
        """
        Update positions after a fill.
        """
        sign = self._side_sign(order.side)
        current_pos = self.positions.get(order.symbol, 0)
        new_pos = current_pos + sign * order.qty
        self.positions[order.symbol] = new_pos
        print(
            f"Updated position for {order.symbol}: "
            f"{current_pos} -> {new_pos}"
        )
