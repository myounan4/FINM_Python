import unittest
from order import Order, OrderState


class TestOrder(unittest.TestCase):
    def test_initial_state_is_new(self):
        o = Order("AAPL", 100, "1")
        self.assertEqual(o.state, OrderState.NEW)

    def test_valid_transition_new_to_acked(self):
        o = Order("AAPL", 100, "1")
        o.transition(OrderState.ACKED)
        self.assertEqual(o.state, OrderState.ACKED)

    def test_invalid_transition_rejected(self):
        o = Order("AAPL", 100, "1")
        o.transition(OrderState.FILLED)
        # State should remain NEW
        self.assertEqual(o.state, OrderState.NEW)


if __name__ == "__main__":
    unittest.main()
