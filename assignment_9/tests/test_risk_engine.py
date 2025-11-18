import unittest
from order import Order
from risk_engine import RiskEngine


class TestRiskEngine(unittest.TestCase):
    def setUp(self):
        self.risk = RiskEngine(max_order_size=1000, max_position=2000)

    def test_order_size_limit(self):
        o = Order("AAPL", 1500, "1")
        with self.assertRaises(ValueError):
            self.risk.check(o)

    def test_position_limit(self):
        # First order passes and updates position
        o1 = Order("AAPL", 1000, "1")
        self.risk.check(o1)
        self.risk.update_position(o1)
        # Next order would exceed position
        o2 = Order("AAPL", 1500, "1")
        with self.assertRaises(ValueError):
            self.risk.check(o2)

    def test_update_position_buy_and_sell(self):
        buy = Order("MSFT", 500, "1")
        self.risk.check(buy)
        self.risk.update_position(buy)
        self.assertEqual(self.risk.positions["MSFT"], 500)

        sell = Order("MSFT", 200, "2")
        self.risk.check(sell)
        self.risk.update_position(sell)
        self.assertEqual(self.risk.positions["MSFT"], 300)


if __name__ == "__main__":
    unittest.main()
