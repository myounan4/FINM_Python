import unittest
from fix_parser import FixParser


class TestFixParser(unittest.TestCase):
    def setUp(self):
        self.parser = FixParser()

    def test_parse_valid_message(self):
        raw = "8=FIX.4.2|35=D|55=AAPL|54=1|38=100|40=2|10=128"
        msg = self.parser.parse(raw)
        self.assertEqual(msg["55"], "AAPL")
        self.assertEqual(msg["54"], "1")
        self.assertEqual(msg["38"], "100")

    def test_missing_required_tag_raises(self):
        raw = "8=FIX.4.2|35=D|55=AAPL|38=100|40=2|10=128"  # missing 54
        with self.assertRaises(ValueError):
            self.parser.parse(raw)

    def test_malformed_field_raises(self):
        raw = "8=FIX.4.2|35D|55=AAPL|54=1|38=100"
        with self.assertRaises(ValueError):
            self.parser.parse(raw)


if __name__ == "__main__":
    unittest.main()
