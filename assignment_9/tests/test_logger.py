import os
import json
import unittest
from logger import Logger


class TestLogger(unittest.TestCase):
    def setUp(self):
        # Use a test file so you don't overwrite real logs
        self.log_path = "test_events.json"
        # Reset singleton instance in case of reuse
        Logger._instance = None
        self.logger = Logger(path=self.log_path)

    def tearDown(self):
        if os.path.exists(self.log_path):
            os.remove(self.log_path)

    def test_log_and_save(self):
        self.logger.log("TestEvent", {"foo": "bar"})
        self.logger.save()

        self.assertTrue(os.path.exists(self.log_path))
        with open(self.log_path, "r", encoding="utf-8") as f:
            events = json.load(f)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["type"], "TestEvent")
        self.assertEqual(events[0]["data"]["foo"], "bar")


if __name__ == "__main__":
    unittest.main()
