# logger.py
from datetime import datetime
import json


class Logger:
    """
    Simple singleton logger that collects events and writes to JSON.
    """

    _instance = None

    def __new__(cls, path="events.json"):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            # Mark as not yet initialized
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, path="events.json"):
        if getattr(self, "_initialized", False):
            # Avoid re-initializing on subsequent constructions
            return
        self.path = path
        self.events = []
        self._initialized = True

    def log(self, event_type: str, data: dict):
        event = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "type": event_type,
            "data": data,
        }
        self.events.append(event)
        print(f"[LOG] {event_type} → {data}")

    def save(self):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.events, f, indent=2)
        print(f"Saved {len(self.events)} events to {self.path}")
