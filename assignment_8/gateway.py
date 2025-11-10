import socket
import threading
import time
import random
from typing import Dict, List
import csv
from pathlib import Path

MESSAGE_DELIM = b"*"

# --- Metrics: one row per broadcasted price tick (for throughput) ---
_METRICS_DIR = Path("metrics")
_METRICS_DIR.mkdir(exist_ok=True)
_PRICE_TICKS_CSV = _METRICS_DIR / "price_ticks.csv"

def _ensure_price_csv():
    if not _PRICE_TICKS_CSV.exists():
        with _PRICE_TICKS_CSV.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "tick_ts_ms",
                "symbols_count",
                "bytes_sent_per_client"
            ])

def _send_all(sock: socket.socket, payload: bytes):
    total = 0
    while total < len(payload):
        sent = sock.send(payload[total:])
        if sent == 0:
            raise ConnectionError("socket connection broken")
        total += sent
    return total

def run_price_server(host: str, port: int, symbols: List[str], tick_hz: float = 10.0):
    _ensure_price_csv()

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(5)
    print(f"[Gateway:Prices] listening on {host}:{port}")

    prices: Dict[str, float] = {s: 100.0 + random.random()*10 for s in symbols}
    clients: List[socket.socket] = []
    srv.settimeout(1.0)

    def accept_loop():
        while True:
            try:
                c, addr = srv.accept()
                c.setblocking(True)
                clients.append(c)
                print(f"[Gateway:Prices] client connected: {addr}")
            except socket.timeout:
                continue

    threading.Thread(target=accept_loop, daemon=True).start()

    dt = 1.0 / tick_hz
    while True:
        for s in symbols:
            prices[s] = max(0.01, prices[s] + random.gauss(0, 0.1))
        now_ms = int(time.time() * 1000)
        msg = MESSAGE_DELIM.join(
            [f"{s},{prices[s]:.5f},{now_ms}".encode("utf-8") for s in symbols]
        ) + MESSAGE_DELIM

        dead = []
        bytes_per_client = 0
        for i, c in enumerate(clients):
            try:
                bytes_per_client = _send_all(c, msg)
            except Exception:
                dead.append(i)
        for i in reversed(dead):
            try: clients[i].close()
            except Exception: pass
            clients.pop(i)

        # Write one row (tick happens regardless of number of clients)
        with _PRICE_TICKS_CSV.open("a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([now_ms, len(symbols), bytes_per_client])

        time.sleep(dt)

def run_news_server(host: str, port: int, tick_hz: float = 2.0):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(5)
    print(f"[Gateway:News] listening on {host}:{port}")
    clients: List[socket.socket] = []
    srv.settimeout(1.0)

    def accept_loop():
        while True:
            try:
                c, addr = srv.accept()
                clients.append(c)
                print(f"[Gateway:News] client connected: {addr}")
            except socket.timeout:
                continue

    threading.Thread(target=accept_loop, daemon=True).start()

    dt = 1.0 / tick_hz
    while True:
        sentiment = random.randint(0, 100)
        now_ms = int(time.time() * 1000)
        payload = f"{sentiment},{now_ms}".encode("utf-8") + MESSAGE_DELIM
        dead = []
        for i, c in enumerate(clients):
            try:
                _send_all(c, payload)
            except Exception:
                dead.append(i)
        for i in reversed(dead):
            try: clients[i].close()
            except Exception: pass
            clients.pop(i)
        time.sleep(dt)

def run_gateway(host: str, price_port: int, news_port: int, symbols: List[str]):
    t1 = threading.Thread(target=run_price_server, args=(host, price_port, symbols), daemon=True)
    t2 = threading.Thread(target=run_news_server, args=(host, news_port), daemon=True)
    t1.start(); t2.start()
    print("[Gateway] running. Press Ctrl+C to stop.")
    t1.join(); t2.join()
