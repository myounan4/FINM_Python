import socket
import threading
import json
import time
import csv
from pathlib import Path

MESSAGE_DELIM = b"*"

# --- Metrics setup (CSV append with header if missing) ---
_METRICS_DIR = Path("metrics")
_METRICS_DIR.mkdir(exist_ok=True)
_ORDERS_CSV = _METRICS_DIR / "orders_log.csv"

def _ensure_orders_csv():
    if not _ORDERS_CSV.exists():
        with _ORDERS_CSV.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "recv_ts_ms",
                "order_id",
                "symbol",
                "side",
                "qty",
                "price",
                "price_ts_ms",
                "strategy_ts_ms",
                "latency_price_to_recv_ms",
                "latency_strategy_to_recv_ms",
            ])

def _append_order_row(order, recv_ts_ms: int):
    price_ts_ms = order.get("price_ts_ms")
    strat_ts_ms = order.get("ts_ms")
    try:
        l_price = None if price_ts_ms is None else (recv_ts_ms - int(price_ts_ms))
    except Exception:
        l_price = None
    try:
        l_strat = None if strat_ts_ms is None else (recv_ts_ms - int(strat_ts_ms))
    except Exception:
        l_strat = None

    with _ORDERS_CSV.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            recv_ts_ms,
            order.get("order_id"),
            order.get("symbol"),
            order.get("side"),
            order.get("qty"),
            order.get("price"),
            price_ts_ms,
            strat_ts_ms,
            l_price,
            l_strat,
        ])

def run_order_manager(host: str, port: int):
    _ensure_orders_csv()

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(5)
    print(f"[OrderManager] listening on {host}:{port}")

    def handle_client(c: socket.socket, addr):
        print(f"[OrderManager] client {addr} connected")
        buf = b""
        try:
            while True:
                data = c.recv(4096)
                if not data:
                    break
                buf += data
                while True:
                    idx = buf.find(MESSAGE_DELIM)
                    if idx < 0:
                        break
                    payload = buf[:idx]
                    buf = buf[idx+1:]
                    if not payload:
                        continue
                    try:
                        order = json.loads(payload.decode("utf-8"))
                        recv_ms = int(time.time() * 1000)
                        _append_order_row(order, recv_ms)

                        oid = order.get("order_id", "?")
                        side = order.get("side")
                        qty = order.get("qty")
                        sym = order.get("symbol")
                        px = order.get("price")
                        price_ts_ms = order.get("price_ts_ms")
                        latency_ms = None if price_ts_ms is None else (recv_ms - int(price_ts_ms))
                        print(f"Received Order {oid}: {side.upper()} {qty} {sym} @ {px:.4f}  (latency ~{latency_ms} ms)")
                    except Exception as e:
                        print(f"[OrderManager] bad order: {e}")
        finally:
            c.close()
            print(f"[OrderManager] client {addr} disconnected")

    while True:
        c, addr = srv.accept()
        threading.Thread(target=handle_client, args=(c, addr), daemon=True).start()
