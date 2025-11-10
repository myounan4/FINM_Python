import socket
import time
from typing import List
from shared_memory_utils import SharedPriceBook

MESSAGE_DELIM = b"*"

def _connect(host: str, port: int) -> socket.socket:
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host, port))
            s.setblocking(True)
            print(f"[OrderBook] connected to gateway {host}:{port}")
            return s
        except Exception as e:
            print(f"[OrderBook] connect failed, retrying in 1s: {e}")
            time.sleep(1)

def run_orderbook(host: str, price_port: int, shm_name: str, symbols: List[str], lock):
    book = SharedPriceBook(symbols, name=shm_name, create=False)
    book.attach_lock(lock)

    s = _connect(host, price_port)
    buf = b""
    while True:
        try:
            chunk = s.recv(4096)
            if not chunk: raise ConnectionError("gateway closed")
            buf += chunk
            while True:
                idx = buf.find(MESSAGE_DELIM)
                if idx < 0: break
                message = buf[:idx].decode("utf-8")
                buf = buf[idx+1:]
                parts = message.split(",")
                if len(parts) != 3: continue
                sym, price_str, _ts = parts
                try: price = float(price_str)
                except ValueError: continue
                book.update(sym, price)
        except Exception as e:
            print(f"[OrderBook] error: {e}, reconnecting...")
            try: s.close()
            except Exception: pass
            time.sleep(1.0)
            s = _connect(host, price_port)
