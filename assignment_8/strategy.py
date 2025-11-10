import socket, time, json, math
from collections import deque
from typing import Dict, List, Optional
from shared_memory_utils import SharedPriceBook

MESSAGE_DELIM = b"*"

def _connect(host: str, port: int) -> socket.socket:
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host, port))
            s.setblocking(True)
            print(f"[Strategy] connected to {host}:{port}")
            return s
        except Exception as e:
            print(f"[Strategy] connect failed, retrying in 1s: {e}")
            time.sleep(1)

def _recv_delimited(sock: socket.socket):
    buf = b""
    while True:
        data = sock.recv(4096)
        if not data: raise ConnectionError("server closed")
        buf += data
        while True:
            idx = buf.find(MESSAGE_DELIM)
            if idx < 0: break
            msg = buf[:idx]; buf = buf[idx+1:]; yield msg

def run_strategy(
    shm_name: str,
    symbols: List[str],
    lock,
    news_host: str,
    news_port: int,
    om_host: str,
    om_port: int,
    short_win: int = 10,
    long_win: int = 40,
    bullish_threshold: int = 60,
    bearish_threshold: int = 40,
    trade_qty: int = 10,
):
    book = SharedPriceBook(symbols, name=shm_name, create=False)
    book.attach_lock(lock)

    news = _connect(news_host, news_port)
    om = _connect(om_host, om_port)

    buffers: Dict[str, deque] = {s: deque(maxlen=long_win) for s in symbols}
    position: Dict[str, Optional[str]] = {s: None for s in symbols}
    order_id = 0

    def price_signal(sym: str) -> Optional[str]:
        buf = buffers[sym]
        if len(buf) < long_win: return None
        short_ma = sum(list(buf)[-short_win:]) / short_win
        long_ma = sum(buf) / long_win
        if short_ma > long_ma: return "buy"
        elif short_ma < long_ma: return "sell"
        else: return None

    last_poll, poll_dt = 0.0, 0.02
    for payload in _recv_delimited(news):
        try:
            sentiment_str, sent_ts = payload.decode("utf-8").split(",")
            sentiment = int(sentiment_str); sent_ts_ms = int(sent_ts)
        except Exception: continue

        now = time.time()
        if now - last_poll >= poll_dt:
            last_poll = now
            for s in symbols:
                px = book.read(s)
                if math.isnan(px): continue
                buffers[s].append(px)

        news_sig = "buy" if sentiment > bullish_threshold else \
                   "sell" if sentiment < bearish_threshold else None

        for s in symbols:
            ps = price_signal(s)
            if ps is None or news_sig is None or ps != news_sig: continue
            if position[s] == ps: continue
            position[s] = ps
            px = book.read(s)
            order = {
                "order_id": order_id,
                "symbol": s,
                "side": ps,
                "qty": trade_qty,
                "price": float(px),
                "price_ts_ms": sent_ts_ms,
                "ts_ms": int(time.time()*1000),
            }
            order_id += 1
            wire = json.dumps(order).encode("utf-8") + MESSAGE_DELIM
            try:
                om.sendall(wire)
                print(f"[Strategy] sent order {order['order_id']}: {order['side']} {s} @ {order['price']:.4f}")
            except Exception as e:
                print(f"[Strategy] order send failed: {e}")
                try: om.close()
                except Exception: pass
                om = _connect(om_host, om_port)
