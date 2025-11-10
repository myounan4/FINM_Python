import time
from multiprocessing import Process, Lock
from shared_memory_utils import SharedPriceBook
from gateway import run_gateway
from orderbook import run_orderbook
from strategy import run_strategy
from order_manager import run_order_manager

HOST = "127.0.0.1"
PRICE_PORT = 51051
NEWS_PORT = 51052
OM_PORT = 51053
SYMBOLS = ["AAPL", "MSFT"]

def test_end_to_end_smoke():
    lock = Lock()
    book = SharedPriceBook(SYMBOLS, create=True)
    shm_name = book.name
    book.attach_lock(lock)

    procs = [
        Process(target=run_gateway, args=(HOST, PRICE_PORT, NEWS_PORT, SYMBOLS), daemon=True),
        Process(target=run_orderbook, args=(HOST, PRICE_PORT, shm_name, SYMBOLS, lock), daemon=True),
        Process(target=run_strategy, args=(shm_name, SYMBOLS, lock, HOST, NEWS_PORT, HOST, OM_PORT), daemon=True),
        Process(target=run_order_manager, args=(HOST, OM_PORT), daemon=True),
    ]
    for p in procs: p.start()
    time.sleep(5)
    for p in procs: p.terminate(); p.join()
    book.close(); book.unlink()
    assert True
