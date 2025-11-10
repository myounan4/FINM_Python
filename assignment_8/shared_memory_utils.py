import numpy as np
from multiprocessing import shared_memory, Lock
from typing import Dict, List, Optional

class SharedPriceBook:
    # A shared-memory price book storing latest prices for a fixed symbol set.
    # Memory layout: float64 array of length N (N = len(symbols))
    def __init__(self, symbols: List[str], name: Optional[str] = None, create: bool = True):
        self.symbols = list(symbols)
        self.index: Dict[str, int] = {s: i for i, s in enumerate(self.symbols)}
        self.dtype = np.float64
        self.shape = (len(self.symbols),)
        self.nbytes = np.zeros(self.shape, dtype=self.dtype).nbytes
        self.lock = None  # to be attached externally

        if create:
            self.shm = shared_memory.SharedMemory(create=True, size=self.nbytes, name=name)
            self.arr = np.ndarray(self.shape, dtype=self.dtype, buffer=self.shm.buf)
            self.arr[:] = np.nan  # initialize as NaN
        else:
            if name is None:
                raise ValueError("name required when create=False")
            self.shm = shared_memory.SharedMemory(name=name, create=False)
            self.arr = np.ndarray(self.shape, dtype=self.dtype, buffer=self.shm.buf)

    @property
    def name(self) -> str:
        return self.shm.name

    def attach_lock(self, lock: Lock):
        self.lock = lock

    def update(self, symbol: str, price: float) -> None:
        if self.lock is not None:
            with self.lock:
                self._update_nolock(symbol, price)
        else:
            self._update_nolock(symbol, price)

    def _update_nolock(self, symbol: str, price: float) -> None:
        i = self.index.get(symbol)
        if i is None:
            return
        self.arr[i] = float(price)

    def read(self, symbol: str) -> float:
        i = self.index.get(symbol)
        if i is None:
            return float("nan")
        return float(self.arr[i])

    def read_all(self) -> dict:
        return {s: float(self.arr[i]) for s, i in self.index.items()}

    def close(self):
        self.shm.close()

    def unlink(self):
        try:
            self.shm.unlink()
        except FileNotFoundError:
            pass
