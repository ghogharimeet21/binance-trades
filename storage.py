import numpy as np
import threading
from typing import Dict, Optional, Tuple
from models import Quote


class Cols:
    """Column index constants for the OHLCV numpy array."""
    DATE   = 0
    TIME   = 1
    OPEN   = 2
    HIGH   = 3
    LOW    = 4
    CLOSE  = 5
    VOLUME = 6
    TOTAL  = 7  # total number of columns


class OHLCBuffer:
    """
    Pre-allocated numpy buffer for one (symbol, interval) pair.
    Doubles in capacity when full — avoids per-insert array copies.
    Thread-safe via a per-buffer RLock.
    """

    def __init__(self, initial_capacity: int = 512):
        self._capacity = initial_capacity
        self._size = 0
        self._data = np.empty((initial_capacity, Cols.TOTAL), dtype=np.float64)
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ #
    #  Writing                                                             #
    # ------------------------------------------------------------------ #

    def append(self, quote: Quote) -> None:
        row = _quote_to_row(quote)
        with self._lock:
            # Update in-place if the last candle has the same timestamp
            if self._size > 0 and self._data[self._size - 1, Cols.TIME] == row[Cols.TIME]:
                self._data[self._size - 1] = row
                return

            if self._size == self._capacity:
                self._grow()

            self._data[self._size] = row
            self._size += 1

    def _grow(self) -> None:
        """Double the buffer capacity (called with lock held)."""
        self._capacity *= 2
        new = np.empty((self._capacity, Cols.TOTAL), dtype=np.float64)
        new[: self._size] = self._data[: self._size]
        self._data = new

    # ------------------------------------------------------------------ #
    #  Reading — all return *copies* so callers can't corrupt the buffer  #
    # ------------------------------------------------------------------ #

    def get_array(self, n: Optional[int] = None) -> np.ndarray:
        """Return the last *n* rows (or all rows) as a copy."""
        with self._lock:
            view = self._data[: self._size]
            if n is not None:
                view = view[-n:]
            return view.copy()

    def get_column(self, col: int, n: Optional[int] = None) -> np.ndarray:
        """Return a single column (e.g. CLOSE) as a 1-D array copy."""
        with self._lock:
            view = self._data[: self._size, col]
            if n is not None:
                view = view[-n:]
            return view.copy()

    def last(self) -> Optional[np.ndarray]:
        """Return the most recent row, or None if empty."""
        with self._lock:
            if self._size == 0:
                return None
            return self._data[self._size - 1].copy()

    @property
    def size(self) -> int:
        with self._lock:
            return self._size

    def __len__(self) -> int:
        return self.size


# ------------------------------------------------------------------ #
#  Top-level store                                                     #
# ------------------------------------------------------------------ #

class CryptoDataStore:
    """
    Central store for all symbols and intervals.

    Usage
    -----
    store = CryptoDataStore()
    store.add_quote(quote)                        # from any thread

    closes = store.closes("BTCUSDT", "1m")        # 1-D numpy array
    ohlcv  = store.ohlcv("BTCUSDT", "1m", n=200) # last 200 candles
    """

    def __init__(self, initial_capacity: int = 512):
        self._initial_capacity = initial_capacity
        # { symbol: { interval: OHLCBuffer } }
        self._buffers: Dict[str, Dict[str, OHLCBuffer]] = {}
        self._store_lock = threading.Lock()   # only for creating new buffers

    # ------------------------------------------------------------------ #
    #  Writing                                                             #
    # ------------------------------------------------------------------ #

    def add_quote(self, quote: Quote) -> None:
        buf = self._get_or_create(quote.symbol, quote.interval)
        buf.append(quote)

    # ------------------------------------------------------------------ #
    #  Reading helpers                                                     #
    # ------------------------------------------------------------------ #

    def ohlcv(self, symbol: str, interval: str, n: Optional[int] = None) -> np.ndarray:
        """Full OHLCV matrix — shape (rows, Cols.TOTAL)."""
        return self._buffer(symbol, interval).get_array(n)

    def closes(self, symbol: str, interval: str, n: Optional[int] = None) -> np.ndarray:
        return self._buffer(symbol, interval).get_column(Cols.CLOSE, n)

    def highs(self, symbol: str, interval: str, n: Optional[int] = None) -> np.ndarray:
        return self._buffer(symbol, interval).get_column(Cols.HIGH, n)

    def lows(self, symbol: str, interval: str, n: Optional[int] = None) -> np.ndarray:
        return self._buffer(symbol, interval).get_column(Cols.LOW, n)

    def volumes(self, symbol: str, interval: str, n: Optional[int] = None) -> np.ndarray:
        return self._buffer(symbol, interval).get_column(Cols.VOLUME, n)

    def last_quote(self, symbol: str, interval: str) -> Optional[np.ndarray]:
        """Most recent row for a symbol/interval, or None."""
        return self._buffer(symbol, interval).last()

    def symbols(self) -> list:
        with self._store_lock:
            return list(self._buffers.keys())

    def intervals(self, symbol: str) -> list:
        with self._store_lock:
            return list(self._buffers.get(symbol, {}).keys())

    def candle_count(self, symbol: str, interval: str) -> int:
        return len(self._buffer(symbol, interval))

    # ------------------------------------------------------------------ #
    #  Internals                                                           #
    # ------------------------------------------------------------------ #

    def _get_or_create(self, symbol: str, interval: str) -> OHLCBuffer:
        # Fast path — no lock needed once the buffer exists
        sym_map = self._buffers.get(symbol)
        if sym_map is not None:
            buf = sym_map.get(interval)
            if buf is not None:
                return buf

        # Slow path — create new buffer under lock
        with self._store_lock:
            self._buffers.setdefault(symbol, {})
            if interval not in self._buffers[symbol]:
                self._buffers[symbol][interval] = OHLCBuffer(self._initial_capacity)
            return self._buffers[symbol][interval]

    def _buffer(self, symbol: str, interval: str) -> OHLCBuffer:
        try:
            return self._buffers[symbol][interval]
        except KeyError:
            raise KeyError(f"No data for ({symbol!r}, {interval!r}). "
                           "Call add_quote() first.")


# ------------------------------------------------------------------ #
#  Utility                                                             #
# ------------------------------------------------------------------ #

def _quote_to_row(q: Quote) -> np.ndarray:
    """Pack a Quote into a fixed-order float64 row."""
    return np.array([
        float(q.quote_date),        # Cols.DATE   — store as YYYYMMDD int → float
        float(q.quote_open_time),   # Cols.TIME   — unix ms timestamp
        float(q.open_price),        # Cols.OPEN
        float(q.high_price),        # Cols.HIGH
        float(q.low_price),         # Cols.LOW
        float(q.close_price),       # Cols.CLOSE
        float(q.volume),            # Cols.VOLUME
    ], dtype=np.float64)


# ------------------------------------------------------------------ #
#  Singleton — import and use directly                                #
# ------------------------------------------------------------------ #

data_store = CryptoDataStore()