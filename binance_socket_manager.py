from datetime import datetime

import websocket
import threading
import json
import time
import queue
import logging
import os
from commons.utils import filter_date_time, hms_to_seconds, seconds_to_hms
import logging
from storage import meta_data
from models import Quote


logger = logging.getLogger(__name__)

# =========================================================
# BINANCE WEBSOCKET MANAGER
# =========================================================


class BinanceWebSocketManager:

    def __init__(
        self,
        symbols: list,
        intervals: list,
        use_testnet: bool = False,
        print_live_updates: bool = True,
        max_reconnect_delay: int = 60,
    ):
        self.symbols = symbols
        self.intervals = intervals
        self.use_testnet = use_testnet
        self.print_live_updates = print_live_updates
        self.max_reconnect_delay = max_reconnect_delay
        self.ws = None
        self.running = False
        self.message_queue = queue.Queue()
        self.reconnect_attempts = 0

        self.streams = self._build_streams()

    # -----------------------------------------------------
    # Build stream list dynamically
    # -----------------------------------------------------
    def _build_streams(self):
        streams = []
        for symbol in self.symbols:
            for interval in self.intervals:
                streams.append(f"{symbol.lower()}@kline_{interval}")
        return streams

    # -----------------------------------------------------
    # Build WebSocket URL
    # -----------------------------------------------------
    def _build_url(self):
        base_url = "wss://stream.binance.com:9443/stream"
        stream_path = "/".join(self.streams)
        return f"{base_url}?streams={stream_path}"

    # -----------------------------------------------------
    # WebSocket Callbacks
    # -----------------------------------------------------
    def _on_open(self, ws):
        logger.info("Connected to Binance WebSocket")
        self.reconnect_attempts = 0

    def _on_message(self, ws, message):
        self.message_queue.put(message)

    def _on_error(self, ws, error):
        logging.error(f"WebSocket error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        logging.warning("WebSocket closed")
        if self.running:
            self._reconnect()

    # -----------------------------------------------------
    # Reconnect Logic (Prevents Ban)
    # -----------------------------------------------------
    def _reconnect(self):
        self.reconnect_attempts += 1
        delay = min(2**self.reconnect_attempts, self.max_reconnect_delay)
        logger.info(f"Reconnecting in {delay} seconds...")
        time.sleep(delay)
        self._connect()

    # -----------------------------------------------------
    # Connect
    # -----------------------------------------------------
    def _connect(self):
        self.ws = websocket.WebSocketApp(
            self._build_url(),
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )

        self.ws.run_forever(ping_interval=20, ping_timeout=10)

    # -----------------------------------------------------
    # Message Processor Thread
    # -----------------------------------------------------
    def _process_messages(self):
        while self.running:
            try:
                message = self.message_queue.get(timeout=1)
                data = json.loads(message)
                self._handle_data(data)
            except queue.Empty:
                continue
            except Exception as e:
                logging.error(f"Processing error: {e}")

    # -----------------------------------------------------
    # Customizable Data Handler
    # -----------------------------------------------------
    def _handle_data(self, data):
        stream = data.get("stream")
        payload = data.get("data")

        if not payload:
            return

        k = payload.get("k")
        if not k:
            return

        symbol = payload["s"]
        interval = k["i"]
        
        current_local_time = hms_to_seconds(datetime.now().strftime("%H:%M:%S"))
        date, time = filter_date_time(k["t"])
        # print(seconds_to_hms(time))
        open_price = float(k["o"])
        high_price = float(k["h"])
        low_price = float(k["l"])
        close_price = float(k["c"])
        volume = float(k["v"])
        closed = k["x"]

        quote = Quote(
            symbol=symbol,
            interval=interval,
            quote_open_time=(current_local_time - int(interval.replace("m", "")) * 60) if closed else None,
            quote_date=date,
            quote_time=current_local_time,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
            volume=volume,
            closed=closed,
            quote_close_time=current_local_time if closed else None,
        )

        meta_data.add_quote(quote)

        if closed:
            print(
                f"meta_data = {meta_data.quote_data[symbol][interval][quote.quote_time]}"
            )


        # Print Live Updates (Optional)
        if self.print_live_updates:
            logger.info(f"{symbol} {interval} -> {close_price}")

            # Only print when candle closes
            if closed:
                logger.info("=" * 60)
                logger.info(f"[CLOSED] {symbol} {interval} started at {seconds_to_hms(quote.quote_open_time)} and closed at {seconds_to_hms(quote.quote_close_time)}")
                logger.info(f"time defference = {current_local_time - quote.quote_close_time} seconds")
                logger.info(quote)
                logger.info("=" * 60)

    # -----------------------------------------------------
    # Start
    # -----------------------------------------------------
    def start(self):
        self.running = True
        threading.Thread(target=self._connect, daemon=True).start()
        threading.Thread(target=self._process_messages, daemon=True).start()

    # -----------------------------------------------------
    # Stop
    # -----------------------------------------------------
    def stop(self):
        self.running = False
        if self.ws:
            self.ws.close()
