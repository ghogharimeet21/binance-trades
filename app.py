from binance_socket_manager import BinanceWebSocketManager
import time

import logging



logging.basicConfig(
    format='%(asctime)s [%(levelname)s] [%(name)s] [%(threadName)s] : %(message)s',
    level=logging.INFO,
)


logger = logging.getLogger(__name__)


ws_manager = BinanceWebSocketManager(
    symbols=["btcusdt"],
    intervals=["1m"],
    use_testnet=False,
    print_live_updates=True,
    max_reconnect_delay=60
)


ws_manager.start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopping...")
    ws_manager.stop()