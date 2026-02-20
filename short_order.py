import asyncio
import logging
import logging.handlers
import configparser
import datetime
import os
import time

from binance_common.configuration import ConfigurationWebSocketAPI, ConfigurationWebSocketStreams, WebsocketMode
from binance_common.constants import (
    DERIVATIVES_TRADING_USDS_FUTURES_WS_API_TESTNET_URL,
    DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_TESTNET_URL,
    DERIVATIVES_TRADING_USDS_FUTURES_WS_API_PROD_URL,
    DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_PROD_URL
)
from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import DerivativesTradingUsdsFutures
from binance_sdk_derivatives_trading_usds_futures.websocket_api.models import NewOrderSideEnum

from utils import setup_async_logger, console_listener, aprint

# ----------------------------
# Config / constants
# ----------------------------
SYMBOL          = "AZTECUSDT"
QUANTITY        = "1000"
LOG_DIR         = "Logs"

TARGET_HOUR     = 1    # Set target time here
TARGET_MINUTE   = 0
TARGET_SECOND   = 0
TARGET_MILLISECOND = 200

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.handlers.QueueHandler(console_listener.queue)],
)

# ----------------------------
# Read config.ini
# ----------------------------
config = configparser.ConfigParser()
config.read("config.ini")
api_key      = config.get("live_auth", "api_key")
private_key  = config.get("live_auth", "private_key")

# ----------------------------
# Binance client (testnet)
# ----------------------------
configuration_ws_api = ConfigurationWebSocketAPI(
    api_key=api_key,
    private_key=private_key,
    stream_url=DERIVATIVES_TRADING_USDS_FUTURES_WS_API_PROD_URL,
    mode=WebsocketMode.POOL,
    pool_size=3,
)

configuration_ws_streams = ConfigurationWebSocketStreams(
    stream_url=DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_PROD_URL,
    mode=WebsocketMode.POOL,
    pool_size=3,
)

client = DerivativesTradingUsdsFutures(
    config_ws_api=configuration_ws_api,
    config_ws_streams=configuration_ws_streams,
)


def _next_target_time() -> datetime.datetime:
    """Return the next upcoming HH:MM:SS.mmm today (or tomorrow if already past)."""
    now    = datetime.datetime.now()
    target = now.replace(hour=TARGET_HOUR, minute=TARGET_MINUTE, second=TARGET_SECOND, microsecond=TARGET_MILLISECOND * 1000)
    if target <= now:
        target += datetime.timedelta(days=1)
    return target


async def new_order():
    os.makedirs(LOG_DIR, exist_ok=True)

    connection = await client.websocket_api.create_connection()
    aprint("WS connection established.")

    try:
        while True:
            target_time = _next_target_time()

            log_filename = os.path.join(LOG_DIR, f"order_{SYMBOL}_{target_time.strftime('%Y%m%d_%H%M%S')}.txt")
            logger, listener = setup_async_logger(log_filename)
            listener.start()
            aprint(f"Next order scheduled at: {int(target_time.timestamp() * 1000)} ({target_time.isoformat()})")

            # Coarse sleep until 50ms before target
            coarse_wait = (target_time - datetime.datetime.now()).total_seconds() - 0.05
            if coarse_wait > 0:
                await asyncio.sleep(coarse_wait)

            # Busy-wait the final 50ms
            target_epoch_s = target_time.timestamp()
            while time.time() < target_epoch_s:
                pass

            # Fire
            send_time = time.time()
            logger.info(f"Order sent at: {int(send_time * 1000)}")

            response = await connection.new_order(
                symbol=SYMBOL,
                side=NewOrderSideEnum["SELL"].value,
                type="MARKET",
                quantity=QUANTITY,
            )

            recv_time  = time.time()
            latency_ms = (recv_time - send_time) * 1000
            data       = response.data()
            logger.info(f"Response received at: {int(recv_time * 1000)} (latency: {latency_ms:.2f}ms)")
            logger.info(f"Response: {data}")

            listener.stop()
            aprint(f"Order log saved to: {log_filename}")

    except asyncio.CancelledError:
        pass
    except Exception as e:
        aprint(f"new_order() error: {e}")
    finally:
        await connection.close_connection(close_session=True)


if __name__ == "__main__":
    console_listener.start()
    asyncio.run(new_order())
    console_listener.stop()
