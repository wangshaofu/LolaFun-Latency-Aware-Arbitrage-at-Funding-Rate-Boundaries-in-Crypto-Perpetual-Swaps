import asyncio
import configparser
import datetime
import json
import logging
import os

from binance_common.configuration import ConfigurationWebSocketAPI, ConfigurationWebSocketStreams
from binance_common.constants import (
    DERIVATIVES_TRADING_USDS_FUTURES_WS_API_TESTNET_URL,
    DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_TESTNET_URL,
    DERIVATIVES_TRADING_USDS_FUTURES_WS_API_PROD_URL,
    DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_PROD_URL
)
from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import DerivativesTradingUsdsFutures

from utils import setup_async_logger

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

config = configparser.ConfigParser()
config.read("config.ini")

api_key     = config.get("live_auth", "api_key")
private_key = config.get("live_auth", "private_key")

client = DerivativesTradingUsdsFutures(
    config_ws_api=ConfigurationWebSocketAPI(
        api_key=api_key,
        private_key=private_key,
        stream_url=DERIVATIVES_TRADING_USDS_FUTURES_WS_API_PROD_URL,
    ),
    config_ws_streams=ConfigurationWebSocketStreams(
        stream_url=DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_PROD_URL,
    ),
)

_stream_logger = None


def handle_user_data(data):
    raw = data if isinstance(data, dict) else data.model_dump(by_alias=True)
    msg = json.dumps(raw, indent=2)
    logging.info(f"User data event:\n{msg}")
    if _stream_logger:
        _stream_logger.info(f"User data event:\n{msg}")


async def keepalive_loop(api_connection):
    """Sends a keepalive ping every 50 minutes to prevent the listenKey from expiring."""
    while True:
        await asyncio.sleep(50 * 60)
        try:
            await api_connection.keepalive_user_data_stream()
            logging.info("Sent keepalive for user data stream.")
        except Exception as e:
            logging.error(f"Keepalive failed: {e}")


async def listen_order_updates():
    global _stream_logger
    api_connection     = None
    streams_connection = None
    keepalive_task     = None
    listener           = None
    try:
        # Set up file logger
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = os.path.join("Logs", f"user_data_{ts}.txt")
        _stream_logger, listener = setup_async_logger(log_path)
        listener.start()
        logging.info(f"Logging user data stream to {log_path}")

        # Step 1: Start user data stream via WS API → get listenKey
        api_connection = await client.websocket_api.create_connection()
        response   = await api_connection.start_user_data_stream()
        listen_key = response.data().result.listen_key
        logging.info(f"listenKey: {listen_key}")

        # Step 2: Subscribe to the user data stream via WS Streams
        streams_connection = await client.websocket_streams.create_connection()
        stream = await streams_connection.user_data(listenKey=listen_key)
        stream.on("message", handle_user_data)
        logging.info("Listening for events...")

        # Step 3: Run keepalive in background while stream stays open
        keepalive_task = asyncio.create_task(keepalive_loop(api_connection))
        await asyncio.Future()  # block forever until cancelled

    except asyncio.CancelledError:
        pass
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        if keepalive_task:
            keepalive_task.cancel()
        if streams_connection:
            await streams_connection.close_connection(close_session=True)
        if api_connection:
            await api_connection.close_connection(close_session=True)
        if listener:
            listener.stop()


if __name__ == "__main__":
    asyncio.run(listen_order_updates())
