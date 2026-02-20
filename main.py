import asyncio
import logging
import logging.handlers
import configparser
import datetime
from binance_common.configuration import ConfigurationWebSocketAPI, ConfigurationWebSocketStreams, WebsocketMode
from binance_common.constants import (
    DERIVATIVES_TRADING_USDS_FUTURES_WS_API_TESTNET_URL,
    DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_TESTNET_URL,
    DERIVATIVES_TRADING_USDS_FUTURES_WS_API_PROD_URL,
    DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_PROD_URL
)
from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import DerivativesTradingUsdsFutures

from streams import start_funding_rate_stream, run_logging_session
import streams
from utils import console_listener

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.handlers.QueueHandler(console_listener.queue)]
)

# Read config.ini
config = configparser.ConfigParser()
config.read("config.ini")

api_key     = config.get("live_auth", "api_key")
private_key = config.get("live_auth", "private_key")

# WebSocket API with pooling
configuration_ws_api = ConfigurationWebSocketAPI(
    api_key=api_key,
    private_key=private_key,
    stream_url=DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_PROD_URL,
    mode=WebsocketMode.POOL,
    pool_size=3,
)

# WebSocket Streams with pooling
configuration_ws_streams = ConfigurationWebSocketStreams(
    stream_url=DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_PROD_URL,
    mode=WebsocketMode.POOL,
    pool_size=3,
)

# Initialize client with both pooled configurations
client = DerivativesTradingUsdsFutures(
    config_ws_api=configuration_ws_api,
    config_ws_streams=configuration_ws_streams
)


def _is_pre_settlement(now: datetime.datetime, interval_hours: int | None) -> bool:
    """Returns True if now is x:59:30 where (current hour + 1) is a settlement hour for the given interval."""
    if not interval_hours:
        return False
    settlement_hours = set(range(0, 24, interval_hours))
    next_hour = (now.hour + 1) % 24
    return next_hour in settlement_hours and now.minute == 59 and now.second == 30


async def scheduler():
    funding_stream_connection = await start_funding_rate_stream(client)

    try:
        logging.info("Waiting for funding rate data...")
        while not streams.funding_rates:
            await asyncio.sleep(0.1)
        logging.info(f"Funding rate data received ({len(streams.funding_rates)} symbols). Scheduler running...")

        while True:
            now = datetime.datetime.now()

            symbols_to_log = [
                s for s in streams.qualifying_symbols()
                if _is_pre_settlement(now, streams.cached_intervals.get(s, streams.cached_interval))
            ]

            if symbols_to_log:
                logging.info(f"Settlement in 30s for {len(symbols_to_log)} symbol(s): {symbols_to_log}. Starting logging sessions.")
                await asyncio.gather(*(run_logging_session(client, s, 60) for s in symbols_to_log))
                # Sleep until the next whole second so we don't re-trigger on the same tick
                await asyncio.sleep(1 - datetime.datetime.now().microsecond / 1_000_000)
            else:
                await asyncio.sleep(0.1)

    except Exception as e:
        logging.error(f"Scheduler failed: {e}")
    finally:
        if funding_stream_connection:
            await funding_stream_connection.close_connection(close_session=True)


if __name__ == "__main__":
    console_listener.start()
    try:
        asyncio.run(scheduler())
    finally:
        console_listener.stop()
