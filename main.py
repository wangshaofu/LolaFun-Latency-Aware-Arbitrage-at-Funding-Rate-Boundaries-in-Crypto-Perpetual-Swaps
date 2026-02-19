import asyncio
import logging
import configparser
import datetime
from binance_common.configuration import ConfigurationWebSocketAPI, ConfigurationWebSocketStreams, WebsocketMode
from binance_common.constants import DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_PROD_URL
from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import DerivativesTradingUsdsFutures

from streams import start_funding_rate_stream, run_logging_session
import streams

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Read config.ini
config = configparser.ConfigParser()
config.read("config.ini")

api_key     = config.get("auth", "api_key")
private_key = config.get("auth", "private_key")

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
    """Returns True if now is exactly x:59:30 before a settlement hour for the given interval."""
    if not interval_hours:
        return False
    settlement_hours = set(range(0, 24, interval_hours))
    next_hour = (now + datetime.timedelta(hours=1)).hour
    return next_hour in settlement_hours and now.minute == 59 and now.second == 30


async def scheduler():
    funding_stream_connection = await start_funding_rate_stream(client)

    try:
        logging.info("Scheduler running...")

        while True:
            now      = datetime.datetime.now()
            interval = streams.cached_interval

            if _is_pre_settlement(now, interval):
                logging.info(f"Settlement in 30s for {streams.most_negative_symbol} (interval={interval}h). Starting logging session.")
                await run_logging_session(client, streams.most_negative_symbol, 60)
                await asyncio.sleep(5)
            else:
                await asyncio.sleep(0)

    except Exception as e:
        logging.error(f"Scheduler failed: {e}")
    finally:
        if funding_stream_connection:
            await funding_stream_connection.close_connection(close_session=True)


if __name__ == "__main__":
    asyncio.run(scheduler())
