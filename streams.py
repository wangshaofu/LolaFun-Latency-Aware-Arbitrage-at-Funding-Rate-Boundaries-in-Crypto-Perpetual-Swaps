import asyncio
import logging
import datetime
import os
import subprocess
import sys

from utils import setup_logger, print_most_negative_funding_rate

# Tracks the latest funding rate per symbol from the mark price stream
funding_rates: dict[str, float] = {}

# Cache for funding rate interval info
_last_most_negative_symbol: str | None = None
_last_print_minute: int | None = None
_last_refresh_minute: int | None = None

cached_interval: int | None = None  # exposed for use in main.py
most_negative_symbol: str = ""


def _get_funding_interval(client, symbol: str) -> int | None:
    try:
        info_response = client.rest_api.get_funding_rate_info()
        info_list = info_response.data() if hasattr(info_response, "data") else info_response
        if isinstance(info_list, list):
            for item in info_list:
                item_symbol = item.get("symbol") if isinstance(item, dict) else getattr(item, "symbol", None)
                if item_symbol == symbol:
                    return item.get("fundingIntervalHours") if isinstance(item, dict) else getattr(item, "funding_interval_hours", None)
    except Exception as e:
        logging.error(f"Failed to fetch funding rate info: {e}")
    return None


def _fetch_and_cache_interval(client, symbol: str) -> None:
    global cached_interval
    cached_interval = _get_funding_interval(client, symbol)


async def start_funding_rate_stream(client):
    global _last_most_negative_symbol, cached_interval

    try:
        streams_connection = await client.websocket_streams.create_connection()
        logging.info("Subscribing to mark price stream for all markets...")
        funding_stream = await streams_connection.mark_price_stream_for_all_market()
        funding_stream.on("message", lambda data: _handle_mark_price(client, data))
        return streams_connection
    except Exception as e:
        logging.error(f"Failed to start funding rate stream: {e}")
        return None


def _handle_mark_price(client, data):
    global _last_most_negative_symbol, cached_interval, _last_print_minute, _last_refresh_minute, most_negative_symbol

    for entry in (data if isinstance(data, list) else [data]):
        symbol = getattr(entry, "s", None)
        rate   = getattr(entry, "r", None)
        if symbol and rate is not None:
            funding_rates[symbol] = float(rate)

    if not funding_rates:
        return

    most_negative_symbol = min(funding_rates, key=funding_rates.get)
    most_negative_rate   = funding_rates[most_negative_symbol]
    now            = datetime.datetime.now()
    current_minute = now.minute
    current_hour   = now.hour

    should_refresh = (
        most_negative_symbol != _last_most_negative_symbol
        or cached_interval is None
        or (current_minute == 50 and _last_refresh_minute != (current_hour, 50))
    )

    if should_refresh:
        _last_most_negative_symbol = most_negative_symbol
        _last_refresh_minute       = (current_hour, 50)
        asyncio.get_event_loop().run_in_executor(None, _fetch_and_cache_interval, client, most_negative_symbol)

    if _last_print_minute != current_minute:
        _last_print_minute = current_minute
        print_most_negative_funding_rate(most_negative_symbol, most_negative_rate, cached_interval)


async def run_logging_session(client, target_symbol: str, duration: int) -> None:
    timestamp    = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join("Logs", f"log_{target_symbol}_{timestamp}.txt")
    logger       = setup_logger(log_filename)

    logger.info(f"Starting latency logging for {target_symbol} for {duration} seconds.")

    streams_connection = None
    try:
        streams_connection = await client.websocket_streams.create_connection()

        book_ticker = await streams_connection.individual_symbol_book_ticker_streams(symbol=target_symbol)
        book_ticker.on("message", lambda data: logger.info(f"Stream message: {data}"))

        agg_trade = await streams_connection.aggregate_trade_streams(symbol=target_symbol)
        agg_trade.on("message", lambda data: logger.info(f"Agg trade: {data}"))

        await asyncio.sleep(duration)
    except Exception as e:
        logger.error(f"Error in logging session: {e}")
    finally:
        if streams_connection:
            await streams_connection.close_connection(close_session=False)
        logger.info("Session ended. Running analysis...")
        subprocess.Popen([sys.executable, "analyze_latency.py", log_filename])
