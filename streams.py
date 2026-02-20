import asyncio
import logging
import datetime
import os
import subprocess
import sys

from utils import setup_async_logger, capture_stream_record, print_qualifying_funding_rates

FUNDING_RATE_THRESHOLD = -0.003   # log any symbol whose rate is below this

# Tracks the latest funding rate per symbol from the mark price stream
funding_rates: dict[str, float] = {}

# Cache for funding rate interval info  (symbol -> interval hours)
cached_intervals: dict[str, int] = {}

_last_print_minute: int | None = None
_last_refresh_minute: int | None = None

# --- legacy aliases kept for any code that still references them ---
cached_interval: int | None = None
most_negative_symbol: str = ""


def qualifying_symbols() -> list[str]:
    """Return all symbols whose current funding rate is below the threshold."""
    return [s for s, r in funding_rates.items() if r < FUNDING_RATE_THRESHOLD]


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
    global cached_interval, cached_intervals
    interval = _get_funding_interval(client, symbol)
    if interval is not None:
        cached_intervals[symbol] = interval
        # keep legacy alias pointing at the most-negative symbol's interval
        if symbol == most_negative_symbol:
            cached_interval = interval


async def start_funding_rate_stream(client):
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
    global cached_interval, _last_print_minute, _last_refresh_minute, most_negative_symbol

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

    # Refresh intervals for all qualifying symbols that we haven't cached yet,
    # or at the periodic refresh window (xx:50).
    periodic_refresh = (current_minute == 50 and _last_refresh_minute != (current_hour, 50))
    if periodic_refresh:
        _last_refresh_minute = (current_hour, 50)

    for symbol in qualifying_symbols():
        if symbol not in cached_intervals or periodic_refresh:
            asyncio.get_event_loop().run_in_executor(None, _fetch_and_cache_interval, client, symbol)

    # Keep legacy cached_interval in sync
    cached_interval = cached_intervals.get(most_negative_symbol)

    if _last_print_minute != current_minute:
        _last_print_minute = current_minute
        if len(funding_rates) > 100:   # wait until we have a meaningful snapshot
            print_qualifying_funding_rates(funding_rates, cached_intervals, FUNDING_RATE_THRESHOLD)


async def run_logging_session(client, target_symbol: str, duration: int) -> None:
    timestamp    = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    current_rate = funding_rates.get(target_symbol, None)
    rate_str     = f"{current_rate:+.8f}".replace('.', 'p') if current_rate is not None else 'unknown'
    rate_log_str = f"{current_rate:.8f}" if current_rate is not None else 'unknown'
    interval     = cached_intervals.get(target_symbol, cached_interval)
    log_filename = os.path.join("Logs", f"log_{target_symbol}_{timestamp}_fr{rate_str}.txt")
    logger, listener = setup_async_logger(log_filename)
    listener.start()

    logger.info(f"Starting latency logging for {target_symbol} | funding_rate={rate_log_str} | interval={interval}h | duration={duration}s")

    book_ticker = None
    try:
        streams_connection = await client.websocket_streams.create_connection()
        book_ticker = await streams_connection.individual_symbol_book_ticker_streams(symbol=target_symbol)
        book_ticker.on("message", lambda data: capture_stream_record(logger, getattr(data, "E", 0), data))

        await asyncio.sleep(duration)
    except Exception as e:
        logger.error(f"Error in logging session: {e}")
    finally:
        if book_ticker:
            try:
                await book_ticker.unsubscribe()
            except Exception:
                pass
        logger.info("Session ended. Running analysis...")
        listener.stop()
        subprocess.Popen([sys.executable, "analyze_latency.py", log_filename])
