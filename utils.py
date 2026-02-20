import os
import logging
import logging.handlers
import queue
import datetime

# ---------------------------------------------------------------------------
# Shared async-safe console logger
# ---------------------------------------------------------------------------
# One queue + listener for all console output so nothing blocks the event loop.

_console_queue    = queue.SimpleQueue()
_console_handler  = logging.StreamHandler()
_console_handler.setFormatter(logging.Formatter('%(message)s'))

console_listener = logging.handlers.QueueListener(
    _console_queue,
    _console_handler,
    respect_handler_level=True,
)

_console_logger = logging.getLogger("console")
_console_logger.setLevel(logging.INFO)
_console_logger.propagate = False
_console_logger.addHandler(logging.handlers.QueueHandler(_console_queue))


def aprint(msg: str) -> None:
    """Non-blocking print — enqueues the message for the background console thread."""
    _console_logger.info(msg)


def setup_logger(log_filename: str) -> logging.Logger:
    """Creates a logger that writes to both the given log file and the console."""
    logger = logging.getLogger(log_filename)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        os.makedirs(os.path.dirname(log_filename), exist_ok=True)
        file_handler = logging.FileHandler(log_filename)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger


def setup_async_logger(log_filename: str) -> tuple[logging.Logger, logging.handlers.QueueListener]:
    """
    Creates a high-throughput logger for stream sessions.

    The returned logger uses a QueueHandler so that calls to logger.info()
    on the asyncio event loop return instantly — actual disk/console writes
    happen on a dedicated background thread via QueueListener.

    Call  listener.stop()  when the session ends to flush and shut down.

    Usage:
        logger, listener = setup_async_logger(path)
        listener.start()
        ...
        listener.stop()
    """
    os.makedirs(os.path.dirname(log_filename), exist_ok=True)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    log_queue    = queue.SimpleQueue()
    queue_handler = logging.handlers.QueueHandler(log_queue)

    listener = logging.handlers.QueueListener(
        log_queue,
        file_handler,
        console_handler,
        respect_handler_level=True,
    )

    logger = logging.getLogger(log_filename)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.addHandler(queue_handler)
    logger.propagate = False

    return logger, listener


def capture_stream_record(logger: logging.Logger, event_time_ms: int, data: object) -> None:
    """
    Stamp the log record with the exact wall-clock time of arrival
    (before any processing), then enqueue it for background writing.

    The formatter's %(asctime)s will reflect this captured time, not
    the time the background thread eventually writes to disk.
    """
    arrival_ms = datetime.datetime.now().timestamp() * 1000
    record = logging.LogRecord(
        name    = logger.name,
        level   = logging.INFO,
        pathname= "",
        lineno  = 0,
        msg     = f"Stream message: {data}",
        args    =(),
        exc_info= None,
    )
    # Override created/msecs so %(asctime)s reflects true arrival time
    record.created = arrival_ms / 1000
    record.msecs   = arrival_ms % 1000
    logger.handle(record)


def print_qualifying_funding_rates(funding_rates: dict, cached_intervals: dict, threshold: float) -> None:
    """Prints all symbols whose funding rate is below the threshold, sorted by rate ascending."""
    qualifying = sorted(
        [(s, r) for s, r in funding_rates.items() if r < threshold],
        key=lambda x: x[1]
    )

    now       = datetime.datetime.now()
    next_hour = (now.hour + 1) % 24

    if not qualifying:
        aprint(f"[Funding Rates] No symbols below threshold ({threshold})")
        return

    lines = [f"[Funding Rates < {threshold}]  ({len(qualifying)} symbols)  {now.strftime('%H:%M:%S')}"]
    for symbol, rate in qualifying:
        interval = cached_intervals.get(symbol)
        if interval is not None:
            settlement_hours = set(range(0, 24, interval))
            settling = " ⚡" if next_hour in settlement_hours else ""
            interval_str = f"{interval}h"
        else:
            settling     = ""
            interval_str = "?h"
        lines.append(f"  {symbol:<20} {rate:+.8f}  [{interval_str}]{settling}")
    aprint("\n".join(lines))
