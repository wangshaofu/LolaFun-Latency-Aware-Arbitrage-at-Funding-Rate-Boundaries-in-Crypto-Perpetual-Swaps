import os
import logging


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


def print_most_negative_funding_rate(symbol: str, rate: float, interval: int | None) -> None:
    """Prints the most negative funding rate in a formatted line."""
    print(
        f"[Most Negative Funding Rate] "
        f"Symbol: {symbol} | "
        f"Rate: {rate:.8f} | "
        f"Interval: {interval}h"
    )

