"""
Logging utility for the sales pipeline.
"""
import logging
import os
from datetime import datetime


def get_logger(name: str, log_dir: str = None, level=logging.INFO) -> logging.Logger:
    """Returns a configured logger with console and file handlers."""
    if log_dir is None:
        log_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        ch = logging.StreamHandler()
        ch.setLevel(level)
        date_str = datetime.now().strftime("%Y-%m-%d")
        fh = logging.FileHandler(os.path.join(log_dir, f"pipeline_{date_str}.log"))
        fh.setLevel(level)
        fmt = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
        ch.setFormatter(fmt)
        fh.setFormatter(fmt)
        logger.addHandler(ch)
        logger.addHandler(fh)

    return logger
