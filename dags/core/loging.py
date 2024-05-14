import logging
import sys


def start_logging() -> logging.Logger:
    """Start logging."""
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s  [%(levelname)s]  %(message)s',
    )
    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler(stream=sys.stdout)
    logger.addHandler(handler)
    return logger
