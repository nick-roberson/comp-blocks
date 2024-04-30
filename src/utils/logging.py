import logging

LOG_MAPPING = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
}


def init_logging(level: str = "INFO"):
    """Initialize the logging with the given level."""
    if level not in LOG_MAPPING:
        raise ValueError(f"Invalid logging level: {level}")

    logging.basicConfig(
        level=LOG_MAPPING[level],
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
