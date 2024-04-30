import logging


def init_logging(level: str = "INFO"):
    """Initialize the logging with the given level."""
    if level == "DEBUG":
        log_level = logging.DEBUG
    elif level == "INFO":
        log_level = logging.INFO
    elif level == "WARNING":
        log_level = logging.WARNING
    elif level == "ERROR":
        log_level = logging.ERROR
    else:
        raise ValueError(f"Invalid logging level: {level}")

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
