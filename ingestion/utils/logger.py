import logging
from pathlib import Path

from ingestion.config.settings import settings


def get_logger(name: str) -> logging.Logger:

    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(settings.LOG_LEVEL.upper())

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)

    # File Handler
    log_dir = settings.BASE_DIR / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(
        log_dir / "ingestion.log",
        encoding="utf-8"
    )

    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    return logger
