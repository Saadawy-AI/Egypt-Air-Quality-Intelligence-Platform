import time
import functools

from ingestion.config.settings import settings
from ingestion.utils.logger import get_logger

logger = get_logger("Retry")


def retry(max_retries=None, backoff_factor=None, exceptions=(Exception,)):
    max_retries = settings.MAX_RETRIES if max_retries is None else max_retries
    backoff_factor = settings.RETRY_BACKOFF if backoff_factor is None else backoff_factor

    def decorator(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)

                except exceptions as error:
                    if attempt == max_retries:
                        logger.error(
                            "%s failed after %s attempts: %s",
                            func.__name__,
                            max_retries + 1,
                            error
                        )
                        raise

                    delay = backoff_factor * (2 ** attempt)

                    logger.warning(
                        "%s failed (attempt %s/%s): %s. Retrying in %.2f seconds...",
                        func.__name__,
                        attempt + 1,
                        max_retries + 1,
                        error,
                        delay
                    )

                    time.sleep(delay)

        return wrapper

    return decorator
