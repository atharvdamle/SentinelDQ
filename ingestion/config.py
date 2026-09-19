"""Process bootstrap shared by the producer and both consumers."""

import logging
import os

from pythonjsonlogger import jsonlogger


def configure_logging():
    """Install the single JSON log format all three ingestion processes use.

    The three used to emit three different formats -- JSON, plain text, and
    plain text with a fourth field -- which made aggregated container logs
    unparseable.
    """
    handler = logging.StreamHandler()
    handler.setFormatter(jsonlogger.JsonFormatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)


def require(name):
    """Read a mandatory environment variable, or raise.

    Failing at startup beats passing None into requests.get or a Kafka config,
    where it surfaces much later as an unrelated error.
    """
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value
