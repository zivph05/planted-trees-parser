"""
Author: Ziv P.H
Date: 2025-7-12
Description:
Main entry point for the PlantedTrees parser.

Handles configuration loading, logging setup, metrics server startup, and parser execution.
"""

import logging
import sys

from kafka.errors import KafkaError
from pika.exceptions import AMQPConnectionError, AMQPError

from src.config import get_config
from src.parsers import get_parser
from src.metrics import start_metrics_server


def setup_logging(level: str):
    root = logging.getLogger()
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("kafka.protocol").setLevel(logging.WARNING)
    logging.getLogger("kafka.consumer").setLevel(logging.WARNING)
    logging.getLogger("pika").setLevel(logging.ERROR)
    logging.getLogger("pika.adapters").setLevel(logging.ERROR)
    # Remove default handlers
    for handler in root.handlers[:]:
        root.removeHandler(handler)
    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)-5s %(name)s: %(message)s"))
    root.addHandler(ch)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))


def main():
    cfg = get_config()

    setup_logging(cfg.logging.level)
    logging.info("Starting PlantedTrees Parser")

    # Start Prometheus metrics server
    start_metrics_server()

    # Instantiate parser based on type
    parser = get_parser(cfg)
    try:
        parser.run()
    except KeyboardInterrupt:
        logging.info("Parser interrupted by user, shutting down")
    except (AMQPConnectionError, AMQPError, KafkaError):
        logging.exception("Fatal error in parser")
        sys.exit(1)
    finally:
        output_handler = parser.output_handler
        output_handler.close()


if __name__ == '__main__':
    main()
