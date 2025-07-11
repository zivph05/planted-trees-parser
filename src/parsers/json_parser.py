import json
import logging
from src.config import ParserConfig
from src.input_handler import get_input_handler
from src.fields_parser import parse_line
from src.output_handler import get_output_handler
from src.metrics import (
    MESSAGES_IN,
    MESSAGES_OUT,
    MESSAGES_ERROR,
    PARSE_DURATION,
)

logger = logging.getLogger(__name__)


class JSONParser:
    """
    Parser that reads raw messages, transforms text to JSON, and publishes the result.
    """

    def __init__(self, cfg: ParserConfig):
        """
        Initialize the JSONParser with the provided configuration.

        Args:
            cfg (ParserConfig): The configuration object containing input, output, and parsing settings.
        """
        self.cfg = cfg
        self.input_handler = get_input_handler(cfg.input)
        self.output_handler = get_output_handler(cfg.output)

    def handle_message(self, raw: bytes):
        """
        Process a raw message, parse it into a structured format, convert it to JSON, and publish it.

        Args:
            raw (bytes): The raw message data to be processed.
        """
        MESSAGES_IN.inc()
        with PARSE_DURATION.time():
            try:
                record = parse_line(raw, self.cfg.parser, self.cfg.fields)
                json_data = json.dumps(record, indent=4, sort_keys=True, default=str)
                logger.debug("Parsed message: %s", json_data)
                self.output_handler.publish(json_data)
                MESSAGES_OUT.inc()
            except ValueError as e:
                logger.error(
                    f"Failed to parse message: {e}, sending to error output"
                )
                json_data = json.dumps(
                    {"raw_message": raw, "error": str(e)}, indent=4
                )
                MESSAGES_ERROR.inc()

    def run(self):
        """
        Start the parsing loop to continuously consume, parse, and publish messages.
        """
        self.input_handler.consume(self.handle_message)
