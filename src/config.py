import os
from typing import List, Literal, Optional, Union

import yaml
import logging
from pydantic import BaseModel, Field, validator

logger = logging.getLogger(__name__)

# where to find your YAML
CONFIG_PATH = os.getenv("CONFIG_PATH", "config/parser-config.yaml")


class KafkaInput(BaseModel):
    type: Literal["kafka"]
    brokers: List[str] = Field(..., description="List of Kafka bootstrap servers")
    topic: str = Field(..., description="Kafka topic to consume from")
    group_id: str = Field(..., description="Consumer group id")
    auto_offset_reset: Literal["earliest", "latest"] = Field(
        "earliest", description="Where to start if no offset exists"
    )
    commit_interval_ms: int = Field(
        5000, ge=100, description="How often (ms) to commit offsets"
    )

    def __init__(self, **data):
        logger.debug(f"Initializing KafkaInput with data: {data}")
        super().__init__(**data)


class RabbitMQInput(BaseModel):
    type: Literal["rabbitmq"]
    host: str = Field(..., description="RabbitMQ host to connect to")
    port: int = Field(..., description="RabbitMQ port to connect to")
    queue: str = Field(..., description="Queue name to consume from")
    prefetch_count: Optional[int] = Field(
        None, ge=1, description="Prefetch count for RabbitMQ consumer"
    )

    def __init__(self, **data):
        logger.debug(f"Initializing RabbitMQInput with data: {data}")
        super().__init__(**data)


InputConfig = Union[KafkaInput, RabbitMQInput]


class ParserSettings(BaseModel):
    parse_to: Literal["json"] = Field(..., description="Output serialization format")
    delimiter: str = Field(
        "|", min_length=1, description="Character to split incoming lines on"
    )

    def __init__(self, **data):
        logger.debug(f"Initializing ParserSettings with data: {data}")
        super().__init__(**data)


class FieldSpec(BaseModel):
    name: str = Field(..., description="Field name in output JSON")
    type: Literal["str", "datetime", "int", "float", "bool"] = Field(
        ..., description="Data type for casting"
    )
    format: Optional[str] = Field(
        None,
        description="Datetime format (strftime) if type == datetime; ignored otherwise",
    )

    @validator("format", always=True)
    def check_format_for_datetime(cls, v, values):
        logger.debug(
            f"Validating format for field '{values.get('name')}' of type '{values.get('type')}'"
        )
        if values.get("type") == "datetime":
            if not v:
                logger.error("`format` must be provided for datetime fields")
                raise ValueError("`format` must be provided for datetime fields")
        return v

    def __init__(self, **data):
        logger.debug(f"Initializing FieldSpec with data: {data}")
        super().__init__(**data)


class RabbitMQOutput(BaseModel):
    type: Literal["rabbitmq"]
    host: str = Field(..., description="RabbitMQ host to publish to")
    port: int = Field(..., description="RabbitMQ port to publish to")
    exchange: str = Field(..., description="Exchange to publish to")
    routing_key: str = Field(..., description="Routing key for normal records")

    def __init__(self, **data):
        logger.debug(f"Initializing RabbitMQOutput with data: {data}")
        super().__init__(**data)


OutputConfig = RabbitMQOutput


class LoggingConfig(BaseModel):
    level: Literal["DEBUG", "INFO", "WARN", "ERROR"] = Field(
        "INFO", description="Logging level"
    )

    def __init__(self, **data):
        logger.debug(f"Initializing LoggingConfig with data: {data}")
        super().__init__(**data)


class ParserConfig(BaseModel):
    input: InputConfig
    parser: ParserSettings
    fields: List[FieldSpec]
    output: OutputConfig
    logging: LoggingConfig

    @classmethod
    def load(cls) -> "ParserConfig":
        """
        Load and validate the parser configuration from YAML.
        Raises a clear exception if the file is missing or invalid.
        """
        logger.info(f"Loading configuration from {CONFIG_PATH}")
        try:
            with open(CONFIG_PATH, "r") as f:
                data = yaml.safe_load(f)
                logger.debug(f"Raw config data: {data}")
        except FileNotFoundError as e:
            logger.exception(f"Configuration file not found at {CONFIG_PATH}")
            raise FileNotFoundError(f"Configuration file not found at {CONFIG_PATH}") from e

        try:
            config = cls(**data)
            logger.info("Configuration loaded and validated successfully")
            logger.debug(f"Final config object: {config}")
            return config
        except Exception as e:
            logger.exception("Invalid configuration provided")
            raise ValueError(f"Invalid configuration: {e}") from e


def get_config() -> ParserConfig:
    logger.info("Retrieving parser configuration")
    config = ParserConfig.load()
    logger.debug(f"Parsed configuration object: {config}")
    return config
