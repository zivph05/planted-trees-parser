import logging
from abc import ABC, abstractmethod
from typing import Type, Dict, Union, Optional
from pika import BlockingConnection, ConnectionParameters
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError, AMQPError
from kafka import KafkaProducer, errors as kafka_errors

from src.config import RabbitMQOutput

logger = logging.getLogger(__name__)


class BaseOutputHandler(ABC):
    """
    Abstract base class for output handlers.
    Defines the interface for connecting, publishing, and closing connections to output systems.
    """

    def __init__(self, cfg, metrics_handler: Optional[object] = None):
        """
        Initialize the output handler with configuration and optional metrics handler.

        Args:
            cfg: Configuration object for the output system.
            metrics_handler (Optional[object]): Optional metrics handler for tracking metrics.
        """
        self.cfg = cfg
        self.metrics = metrics_handler
        self.conn = None
        self.ch = None
        self.producer = None
        logger.debug("OutputHandler initialized with config: %s", cfg)

    @abstractmethod
    def connect(self):
        """
        Establish the connection or producer.
        Should be idempotent and implemented by subclasses.
        """
        raise NotImplementedError

    @abstractmethod
    def publish(self, body: bytes) -> None:
        """
        Publish a message to the output system.
        Must call `connect()` first.

        Args:
            body (bytes): The message body to be published.
        """
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        """
        Cleanly shut down connections or producers.
        Should be implemented by subclasses.
        """
        raise NotImplementedError


class RabbitMQOutputHandler(BaseOutputHandler):
    """
    Output handler for RabbitMQ.
    Manages connections, publishing, and closing for RabbitMQ.
    """

    def connect(self):
        """
        Establish a connection to RabbitMQ and create a channel.
        Returns the channel if already connected.

        Returns:
            BlockingChannel: The RabbitMQ channel.

        Raises:
            AMQPConnectionError: If the connection to RabbitMQ fails.
        """
        logger.debug("Attempting to connect to RabbitMQ")
        if self.ch is not None:
            logger.debug("RabbitMQ channel already connected")
            return self.ch
        params = ConnectionParameters(
            host=self.cfg.host,
            port=self.cfg.port,
            heartbeat=0,
            blocked_connection_timeout=None
        )
        try:
            self.conn: BlockingConnection = BlockingConnection(params)
            self.ch: BlockingChannel = self.conn.channel()
            logger.info("Successfully connected to RabbitMQ")
        except AMQPConnectionError as e:
            logger.error("RabbitMQ connection error: %s", e)
            raise
        except Exception as e:
            logger.exception("Unexpected error while connecting to RabbitMQ: %s", e)
            raise
        return self.ch

    def publish(self, body: bytes):
        """
        Publish a message to RabbitMQ.

        Args:
            body (bytes): The message body to be published.

        Raises:
            AMQPError: If publishing the message fails.
        """
        logger.debug("Publishing message to RabbitMQ")
        try:
            ch = self.connect()
            ch.basic_publish(
                exchange=self.cfg.exchange,
                routing_key=self.cfg.routing_key,
                body=body
            )
            logger.info("Message published to RabbitMQ successfully")
        except AMQPError as e:
            logger.error("RabbitMQ publishing error: %s", e)
            raise
        except Exception as e:
            logger.exception("Unexpected error while publishing to RabbitMQ: %s", e)
            raise

    def close(self):
        """
        Close the RabbitMQ channel and connection cleanly.
        Logs any errors encountered during closure.
        """
        logger.debug("Closing RabbitMQ connection and channel")
        try:
            if self.ch and self.ch.is_open:
                self.ch.close()
                logger.info("RabbitMQ channel closed")
        except Exception as e:
            logger.warning("Error closing RabbitMQ channel: %s", e, exc_info=True)
        try:
            if self.conn and self.conn.is_open:
                self.conn.close()
                logger.info("RabbitMQ connection closed")
        except Exception as e:
            logger.warning("Error closing RabbitMQ connection: %s", e, exc_info=True)


class KafkaOutputHandler(BaseOutputHandler):
    """
    Output handler for Kafka.
    Manages connections, publishing, and closing for Kafka.
    """

    def connect(self):
        """
        Establish a connection to Kafka and create a producer.
        Returns the producer if already connected.

        Returns:
            KafkaProducer: The Kafka producer.

        Raises:
            kafka_errors.KafkaError: If the connection to Kafka fails.
        """
        logger.debug("Attempting to connect to Kafka")
        if self.producer is not None:
            logger.debug("Kafka producer already connected")
            return self.producer
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.cfg.brokers,
                linger_ms=5,  # Ensure maximum throughput
                retries=5,
                acks='all'
            )
            logger.info("Successfully connected to Kafka")
        except kafka_errors.KafkaError as e:
            logger.error("Kafka connection error: %s", e)
            raise
        except Exception as e:
            logger.exception("Unexpected error while connecting to Kafka: %s", e)
            raise
        return self.producer

    def publish(self, body: bytes):
        """
        Publish a message to Kafka.

        Args:
            body (bytes): The message body to be published.
        Raises:
            kafka_errors.KafkaError: If publishing the message fails.
        """
        logger.debug("Publishing message to Kafka")
        try:
            producer = self.connect()
            future = producer.send(
                topic=self.cfg.topic,
                value=body,
                key=None
            )
            future.get(timeout=10)  # Block until sent or error
            logger.info("Message published to Kafka successfully")
        except kafka_errors.KafkaError as e:
            logger.error("Kafka publishing error: %s", e)
            raise
        except Exception as e:
            logger.exception("Unexpected error while publishing to Kafka: %s", e)
            raise

    def close(self):
        """
        Close the Kafka producer cleanly.
        Flushes any pending messages before closing.
        Logs any errors encountered during closure.
        """
        logger.debug("Closing Kafka producer")
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close()
                logger.info("Kafka producer closed successfully")
            except Exception as e:
                logger.warning("Error closing Kafka producer: %s", e, exc_info=True)


_output_handlers_map: Dict[Type, Type[BaseOutputHandler]] = {
    RabbitMQOutput: RabbitMQOutputHandler,
}


def get_output_handler(
        output_cfg: Union[RabbitMQOutput],
        metrics_handler: Optional[object] = None
) -> BaseOutputHandler:
    """
    Factory function to get the appropriate output handler based on the configuration type.

    Args:
        output_cfg (Union[RabbitMQOutput]): The output configuration object.
        metrics_handler (Optional[object]): Optional metrics handler for tracking metrics.

    Returns:
        BaseOutputHandler: The appropriate output handler instance.

    Raises:
        ValueError: If the output type is unsupported.
    """
    logger.debug("Getting output handler for configuration: %s", type(output_cfg).__name__)
    handler_cls = _output_handlers_map.get(type(output_cfg))
    if not handler_cls:
        logger.error("Unsupported output type: %s", type(output_cfg).__name__)
        raise ValueError(f"Unsupported output type: {type(output_cfg)}")
    logger.info("Output handler %s selected", handler_cls.__name__)
    return handler_cls(output_cfg, metrics_handler)
