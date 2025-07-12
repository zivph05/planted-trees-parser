"""
Author: Ziv P.H
Date: 2025-7-12
Description:
Input handler classes for consuming messages from Kafka and RabbitMQ.

Defines abstract and concrete handlers for connecting to and consuming messages from supported input systems.
"""

import logging
from abc import ABC, abstractmethod
from typing import Union, Type, Optional, Callable, Dict, Any

import pika
from kafka import KafkaConsumer, errors as kafka_errors
from pika.exceptions import AMQPConnectionError

from src.config import KafkaInput, RabbitMQInput

logger = logging.getLogger(__name__)


class BaseInputHandler(ABC):
    """
    Abstract base class for input handlers.
    Defines the interface for connecting to input systems and consuming messages.
    """

    def __init__(self, cfg, metrics_handler=None):
        """
        Initialize the input handler with configuration and optional metrics handler.

        Args:
            cfg: Configuration object for the input system.
            metrics_handler (Optional[object]): Optional metrics handler for tracking metrics.
        """
        self.cfg = cfg
        self.metrics = metrics_handler
        self.consumer = None
        self.conn = None
        self.ch = None
        logger.debug("BaseInputHandler initialized with config: %s", cfg)

    @abstractmethod
    def connect(self):
        """
        Establish the connection or setup resources (e.g., Kafka consumer or RabbitMQ channel).
        Should be idempotent and implemented by subclasses.
        """
        raise NotImplementedError

    @abstractmethod
    def consume(self, on_message: Callable[[bytes], None]):
        """
        Start consuming messages and dispatch them to the provided callback.

        Args:
            on_message (Callable[[bytes], None]): Callback function to process each message.
        """
        raise NotImplementedError


class KafkaInputHandler(BaseInputHandler):
    """
    Input handler for Kafka.
    Manages connections and message consumption from Kafka topics.
    """

    def _common_kwargs(self) -> Dict[str, Any]:
        """
        Prepare common keyword arguments for KafkaConsumer based on the configuration.
        Returns:
            Dict[str, Any]: Common keyword arguments for KafkaConsumer.
        """
        kw = dict(
            bootstrap_servers=self.cfg.brokers,
            group_id=self.cfg.group_id,
            auto_offset_reset=self.cfg.auto_offset_reset,
            enable_auto_commit=True,
            auto_commit_interval_ms=self.cfg.commit_interval_ms,
        )

        if self.cfg.user and self.cfg.password:
            kw.update(
                security_protocol="SASL_PLAINTEXT",
                sasl_mechanism="PLAIN",
                sasl_plain_username=self.cfg.user,
                sasl_plain_password=self.cfg.password,
            )
        return kw

    def connect(self):
        """
        Open a Kafka consumer connection to the specified topic.
        Returns:
            KafkaConsumer: The Kafka consumer instance.
        Raises:
            KafkaError: If the connection to Kafka fails.
        """
        if self.consumer is not None:
            logger.debug("Kafka consumer already connected")
            return self.consumer

        logger.debug("Connecting to Kafka brokers: %s", self.cfg.brokers)
        try:
            self.consumer = KafkaConsumer(
                self.cfg.topic,
                **self._common_kwargs(),
            )
            logger.info("Successfully connected to Kafka")
        except kafka_errors.KafkaError as e:
            logger.error("Failed to connect to Kafka brokers: %s", e)
            raise
        return self.consumer

    def consume(self, on_message: Callable[[bytes], None]):
        """
        Consume messages from the Kafka topic and process them using the provided callback.
        Args:
            on_message (Callable[[bytes], None]): Callback function to process each message.
        """
        consumer = self.connect()
        logger.debug("Starting Kafka message consumption")
        try:
            for msg in consumer:
                logger.debug("Processing Kafka message: %s", msg.value)
                on_message(msg.value)
        except KeyboardInterrupt:
            logger.info("Kafka consumption interrupted by user")
        finally:
            logger.debug("Closing Kafka consumer")
            consumer.close()


class RabbitMQInputHandler(BaseInputHandler):
    """
    Input handler for RabbitMQ.
    Manages connections and message consumption from RabbitMQ queues.
    """

    def connect(self):
        """
        Open a RabbitMQ connection and channel.

        Returns:
            BlockingChannel: The RabbitMQ channel instance.

        Raises:
            AMQPConnectionError: If the connection to RabbitMQ fails.
        """
        credentials = pika.PlainCredentials('guest', 'guest')
        if self.cfg.user and self.cfg.password:
            credentials = pika.PlainCredentials(self.cfg.user, self.cfg.password)

        if self.ch is not None:
            logger.debug("RabbitMQ channel already connected")
            return self.ch
        logger.debug("Connecting to RabbitMQ host: %s port: %s", self.cfg.host, self.cfg.port)
        params = pika.ConnectionParameters(
            host=self.cfg.host,
            port=self.cfg.port,
            credentials=credentials,
            heartbeat=0,
            blocked_connection_timeout=None,
        )
        try:
            self.conn = pika.BlockingConnection(params)
            self.ch = self.conn.channel()
            self.ch.queue_declare(
                queue=self.cfg.queue,
                durable=True,
            )

            if self.cfg.prefetch_count:
                self.ch.basic_qos(prefetch_count=self.cfg.prefetch_count)
            logger.info("Successfully connected to RabbitMQ")
        except AMQPConnectionError as e:
            logger.error("Failed to connect to RabbitMQ: %s", e)
            raise
        return self.ch

    def consume(self, on_message: Callable[[bytes], None]):
        """
        Consume messages from the RabbitMQ queue and process them using the provided callback.

        Args:
            on_message (Callable[[bytes], None]): Callback function to process each message.

        Raises:
            Exception: If an error occurs while processing a message.
        """
        ch = self.connect()
        logger.debug("Starting RabbitMQ message consumption")

        def _callback(ch, method, properties, body):
            logger.debug("Received RabbitMQ message: %s", body)
            ch.basic_ack(method.delivery_tag)
            on_message(body)

        ch.basic_consume(queue=self.cfg.queue, on_message_callback=_callback)
        try:
            ch.start_consuming()
        except KeyboardInterrupt:
            logger.info("RabbitMQ consumption interrupted by user")
        finally:
            logger.debug("Closing RabbitMQ connection")
            if self.conn and not self.conn.is_closed:
                self.conn.close()


_input_handlers_map: dict[Type, Type] = {
    KafkaInput: KafkaInputHandler,
    RabbitMQInput: RabbitMQInputHandler,
}


def get_input_handler(
    input_cfg: Union[KafkaInput, RabbitMQInput],
    metrics_handler: Optional[object] = None
) -> BaseInputHandler:
    """
    Factory function to get the appropriate input handler based on the configuration type.

    Args:
        input_cfg (Union[KafkaInput, RabbitMQInput]): The input configuration object.
        metrics_handler (Optional[object]): Optional metrics handler for tracking metrics.

    Returns:
        BaseInputHandler: The appropriate input handler instance.

    Raises:
        ValueError: If the input type is unsupported.
    """
    logger.debug("Getting input handler for configuration: %s", type(input_cfg).__name__)
    handler_cls = _input_handlers_map.get(type(input_cfg))
    if not handler_cls:
        logger.error("Unsupported input type: %s", type(input_cfg).__name__)
        raise ValueError(f"Unsupported input type: {type(input_cfg)}")
    logger.info("Input handler %s selected", handler_cls.__name__)
    return handler_cls(input_cfg, metrics_handler)
