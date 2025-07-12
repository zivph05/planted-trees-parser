"""
Author: Ziv P.H
Date: 2025-7-12
Description:
Prometheus metrics for the parser.

Defines counters and summaries for tracking message flow and parsing durations.
"""

from prometheus_client import Counter, Summary, start_http_server

MESSAGES_IN = Counter(
    'messages_in_total', 'Total number of messages received by the parser'
)
MESSAGES_OUT = Counter(
    'messages_out_total', 'Total number of messages successfully processed'
)
MESSAGES_ERROR = Counter(
    'messages_error_total', 'Total number of messages that resulted in an error'
)

PARSE_DURATION = Summary(
    'message_parse_duration_seconds', 'Time spent parsing individual messages'
)


def start_metrics_server(port: int = 8000) -> None:
    """Start the Prometheus metrics HTTP server."""
    start_http_server(port)
