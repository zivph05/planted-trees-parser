"""
Author: Ziv P.H
Date: 2025-7-12
Description:
Exception classes for parser errors.

Defines custom exceptions for decoding, empty lines, field count mismatches, and type casting errors.
"""


class ParserError(Exception):
    """Base class for all parse_* errors – makes catching easy."""
    pass


class DecodeError(ParserError):
    """Input could not be decoded as UTF-8."""
    pass


class EmptyLineError(ParserError):
    """Line was empty / whitespace only."""
    pass


class FieldCountMismatch(ParserError):
    """Splitting produced the wrong number of columns."""
    pass


class CastError(ParserError):
    """Value could not be cast to required type."""
    pass
