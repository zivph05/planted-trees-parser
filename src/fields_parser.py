# src/parse_fields.py
"""
Author: Ziv P.H
Date: 2025-07-12
Description:

Turn one raw delimited line into a {field_name: typed_value} dict.

The public surface (parse_line(raw, cfg, fields))
Internally we follow single-responsibility:
decode ➜ split ➜ cast ➜ assemble.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Union

from src.config import ParserSettings, FieldSpec
from src.exceptions import (
    DecodeError,
    EmptyLineError,
    FieldCountMismatch,
    CastError,
)

logger = logging.getLogger(__name__)


def _to_str(raw: Union[str, bytes]) -> str:
    """
    Return *raw* as UTF-8 text or raise ValueError.
    Args:
        raw (Union[str, bytes]): The raw input to convert to a string.
    Returns:
        str: The input converted to a UTF-8 string.
    Raises:
        ValueError: If the input is empty, whitespace only, or cannot be decoded as UTF-8.
    """
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError as e:
            logger.exception("Could not decode incoming bytes")
            raise DecodeError() from e
    raw = raw.strip()
    if not raw:
        raise EmptyLineError("Incoming line is empty or whitespace only")
    return raw


def _split_line(line: str, delimiter: str, expected: int) -> List[str]:
    """
    Split *line* and make sure we got exactly *expected* parts.
    Args:
        line (str): The line to split.
        delimiter (str): The character used to split the line.
        expected (int): The expected number of parts after splitting.
    Returns:
        List[str]: A list of parts after splitting the line.
    Raises:
        ValueError: If the number of parts does not match the expected count.
    """
    parts = line.split(delimiter)
    if len(parts) != expected:
        raise FieldCountMismatch(f"Expected {expected} fields, got {len(parts)}: '{line}'")
    return [p.strip() for p in parts]


_TRUE = {"true", "1", "yes"}
_FALSE = {"false", "0", "no"}


def _parse_bool(v: str) -> bool:
    v_lc = v.lower()
    if v_lc in _TRUE:
        return True
    if v_lc in _FALSE:
        return False
    raise ValueError(f"Invalid boolean literal '{v}'")


# lookup table → cleaner than an if/elif ladder
_CASTERS = {
    "str": lambda v, fmt: v,
    "int": lambda v, fmt: int(v),
    "float": lambda v, fmt: float(v),
    "bool": lambda v, fmt: _parse_bool(v),
    "datetime": lambda v, fmt: datetime.strptime(v, fmt),
}


def _cast(value: str, spec: FieldSpec) -> Any:
    """
    Cast *value* to the type specified in *spec*.
    Args:
        value (str): The value to cast.
        spec (FieldSpec): The field specification containing the type and format.
    Returns:
        Any: The value cast to the specified type.

    """
    typ, fmt = spec.type, spec.format

    caster = _CASTERS.get(typ)
    if caster is None:
        raise TypeError(f"Unsupported field type '{typ}' on '{spec.name}'")

    if typ == "datetime" and not fmt:
        raise CastError(f"Field '{spec.name}' is datetime but has no format string")

    try:
        return caster(value, fmt)
    except (ValueError, OverflowError) as e:
        raise CastError(f"Failed casting '{value}' → {typ}") from e


def parse_line(
        raw: Union[str, bytes],
        cfg: ParserSettings,
        fields: List[FieldSpec],
) -> Dict[str, Any]:
    """
    Parse a raw delimited line into a dictionary of field names and typed values.
    Args:
        raw (Union[str, bytes]): The raw input line to parse.
        cfg (ParserSettings): Configuration settings for the parser.
        fields (List[FieldSpec]): List of field specifications defining the output structure.
    Returns:
        Dict[str, Any]: A dictionary mapping field names to their corresponding typed values.
    Raises:
        ValueError: If the input line is invalid or does not match the expected format.
    """
    line = _to_str(raw)
    parts = _split_line(line, cfg.delimiter, len(fields))
    record = {spec.name: _cast(val, spec) for val, spec in zip(parts, fields, strict=True)}
    return record
