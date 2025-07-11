import logging
from datetime import datetime
from typing import Any, Dict, Union, List
from src.config import ParserSettings, FieldSpec

logger = logging.getLogger(__name__)


def parse_line(raw: Union[bytes, str], cfg: ParserSettings, fields: List[FieldSpec]) -> Dict[str, Any]:
    """
    Parse a raw message (bytes or str) into a JSON-ready dict based on cfg.fields and cfg.parser.delimiter.
    """
    if isinstance(raw, bytes):
        try:
            line = raw.decode('utf-8')
        except Exception:
            logger.exception('Failed to decode raw bytes')
            raise
    else:
        line = raw
    if line is None:
        raise ValueError('Invalid line format: Input line is None')
    line = line.strip()
    parts = line.split(cfg.delimiter)
    if not parts or all(part.strip() == '' for part in parts):
        raise ValueError('Invalid line format: empty or whitespace-only input')
    if len(parts) != len(fields):
        raise ValueError(f'Expected {len(fields)} fields, got {len(parts)}: {line}')

    record: Dict[str, Any] = {}
    for part, spec in zip(parts, fields):
        name = spec.name
        typ = spec.type
        raw_value = part.strip()
        try:
            if typ == 'str':
                value = raw_value
            elif typ == 'int':
                value = int(raw_value)
            elif typ == 'float':
                value = float(raw_value)
            elif typ == 'bool':
                if raw_value.lower() in ('true', '1', 'yes'):
                    value = True
                elif raw_value.lower() in ('false', '0', 'no'):
                    value = False
                else:
                    raise ValueError(f"Invalid boolean value: {raw_value}")
            elif typ == 'datetime':
                fmt = spec.format
                value = datetime.strptime(raw_value, fmt)
            else:
                raise ValueError(f'Unsupported field type: {typ}')
        except ValueError as ve:
            logger.error(f"Invalid type for field '{name}': {raw_value} ({ve})")
            raise ValueError(f"Invalid type for field '{name}': {raw_value}") from ve
        except Exception as e:
            logger.error(f"Error parsing field '%s': %s", name, e)
            raise
        record[name] = value
    return record
