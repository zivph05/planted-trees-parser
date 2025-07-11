from src.config import ParserConfig
from .json_parser import JSONParser


def get_parser(cfg: ParserConfig):
    """
    Factory to return the correct parser instance based on cfg.parser.parse_to.
    """
    parse_type = cfg.parser.parse_to.lower()
    if parse_type == "json":
        return JSONParser(cfg)
    else:
        raise ValueError(f"Unsupported parser type: {cfg.parser.parse_to}")