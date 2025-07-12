"""
Author: Ziv P.H
Date: 2025-7-12
util for converting flat dictionaries with dot-separated keys
"""


def densify(flat: dict) -> dict:
    """
    Fold keys with dots into nested objects.

    {"a.b": 1, "a.c": 2, "x": 3}
    → {"a": {"b": 1, "c": 2}, "x": 3}
    Args:
        flat (dict): A dictionary with keys that may contain dots.
    Returns:
        dict: A new dictionary with nested structure based on the keys.
    """
    out = {}
    for k, v in flat.items():
        if "." not in k:
            out[k] = v
            continue

        cur = out
        *parts, leaf = k.split(".")
        for part in parts:
            cur = cur.setdefault(part, {})
        cur[leaf] = v
    return out
