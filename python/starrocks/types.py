"""StarRocks type system and type mapping."""

from __future__ import annotations

# Mapping from StarRocks INFORMATION_SCHEMA column types to friendly names.
SR_TYPE_MAP: dict[str, str] = {
    "tinyint": "TINYINT",
    "smallint": "SMALLINT",
    "int": "INT",
    "bigint": "BIGINT",
    "largeint": "LARGEINT",
    "float": "FLOAT",
    "double": "DOUBLE",
    "decimal": "DECIMAL",
    "char": "CHAR",
    "varchar": "VARCHAR",
    "string": "STRING",
    "date": "DATE",
    "datetime": "DATETIME",
    "boolean": "BOOLEAN",
    "json": "JSON",
    "array": "ARRAY",
    "map": "MAP",
    "struct": "STRUCT",
    "bitmap": "BITMAP",
    "hll": "HLL",
}


def normalize_type(raw: str) -> str:
    """Normalize a raw StarRocks type string to upper-case canonical form."""
    lower = raw.strip().lower()
    for prefix, canonical in SR_TYPE_MAP.items():
        if lower.startswith(prefix):
            return canonical
    return raw.upper()
