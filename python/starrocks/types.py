"""StarRocks type system and type mapping."""

from __future__ import annotations

from dataclasses import dataclass, field


# -- Multimodal type annotations ---------------------------------------------

@dataclass(frozen=True)
class Image:
    """Annotates a VARCHAR/STRING column as containing image references.

    Args:
        mode: "url" if the column stores image URLs, "bytes" if it stores
              raw image bytes (e.g., hex-encoded or base64).
    """
    mode: str = "url"

    def __repr__(self) -> str:
        return f"Image(mode={self.mode!r})"


@dataclass(frozen=True)
class Embedding:
    """Annotates an ARRAY<FLOAT> column as a fixed-dimension embedding vector.

    Args:
        dim: Dimensionality of the embedding (e.g., 384, 768, 1536).
    """
    dim: int = 0

    def __repr__(self) -> str:
        return f"Embedding(dim={self.dim})"


@dataclass(frozen=True)
class Tensor:
    """Annotates a column as containing tensor data.

    Args:
        shape: Expected tensor shape, or None for dynamic shape.
        dtype: Element data type (e.g., "float32", "int64").
    """
    shape: tuple[int, ...] | None = None
    dtype: str = "float32"

    def __repr__(self) -> str:
        return f"Tensor(shape={self.shape}, dtype={self.dtype!r})"


# A multimodal type is one of Image, Embedding, or Tensor.
MultimodalType = Image | Embedding | Tensor


# -- StarRocks type mapping ---------------------------------------------------

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
