"""Type mapping between StarRocks types + multimodal annotations and Daft types."""

from __future__ import annotations

from starrocks.types import Embedding, Image, Tensor


def sr_to_daft_type(sr_type: str, multimodal=None):
    """Map a StarRocks type string (+ optional multimodal annotation) to a Daft DataType.

    Args:
        sr_type: Normalized StarRocks type (e.g., "VARCHAR", "ARRAY", "DOUBLE").
        multimodal: Optional multimodal annotation (Image, Embedding, Tensor).
    Returns:
        A daft.DataType instance.
    """
    import daft

    # Multimodal overrides
    if isinstance(multimodal, Image):
        return daft.DataType.image()
    if isinstance(multimodal, Embedding):
        if multimodal.dim > 0:
            return daft.DataType.embedding(daft.DataType.float32(), multimodal.dim)
        return daft.DataType.list(daft.DataType.float32())
    if isinstance(multimodal, Tensor):
        dtype = _str_to_daft_dtype(multimodal.dtype)
        if multimodal.shape is not None:
            return daft.DataType.fixed_size_list(dtype, int(multimodal.shape[-1]))
        return daft.DataType.list(dtype)

    # Standard type mapping
    upper = sr_type.upper()
    if upper in ("TINYINT",):
        return daft.DataType.int8()
    if upper in ("SMALLINT",):
        return daft.DataType.int16()
    if upper in ("INT",):
        return daft.DataType.int32()
    if upper in ("BIGINT",):
        return daft.DataType.int64()
    if upper in ("LARGEINT",):
        return daft.DataType.int64()  # best approximation
    if upper in ("FLOAT",):
        return daft.DataType.float32()
    if upper in ("DOUBLE",):
        return daft.DataType.float64()
    if upper in ("BOOLEAN",):
        return daft.DataType.bool()
    if upper in ("VARCHAR", "CHAR", "STRING"):
        return daft.DataType.string()
    if upper in ("DATE",):
        return daft.DataType.date()
    if upper in ("DATETIME",):
        return daft.DataType.timestamp("us")
    if upper.startswith("DECIMAL"):
        return daft.DataType.float64()  # approximate
    if upper.startswith("ARRAY"):
        return daft.DataType.list(daft.DataType.string())  # generic fallback
    if upper in ("JSON",):
        return daft.DataType.string()

    return daft.DataType.string()  # fallback


def _str_to_daft_dtype(dtype_str: str):
    """Convert a string dtype name to a Daft DataType."""
    import daft

    mapping = {
        "float16": daft.DataType.float16,
        "float32": daft.DataType.float32,
        "float64": daft.DataType.float64,
        "int8": daft.DataType.int8,
        "int16": daft.DataType.int16,
        "int32": daft.DataType.int32,
        "int64": daft.DataType.int64,
    }
    factory = mapping.get(dtype_str.lower())
    if factory is not None:
        return factory()
    return daft.DataType.float32()
