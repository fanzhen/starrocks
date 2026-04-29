"""Built-in SQL functions exposed as Python helpers."""

from __future__ import annotations

from starrocks.column import Column
from starrocks.plan.expr import FunctionCall, Literal


def lit(value: object) -> Column:
    """Create a literal value column."""
    return Column(Literal(value))
