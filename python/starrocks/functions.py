"""Built-in SQL functions exposed as Python helpers."""

from __future__ import annotations

from starrocks.column import Column
from starrocks.plan.expr import ColumnRef, FunctionCall, Literal, Star


def lit(value: object) -> Column:
    """Create a literal value column."""
    return Column(Literal(value))


# -- aggregate functions ------------------------------------------------------

def _agg(func_name: str, col_name: str) -> Column:
    """Helper: create an aggregate function call on a column."""
    if col_name == "*":
        return Column(FunctionCall(func_name, (Star(),)))
    return Column(FunctionCall(func_name, (ColumnRef(col_name),)))


def sum(col_name: str) -> Column:
    return _agg("SUM", col_name)


def avg(col_name: str) -> Column:
    return _agg("AVG", col_name)


def count(col_name: str) -> Column:
    return _agg("COUNT", col_name)


def min(col_name: str) -> Column:
    return _agg("MIN", col_name)


def max(col_name: str) -> Column:
    return _agg("MAX", col_name)


def count_distinct(col_name: str) -> Column:
    """COUNT(DISTINCT col)."""
    return Column(FunctionCall("COUNT", (ColumnRef(col_name),), distinct=True))
