"""Built-in SQL functions exposed as Python helpers."""

from __future__ import annotations

from starrocks.column import Column
from starrocks.plan.expr import (
    CaseWhen,
    ColumnRef,
    FunctionCall,
    Literal,
    Star,
)


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


# -- helper: convert to Expr --------------------------------------------------

def _to_col(v: object) -> Column:
    """Convert a value to a Column if it isn't one already."""
    if isinstance(v, Column):
        return v
    return Column(Literal(v))


# -- string functions ---------------------------------------------------------

def concat(*args: str | Column) -> Column:
    """CONCAT(a, b, ...)."""
    return Column(FunctionCall("CONCAT", tuple(_to_col(a).expr for a in args)))


def substring(col: Column, pos: int, length: int) -> Column:
    """SUBSTRING(col, pos, length)."""
    return Column(FunctionCall("SUBSTRING", (col.expr, Literal(pos), Literal(length))))


def upper(col: Column) -> Column:
    """UPPER(col)."""
    return Column(FunctionCall("UPPER", (col.expr,)))


def lower(col: Column) -> Column:
    """LOWER(col)."""
    return Column(FunctionCall("LOWER", (col.expr,)))


def length(col: Column) -> Column:
    """LENGTH(col)."""
    return Column(FunctionCall("LENGTH", (col.expr,)))


def trim(col: Column) -> Column:
    """TRIM(col)."""
    return Column(FunctionCall("TRIM", (col.expr,)))


def replace(col: Column, old: str, new: str) -> Column:
    """REPLACE(col, old, new)."""
    return Column(FunctionCall("REPLACE", (col.expr, Literal(old), Literal(new))))


# -- date functions -----------------------------------------------------------

def year(col: Column) -> Column:
    """YEAR(col)."""
    return Column(FunctionCall("YEAR", (col.expr,)))


def month(col: Column) -> Column:
    """MONTH(col)."""
    return Column(FunctionCall("MONTH", (col.expr,)))


def day(col: Column) -> Column:
    """DAY(col)."""
    return Column(FunctionCall("DAY", (col.expr,)))


def date_trunc(unit: str, col: Column) -> Column:
    """DATE_TRUNC('unit', col)."""
    return Column(FunctionCall("DATE_TRUNC", (Literal(unit), col.expr)))


def now() -> Column:
    """NOW()."""
    return Column(FunctionCall("NOW"))


def current_date() -> Column:
    """CURRENT_DATE()."""
    return Column(FunctionCall("CURRENT_DATE"))


# -- math functions -----------------------------------------------------------

def abs(col: Column) -> Column:
    """ABS(col)."""
    return Column(FunctionCall("ABS", (col.expr,)))


def round(col: Column, decimals: int = 0) -> Column:
    """ROUND(col, decimals)."""
    return Column(FunctionCall("ROUND", (col.expr, Literal(decimals))))


def ceil(col: Column) -> Column:
    """CEIL(col)."""
    return Column(FunctionCall("CEIL", (col.expr,)))


def floor(col: Column) -> Column:
    """FLOOR(col)."""
    return Column(FunctionCall("FLOOR", (col.expr,)))


# -- conditional functions ----------------------------------------------------

class WhenBuilder:
    """Builder for CASE WHEN expressions."""

    def __init__(self, condition: Column, value: object) -> None:
        self._conditions: list[tuple] = [(condition.expr, _to_col(value).expr)]
        self._else_expr = None

    def when(self, condition: Column, value: object) -> WhenBuilder:
        """Add another WHEN clause."""
        self._conditions.append((condition.expr, _to_col(value).expr))
        return self

    def otherwise(self, value: object) -> Column:
        """Set the ELSE clause and return the final Column."""
        return Column(CaseWhen(
            conditions=tuple(self._conditions),
            else_expr=_to_col(value).expr,
        ))

    def end(self) -> Column:
        """Finalize without ELSE clause."""
        return Column(CaseWhen(conditions=tuple(self._conditions)))


def when(condition: Column, value: object) -> WhenBuilder:
    """Start a CASE WHEN expression."""
    return WhenBuilder(condition, value)


def coalesce(*args: Column | object) -> Column:
    """COALESCE(a, b, ...)."""
    return Column(FunctionCall("COALESCE", tuple(_to_col(a).expr for a in args)))


def if_(condition: Column, true_val: object, false_val: object) -> Column:
    """IF(condition, true_val, false_val)."""
    return Column(FunctionCall("IF", (
        condition.expr,
        _to_col(true_val).expr,
        _to_col(false_val).expr,
    )))


# -- window functions ---------------------------------------------------------

def row_number() -> Column:
    """ROW_NUMBER() — use with .over(window)."""
    return Column(FunctionCall("ROW_NUMBER"))


def rank() -> Column:
    """RANK() — use with .over(window)."""
    return Column(FunctionCall("RANK"))


def dense_rank() -> Column:
    """DENSE_RANK() — use with .over(window)."""
    return Column(FunctionCall("DENSE_RANK"))


def lag(col: Column, offset: int = 1, default: object = None) -> Column:
    """LAG(col, offset, default) — use with .over(window)."""
    args = [col.expr, Literal(offset)]
    if default is not None:
        args.append(_to_col(default).expr)
    return Column(FunctionCall("LAG", tuple(args)))


def lead(col: Column, offset: int = 1, default: object = None) -> Column:
    """LEAD(col, offset, default) — use with .over(window)."""
    args = [col.expr, Literal(offset)]
    if default is not None:
        args.append(_to_col(default).expr)
    return Column(FunctionCall("LEAD", tuple(args)))


# -- StarRocks-specific functions ---------------------------------------------

def bitmap_union(col: Column) -> Column:
    """BITMAP_UNION(col)."""
    return Column(FunctionCall("BITMAP_UNION", (col.expr,)))


def bitmap_count(col: Column) -> Column:
    """BITMAP_COUNT(col)."""
    return Column(FunctionCall("BITMAP_COUNT", (col.expr,)))


def bitmap_union_count(col: Column) -> Column:
    """BITMAP_UNION_COUNT(col)."""
    return Column(FunctionCall("BITMAP_UNION_COUNT", (col.expr,)))


def hll_union(col: Column) -> Column:
    """HLL_UNION(col)."""
    return Column(FunctionCall("HLL_UNION", (col.expr,)))


def hll_union_agg(col: Column) -> Column:
    """HLL_UNION_AGG(col)."""
    return Column(FunctionCall("HLL_UNION_AGG", (col.expr,)))


def approx_count_distinct(col_name: str) -> Column:
    """APPROX_COUNT_DISTINCT(col)."""
    return Column(FunctionCall("APPROX_COUNT_DISTINCT", (ColumnRef(col_name),)))


def array_agg(col: Column) -> Column:
    """ARRAY_AGG(col)."""
    return Column(FunctionCall("ARRAY_AGG", (col.expr,)))


def bm25(col: Column, query: str) -> Column:
    """BM25(col, 'query') — StarRocks full-text search scoring."""
    return Column(FunctionCall("BM25", (col.expr, Literal(query))))
