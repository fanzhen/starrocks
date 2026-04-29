"""Column class with operator overloading for building expressions."""

from __future__ import annotations

from starrocks.plan.expr import (
    Alias,
    Between,
    BinaryOp,
    ColumnRef,
    Expr,
    InList,
    Like,
    Literal,
    MatchExpr,
    UnaryOp,
    WindowExpr,
)


def col(name: str) -> Column:
    """Create a Column reference by name.

    Supports dotted notation: ``col("t.id")`` → ```t`.`id```.
    """
    if "." in name:
        parts = name.split(".", 1)
        return Column(ColumnRef(parts[1], table_alias=parts[0]))
    return Column(ColumnRef(name))


class Column:
    """Represents a column expression with operator overloading.

    Wraps an ``Expr`` node and provides Python operators that build
    the corresponding SQL expression tree lazily.
    """

    def __init__(self, expr: Expr) -> None:
        self._expr = expr

    @property
    def expr(self) -> Expr:
        return self._expr

    def to_sql(self) -> str:
        return self._expr.to_sql()

    # -- alias ----------------------------------------------------------------

    def alias(self, name: str) -> Column:
        return Column(Alias(self._expr, name))

    # -- multimodal type annotation -------------------------------------------

    def cast(self, target_type) -> Column:
        """Annotate this column with a multimodal type (Image, Embedding, Tensor).

        This attaches type metadata for Daft type mapping without changing
        the underlying SQL expression.

        Args:
            target_type: An instance of Image, Embedding, or Tensor.
        Returns:
            A new Column with the type annotation attached.
        """
        from starrocks.types import Image, Embedding, Tensor
        if not isinstance(target_type, (Image, Embedding, Tensor)):
            raise TypeError(
                f"cast() expects Image, Embedding, or Tensor, got {type(target_type).__name__}"
            )
        col = Column(self._expr)
        col._multimodal_type = target_type
        return col

    # -- comparison operators -------------------------------------------------

    def __eq__(self, other: object) -> Column:  # type: ignore[override]
        return Column(BinaryOp("=", self._expr, _to_expr(other)))

    def __ne__(self, other: object) -> Column:  # type: ignore[override]
        return Column(BinaryOp("!=", self._expr, _to_expr(other)))

    def __gt__(self, other: object) -> Column:
        return Column(BinaryOp(">", self._expr, _to_expr(other)))

    def __ge__(self, other: object) -> Column:
        return Column(BinaryOp(">=", self._expr, _to_expr(other)))

    def __lt__(self, other: object) -> Column:
        return Column(BinaryOp("<", self._expr, _to_expr(other)))

    def __le__(self, other: object) -> Column:
        return Column(BinaryOp("<=", self._expr, _to_expr(other)))

    # -- logical operators ----------------------------------------------------

    def __and__(self, other: Column) -> Column:
        return Column(BinaryOp("AND", self._expr, other._expr))

    def __or__(self, other: Column) -> Column:
        return Column(BinaryOp("OR", self._expr, other._expr))

    def __invert__(self) -> Column:
        return Column(UnaryOp("NOT", self._expr))

    # -- arithmetic operators -------------------------------------------------

    def __add__(self, other: object) -> Column:
        return Column(BinaryOp("+", self._expr, _to_expr(other)))

    def __sub__(self, other: object) -> Column:
        return Column(BinaryOp("-", self._expr, _to_expr(other)))

    def __mul__(self, other: object) -> Column:
        return Column(BinaryOp("*", self._expr, _to_expr(other)))

    def __truediv__(self, other: object) -> Column:
        return Column(BinaryOp("/", self._expr, _to_expr(other)))

    def __mod__(self, other: object) -> Column:
        return Column(BinaryOp("%", self._expr, _to_expr(other)))

    # -- convenience predicates -----------------------------------------------

    def is_null(self) -> Column:
        return Column(UnaryOp("IS NULL", self._expr, prefix=False))

    def is_not_null(self) -> Column:
        return Column(UnaryOp("IS NOT NULL", self._expr, prefix=False))

    def between(self, low: object, high: object) -> Column:
        return Column(Between(self._expr, _to_expr(low).expr, _to_expr(high).expr))

    def is_in(self, *values: object) -> Column:
        return Column(InList(self._expr, tuple(_to_expr(v).expr for v in values)))

    def like(self, pattern: str) -> Column:
        return Column(Like(self._expr, Literal(pattern)))

    def match_phrase(self, pattern: str) -> Column:
        """StarRocks MATCH_PHRASE predicate."""
        return Column(MatchExpr(self._expr, pattern, "MATCH_PHRASE"))

    def match_all(self, pattern: str) -> Column:
        """StarRocks MATCH_ALL predicate."""
        return Column(MatchExpr(self._expr, pattern, "MATCH_ALL"))

    def match_any(self, pattern: str) -> Column:
        """StarRocks MATCH_ANY predicate."""
        return Column(MatchExpr(self._expr, pattern, "MATCH_ANY"))

    def match_phrase_prefix(self, pattern: str) -> Column:
        """StarRocks MATCH_PHRASE_PREFIX predicate."""
        return Column(MatchExpr(self._expr, pattern, "MATCH_PHRASE_PREFIX"))

    # -- sort helpers (used in order_by) --------------------------------------

    def asc(self) -> Column:
        return Column(UnaryOp("ASC", self._expr, prefix=False))

    def desc(self) -> Column:
        return Column(UnaryOp("DESC", self._expr, prefix=False))

    # -- window function support -----------------------------------------------

    def over(self, window: "Window") -> Column:
        """Apply a window specification to this expression."""
        return Column(WindowExpr(
            self._expr,
            partition_by=tuple(window._partition_exprs),
            order_by=tuple(window._order_exprs),
        ))

    # -- repr -----------------------------------------------------------------

    def __repr__(self) -> str:
        return f"Column({self.to_sql()})"


def _to_expr(value: object) -> Column:
    """Convert a Python value or Column to a Column wrapping an Expr."""
    if isinstance(value, Column):
        return value
    return Column(Literal(value))
