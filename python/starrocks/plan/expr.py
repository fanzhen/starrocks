"""Expression nodes for the logical plan."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class Expr:
    """Base class for all expression nodes."""

    def to_sql(self) -> str:
        raise NotImplementedError


@dataclass(frozen=True)
class ColumnRef(Expr):
    """Reference to a table column, optionally qualified by table alias."""

    name: str
    table_alias: str | None = None

    def to_sql(self) -> str:
        if self.table_alias:
            return f"`{self.table_alias}`.`{self.name}`"
        return f"`{self.name}`"


@dataclass(frozen=True)
class Literal(Expr):
    """A constant value."""

    value: Any

    def to_sql(self) -> str:
        if self.value is None:
            return "NULL"
        if isinstance(self.value, bool):
            return "TRUE" if self.value else "FALSE"
        if isinstance(self.value, (int, float)):
            return str(self.value)
        if isinstance(self.value, str):
            escaped = self.value.replace("\\", "\\\\").replace("'", "\\'")
            return f"'{escaped}'"
        return str(self.value)


@dataclass(frozen=True)
class Star(Expr):
    """Represents * (all columns) or a raw SQL fragment."""

    def to_sql(self) -> str:
        return "*"


@dataclass(frozen=True)
class BinaryOp(Expr):
    """Binary operation: left <op> right."""

    op: str
    left: Expr
    right: Expr

    def to_sql(self) -> str:
        return f"{self.left.to_sql()} {self.op} {self.right.to_sql()}"


@dataclass(frozen=True)
class UnaryOp(Expr):
    """Unary operation: <op> operand  or  operand <op>."""

    op: str
    operand: Expr
    prefix: bool = True

    def to_sql(self) -> str:
        if self.prefix:
            return f"{self.op} {self.operand.to_sql()}"
        return f"{self.operand.to_sql()} {self.op}"


@dataclass(frozen=True)
class FunctionCall(Expr):
    """SQL function call: func_name(args...)."""

    func_name: str
    args: tuple[Expr, ...] = ()
    distinct: bool = False

    def to_sql(self) -> str:
        args_sql = ", ".join(a.to_sql() for a in self.args)
        distinct_kw = "DISTINCT " if self.distinct else ""
        return f"{self.func_name}({distinct_kw}{args_sql})"


@dataclass(frozen=True)
class Alias(Expr):
    """Expression with an alias: expr AS alias_name."""

    expr: Expr
    alias_name: str

    def to_sql(self) -> str:
        return f"{self.expr.to_sql()} AS `{self.alias_name}`"


@dataclass(frozen=True)
class Between(Expr):
    """BETWEEN expression: expr BETWEEN low AND high."""

    expr: Expr
    low: Expr
    high: Expr

    def to_sql(self) -> str:
        return f"{self.expr.to_sql()} BETWEEN {self.low.to_sql()} AND {self.high.to_sql()}"


@dataclass(frozen=True)
class InList(Expr):
    """IN expression: expr IN (v1, v2, ...)."""

    expr: Expr
    values: tuple[Expr, ...]

    def to_sql(self) -> str:
        vals = ", ".join(v.to_sql() for v in self.values)
        return f"{self.expr.to_sql()} IN ({vals})"


@dataclass(frozen=True)
class Like(Expr):
    """LIKE expression: expr LIKE pattern."""

    expr: Expr
    pattern: Expr

    def to_sql(self) -> str:
        return f"{self.expr.to_sql()} LIKE {self.pattern.to_sql()}"


@dataclass(frozen=True)
class MatchExpr(Expr):
    """StarRocks MATCH expression: expr MATCH_PHRASE/MATCH_ALL/MATCH_ANY 'pattern'."""

    expr: Expr
    pattern: str
    match_type: str  # "MATCH_PHRASE", "MATCH_ALL", "MATCH_ANY", "MATCH_PHRASE_PREFIX"

    def to_sql(self) -> str:
        escaped = self.pattern.replace("\\", "\\\\").replace("'", "\\'")
        return f"{self.expr.to_sql()} {self.match_type} '{escaped}'"


@dataclass(frozen=True)
class CaseWhen(Expr):
    """CASE WHEN ... THEN ... ELSE ... END expression."""

    conditions: tuple[tuple[Expr, Expr], ...]  # ((when_expr, then_expr), ...)
    else_expr: Expr | None = None

    def to_sql(self) -> str:
        parts = ["CASE"]
        for when_expr, then_expr in self.conditions:
            parts.append(f"WHEN {when_expr.to_sql()} THEN {then_expr.to_sql()}")
        if self.else_expr is not None:
            parts.append(f"ELSE {self.else_expr.to_sql()}")
        parts.append("END")
        return " ".join(parts)


@dataclass(frozen=True)
class WindowExpr(Expr):
    """Window function expression: func OVER (PARTITION BY ... ORDER BY ...)."""

    func: Expr
    partition_by: tuple[Expr, ...] = ()
    order_by: tuple[Expr, ...] = ()

    def to_sql(self) -> str:
        parts: list[str] = []
        if self.partition_by:
            cols = ", ".join(e.to_sql() for e in self.partition_by)
            parts.append(f"PARTITION BY {cols}")
        if self.order_by:
            cols = ", ".join(e.to_sql() for e in self.order_by)
            parts.append(f"ORDER BY {cols}")
        over_clause = " ".join(parts)
        return f"{self.func.to_sql()} OVER ({over_clause})"
