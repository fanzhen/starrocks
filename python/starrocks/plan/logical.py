"""Logical plan nodes."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from starrocks.plan.expr import Expr


class LogicalPlan:
    """Base class for all logical plan nodes."""
    pass


@dataclass
class TableScan(LogicalPlan):
    """Scan a table by name, optionally in a specific database/catalog."""

    table_name: str
    database: str | None = None
    columns: list[str] | None = None
    catalog: str | None = None

    @property
    def qualified_name(self) -> str:
        if self.catalog and self.database:
            return f"`{self.catalog}`.`{self.database}`.`{self.table_name}`"
        if self.database:
            return f"`{self.database}`.`{self.table_name}`"
        return f"`{self.table_name}`"


@dataclass
class Filter(LogicalPlan):
    """Apply a predicate to the child plan."""

    child: LogicalPlan
    predicate: Expr


@dataclass
class Projection(LogicalPlan):
    """Select specific columns / expressions from the child plan."""

    child: LogicalPlan
    expressions: list[Expr]


@dataclass
class Aggregate(LogicalPlan):
    """GROUP BY + aggregate expressions."""

    child: LogicalPlan
    group_keys: list[Expr]
    agg_exprs: list[Expr]


@dataclass
class Sort(LogicalPlan):
    """ORDER BY clause."""

    child: LogicalPlan
    sort_exprs: list[Expr]


@dataclass
class Limit(LogicalPlan):
    """LIMIT clause."""

    child: LogicalPlan
    count: int


@dataclass
class Distinct(LogicalPlan):
    """SELECT DISTINCT."""

    child: LogicalPlan


@dataclass
class Join(LogicalPlan):
    """JOIN two plans."""

    left: LogicalPlan
    right: LogicalPlan
    on: Expr | None = None
    how: str = "inner"  # inner, left, right, full, cross


@dataclass
class SetOperation(LogicalPlan):
    """UNION / UNION ALL / INTERSECT / EXCEPT."""

    left: LogicalPlan
    right: LogicalPlan
    op: str = "UNION ALL"  # "UNION ALL", "UNION DISTINCT"


@dataclass
class RawSQL(LogicalPlan):
    """A raw SQL query as a leaf node."""

    query: str


@dataclass
class SubqueryAlias(LogicalPlan):
    """Wrap a child plan with a table alias."""

    child: LogicalPlan
    alias: str


@dataclass
class MapBatches(LogicalPlan):
    """Apply a Python UDF via Daft map_batches.

    This is a lazy transformation node. Execution is deferred until an
    action (to_pandas, show, etc.) triggers the pipeline executor.
    """

    child: LogicalPlan
    func: Callable
    result_columns: dict | None = None
