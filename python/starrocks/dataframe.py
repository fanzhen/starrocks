"""DataFrame: lazy query builder with action methods."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from starrocks.column import Column, col
from starrocks.compiler.sql_compiler import SQLCompiler
from starrocks.plan.expr import ColumnRef, Expr
from starrocks.plan.logical import Filter, LogicalPlan, Projection, TableScan

if TYPE_CHECKING:
    from starrocks.session import Session


class DataFrame:
    """Lazy DataFrame backed by a logical plan.

    All transformation methods (``filter``, ``select``, …) return a new
    ``DataFrame`` without triggering execution.  Action methods (``show``,
    ``to_pandas``, ``count``, …) compile the plan to SQL and execute it.
    """

    def __init__(
        self,
        plan: LogicalPlan,
        session: Session,
        schema: list[tuple[str, str]] | None = None,
    ) -> None:
        self._plan = plan
        self._session = session
        self._schema = schema or []
        self._compiler = SQLCompiler()

    # -- schema properties ----------------------------------------------------

    @property
    def columns(self) -> list[str]:
        """Column names."""
        return [c[0] for c in self._schema]

    @property
    def schema(self) -> list[tuple[str, str]]:
        """List of (column_name, type_string) pairs."""
        return list(self._schema)

    # -- transformations (lazy) -----------------------------------------------

    def filter(self, condition: Column) -> DataFrame:
        """Apply a row filter (WHERE clause)."""
        return DataFrame(
            Filter(self._plan, condition.expr),
            self._session,
            schema=self._schema,
        )

    def where(self, condition: Column) -> DataFrame:
        """Alias for ``filter``."""
        return self.filter(condition)

    def select(self, *cols: str | Column) -> DataFrame:
        """Project specific columns or expressions."""
        exprs: list[Expr] = []
        new_schema: list[tuple[str, str]] = []
        for c in cols:
            if isinstance(c, str):
                exprs.append(ColumnRef(c))
                # Try to find the type in current schema
                for name, dtype in self._schema:
                    if name == c:
                        new_schema.append((name, dtype))
                        break
                else:
                    new_schema.append((c, "UNKNOWN"))
            elif isinstance(c, Column):
                exprs.append(c.expr)
                new_schema.append((_expr_name(c.expr), "UNKNOWN"))
            else:
                raise TypeError(f"select() expects str or Column, got {type(c)}")
        return DataFrame(
            Projection(self._plan, exprs),
            self._session,
            schema=new_schema,
        )

    # -- actions (trigger execution) ------------------------------------------

    def to_sql(self) -> str:
        """Compile the logical plan to a SQL string without executing."""
        return self._compiler.compile(self._plan)

    def show(self, limit: int = 20) -> None:
        """Execute and pretty-print results."""
        sql = self.to_sql()
        self._session.fetcher.execute_show(sql, limit=limit)

    def to_pandas(self) -> Any:
        """Execute and return a pandas DataFrame."""
        sql = self.to_sql()
        return self._session.fetcher.execute_to_pandas(sql)

    def count(self) -> int:
        """Return the number of rows."""
        sql = self.to_sql()
        count_sql = f"SELECT COUNT(*) AS cnt FROM ({sql}) _t"
        return self._session.fetcher.execute_count(count_sql)

    def first(self) -> dict[str, Any] | None:
        """Return the first row as a dict, or None."""
        sql = self.to_sql()
        rows = self._session.fetcher.execute_to_dicts(f"{sql} LIMIT 1")
        return rows[0] if rows else None

    def explain(self) -> str:
        """Return the StarRocks execution plan."""
        sql = self.to_sql()
        return self._session.fetcher.execute_explain(sql)

    # -- repr -----------------------------------------------------------------

    def __repr__(self) -> str:
        return f"DataFrame(columns={self.columns})"


def _expr_name(expr: Expr) -> str:
    """Best-effort name extraction from an expression node."""
    if isinstance(expr, ColumnRef):
        return expr.name
    return expr.to_sql()
