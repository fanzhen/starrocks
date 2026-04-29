"""DataFrame: lazy query builder with action methods."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from starrocks.column import Column, col
from starrocks.compiler.sql_compiler import SQLCompiler
from starrocks.plan.expr import Alias, BinaryOp, ColumnRef, Expr, FunctionCall, Literal
from starrocks.plan.logical import (
    Aggregate,
    Distinct,
    Filter,
    Join,
    Limit,
    LogicalPlan,
    MapBatches,
    Projection,
    SetOperation,
    Sort,
    SubqueryAlias,
    TableScan,
)

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

    def group_by(self, *cols: str | Column) -> GroupedDataFrame:
        """Group rows by one or more columns."""
        exprs = [ColumnRef(c) if isinstance(c, str) else c.expr for c in cols]
        return GroupedDataFrame(self._plan, self._session, exprs, self._schema)

    def order_by(self, *cols: Column) -> DataFrame:
        """Sort rows by one or more columns."""
        sort_exprs = [c.expr for c in cols]
        return DataFrame(
            Sort(self._plan, sort_exprs),
            self._session,
            schema=self._schema,
        )

    def limit(self, count: int) -> DataFrame:
        """Limit the number of rows returned."""
        return DataFrame(
            Limit(self._plan, count),
            self._session,
            schema=self._schema,
        )

    def distinct(self) -> DataFrame:
        """Return distinct rows."""
        return DataFrame(
            Distinct(self._plan),
            self._session,
            schema=self._schema,
        )

    def join(
        self,
        other: DataFrame,
        on: str | Column | None = None,
        how: str = "inner",
    ) -> DataFrame:
        """Join with another DataFrame.

        Args:
            other: Right-side DataFrame.
            on: Join key — column name (str) or expression (Column).
            how: Join type — "inner", "left", "right", "full", "cross".
        """
        if isinstance(on, str):
            # Simple equi-join on a shared column name
            left_alias = self._table_alias() or "_l"
            right_alias = other._table_alias() or "_r"
            on_expr = BinaryOp(
                "=",
                ColumnRef(on, table_alias=left_alias),
                ColumnRef(on, table_alias=right_alias),
            )
            left_plan = self._ensure_aliased(left_alias)
            right_plan = other._ensure_aliased(right_alias)
        elif isinstance(on, Column):
            on_expr = on.expr
            left_plan = self._plan
            right_plan = other._plan
        elif on is None:
            on_expr = None
            left_plan = self._plan
            right_plan = other._plan
        else:
            raise TypeError(f"join on= expects str or Column, got {type(on)}")

        merged_schema = list(self._schema) + list(other._schema)
        return DataFrame(
            Join(left_plan, right_plan, on=on_expr, how=how),
            self._session,
            schema=merged_schema,
        )

    def union(self, other: DataFrame) -> DataFrame:
        """UNION ALL with another DataFrame."""
        return DataFrame(
            SetOperation(self._plan, other._plan, op="UNION ALL"),
            self._session,
            schema=self._schema,
        )

    def union_distinct(self, other: DataFrame) -> DataFrame:
        """UNION DISTINCT with another DataFrame."""
        return DataFrame(
            SetOperation(self._plan, other._plan, op="UNION DISTINCT"),
            self._session,
            schema=self._schema,
        )

    def with_column(self, name: str, expr: Column) -> DataFrame:
        """Add or replace a column expression."""
        # Project all existing columns + the new one
        exprs: list[Expr] = [ColumnRef(c) for c, _ in self._schema]
        exprs.append(Alias(expr.expr, name))
        new_schema = list(self._schema) + [(name, "UNKNOWN")]
        return DataFrame(
            Projection(self._plan, exprs),
            self._session,
            schema=new_schema,
        )

    def drop(self, *col_names: str) -> DataFrame:
        """Remove columns by name."""
        drop_set = set(col_names)
        exprs: list[Expr] = []
        new_schema: list[tuple[str, str]] = []
        for name, dtype in self._schema:
            if name not in drop_set:
                exprs.append(ColumnRef(name))
                new_schema.append((name, dtype))
        return DataFrame(
            Projection(self._plan, exprs),
            self._session,
            schema=new_schema,
        )

    def rename(self, mapping: dict[str, str]) -> DataFrame:
        """Rename columns: {old_name: new_name}."""
        exprs: list[Expr] = []
        new_schema: list[tuple[str, str]] = []
        for name, dtype in self._schema:
            if name in mapping:
                new_name = mapping[name]
                exprs.append(Alias(ColumnRef(name), new_name))
                new_schema.append((new_name, dtype))
            else:
                exprs.append(ColumnRef(name))
                new_schema.append((name, dtype))
        return DataFrame(
            Projection(self._plan, exprs),
            self._session,
            schema=new_schema,
        )

    def with_bm25(self, col_name: str, query: str, alias_name: str = "score") -> DataFrame:
        """Add a BM25 score column for full-text search ranking."""
        exprs: list[Expr] = [ColumnRef(c) for c, _ in self._schema]
        exprs.append(Alias(FunctionCall("BM25", (ColumnRef(col_name), Literal(query))), alias_name))
        new_schema = list(self._schema) + [(alias_name, "DOUBLE")]
        return DataFrame(
            Projection(self._plan, exprs),
            self._session,
            schema=new_schema,
        )

    def alias(self, name: str) -> DataFrame:
        """Give this DataFrame a table alias (for use in joins)."""
        return DataFrame(
            SubqueryAlias(self._plan, name),
            self._session,
            schema=self._schema,
        )

    def _table_alias(self) -> str | None:
        """Extract a simple table name for aliasing, if possible."""
        if isinstance(self._plan, TableScan):
            return self._plan.table_name
        if isinstance(self._plan, SubqueryAlias):
            return self._plan.alias
        return None

    def _ensure_aliased(self, alias: str) -> LogicalPlan:
        """Wrap the plan in a SubqueryAlias if it isn't one already."""
        if isinstance(self._plan, SubqueryAlias):
            return self._plan
        if isinstance(self._plan, TableScan):
            # TableScan uses its own name, wrap for explicit alias
            return SubqueryAlias(self._plan, alias)
        return SubqueryAlias(self._plan, alias)

    # -- internal helpers ------------------------------------------------------

    def _has_map_batches(self) -> bool:
        """Check if the logical plan contains any MapBatches node."""
        return self._contains_map_batches(self._plan)

    @staticmethod
    def _contains_map_batches(node: LogicalPlan) -> bool:
        if isinstance(node, MapBatches):
            return True
        child = getattr(node, "child", None)
        if child is not None:
            return DataFrame._contains_map_batches(child)
        return False

    def _execute_pipeline(self):
        """Execute the hybrid pipeline via PipelineExecutor. Returns a Daft DataFrame."""
        from starrocks.execution.pipeline import PipelineExecutor
        return PipelineExecutor().execute(self._plan, self._session)

    # -- actions (trigger execution) ------------------------------------------

    def to_sql(self) -> str:
        """Compile the logical plan to a SQL string without executing."""
        return self._compiler.compile(self._plan)

    def show(self, limit: int = 20) -> None:
        """Execute and pretty-print results."""
        if self._has_map_batches():
            daft_df = self._execute_pipeline()
            daft_df.show(limit)
            return
        sql = self.to_sql()
        self._session.fetcher.execute_show(sql, limit=limit)

    def to_pandas(self, batch_size: int | None = None) -> Any:
        """Execute and return a pandas DataFrame.

        If the plan contains MapBatches nodes, the pipeline executor runs
        the hybrid SQL+Daft pipeline automatically.

        Uses Arrow Flight SQL for zero-copy transfer when configured,
        otherwise falls back to MySQL protocol.

        Args:
            batch_size: If set, fetch rows in batches to reduce memory usage.
                        Only effective with MySQL protocol (pure SQL path).
        """
        if self._has_map_batches():
            daft_df = self._execute_pipeline()
            return daft_df.to_pandas()
        arrow_conn = self._session.arrow_connection
        if arrow_conn is not None and batch_size is None:
            sql = self.to_sql()
            arrow_table = arrow_conn.execute_to_arrow(sql)
            return arrow_table.to_pandas()
        sql = self.to_sql()
        return self._session.fetcher.execute_to_pandas(sql, batch_size=batch_size)

    def count(self) -> int:
        """Return the number of rows."""
        if self._has_map_batches():
            daft_df = self._execute_pipeline()
            return len(daft_df.to_pandas())
        sql = self.to_sql()
        count_sql = f"SELECT COUNT(*) AS cnt FROM ({sql}) _t"
        return self._session.fetcher.execute_count(count_sql)

    def first(self) -> dict[str, Any] | None:
        """Return the first row as a dict, or None."""
        if self._has_map_batches():
            daft_df = self._execute_pipeline()
            pdf = daft_df.limit(1).to_pandas()
            if pdf.empty:
                return None
            return pdf.iloc[0].to_dict()
        sql = self.to_sql()
        rows = self._session.fetcher.execute_to_dicts(f"{sql} LIMIT 1")
        return rows[0] if rows else None

    def explain(self) -> str:
        """Return the StarRocks execution plan."""
        sql = self.to_sql()
        return self._session.fetcher.execute_explain(sql)

    def to_arrow(self):
        """Execute SQL on StarRocks and return a pyarrow.Table.

        Uses Arrow Flight SQL for zero-copy transfer when configured,
        otherwise falls back to MySQL + pandas conversion.
        """
        if self._has_map_batches():
            import pyarrow as pa
            daft_df = self._execute_pipeline()
            return pa.Table.from_pandas(daft_df.to_pandas())
        arrow_conn = self._session.arrow_connection
        if arrow_conn is not None:
            sql = self.to_sql()
            return arrow_conn.execute_to_arrow(sql)
        # Fallback: MySQL → pandas → pyarrow
        import pyarrow as pa
        pdf = self.to_pandas()
        return pa.Table.from_pandas(pdf)

    def to_daft(self):
        """Execute and return a Daft DataFrame.

        If the plan contains MapBatches nodes, the pipeline executor
        handles hybrid SQL+Daft execution automatically.

        Otherwise, executes SQL on StarRocks and converts results to Daft.
        """
        if self._has_map_batches():
            return self._execute_pipeline()
        import daft
        arrow_conn = self._session.arrow_connection
        if arrow_conn is not None:
            arrow_table = self.to_arrow()
            return daft.from_arrow(arrow_table)
        pdf = self.to_pandas()
        return daft.from_pandas(pdf)

    def map_batches(self, func, *, result_columns=None) -> DataFrame:
        """Apply a Python UDF via Daft (lazy transformation).

        Returns a new DataFrame with a MapBatches logical node. Execution
        is deferred until an action (to_pandas, show, etc.) is called,
        at which point the pipeline executor auto-routes SQL segments to
        StarRocks and UDF segments to Daft.

        Args:
            func: A Python callable or Daft UDF to apply.
            result_columns: Optional dict of {col_name: daft.DataType} for output schema.
        Returns:
            A new DataFrame containing the MapBatches transformation.
        """
        return DataFrame(
            MapBatches(self._plan, func=func, result_columns=result_columns),
            self._session,
            schema=self._schema,
        )

    def to_starrocks(self, table: str, mode: str = "append") -> int:
        """Write results to a StarRocks table.

        If the plan contains MapBatches nodes, the pipeline executor runs
        first to produce a Daft DataFrame, which is then written via INSERT INTO.
        Otherwise, uses INSERT INTO ... SELECT.

        Args:
            table: Target table name.
            mode: "append" (INSERT INTO) or "overwrite" (TRUNCATE + INSERT).
        Returns:
            Number of rows written.
        """
        if self._has_map_batches():
            from starrocks.execution.pipeline import PipelineExecutor
            daft_df = PipelineExecutor().execute(self._plan, self._session)
            return self._session.write_daft(daft_df, table, mode=mode)
        # Pure SQL path: INSERT INTO ... SELECT
        sql = self.to_sql()
        if mode == "overwrite":
            self._session.connection.execute(f"TRUNCATE TABLE `{table}`")
        insert_sql = f"INSERT INTO `{table}` {sql}"
        self._session.connection.execute(insert_sql)
        # Return approximate count
        result = self._session.connection.execute(f"SELECT COUNT(*) AS cnt FROM `{table}`")
        return result[0].get("cnt", 0) if result else 0

    # -- repr -----------------------------------------------------------------

    def __repr__(self) -> str:
        return f"DataFrame(columns={self.columns})"


class GroupedDataFrame:
    """Intermediate object returned by ``DataFrame.group_by()``."""

    def __init__(
        self,
        plan: LogicalPlan,
        session: Session,
        group_keys: list[Expr],
        schema: list[tuple[str, str]],
    ) -> None:
        self._plan = plan
        self._session = session
        self._group_keys = group_keys
        self._schema = schema

    def agg(self, *agg_cols: Column) -> DataFrame:
        """Apply aggregate functions and return a DataFrame."""
        agg_exprs = [c.expr for c in agg_cols]
        agg_plan = Aggregate(self._plan, self._group_keys, agg_exprs)
        # Build schema from group keys + agg expressions
        new_schema: list[tuple[str, str]] = []
        for k in self._group_keys:
            new_schema.append((_expr_name(k), "UNKNOWN"))
        for e in agg_exprs:
            new_schema.append((_expr_name(e), "UNKNOWN"))
        return DataFrame(agg_plan, self._session, schema=new_schema)


class _WindowMethod:
    """Descriptor that works as both classmethod and instance method."""

    def __init__(self, func):
        self._func = func

    def __get__(self, obj, cls=None):
        if obj is None:
            # Called on the class: Window.partition_by(...) / Window.order_by(...)
            return lambda *args, **kwargs: self._func(cls(), *args, **kwargs)
        # Called on an instance
        return lambda *args, **kwargs: self._func(obj, *args, **kwargs)


class Window:
    """Window specification for window functions.

    Supports both class-level and instance-level calls::

        Window.partition_by("dept").order_by("salary")
        Window.order_by("id")
    """

    def __init__(
        self,
        partition_exprs: list[Expr] | None = None,
        order_exprs: list[Expr] | None = None,
    ) -> None:
        self._partition_exprs: list[Expr] = partition_exprs or []
        self._order_exprs: list[Expr] = order_exprs or []

    @_WindowMethod
    def partition_by(self, *cols: str | Column) -> Window:
        """Set PARTITION BY columns."""
        return Window(
            partition_exprs=[
                ColumnRef(c) if isinstance(c, str) else c.expr for c in cols
            ],
            order_exprs=list(self._order_exprs),
        )

    @_WindowMethod
    def order_by(self, *cols: str | Column) -> Window:
        """Set ORDER BY columns."""
        return Window(
            partition_exprs=list(self._partition_exprs),
            order_exprs=[
                ColumnRef(c) if isinstance(c, str) else c.expr for c in cols
            ],
        )


def _expr_name(expr: Expr) -> str:
    """Best-effort name extraction from an expression node."""
    if isinstance(expr, ColumnRef):
        return expr.name
    from starrocks.plan.expr import Alias
    if isinstance(expr, Alias):
        return expr.alias_name
    return expr.to_sql()
