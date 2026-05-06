"""SQL compiler: converts a logical plan tree into a SQL string."""

from __future__ import annotations

from starrocks.plan.logical import (
    Aggregate,
    Distinct,
    Filter,
    Join,
    Limit,
    LogicalPlan,
    MapBatches,
    Projection,
    RawSQL,
    SetOperation,
    Sort,
    SubqueryAlias,
    TableScan,
)

_JOIN_TYPE_MAP = {
    "inner": "INNER JOIN",
    "left": "LEFT JOIN",
    "right": "RIGHT JOIN",
    "full": "FULL OUTER JOIN",
    "cross": "CROSS JOIN",
}


class SQLCompiler:
    """Compile a ``LogicalPlan`` tree into a SQL query string.

    Uses recursive compilation: each node type knows how to render itself,
    and complex sub-plans are wrapped as subqueries when needed.
    """

    def __init__(self) -> None:
        self._alias_counter = 0

    def compile(self, plan: LogicalPlan) -> str:
        self._alias_counter = 0
        return self._compile(plan)

    def _next_alias(self) -> str:
        self._alias_counter += 1
        return f"_t{self._alias_counter}"

    def _compile(self, plan: LogicalPlan) -> str:
        # SetOperation (UNION) — handle before the single-stream peel
        if isinstance(plan, SetOperation):
            return self._compile_set_op(plan)

        # Join — two children
        if isinstance(plan, Join):
            return self._compile_join(plan)

        # Single-stream plan: peel layers top-down
        return self._compile_single(plan)

    def _compile_single(self, plan: LogicalPlan) -> str:
        node = plan

        limit_clause = ""
        order_clause = ""
        select_exprs = "*"
        distinct = False
        group_clause = ""
        agg_select: str | None = None
        where_clause = ""

        # Peel Limit
        if isinstance(node, Limit):
            limit_clause = f" LIMIT {node.count}"
            node = node.child

        # Peel Sort
        if isinstance(node, Sort):
            parts = [e.to_sql() for e in node.sort_exprs]
            order_clause = f" ORDER BY {', '.join(parts)}"
            node = node.child

        # Peel Projection or Distinct
        if isinstance(node, Distinct):
            distinct = True
            node = node.child
            if isinstance(node, Projection):
                select_exprs = ", ".join(e.to_sql() for e in node.expressions)
                node = node.child
        elif isinstance(node, Projection):
            select_exprs = ", ".join(e.to_sql() for e in node.expressions)
            node = node.child

        # Peel Aggregate
        if isinstance(node, Aggregate):
            key_parts = [e.to_sql() for e in node.group_keys]
            agg_parts = [e.to_sql() for e in node.agg_exprs]
            agg_select = ", ".join(key_parts + agg_parts)
            group_clause = f" GROUP BY {', '.join(key_parts)}"
            node = node.child

        # Peel Filter
        if isinstance(node, Filter):
            where_clause = f" WHERE {node.predicate.to_sql()}"
            node = node.child

        # Leaf: TableScan, RawSQL, SubqueryAlias, Join, or SetOperation
        from_clause = self._compile_source(node)

        # Build SELECT
        final_select = agg_select if agg_select is not None else select_exprs
        distinct_kw = "DISTINCT " if distinct else ""
        return f"SELECT {distinct_kw}{final_select} FROM {from_clause}{where_clause}{group_clause}{order_clause}{limit_clause}"

    def _compile_source(self, node: LogicalPlan) -> str:
        """Compile a leaf/source node into a FROM-clause fragment."""
        if isinstance(node, TableScan):
            return node.qualified_name
        if isinstance(node, RawSQL):
            alias = self._next_alias()
            return f"({node.query}) {alias}"
        if isinstance(node, SubqueryAlias):
            inner = self._compile(node.child)
            return f"({inner}) `{node.alias}`"
        if isinstance(node, MapBatches) and node.remote_func_name:
            inner = self._compile_map_batches(node)
            alias = self._next_alias()
            return f"({inner}) {alias}"
        # Any other plan node (Join, SetOperation, Projection, Filter, etc.)
        # gets wrapped as a subquery
        inner = self._compile(node)
        alias = self._next_alias()
        return f"({inner}) {alias}"

    def _compile_join(self, plan: Join) -> str:
        """Compile a JOIN node."""
        left_sql = self._compile_source(plan.left)
        right_sql = self._compile_source(plan.right)
        join_type = _JOIN_TYPE_MAP.get(plan.how, "INNER JOIN")

        if plan.on is not None:
            on_sql = plan.on.to_sql()
            return f"SELECT * FROM {left_sql} {join_type} {right_sql} ON {on_sql}"
        else:
            return f"SELECT * FROM {left_sql} {join_type} {right_sql}"

    def _compile_map_batches(self, node: MapBatches) -> str:
        """Compile a remote MapBatches node to SQL.

        Generates: SELECT map_batches('func_name') FROM (child_sql) _sub
        """
        child_sql = self._compile(node.child)
        alias = self._next_alias()
        return f"SELECT map_batches('{node.remote_func_name}') FROM ({child_sql}) {alias}"

    def _compile_set_op(self, plan: SetOperation) -> str:
        """Compile a UNION / UNION ALL / etc."""
        left_sql = self._compile(plan.left)
        right_sql = self._compile(plan.right)
        return f"{left_sql} {plan.op} {right_sql}"
