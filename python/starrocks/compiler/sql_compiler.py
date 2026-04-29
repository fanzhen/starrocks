"""SQL compiler: converts a logical plan tree into a SQL string."""

from __future__ import annotations

from starrocks.plan.logical import (
    Aggregate,
    Distinct,
    Filter,
    Limit,
    LogicalPlan,
    Projection,
    Sort,
    TableScan,
)


class SQLCompiler:
    """Compile a ``LogicalPlan`` tree into a SQL query string.

    The compiler walks the plan tree top-down, collecting SQL clauses.
    The tree is always rooted at a TableScan and layers are stacked on top:

        Limit → Sort → Projection/Distinct → Aggregate → Filter → TableScan

    Any ordering is supported; the compiler peels layers in order.
    """

    def compile(self, plan: LogicalPlan) -> str:
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
            # Distinct may wrap a Projection
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

        # Leaf must be TableScan
        if not isinstance(node, TableScan):
            raise ValueError(f"Unsupported plan node at leaf: {type(node).__name__}")

        from_clause = node.qualified_name

        # Build SELECT
        if agg_select is not None:
            final_select = agg_select
        else:
            final_select = select_exprs

        distinct_kw = "DISTINCT " if distinct else ""
        sql = f"SELECT {distinct_kw}{final_select} FROM {from_clause}{where_clause}{group_clause}{order_clause}{limit_clause}"
        return sql
