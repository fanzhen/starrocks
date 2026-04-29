"""SQL compiler: converts a logical plan tree into a SQL string."""

from __future__ import annotations

from starrocks.plan.logical import Filter, LogicalPlan, Projection, TableScan


class SQLCompiler:
    """Compile a ``LogicalPlan`` tree into a SQL query string."""

    def compile(self, plan: LogicalPlan) -> str:
        select_exprs = "*"
        from_clause = ""
        where_clause = ""

        # Walk the plan tree from top to bottom, collecting clauses.
        node = plan

        # 1. Outermost Projection?
        if isinstance(node, Projection):
            select_exprs = ", ".join(e.to_sql() for e in node.expressions)
            node = node.child

        # 2. Filter?
        if isinstance(node, Filter):
            where_clause = f" WHERE {node.predicate.to_sql()}"
            node = node.child

        # 3. Must be a TableScan at the bottom.
        if isinstance(node, TableScan):
            from_clause = node.qualified_name
        else:
            raise ValueError(f"Unsupported plan node at leaf: {type(node).__name__}")

        sql = f"SELECT {select_exprs} FROM {from_clause}{where_clause}"
        return sql
