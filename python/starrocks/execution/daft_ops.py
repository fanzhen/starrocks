"""Apply logical plan nodes on a Daft DataFrame."""

from __future__ import annotations

from starrocks.plan.logical import Filter, Limit, LogicalPlan, Projection
from starrocks.plan.expr import Alias, BinaryOp, ColumnRef, Expr, Literal


def apply_plan_on_daft(daft_df, nodes: list[LogicalPlan]):
    """Apply a sequence of logical plan nodes to a Daft DataFrame.

    Supported nodes: Filter, Projection, Limit.
    """
    import daft

    for node in nodes:
        if isinstance(node, Filter):
            expr = _expr_to_daft(node.predicate)
            daft_df = daft_df.where(expr)
        elif isinstance(node, Projection):
            cols = [_expr_to_daft(e) for e in node.expressions]
            daft_df = daft_df.select(*cols)
        elif isinstance(node, Limit):
            daft_df = daft_df.limit(node.count)
        else:
            raise ValueError(f"Unsupported plan node for Daft execution: {type(node).__name__}")
    return daft_df


def _expr_to_daft(expr: Expr):
    """Convert a logical Expr to a Daft expression."""
    import daft

    if isinstance(expr, ColumnRef):
        return daft.col(expr.name)
    elif isinstance(expr, Literal):
        return daft.lit(expr.value)
    elif isinstance(expr, Alias):
        return _expr_to_daft(expr.child).alias(expr.alias_name)
    elif isinstance(expr, BinaryOp):
        left = _expr_to_daft(expr.left)
        right = _expr_to_daft(expr.right)
        op = expr.op
        if op == "=":
            return left == right
        elif op == "!=":
            return left != right
        elif op == ">":
            return left > right
        elif op == ">=":
            return left >= right
        elif op == "<":
            return left < right
        elif op == "<=":
            return left <= right
        elif op == "AND":
            return left & right
        elif op == "OR":
            return left | right
        elif op == "+":
            return left + right
        elif op == "-":
            return left - right
        elif op == "*":
            return left * right
        elif op == "/":
            return left / right
        elif op == "%":
            return left % right
        else:
            raise ValueError(f"Unsupported binary op for Daft: {op}")
    else:
        raise ValueError(f"Unsupported expr type for Daft: {type(expr).__name__}")
