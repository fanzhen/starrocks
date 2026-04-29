"""Pipeline executor: auto-routes SQL segments to StarRocks and UDF segments to Daft."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from starrocks.compiler.sql_compiler import SQLCompiler
from starrocks.execution.daft_ops import apply_plan_on_daft
from starrocks.plan.logical import (
    Filter,
    Limit,
    LogicalPlan,
    MapBatches,
    Projection,
)

if TYPE_CHECKING:
    from starrocks.session import Session


class PipelineExecutor:
    """Executes a hybrid SQL + Python UDF pipeline.

    Splits the logical plan at MapBatches boundaries:
    - Nodes below MapBatches → compiled to SQL, executed on StarRocks
    - MapBatches + nodes above → executed on Daft
    """

    def __init__(self) -> None:
        self._compiler = SQLCompiler()

    def execute(self, plan: LogicalPlan, session: Session):
        """Execute a plan containing MapBatches nodes. Returns a Daft DataFrame."""
        segments = self._split_pipeline(plan)
        return self._execute_segments(segments, session)

    def _split_pipeline(self, plan: LogicalPlan) -> list[dict[str, Any]]:
        """Walk the plan tree bottom-up, splitting at MapBatches boundaries.

        Returns a list of segments in execution order:
          [{"type": "sql", "plan": ...},
           {"type": "map_batches", "func": ..., "result_columns": ...},
           {"type": "daft_ops", "nodes": [Filter, Projection, ...]},
           {"type": "map_batches", "func": ..., "result_columns": ...},
           {"type": "daft_ops", "nodes": [...]},
           ...]
        """
        segments: list[dict[str, Any]] = []
        self._walk(plan, segments)
        return segments

    def _walk(self, node: LogicalPlan, segments: list[dict[str, Any]]) -> None:
        """Recursively walk the plan, collecting segments bottom-up."""
        if isinstance(node, MapBatches):
            # First, process everything below this MapBatches
            self._walk(node.child, segments)
            # Then add this MapBatches as a segment
            segments.append({
                "type": "map_batches",
                "func": node.func,
                "result_columns": node.result_columns,
            })
        elif isinstance(node, (Filter, Projection, Limit)):
            # Check if child contains a MapBatches
            if self._contains_map_batches(node.child):
                # Process child first
                self._walk(node.child, segments)
                # This node goes into daft_ops
                self._append_daft_op(segments, node)
            else:
                # Pure SQL subtree — will be handled as the SQL base
                self._set_sql_base(segments, node)
        else:
            # Leaf or other SQL-only node
            self._set_sql_base(segments, node)

    def _contains_map_batches(self, node: LogicalPlan) -> bool:
        """Check if a plan subtree contains any MapBatches node."""
        if isinstance(node, MapBatches):
            return True
        child = getattr(node, "child", None)
        if child is not None:
            return self._contains_map_batches(child)
        return False

    def _set_sql_base(self, segments: list[dict[str, Any]], node: LogicalPlan) -> None:
        """Set the SQL base segment (should be first)."""
        if not segments or segments[0].get("type") != "sql":
            segments.insert(0, {"type": "sql", "plan": node})

    def _append_daft_op(self, segments: list[dict[str, Any]], node: LogicalPlan) -> None:
        """Append a plan node to the latest daft_ops segment, or create one."""
        if segments and segments[-1].get("type") == "daft_ops":
            segments[-1]["nodes"].append(node)
        else:
            segments.append({"type": "daft_ops", "nodes": [node]})

    def _execute_segments(self, segments: list[dict[str, Any]], session: Session):
        """Execute segments in order, threading the Daft DataFrame through."""
        import daft

        daft_df = None

        for seg in segments:
            if seg["type"] == "sql":
                plan = seg["plan"]
                sql = self._compiler.compile(plan)
                # Execute on StarRocks, get Arrow table, convert to Daft
                arrow_conn = session.arrow_connection
                if arrow_conn is not None:
                    arrow_table = arrow_conn.execute_to_arrow(sql)
                    daft_df = daft.from_arrow(arrow_table)
                else:
                    pdf = session.fetcher.execute_to_pandas(sql)
                    daft_df = daft.from_pandas(pdf)

            elif seg["type"] == "map_batches":
                func = seg["func"]
                result_columns = seg["result_columns"]
                try:
                    if result_columns is not None:
                        expressions = [
                            daft.col(name).apply(func, return_dtype=dtype)
                            for name, dtype in result_columns.items()
                        ]
                        daft_df = daft_df.with_columns(*expressions)
                    else:
                        daft_df = func(daft_df)
                except Exception as e:
                    from starrocks.exceptions import QueryError
                    raise QueryError(
                        f"UDF execution failed: {type(e).__name__}: {e}"
                    ) from e

            elif seg["type"] == "daft_ops":
                daft_df = apply_plan_on_daft(daft_df, seg["nodes"])

        return daft_df
