"""Daft Driver — executes Daft operations on Ray, returning Arrow IPC batches."""

from __future__ import annotations

import logging
import uuid
from typing import Iterator

from starrocks.coordinator.function_registry import FunctionRegistry

logger = logging.getLogger(__name__)

# Maximum rows per Arrow IPC batch in the response stream.
_BATCH_SIZE = 4096


class DaftDriver:
    """Executes Daft operations on Ray, wrapping Daft's native API."""

    def __init__(self, registry: FunctionRegistry) -> None:
        self._registry = registry

    def execute(self, request) -> Iterator[bytes]:
        """Execute a Daft plan and yield Arrow IPC RecordBatch bytes.

        Args:
            request: A DaftPlanRequest protobuf message.

        Yields:
            bytes — each chunk is a serialized Arrow IPC RecordBatch.
        """
        import daft
        import pyarrow as pa

        request_id = request.request_id or str(uuid.uuid4())
        logger.info("[%s] Executing Daft plan: source_sql=%r, ops=%d",
                     request_id, request.source_sql, len(request.operations))

        # 1. Pull SQL segment results from StarRocks via Arrow Flight (ADBC).
        arrow_table = self._fetch_source_data(request)
        logger.info("[%s] Fetched %d rows from source", request_id, arrow_table.num_rows)

        # 2. Convert to Daft DataFrame.
        daft_df = daft.from_arrow(arrow_table)

        # 3. Apply operations in order.
        daft_df = self._apply_operations(daft_df, request.operations)

        # 4. Collect results and stream as Arrow IPC batches.
        result_table = daft_df.to_arrow()
        logger.info("[%s] Result: %d rows, %d columns",
                     request_id, result_table.num_rows, result_table.num_columns)

        yield from self._table_to_ipc_batches(result_table)

    def _fetch_source_data(self, request) -> "pa.Table":
        """Fetch data from StarRocks via Arrow Flight SQL (ADBC)."""
        import adbc_driver_flightsql.dbapi as dbapi

        endpoint = request.arrow_flight_endpoint
        sql = request.source_sql

        if not endpoint:
            raise ValueError("arrow_flight_endpoint is required")
        if not sql:
            raise ValueError("source_sql is required")

        conn = dbapi.connect(endpoint, db_kwargs={"username": "root", "password": ""})
        try:
            conn.autocommit = True
            cur = conn.cursor()
            try:
                cur.execute(sql)
                return cur.fetch_arrow_table()
            finally:
                cur.close()
        finally:
            conn.close()

    def _apply_operations(self, daft_df, operations):
        """Apply a sequence of DaftOperation protos to a Daft DataFrame."""
        import daft

        for i, op in enumerate(operations):
            which = op.WhichOneof("op")
            if which == "map_batches":
                func_name = op.map_batches.function_name
                func = self._registry.get(func_name)
                daft_df = func(daft_df)
            elif which == "filter":
                expr_json = op.filter.expr_json
                daft_df = self._apply_filter(daft_df, expr_json)
            elif which == "projection":
                cols = list(op.projection.columns)
                daft_df = daft_df.select(*[daft.col(c) for c in cols])
            elif which == "limit":
                daft_df = daft_df.limit(op.limit.count)
            else:
                raise ValueError(f"Unknown operation at index {i}: {which}")
        return daft_df

    def _apply_filter(self, daft_df, expr_json: str):
        """Apply a JSON-encoded filter expression to a Daft DataFrame.

        Supports simple comparison expressions: {"op": ">", "col": "x", "value": 10}
        """
        import json
        import daft

        spec = json.loads(expr_json)
        col_name = spec["col"]
        op = spec["op"]
        value = spec["value"]
        col = daft.col(col_name)

        ops = {
            "=": lambda c, v: c == v,
            "==": lambda c, v: c == v,
            "!=": lambda c, v: c != v,
            ">": lambda c, v: c > v,
            ">=": lambda c, v: c >= v,
            "<": lambda c, v: c < v,
            "<=": lambda c, v: c <= v,
        }
        if op not in ops:
            raise ValueError(f"Unsupported filter op: {op}")
        return daft_df.where(ops[op](col, value))

    @staticmethod
    def _table_to_ipc_batches(table: "pa.Table") -> Iterator[bytes]:
        """Convert a pyarrow Table to a sequence of IPC-serialized RecordBatches."""
        import pyarrow as pa

        batches = table.to_batches(max_chunksize=_BATCH_SIZE)
        for batch in batches:
            sink = pa.BufferOutputStream()
            writer = pa.ipc.new_stream(sink, batch.schema)
            writer.write_batch(batch)
            writer.close()
            yield bytes(sink.getvalue())
