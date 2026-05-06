"""Daft Driver — executes Daft operations on Ray, returning Arrow IPC batches."""

from __future__ import annotations

import logging
import time
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
        result_table, _stats = self._execute_to_arrow(request)
        yield from self._table_to_ipc_batches(result_table)

    def execute_text(self, request) -> tuple[list[str], list[list[str]], dict]:
        """Execute a Daft plan and return text results with execution stats.

        Returns:
            (column_names, rows, stats) where each row is a list of string values.
            Python None values are serialized as the string "NULL".
            stats is a dict with timing and row count information.
        """
        result_table, stats = self._execute_to_arrow(request)
        col_names = result_table.column_names
        rows = []
        for i in range(result_table.num_rows):
            row = []
            for c in range(result_table.num_columns):
                val = result_table.column(c)[i].as_py()
                row.append("NULL" if val is None else str(val))
            rows.append(row)
        stats["output_rows"] = result_table.num_rows
        return col_names, rows, stats

    def _execute_to_arrow(self, request):
        """Execute a Daft plan and return (pyarrow.Table, stats_dict)."""
        import daft
        import pyarrow as pa

        request_id = request.request_id or str(uuid.uuid4())
        logger.info("[%s] Executing Daft plan: source_sql=%r, ops=%d, direct_read=%s",
                     request_id, request.source_sql, len(request.operations),
                     request.use_direct_read)

        t_start = time.monotonic()

        # Choose data fetch strategy based on use_direct_read flag.
        # Both paths fetch via ADBC first, so we capture input stats from the Arrow table.
        arrow_table = self._fetch_source_data(request)
        input_rows = arrow_table.num_rows
        input_bytes = arrow_table.nbytes

        if request.use_direct_read and request.arrow_flight_endpoint:
            daft_df = self._arrow_to_daft_via_ray(arrow_table, request_id)
        else:
            logger.info("[%s] Fetched %d rows from source (legacy path)",
                         request_id, arrow_table.num_rows)
            daft_df = daft.from_arrow(arrow_table)

        t_fetch = time.monotonic()

        # Apply operations in order.
        daft_df = self._apply_operations(daft_df, request.operations)

        # Collect results.
        result_table = daft_df.to_arrow()
        t_end = time.monotonic()

        stats = {
            "data_fetch_ms": int((t_fetch - t_start) * 1000),
            "daft_execute_ms": int((t_end - t_fetch) * 1000),
            "total_ms": int((t_end - t_start) * 1000),
            "input_rows": input_rows,
            "output_rows": result_table.num_rows,
            "input_bytes": input_bytes,
        }

        logger.info("[%s] Result: %d rows, %d columns; stats=%s",
                     request_id, result_table.num_rows, result_table.num_columns, stats)

        return result_table, stats

    def _arrow_to_daft_via_ray(self, arrow_table, request_id: str):
        """Transfer Arrow table to Daft via Ray object store if available.

        The Arrow table is placed into Ray shared memory so that the
        Coordinator process does not hold it in its own Python heap.

        Falls back to plain daft.from_arrow() if Ray is unavailable.
        """
        import daft

        logger.info("[%s] Fetched %d rows via ADBC (direct read path)",
                     request_id, arrow_table.num_rows)

        try:
            import ray
            if ray.is_initialized():
                ds = ray.data.from_arrow(arrow_table)
                daft_df = daft.from_ray_dataset(ds)
                logger.info("[%s] Data transferred to Ray object store", request_id)
                return daft_df
        except (ImportError, Exception) as e:
            logger.warning("[%s] Ray transfer failed (%s), using local Daft", request_id, e)

        return daft.from_arrow(arrow_table)

    def _fetch_source_data(self, request) -> "pa.Table":
        """Fetch data from StarRocks via Arrow Flight SQL (ADBC).

        Legacy path: Coordinator pulls all data into its own memory.
        """
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
            elif which == "write_back":
                daft_df = self._apply_write_back(daft_df, op.write_back)
            else:
                raise ValueError(f"Unknown operation at index {i}: {which}")
        return daft_df

    def _apply_write_back(self, daft_df, write_back_op):
        """Collect the DataFrame, write it to StarRocks via Stream Load,
        and return a status DataFrame."""
        import daft
        import pyarrow as pa

        from starrocks.coordinator.stream_load_writer import StreamLoadWriter

        result_table = daft_df.to_arrow()
        writer = StreamLoadWriter(
            fe_host=write_back_op.fe_host,
            fe_http_port=write_back_op.fe_http_port,
        )
        writer.write_table(
            result_table,
            write_back_op.database,
            write_back_op.table_name,
        )
        # Return a status summary instead of the consumed data.
        return daft.from_arrow(pa.table({
            "status": ["OK"],
            "rows_loaded": [result_table.num_rows],
        }))

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
