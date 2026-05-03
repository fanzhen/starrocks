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
        result_table = self._execute_to_arrow(request)
        yield from self._table_to_ipc_batches(result_table)

    def execute_text(self, request) -> tuple[list[str], list[list[str]]]:
        """Execute a Daft plan and return text results.

        Returns:
            (column_names, rows) where each row is a list of string values.
            Python None values are serialized as the string "NULL".
        """
        result_table = self._execute_to_arrow(request)
        col_names = result_table.column_names
        rows = []
        for i in range(result_table.num_rows):
            row = []
            for c in range(result_table.num_columns):
                val = result_table.column(c)[i].as_py()
                row.append("NULL" if val is None else str(val))
            rows.append(row)
        return col_names, rows

    def _execute_to_arrow(self, request):
        """Execute a Daft plan and return the result as a pyarrow Table."""
        import daft
        import pyarrow as pa

        request_id = request.request_id or str(uuid.uuid4())
        logger.info("[%s] Executing Daft plan: source_sql=%r, ops=%d, direct_read=%s",
                     request_id, request.source_sql, len(request.operations),
                     request.use_direct_read)

        # Choose data fetch strategy based on use_direct_read flag.
        # Default (use_direct_read=False or unset) uses the legacy path for
        # backward compatibility. When True, Daft reads directly from BE.
        if request.use_direct_read and request.source_sql and request.arrow_flight_endpoint:
            daft_df = self._read_sql_direct(request, request_id)
        else:
            # Legacy path: Coordinator fetches all data, then passes to Daft.
            arrow_table = self._fetch_source_data(request)
            logger.info("[%s] Fetched %d rows from source (legacy path)",
                         request_id, arrow_table.num_rows)
            daft_df = daft.from_arrow(arrow_table)

        # Apply operations in order.
        daft_df = self._apply_operations(daft_df, request.operations)

        # Collect results.
        result_table = daft_df.to_arrow()
        logger.info("[%s] Result: %d rows, %d columns",
                     request_id, result_table.num_rows, result_table.num_columns)

        return result_table

    def _read_sql_direct(self, request, request_id: str):
        """Use daft.read_sql() so Ray workers pull data directly from BE.

        Falls back to legacy _fetch_source_data() if daft.read_sql() is
        unavailable or fails.
        """
        import daft

        endpoint = request.arrow_flight_endpoint
        sql = request.source_sql

        # Connection factory — called by each Ray worker independently.
        def make_conn():
            import adbc_driver_flightsql.dbapi as dbapi
            conn = dbapi.connect(endpoint, db_kwargs={
                "username": "root", "password": "",
            })
            conn.autocommit = True
            return conn

        try:
            daft_df = daft.read_sql(sql, make_conn)
            logger.info("[%s] Daft reading directly from %s: %s",
                         request_id, endpoint, sql)
            return daft_df
        except Exception as e:
            logger.warning("[%s] daft.read_sql() failed (%s), falling back to legacy path",
                            request_id, e)
            arrow_table = self._fetch_source_data(request)
            logger.info("[%s] Fetched %d rows from source (fallback)",
                         request_id, arrow_table.num_rows)
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
