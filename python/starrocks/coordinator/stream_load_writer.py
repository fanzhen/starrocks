"""Write Arrow data back to StarRocks via Stream Load HTTP API."""

from __future__ import annotations

import io
import logging
import uuid

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import requests

logger = logging.getLogger(__name__)


class StreamLoadWriter:
    """Writes Arrow Tables to StarRocks via Stream Load."""

    def __init__(self, fe_host: str, fe_http_port: int = 8030,
                 user: str = "root", password: str = "") -> None:
        self._fe_host = fe_host
        self._fe_http_port = fe_http_port
        self._user = user
        self._password = password

    @staticmethod
    def _replace_nulls_with_marker(table: pa.Table) -> pa.Table:
        """Replace null values with \\N marker for StarRocks Stream Load.

        StarRocks CSV format uses \\N to denote NULL. PyArrow CSV writer
        outputs null as empty string, which StarRocks interprets as an
        empty string (not NULL). This method casts all columns to string
        and replaces nulls with \\N.

        Known limitation: if a string column contains the literal value
        '\\N', it will be indistinguishable from NULL after Stream Load.
        This is inherent to the StarRocks CSV \\N convention.
        """
        new_columns = []
        for col in table.columns:
            str_col = col.cast(pa.string())
            filled = pc.if_else(pc.is_null(col), pa.scalar("\\N"), str_col)
            new_columns.append(filled)
        return pa.table(
            {name: col for name, col in zip(table.column_names, new_columns)}
        )

    @staticmethod
    def _generate_csv_chunks(table: pa.Table, max_chunksize: int = 8192):
        """Yield CSV bytes chunk-by-chunk from an Arrow Table.

        Each chunk is a batch of rows converted to CSV (no header).
        NULL values are replaced with \\N per batch before conversion.
        This avoids materializing the entire CSV in memory at once.
        """
        for batch in table.to_batches(max_chunksize=max_chunksize):
            batch_table = pa.Table.from_batches([batch], schema=table.schema)
            batch_table = StreamLoadWriter._replace_nulls_with_marker(batch_table)
            buf = io.BytesIO()
            pcsv.write_csv(batch_table, buf,
                           write_options=pcsv.WriteOptions(include_header=False))
            yield buf.getvalue()

    def write_table(self, table: pa.Table, database: str, table_name: str,
                    label: str | None = None) -> dict:
        """Write a pyarrow Table to StarRocks via Stream Load.

        Uses CSV format with chunked transfer encoding to avoid
        materializing the entire CSV payload in memory. Returns the
        Stream Load JSON response.
        """
        if label is None:
            label = f"daft_writeback_{uuid.uuid4().hex[:12]}"

        # Stream Load HTTP PUT
        url = (f"http://{self._fe_host}:{self._fe_http_port}"
               f"/api/{database}/{table_name}/_stream_load")

        headers = {
            "Expect": "100-continue",
            "format": "csv",
            "column_separator": ",",
            "enclose": '"',
            "label": label,
        }
        auth = (self._user, self._password)

        # Use a generator for chunked transfer encoding.
        # First request — FE returns 307 redirect to BE.
        # Handle manually to preserve auth header across redirect.
        # Note: generators can only be consumed once, so we use the
        # full-materialization fallback for the redirect path since
        # the redirect requires re-sending the body.
        csv_gen = self._generate_csv_chunks(table)
        resp = requests.put(
            url,
            data=csv_gen,
            headers=headers,
            auth=auth,
            allow_redirects=False,
        )
        if resp.status_code == 307:
            redirect_url = resp.headers["Location"]
            # Re-create generator for the redirect target
            csv_gen2 = self._generate_csv_chunks(table)
            resp = requests.put(
                redirect_url,
                data=csv_gen2,
                headers=headers,
                auth=auth,
            )

        if resp.status_code not in (200, 307):
            raise RuntimeError(
                f"Stream Load HTTP error {resp.status_code}: {resp.text[:500]}")
        result = resp.json()
        status = result.get("Status")
        if status not in ("Success", "Publish Timeout"):
            raise RuntimeError(f"Stream Load failed: {result}")
        logger.info("Stream Load success: %d rows loaded to %s.%s (label=%s)",
                    table.num_rows, database, table_name, label)
        return result
