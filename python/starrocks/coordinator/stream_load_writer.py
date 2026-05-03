"""Write Arrow data back to StarRocks via Stream Load HTTP API."""

from __future__ import annotations

import io
import logging
import uuid

import pyarrow as pa
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

    def write_table(self, table: pa.Table, database: str, table_name: str,
                    label: str | None = None) -> dict:
        """Write a pyarrow Table to StarRocks via Stream Load.

        Uses CSV format. Returns the Stream Load JSON response.
        """
        if label is None:
            label = f"daft_writeback_{uuid.uuid4().hex[:12]}"

        # Convert Arrow Table to CSV bytes (no header — Stream Load treats
        # every row as data, so a header row would cause type errors).
        buf = io.BytesIO()
        pcsv.write_csv(table, buf,
                       write_options=pcsv.WriteOptions(include_header=False))
        csv_data = buf.getvalue()

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

        # First request — FE returns 307 redirect to BE.
        # Handle manually to preserve auth header across redirect.
        resp = requests.put(
            url,
            data=csv_data,
            headers=headers,
            auth=auth,
            allow_redirects=False,
        )
        if resp.status_code == 307:
            redirect_url = resp.headers["Location"]
            resp = requests.put(
                redirect_url,
                data=csv_data,
                headers=headers,
                auth=auth,
            )

        result = resp.json()
        status = result.get("Status")
        if status not in ("Success", "Publish Timeout"):
            raise RuntimeError(f"Stream Load failed: {result}")
        logger.info("Stream Load success: %d rows loaded to %s.%s (label=%s)",
                    table.num_rows, database, table_name, label)
        return result
