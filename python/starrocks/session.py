"""Session: entry point for connecting to StarRocks and creating DataFrames."""

from __future__ import annotations

from typing import Any

from starrocks.connection.mysql import MySQLConnection
from starrocks.result.fetcher import ResultFetcher
from starrocks.types import normalize_type


class Session:
    """A connection session to a StarRocks cluster."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9030,
        user: str = "root",
        password: str = "",
        database: str | None = None,
    ) -> None:
        self._conn = MySQLConnection(host=host, port=port, user=user, password=password, database=database)
        self._fetcher = ResultFetcher(self._conn)
        self._database = database

    @property
    def connection(self) -> MySQLConnection:
        return self._conn

    @property
    def fetcher(self) -> ResultFetcher:
        return self._fetcher

    @property
    def database(self) -> str | None:
        return self._database

    def table(self, name: str) -> "DataFrame":
        """Create a DataFrame representing a table scan."""
        from starrocks.dataframe import DataFrame
        from starrocks.plan.logical import TableScan

        # Fetch schema from StarRocks
        schema = self._fetch_schema(name)
        plan = TableScan(table_name=name, database=self._database, columns=[c[0] for c in schema])
        return DataFrame(plan, self, schema=schema)

    def sql(self, query: str) -> "DataFrame":
        """Create a DataFrame from a raw SQL query (future Phase 3)."""
        raise NotImplementedError("Session.sql() will be available in Phase 3")

    def execute(self, sql: str) -> list[dict[str, Any]]:
        """Execute an arbitrary SQL statement."""
        return self._conn.execute(sql)

    def close(self) -> None:
        self._conn.close()

    def _fetch_schema(self, table_name: str) -> list[tuple[str, str]]:
        """Return [(column_name, type_string), ...] for a table."""
        rows = self._conn.execute(f"DESC `{table_name}`")
        schema: list[tuple[str, str]] = []
        for row in rows:
            col_name = row.get("Field") or row.get("field") or ""
            col_type = row.get("Type") or row.get("type") or ""
            schema.append((str(col_name), normalize_type(str(col_type))))
        return schema
