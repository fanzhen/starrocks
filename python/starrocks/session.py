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
        connect_timeout: int = 10,
        read_timeout: int = 300,
        arrow_flight_port: int | None = None,
    ) -> None:
        self._conn = MySQLConnection(
            host=host, port=port, user=user, password=password,
            database=database, connect_timeout=connect_timeout,
            read_timeout=read_timeout,
        )
        self._fetcher = ResultFetcher(self._conn)
        self._database = database

        # Arrow Flight SQL connection (optional, for zero-copy data transfer)
        self._arrow_conn = None
        if arrow_flight_port is not None:
            try:
                from starrocks.connection.arrow_flight import ArrowFlightConnection
                self._arrow_conn = ArrowFlightConnection(
                    host=host, port=arrow_flight_port,
                    user=user, password=password,
                    database=database,
                )
            except ImportError:
                pass  # adbc_driver_flightsql not installed

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

    def catalog(self, catalog_name: str) -> "CatalogSession":
        """Switch to a different catalog for external table access."""
        return CatalogSession(self, catalog_name)

    def sql(self, query: str) -> "DataFrame":
        """Create a DataFrame from a raw SQL query.

        The query is wrapped as a subquery and can be further
        transformed with DataFrame operations.
        """
        from starrocks.dataframe import DataFrame
        from starrocks.plan.logical import RawSQL

        # Infer schema by running DESCRIBE on the query
        schema = self._fetch_query_schema(query)
        return DataFrame(RawSQL(query), self, schema=schema)

    def execute(self, sql: str) -> list[dict[str, Any]]:
        """Execute an arbitrary SQL statement."""
        result = self._conn.execute(sql)
        # Sync database context to Arrow Flight connection on USE statements
        stripped = sql.strip().rstrip(";").strip()
        if stripped.upper().startswith("USE "):
            db_name = stripped[4:].strip().strip("`").strip('"').strip("'")
            self._database = db_name
            if self._arrow_conn is not None:
                self._arrow_conn.set_database(db_name)
        return result

    def write_daft(self, daft_df, table: str, mode: str = "append") -> int:
        """Write a Daft DataFrame to a StarRocks table via INSERT INTO.

        Args:
            daft_df: A daft.DataFrame to write.
            table: Target table name.
            mode: "append" (INSERT INTO) or "overwrite" (TRUNCATE + INSERT).
        Returns:
            Number of rows written.
        """
        import pandas as pd
        if isinstance(daft_df, pd.DataFrame):
            pdf = daft_df
        else:
            pdf = daft_df.to_pandas()

        if pdf.empty:
            return 0

        if mode == "overwrite":
            self._conn.execute(f"TRUNCATE TABLE `{table}`")

        columns = list(pdf.columns)
        col_list = ", ".join(f"`{c}`" for c in columns)
        total = 0
        batch_size = 1000

        for start in range(0, len(pdf), batch_size):
            batch = pdf.iloc[start : start + batch_size]
            value_rows = []
            for _, row in batch.iterrows():
                vals = []
                for v in row:
                    if v is None or (isinstance(v, float) and pd.isna(v)):
                        vals.append("NULL")
                    elif isinstance(v, str):
                        escaped = v.replace("\\", "\\\\").replace("'", "\\'")
                        vals.append(f"'{escaped}'")
                    elif isinstance(v, (list, dict)):
                        import json
                        escaped = json.dumps(v).replace("\\", "\\\\").replace("'", "\\'")
                        vals.append(f"'{escaped}'")
                    else:
                        vals.append(str(v))
                value_rows.append(f"({', '.join(vals)})")
            sql = f"INSERT INTO `{table}` ({col_list}) VALUES {', '.join(value_rows)}"
            self._conn.execute(sql)
            total += len(batch)

        return total

    @property
    def arrow_connection(self):
        """Arrow Flight SQL connection, or None if not configured."""
        return self._arrow_conn

    def close(self) -> None:
        self._conn.close()
        if self._arrow_conn is not None:
            self._arrow_conn.close()

    def _fetch_schema(self, table_name: str) -> list[tuple[str, str]]:
        """Return [(column_name, type_string), ...] for a table."""
        rows = self._conn.execute(f"DESC `{table_name}`")
        schema: list[tuple[str, str]] = []
        for row in rows:
            col_name = row.get("Field") or row.get("field") or ""
            col_type = row.get("Type") or row.get("type") or ""
            schema.append((str(col_name), normalize_type(str(col_type))))
        return schema

    def _fetch_catalog_schema(self, catalog: str, db: str | None, table: str) -> list[tuple[str, str]]:
        """Return [(col_name, type)] for a table in a specific catalog."""
        if db:
            qualified = f"`{catalog}`.`{db}`.`{table}`"
        else:
            qualified = f"`{catalog}`.`{table}`"
        rows = self._conn.execute(f"DESC {qualified}")
        schema: list[tuple[str, str]] = []
        for row in rows:
            col_name = row.get("Field") or row.get("field") or ""
            col_type = row.get("Type") or row.get("type") or ""
            schema.append((str(col_name), normalize_type(str(col_type))))
        return schema

    def _fetch_query_schema(self, query: str) -> list[tuple[str, str]]:
        """Infer schema from a SQL query by executing LIMIT 0."""
        _, desc = self._conn.execute_raw(f"SELECT * FROM ({query}) _q LIMIT 0")
        return [(str(d[0]), "UNKNOWN") for d in desc]


class CatalogSession:
    """Proxy for accessing tables in a specific catalog."""

    def __init__(self, session: Session, catalog_name: str) -> None:
        self._session = session
        self._catalog = catalog_name

    def table(self, name: str) -> "DataFrame":
        """Create a DataFrame for a table in this catalog.

        Args:
            name: Table name, optionally qualified as "db.table".
        """
        from starrocks.dataframe import DataFrame
        from starrocks.plan.logical import TableScan

        if "." in name:
            db, tbl = name.split(".", 1)
        else:
            db = self._session.database
            tbl = name

        # Try to fetch schema via catalog-qualified DESC
        try:
            schema = self._session._fetch_catalog_schema(self._catalog, db, tbl)
        except Exception:
            schema = []

        plan = TableScan(table_name=tbl, database=db, catalog=self._catalog)
        return DataFrame(plan, self._session, schema=schema)
