"""Arrow Flight SQL connection to StarRocks for zero-copy columnar data transfer."""

from __future__ import annotations

from typing import Any


class ArrowFlightConnection:
    """Connects to StarRocks via Arrow Flight SQL (ADBC driver).

    Provides zero-copy columnar data retrieval using Apache Arrow.
    Requires: ``pip install adbc_driver_flightsql adbc_driver_manager pyarrow``
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9408,
        user: str = "root",
        password: str = "",
        database: str | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._conn = self._connect()

    def _connect(self):
        """Establish ADBC Flight SQL connection."""
        import adbc_driver_flightsql.dbapi as dbapi

        db_kwargs: dict[str, str] = {
            "username": self._user,
            "password": self._password,
        }
        if self._database:
            db_kwargs["adbc.flight.sql.rpc.call_header.database"] = self._database

        conn = dbapi.connect(
            f"grpc://{self._host}:{self._port}",
            db_kwargs=db_kwargs,
        )
        conn.autocommit = True
        return conn

    def set_database(self, database: str) -> None:
        """Switch database context by reconnecting with new header."""
        if database != self._database:
            self._database = database
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = self._connect()

    def execute_to_arrow(self, sql: str) -> Any:
        """Execute SQL and return a pyarrow.Table (zero-copy).

        Args:
            sql: SQL query to execute.
        Returns:
            A pyarrow.Table with the query results.
        """
        cur = self._conn.cursor()
        try:
            cur.execute(sql)
            return cur.fetch_arrow_table()
        finally:
            cur.close()

    def close(self) -> None:
        """Close the Flight SQL connection."""
        try:
            self._conn.close()
        except Exception:
            pass
