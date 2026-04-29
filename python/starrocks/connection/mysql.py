"""MySQL protocol connection to StarRocks."""

from __future__ import annotations

import time
from typing import Any, Sequence

import pymysql
import pymysql.cursors

from starrocks.exceptions import ConnectionError, QueryError


class MySQLConnection:
    """Thin wrapper around pymysql for StarRocks connectivity."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9030,
        user: str = "root",
        password: str = "",
        database: str | None = None,
        connect_timeout: int = 10,
        read_timeout: int = 300,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._conn = self._connect()

    def _connect(self) -> pymysql.Connection:
        """Establish connection with retry logic."""
        last_err: Exception | None = None
        for attempt in range(self._max_retries):
            try:
                return pymysql.connect(
                    host=self._host,
                    port=self._port,
                    user=self._user,
                    password=self._password,
                    database=self._database,
                    cursorclass=pymysql.cursors.DictCursor,
                    connect_timeout=self._connect_timeout,
                    read_timeout=self._read_timeout,
                )
            except pymysql.err.OperationalError as e:
                last_err = e
                if attempt < self._max_retries - 1:
                    time.sleep(self._retry_delay)
        raise ConnectionError(
            f"Failed to connect to StarRocks at {self._host}:{self._port} "
            f"after {self._max_retries} attempts"
        ) from last_err

    def _ensure_connected(self) -> None:
        """Reconnect if the connection was lost."""
        try:
            self._conn.ping(reconnect=True)
        except Exception:
            self._conn = self._connect()

    def execute(self, sql: str) -> list[dict[str, Any]]:
        """Execute a SQL statement and return rows as list of dicts."""
        self._ensure_connected()
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
            return list(rows) if rows else []
        except pymysql.err.ProgrammingError as e:
            raise QueryError(str(e), sql=sql, original=e) from e
        except pymysql.err.OperationalError as e:
            raise QueryError(str(e), sql=sql, original=e) from e

    def execute_raw(self, sql: str) -> tuple[list[tuple[Any, ...]], Sequence[Any]]:
        """Execute and return (rows_as_tuples, description)."""
        self._ensure_connected()
        cur = self._conn.cursor(pymysql.cursors.Cursor)
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            desc = cur.description or []
            return list(rows), desc
        except pymysql.err.ProgrammingError as e:
            raise QueryError(str(e), sql=sql, original=e) from e
        except pymysql.err.OperationalError as e:
            raise QueryError(str(e), sql=sql, original=e) from e
        finally:
            cur.close()

    def execute_raw_batched(self, sql: str, batch_size: int) -> Any:
        """Execute and yield batches of rows using SSCursor for streaming."""
        self._ensure_connected()
        cur = self._conn.cursor(pymysql.cursors.SSCursor)
        try:
            cur.execute(sql)
            desc = cur.description or []
            while True:
                batch = cur.fetchmany(batch_size)
                if not batch:
                    break
                yield batch, desc
        except pymysql.err.ProgrammingError as e:
            raise QueryError(str(e), sql=sql, original=e) from e
        except pymysql.err.OperationalError as e:
            raise QueryError(str(e), sql=sql, original=e) from e
        finally:
            cur.close()

    def close(self) -> None:
        self._conn.close()
