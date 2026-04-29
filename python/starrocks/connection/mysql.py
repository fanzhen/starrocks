"""MySQL protocol connection to StarRocks."""

from __future__ import annotations

from typing import Any, Sequence

import pymysql
import pymysql.cursors


class MySQLConnection:
    """Thin wrapper around pymysql for StarRocks connectivity."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9030,
        user: str = "root",
        password: str = "",
        database: str | None = None,
    ) -> None:
        self._conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            cursorclass=pymysql.cursors.DictCursor,
        )

    def execute(self, sql: str) -> list[dict[str, Any]]:
        """Execute a SQL statement and return rows as list of dicts."""
        with self._conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        return list(rows) if rows else []

    def execute_raw(self, sql: str) -> tuple[list[tuple[Any, ...]], Sequence[Any]]:
        """Execute and return (rows_as_tuples, description)."""
        cur = self._conn.cursor(pymysql.cursors.Cursor)
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            desc = cur.description or []
            return list(rows), desc
        finally:
            cur.close()

    def close(self) -> None:
        self._conn.close()
