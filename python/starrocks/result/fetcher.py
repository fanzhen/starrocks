"""Result fetching: execute SQL and return results in various formats."""

from __future__ import annotations

import sys
from typing import Any

from starrocks.connection.mysql import MySQLConnection


class ResultFetcher:
    """Execute SQL via a MySQLConnection and return results."""

    def __init__(self, connection: MySQLConnection) -> None:
        self._conn = connection

    def execute_to_dicts(self, sql: str) -> list[dict[str, Any]]:
        return self._conn.execute(sql)

    def execute_to_pandas(self, sql: str) -> Any:
        """Execute and return a pandas DataFrame."""
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required for to_pandas(). Install with: pip install pandas")
        rows, desc = self._conn.execute_raw(sql)
        columns = [d[0] for d in desc] if desc else []
        return pd.DataFrame(rows, columns=columns)

    def execute_show(self, sql: str, limit: int = 20) -> None:
        """Execute and pretty-print results to stdout."""
        rows, desc = self._conn.execute_raw(sql)
        if not desc:
            print("(no results)")
            return
        columns = [d[0] for d in desc]
        # Compute column widths
        widths = [len(c) for c in columns]
        str_rows = []
        for row in rows[:limit]:
            str_row = [str(v) if v is not None else "NULL" for v in row]
            str_rows.append(str_row)
            for i, v in enumerate(str_row):
                widths[i] = max(widths[i], len(v))
        # Print header
        header = " | ".join(c.ljust(widths[i]) for i, c in enumerate(columns))
        sep = "-+-".join("-" * widths[i] for i in range(len(columns)))
        print(header)
        print(sep)
        for str_row in str_rows:
            line = " | ".join(str_row[i].ljust(widths[i]) for i in range(len(columns)))
            print(line)
        if len(rows) > limit:
            print(f"... ({len(rows) - limit} more rows)")

    def execute_explain(self, sql: str) -> str:
        """Execute EXPLAIN and return the plan as a string."""
        rows, _ = self._conn.execute_raw(f"EXPLAIN {sql}")
        return "\n".join(str(r[0]) for r in rows)

    def execute_count(self, sql: str) -> int:
        """Execute a COUNT query and return the integer result."""
        rows = self._conn.execute(sql)
        if rows:
            first_val = next(iter(rows[0].values()))
            return int(first_val)
        return 0
