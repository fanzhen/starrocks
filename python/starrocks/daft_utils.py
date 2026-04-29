"""Daft DataFrame wrapper with StarRocks write-back support."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from starrocks.session import Session


class DaftDataFrame:
    """Thin wrapper around daft.DataFrame, providing write-back to StarRocks."""

    def __init__(self, daft_df, session: Session) -> None:
        self._daft_df = daft_df
        self._session = session

    def to_pandas(self):
        """Collect to a pandas DataFrame."""
        return self._daft_df.to_pandas()

    def to_starrocks(self, table: str, mode: str = "append") -> int:
        """Write this Daft DataFrame to a StarRocks table.

        Args:
            table: Target table name.
            mode: "append" (INSERT INTO) or "overwrite" (TRUNCATE + INSERT).
        Returns:
            Number of rows written.
        """
        return self._session.write_daft(self._daft_df, table, mode=mode)

    def show(self, limit: int = 20) -> None:
        """Display up to *limit* rows."""
        self._daft_df.show(limit)

    @property
    def daft(self):
        """Access the underlying daft.DataFrame for advanced operations."""
        return self._daft_df

    def __repr__(self) -> str:
        return f"DaftDataFrame({self._daft_df})"
