"""Unit tests for Daft integration (mock-based, no StarRocks/Ray needed)."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Create a mock daft module so @patch("daft.from_pandas") works even without daft installed
if "daft" not in sys.modules:
    _mock_daft = MagicMock()
    sys.modules["daft"] = _mock_daft


# ---------------------------------------------------------------------------
# to_daft() tests
# ---------------------------------------------------------------------------

class TestToDaft:
    """Test DataFrame.to_daft() SQL compilation and conversion."""

    def _make_df(self, plan_sql="SELECT * FROM t", schema=None):
        """Create a DataFrame with mocked session."""
        from starrocks.dataframe import DataFrame
        from starrocks.plan.logical import TableScan

        session = MagicMock()
        # Mock fetcher.execute_to_pandas to return a pandas DF
        mock_pdf = pd.DataFrame({"id": [1, 2, 3], "val": [1.1, 2.2, 3.3]})
        session.fetcher.execute_to_pandas.return_value = mock_pdf

        plan = TableScan(table_name="t", columns=["id", "val"])
        return DataFrame(plan, session, schema=schema or [("id", "INT"), ("val", "DOUBLE")])

    @patch("daft.from_pandas")
    def test_to_daft_calls_to_pandas_then_daft(self, mock_from_pandas):
        """to_daft() should execute SQL via to_pandas, then wrap with daft."""
        df = self._make_df()
        mock_daft_df = MagicMock()
        mock_from_pandas.return_value = mock_daft_df

        result = df.to_daft()

        # Should have called execute_to_pandas
        df._session.fetcher.execute_to_pandas.assert_called_once()
        # Should have called daft.from_pandas with the pandas result
        mock_from_pandas.assert_called_once()
        assert result is mock_daft_df

    def test_to_daft_sql_compilation(self):
        """to_daft() should compile the correct SQL."""
        df = self._make_df()
        sql = df.to_sql()
        assert "t" in sql


# ---------------------------------------------------------------------------
# write_daft() tests
# ---------------------------------------------------------------------------

class TestWriteDaft:
    """Test Session.write_daft() INSERT SQL generation."""

    def _make_session(self):
        """Create a Session with mocked connection."""
        from starrocks.session import Session
        session = Session.__new__(Session)
        session._conn = MagicMock()
        session._conn.execute.return_value = []
        session._fetcher = MagicMock()
        session._database = "test_db"
        return session

    def test_write_daft_append(self):
        """write_daft with mode=append should INSERT without TRUNCATE."""
        session = self._make_session()
        pdf = pd.DataFrame({"id": [1, 2], "name": ["alice", "bob"]})

        count = session.write_daft(pdf, "target")

        assert count == 2
        calls = session._conn.execute.call_args_list
        # Should only have INSERT, no TRUNCATE
        assert len(calls) == 1
        sql = calls[0][0][0]
        assert sql.startswith("INSERT INTO `target`")
        assert "'alice'" in sql
        assert "'bob'" in sql

    def test_write_daft_overwrite(self):
        """write_daft with mode=overwrite should TRUNCATE then INSERT."""
        session = self._make_session()
        pdf = pd.DataFrame({"id": [1]})

        count = session.write_daft(pdf, "target", mode="overwrite")

        assert count == 1
        calls = session._conn.execute.call_args_list
        assert len(calls) == 2
        assert "TRUNCATE" in calls[0][0][0]
        assert "INSERT" in calls[1][0][0]

    def test_write_daft_empty(self):
        """write_daft with empty DataFrame should return 0."""
        session = self._make_session()
        pdf = pd.DataFrame({"id": []})

        count = session.write_daft(pdf, "target")

        assert count == 0
        session._conn.execute.assert_not_called()

    def test_write_daft_null_handling(self):
        """write_daft should handle None/NaN as NULL."""
        session = self._make_session()
        pdf = pd.DataFrame({"id": [1], "name": [None]})

        session.write_daft(pdf, "target")

        sql = session._conn.execute.call_args_list[0][0][0]
        assert "NULL" in sql

    def test_write_daft_string_escaping(self):
        """write_daft should escape single quotes in strings."""
        session = self._make_session()
        pdf = pd.DataFrame({"name": ["it's a test"]})

        session.write_daft(pdf, "target")

        sql = session._conn.execute.call_args_list[0][0][0]
        assert "it\\'s a test" in sql

    def test_write_daft_batching(self):
        """write_daft should batch large DataFrames (1000 rows per batch)."""
        session = self._make_session()
        pdf = pd.DataFrame({"id": list(range(2500))})

        count = session.write_daft(pdf, "target")

        assert count == 2500
        calls = session._conn.execute.call_args_list
        assert len(calls) == 3  # 1000 + 1000 + 500


# ---------------------------------------------------------------------------
# DaftDataFrame wrapper tests
# ---------------------------------------------------------------------------

class TestDaftDataFrame:
    """Test the DaftDataFrame wrapper class."""

    def test_to_pandas(self):
        from starrocks.daft_utils import DaftDataFrame
        mock_daft = MagicMock()
        expected_pdf = pd.DataFrame({"a": [1]})
        mock_daft.to_pandas.return_value = expected_pdf

        wrapper = DaftDataFrame(mock_daft, MagicMock())
        result = wrapper.to_pandas()

        assert result is expected_pdf

    def test_to_starrocks(self):
        from starrocks.daft_utils import DaftDataFrame
        mock_daft = MagicMock()
        mock_session = MagicMock()
        mock_session.write_daft.return_value = 5

        wrapper = DaftDataFrame(mock_daft, mock_session)
        count = wrapper.to_starrocks("my_table", mode="append")

        mock_session.write_daft.assert_called_once_with(mock_daft, "my_table", mode="append")
        assert count == 5

    def test_daft_property(self):
        from starrocks.daft_utils import DaftDataFrame
        mock_daft = MagicMock()
        wrapper = DaftDataFrame(mock_daft, MagicMock())
        assert wrapper.daft is mock_daft

    def test_show(self):
        from starrocks.daft_utils import DaftDataFrame
        mock_daft = MagicMock()
        wrapper = DaftDataFrame(mock_daft, MagicMock())
        wrapper.show(10)
        mock_daft.show.assert_called_once_with(10)


# ---------------------------------------------------------------------------
# map_batches() tests
# ---------------------------------------------------------------------------

class TestMapBatches:
    """Test DataFrame.map_batches()."""

    @patch("daft.from_pandas")
    def test_map_batches_with_callable(self, mock_from_pandas):
        """map_batches with a plain callable should apply it to the Daft DF."""
        from starrocks.dataframe import DataFrame
        from starrocks.plan.logical import TableScan

        session = MagicMock()
        mock_pdf = pd.DataFrame({"id": [1, 2], "val": [10.0, 20.0]})
        session.fetcher.execute_to_pandas.return_value = mock_pdf

        mock_daft_df = MagicMock()
        mock_from_pandas.return_value = mock_daft_df

        plan = TableScan(table_name="t", columns=["id", "val"])
        df = DataFrame(plan, session, schema=[("id", "INT"), ("val", "DOUBLE")])

        transform = MagicMock(return_value=mock_daft_df)
        result = df.map_batches(transform)

        transform.assert_called_once_with(mock_daft_df)
        assert result._daft_df is mock_daft_df
        assert result._session is session
