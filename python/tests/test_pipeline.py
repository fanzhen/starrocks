"""Unit tests for the hybrid pipeline executor (mock-based, no StarRocks/Daft/Ray needed)."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Create a mock daft module so tests work without daft installed
if "daft" not in sys.modules:
    _mock_daft = MagicMock()
    sys.modules["daft"] = _mock_daft


def _make_df(schema=None):
    """Create a DataFrame with mocked session (no MapBatches)."""
    from starrocks.dataframe import DataFrame
    from starrocks.plan.logical import TableScan

    session = MagicMock()
    mock_pdf = pd.DataFrame({"id": [1, 2, 3], "val": [10.0, 20.0, 30.0]})
    session.fetcher.execute_to_pandas.return_value = mock_pdf
    session.arrow_connection = None

    plan = TableScan(table_name="t", columns=["id", "val"])
    return DataFrame(plan, session, schema=schema or [("id", "INT"), ("val", "DOUBLE")])


# ---------------------------------------------------------------------------
# MapBatches lazy transformation
# ---------------------------------------------------------------------------

class TestMapBatchesLazy:
    """map_batches() should return a DataFrame (lazy), not execute immediately."""

    def test_map_batches_returns_dataframe(self):
        from starrocks.dataframe import DataFrame
        df = _make_df()
        result = df.map_batches(lambda x: x)
        assert isinstance(result, DataFrame)

    def test_map_batches_plan_contains_node(self):
        from starrocks.plan.logical import MapBatches
        df = _make_df()
        result = df.map_batches(lambda x: x)
        assert isinstance(result._plan, MapBatches)

    def test_has_map_batches_flag(self):
        df = _make_df()
        assert not df._has_map_batches()
        result = df.map_batches(lambda x: x)
        assert result._has_map_batches()

    def test_chained_map_batches(self):
        from starrocks.plan.logical import MapBatches
        df = _make_df()
        result = df.map_batches(lambda x: x).map_batches(lambda x: x)
        assert isinstance(result._plan, MapBatches)
        assert isinstance(result._plan.child, MapBatches)

    def test_filter_after_map_batches(self):
        """filter() after map_batches() should still be lazy."""
        from starrocks.dataframe import DataFrame
        from starrocks.plan.logical import Filter
        from starrocks.column import col
        df = _make_df()
        result = df.map_batches(lambda x: x).filter(col("id") > 1)
        assert isinstance(result, DataFrame)
        assert isinstance(result._plan, Filter)
        assert result._has_map_batches()

    def test_select_after_map_batches(self):
        """select() after map_batches() should still be lazy."""
        from starrocks.plan.logical import Projection
        df = _make_df()
        result = df.map_batches(lambda x: x).select("id")
        assert isinstance(result._plan, Projection)
        assert result._has_map_batches()


# ---------------------------------------------------------------------------
# Pure SQL path backward compatibility
# ---------------------------------------------------------------------------

class TestPureSQLBackwardCompat:
    """Actions without map_batches should use the existing SQL path."""

    def test_to_pandas_no_map_batches(self):
        df = _make_df()
        result = df.to_pandas()
        df._session.fetcher.execute_to_pandas.assert_called_once()
        assert isinstance(result, pd.DataFrame)

    def test_count_no_map_batches(self):
        df = _make_df()
        df._session.fetcher.execute_count.return_value = 42
        result = df.count()
        assert result == 42
        df._session.fetcher.execute_count.assert_called_once()

    def test_first_no_map_batches(self):
        df = _make_df()
        df._session.fetcher.execute_to_dicts.return_value = [{"id": 1, "val": 10.0}]
        result = df.first()
        assert result == {"id": 1, "val": 10.0}

    def test_show_no_map_batches(self):
        df = _make_df()
        df.show()
        df._session.fetcher.execute_show.assert_called_once()


# ---------------------------------------------------------------------------
# Pipeline splitting
# ---------------------------------------------------------------------------

class TestPipelineSplitting:
    """Test that PipelineExecutor correctly splits plans at MapBatches."""

    def test_simple_sql_then_map(self):
        from starrocks.execution.pipeline import PipelineExecutor
        from starrocks.plan.logical import MapBatches, TableScan

        func = lambda x: x
        plan = MapBatches(
            child=TableScan(table_name="t", columns=["id"]),
            func=func,
        )

        executor = PipelineExecutor()
        segments = executor._split_pipeline(plan)

        assert segments[0]["type"] == "sql"
        assert segments[1]["type"] == "map_batches"
        assert segments[1]["func"] is func

    def test_filter_after_map_splits_correctly(self):
        from starrocks.execution.pipeline import PipelineExecutor
        from starrocks.plan.logical import Filter, MapBatches, TableScan
        from starrocks.plan.expr import BinaryOp, ColumnRef, Literal

        func = lambda x: x
        predicate = BinaryOp(">", ColumnRef("id"), Literal(1))
        plan = Filter(
            child=MapBatches(
                child=TableScan(table_name="t", columns=["id"]),
                func=func,
            ),
            predicate=predicate,
        )

        executor = PipelineExecutor()
        segments = executor._split_pipeline(plan)

        assert segments[0]["type"] == "sql"
        assert segments[1]["type"] == "map_batches"
        assert segments[2]["type"] == "daft_ops"
        assert len(segments[2]["nodes"]) == 1

    def test_chained_map_batches_segments(self):
        from starrocks.execution.pipeline import PipelineExecutor
        from starrocks.plan.logical import MapBatches, TableScan

        f1 = lambda x: x
        f2 = lambda x: x
        plan = MapBatches(
            child=MapBatches(
                child=TableScan(table_name="t", columns=["id"]),
                func=f1,
            ),
            func=f2,
        )

        executor = PipelineExecutor()
        segments = executor._split_pipeline(plan)

        assert segments[0]["type"] == "sql"
        assert segments[1]["type"] == "map_batches"
        assert segments[1]["func"] is f1
        assert segments[2]["type"] == "map_batches"
        assert segments[2]["func"] is f2


# ---------------------------------------------------------------------------
# to_starrocks
# ---------------------------------------------------------------------------

class TestToStarRocks:
    """Test DataFrame.to_starrocks() action."""

    def test_pure_sql_to_starrocks(self):
        df = _make_df()
        df._session.connection.execute.return_value = [{"cnt": 3}]
        count = df.to_starrocks("target")
        # Should use INSERT INTO ... SELECT
        calls = df._session.connection.execute.call_args_list
        insert_sql = calls[0][0][0]
        assert "INSERT INTO `target`" in insert_sql
        assert count == 3
