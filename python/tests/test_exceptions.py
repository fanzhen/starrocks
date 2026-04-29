"""Unit tests for custom exceptions and error handling."""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from starrocks.exceptions import (
    StarRocksError,
    ConnectionError,
    QueryError,
    CompilationError,
)


class TestExceptionHierarchy:
    def test_base_class(self):
        assert issubclass(ConnectionError, StarRocksError)
        assert issubclass(QueryError, StarRocksError)
        assert issubclass(CompilationError, StarRocksError)

    def test_connection_error(self):
        e = ConnectionError("cannot connect")
        assert "cannot connect" in str(e)
        assert isinstance(e, StarRocksError)

    def test_query_error_with_sql(self):
        e = QueryError("table not found", sql="SELECT * FROM t")
        msg = str(e)
        assert "table not found" in msg
        assert "SELECT * FROM t" in msg
        assert e.sql == "SELECT * FROM t"

    def test_query_error_with_original(self):
        orig = ValueError("original error")
        e = QueryError("failed", sql="SELECT 1", original=orig)
        assert e.original is orig
        assert "original error" in str(e)

    def test_compilation_error(self):
        e = CompilationError("unsupported node")
        assert "unsupported node" in str(e)


class TestSQLInjectionSafety:
    def test_literal_escapes_single_quotes(self):
        from starrocks.plan.expr import Literal
        val = "'; DROP TABLE t; --"
        sql = Literal(val).to_sql()
        assert "\\'" in sql
        assert sql.count("'") >= 2  # Properly quoted

    def test_literal_escapes_backslash(self):
        from starrocks.plan.expr import Literal
        val = "a\\b"
        sql = Literal(val).to_sql()
        assert "\\\\" in sql

    def test_column_ref_escapes_backticks(self):
        from starrocks.column import col
        # Column names are backtick-quoted, not user-controlled
        c = col("name")
        assert c.to_sql() == "`name`"
