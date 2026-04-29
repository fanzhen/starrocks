"""Unit tests for Column expression building (no StarRocks connection)."""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from starrocks.column import col


class TestColumnExpressions:
    def test_column_ref(self):
        c = col("name")
        assert c.to_sql() == "`name`"

    def test_gt(self):
        sql = (col("value") > 15).to_sql()
        assert sql == "`value` > 15"

    def test_ge(self):
        sql = (col("value") >= 10).to_sql()
        assert sql == "`value` >= 10"

    def test_lt(self):
        sql = (col("value") < 100).to_sql()
        assert sql == "`value` < 100"

    def test_le(self):
        sql = (col("value") <= 50).to_sql()
        assert sql == "`value` <= 50"

    def test_eq(self):
        sql = (col("name") == "alice").to_sql()
        assert sql == "`name` = 'alice'"

    def test_ne(self):
        sql = (col("name") != "bob").to_sql()
        assert sql == "`name` != 'bob'"

    def test_and(self):
        sql = ((col("a") > 1) & (col("b") < 10)).to_sql()
        assert "AND" in sql

    def test_or(self):
        sql = ((col("a") > 1) | (col("b") < 10)).to_sql()
        assert "OR" in sql

    def test_not(self):
        sql = (~(col("a") > 1)).to_sql()
        assert "NOT" in sql

    def test_is_null(self):
        sql = col("a").is_null().to_sql()
        assert "IS NULL" in sql

    def test_is_not_null(self):
        sql = col("a").is_not_null().to_sql()
        assert "IS NOT NULL" in sql

    def test_between(self):
        sql = col("a").between(1, 10).to_sql()
        assert "BETWEEN" in sql and "AND" in sql

    def test_is_in(self):
        sql = col("a").is_in(1, 2, 3).to_sql()
        assert "IN" in sql

    def test_like(self):
        sql = col("name").like("%alice%").to_sql()
        assert "LIKE" in sql

    def test_alias(self):
        sql = col("a").alias("my_col").to_sql()
        assert "AS `my_col`" in sql

    def test_arithmetic(self):
        sql = (col("a") + col("b")).to_sql()
        assert "+" in sql

    def test_quote_escape(self):
        sql = (col("name") == "O'Brien").to_sql()
        assert "O\\'Brien" in sql or "O''Brien" in sql

    def test_null_literal(self):
        from starrocks.plan.expr import Literal
        assert Literal(None).to_sql() == "NULL"

    def test_bool_literal(self):
        from starrocks.plan.expr import Literal
        assert Literal(True).to_sql() == "TRUE"
        assert Literal(False).to_sql() == "FALSE"

    def test_asc_desc(self):
        assert "ASC" in col("a").asc().to_sql()
        assert "DESC" in col("a").desc().to_sql()
