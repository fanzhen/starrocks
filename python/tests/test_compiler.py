"""Unit tests for SQL compiler (no StarRocks connection)."""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from starrocks.column import col
from starrocks.compiler.sql_compiler import SQLCompiler
from starrocks.plan.expr import ColumnRef
from starrocks.plan.logical import Filter, Projection, TableScan


class TestSQLCompiler:
    def setup_method(self):
        self.compiler = SQLCompiler()

    def test_table_scan(self):
        plan = TableScan("users")
        sql = self.compiler.compile(plan)
        assert sql == "SELECT * FROM `users`"

    def test_table_scan_with_database(self):
        plan = TableScan("users", database="mydb")
        sql = self.compiler.compile(plan)
        assert sql == "SELECT * FROM `mydb`.`users`"

    def test_filter(self):
        scan = TableScan("users")
        plan = Filter(scan, (col("age") > 18).expr)
        sql = self.compiler.compile(plan)
        assert "WHERE" in sql
        assert "`age` > 18" in sql

    def test_projection(self):
        scan = TableScan("users")
        plan = Projection(scan, [ColumnRef("id"), ColumnRef("name")])
        sql = self.compiler.compile(plan)
        assert "`id`" in sql
        assert "`name`" in sql
        assert "FROM `users`" in sql

    def test_filter_then_projection(self):
        scan = TableScan("users")
        filtered = Filter(scan, (col("age") > 18).expr)
        plan = Projection(filtered, [ColumnRef("id"), ColumnRef("name")])
        sql = self.compiler.compile(plan)
        assert "SELECT `id`, `name`" in sql
        assert "WHERE" in sql
        assert "`age` > 18" in sql

    def test_and_predicate(self):
        scan = TableScan("t")
        pred = ((col("a") > 1) & (col("b") < 10)).expr
        plan = Filter(scan, pred)
        sql = self.compiler.compile(plan)
        assert "AND" in sql.upper()

    def test_quote_escape_in_filter(self):
        scan = TableScan("t")
        pred = (col("name") == "O'Brien").expr
        plan = Filter(scan, pred)
        sql = self.compiler.compile(plan)
        assert "O" in sql and "Brien" in sql
