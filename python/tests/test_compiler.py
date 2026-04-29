"""Unit tests for SQL compiler (no StarRocks connection)."""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from starrocks.column import col
from starrocks.compiler.sql_compiler import SQLCompiler
from starrocks.plan.expr import ColumnRef
from starrocks.plan.logical import Aggregate, Distinct, Filter, Limit, Projection, Sort, TableScan


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

    # -- Phase 2: aggregate, sort, limit, distinct ----------------------------

    def test_aggregate_group_by_sum(self):
        from starrocks.plan.expr import Alias, ColumnRef, FunctionCall
        scan = TableScan("sales")
        agg = Aggregate(
            scan,
            group_keys=[ColumnRef("city")],
            agg_exprs=[Alias(FunctionCall("SUM", (ColumnRef("revenue"),)), "total")],
        )
        sql = self.compiler.compile(agg)
        assert "GROUP BY" in sql
        assert "SUM" in sql
        assert "`city`" in sql

    def test_aggregate_count_star(self):
        from starrocks.plan.expr import Alias, ColumnRef, FunctionCall, Star
        scan = TableScan("t")
        agg = Aggregate(
            scan,
            group_keys=[ColumnRef("city")],
            agg_exprs=[Alias(FunctionCall("COUNT", (Star(),)), "cnt")],
        )
        sql = self.compiler.compile(agg)
        assert "COUNT(*)" in sql

    def test_count_distinct(self):
        from starrocks.plan.expr import Alias, ColumnRef, FunctionCall
        scan = TableScan("t")
        agg = Aggregate(
            scan,
            group_keys=[ColumnRef("city")],
            agg_exprs=[Alias(FunctionCall("COUNT", (ColumnRef("user_id"),), distinct=True), "uv")],
        )
        sql = self.compiler.compile(agg)
        assert "COUNT(DISTINCT" in sql

    def test_sort_desc(self):
        scan = TableScan("t")
        plan = Sort(scan, [col("revenue").desc().expr])
        sql = self.compiler.compile(plan)
        assert "ORDER BY" in sql
        assert "DESC" in sql

    def test_limit(self):
        scan = TableScan("t")
        plan = Limit(scan, 10)
        sql = self.compiler.compile(plan)
        assert "LIMIT 10" in sql

    def test_distinct(self):
        scan = TableScan("t")
        proj = Projection(scan, [ColumnRef("city")])
        plan = Distinct(proj)
        sql = self.compiler.compile(plan)
        assert "SELECT DISTINCT" in sql

    def test_full_chain(self):
        """filter → aggregate → sort → limit"""
        from starrocks.plan.expr import Alias, ColumnRef, FunctionCall
        scan = TableScan("sales")
        filtered = Filter(scan, (col("revenue") > 100).expr)
        agg = Aggregate(
            filtered,
            group_keys=[ColumnRef("city")],
            agg_exprs=[Alias(FunctionCall("SUM", (ColumnRef("revenue"),)), "total")],
        )
        sorted_ = Sort(agg, [col("total").desc().expr])
        plan = Limit(sorted_, 10)
        sql = self.compiler.compile(plan)
        assert "WHERE" in sql
        assert "GROUP BY" in sql
        assert "ORDER BY" in sql
        assert "LIMIT 10" in sql
