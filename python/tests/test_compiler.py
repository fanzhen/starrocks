"""Unit tests for SQL compiler (no StarRocks connection)."""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from starrocks.column import col
from starrocks.compiler.sql_compiler import SQLCompiler
from starrocks.plan.expr import ColumnRef
from starrocks.plan.expr import BinaryOp
from starrocks.plan.logical import (
    Aggregate, Distinct, Filter, Join, Limit, Projection, RawSQL,
    SetOperation, Sort, SubqueryAlias, TableScan,
)


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

    # -- Phase 3: join, union, raw SQL ----------------------------------------

    def test_inner_join(self):
        left = TableScan("orders")
        right = TableScan("customers")
        on = BinaryOp("=", ColumnRef("cust_id", "orders"), ColumnRef("cust_id", "customers"))
        plan = Join(left, right, on=on, how="inner")
        sql = self.compiler.compile(plan)
        assert "INNER JOIN" in sql
        assert "ON" in sql

    def test_left_join(self):
        left = TableScan("orders")
        right = TableScan("customers")
        on = BinaryOp("=", ColumnRef("cust_id", "orders"), ColumnRef("cust_id", "customers"))
        plan = Join(left, right, on=on, how="left")
        sql = self.compiler.compile(plan)
        assert "LEFT JOIN" in sql

    def test_cross_join(self):
        left = TableScan("a")
        right = TableScan("b")
        plan = Join(left, right, on=None, how="cross")
        sql = self.compiler.compile(plan)
        assert "CROSS JOIN" in sql

    def test_union_all(self):
        left = TableScan("t1")
        right = TableScan("t2")
        plan = SetOperation(left, right, op="UNION ALL")
        sql = self.compiler.compile(plan)
        assert "UNION ALL" in sql

    def test_union_distinct(self):
        left = TableScan("t1")
        right = TableScan("t2")
        plan = SetOperation(left, right, op="UNION DISTINCT")
        sql = self.compiler.compile(plan)
        assert "UNION DISTINCT" in sql

    def test_raw_sql(self):
        plan = RawSQL("SELECT 1 AS x")
        sql = self.compiler.compile(plan)
        assert "SELECT 1 AS x" in sql

    def test_subquery_alias(self):
        inner = TableScan("orders")
        plan = SubqueryAlias(inner, "o")
        sql = self.compiler.compile(plan)
        assert "`o`" in sql

    def test_filter_on_join(self):
        """Filter applied on top of a join."""
        left = TableScan("orders")
        right = TableScan("customers")
        on = BinaryOp("=", ColumnRef("cust_id", "orders"), ColumnRef("cust_id", "customers"))
        joined = Join(left, right, on=on)
        plan = Filter(joined, (col("amount") > 100).expr)
        sql = self.compiler.compile(plan)
        assert "WHERE" in sql

    # -- Phase 4: expressions + functions + window -----------------------------

    def test_case_when(self):
        from starrocks.plan.expr import CaseWhen, Literal
        expr = CaseWhen(
            conditions=((BinaryOp(">", ColumnRef("a"), Literal(1)), Literal("yes")),),
            else_expr=Literal("no"),
        )
        assert expr.to_sql() == "CASE WHEN `a` > 1 THEN 'yes' ELSE 'no' END"

    def test_window_expr(self):
        from starrocks.plan.expr import FunctionCall, WindowExpr
        expr = WindowExpr(
            func=FunctionCall("ROW_NUMBER"),
            partition_by=(ColumnRef("dept"),),
            order_by=(ColumnRef("salary"),),
        )
        assert expr.to_sql() == "ROW_NUMBER() OVER (PARTITION BY `dept` ORDER BY `salary`)"

    def test_with_column(self):
        scan = TableScan("t", columns=["a", "b"])
        # Simulate with_column by building a Projection
        from starrocks.plan.expr import Alias
        plan = Projection(scan, [ColumnRef("a"), ColumnRef("b"), Alias(BinaryOp("+", ColumnRef("a"), ColumnRef("b")), "c")])
        sql = self.compiler.compile(plan)
        assert "`a`" in sql
        assert "`b`" in sql
        assert "AS `c`" in sql

    def test_rename(self):
        from starrocks.plan.expr import Alias
        scan = TableScan("t")
        plan = Projection(scan, [Alias(ColumnRef("old_name"), "new_name")])
        sql = self.compiler.compile(plan)
        assert "`old_name` AS `new_name`" in sql
