"""Unit tests for StarRocks-specific features (no connection)."""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from starrocks.column import col
from starrocks.compiler.sql_compiler import SQLCompiler
from starrocks.plan.expr import ColumnRef, MatchExpr
from starrocks.plan.logical import Filter, Projection, TableScan
from starrocks import functions as func


class TestMatchExpr:
    def test_match_phrase(self):
        sql = col("content").match_phrase("hello world").to_sql()
        assert sql == "`content` MATCH_PHRASE 'hello world'"

    def test_match_all(self):
        sql = col("content").match_all("a b").to_sql()
        assert sql == "`content` MATCH_ALL 'a b'"

    def test_match_any(self):
        sql = col("content").match_any("a b").to_sql()
        assert sql == "`content` MATCH_ANY 'a b'"

    def test_match_phrase_prefix(self):
        sql = col("content").match_phrase_prefix("hel").to_sql()
        assert sql == "`content` MATCH_PHRASE_PREFIX 'hel'"

    def test_match_in_filter(self):
        compiler = SQLCompiler()
        scan = TableScan("docs")
        plan = Filter(scan, col("content").match_phrase("hello").expr)
        sql = compiler.compile(plan)
        assert "WHERE" in sql
        assert "MATCH_PHRASE" in sql
        assert "'hello'" in sql

    def test_match_escape(self):
        sql = col("content").match_phrase("it's a test").to_sql()
        assert "it\\'s a test" in sql


class TestStarRocksFunctions:
    def test_bitmap_union(self):
        sql = func.bitmap_union(col("bm")).to_sql()
        assert sql == "BITMAP_UNION(`bm`)"

    def test_bitmap_count(self):
        sql = func.bitmap_count(col("bm")).to_sql()
        assert sql == "BITMAP_COUNT(`bm`)"

    def test_bitmap_union_count(self):
        sql = func.bitmap_union_count(col("bm")).to_sql()
        assert sql == "BITMAP_UNION_COUNT(`bm`)"

    def test_hll_union(self):
        sql = func.hll_union(col("h")).to_sql()
        assert sql == "HLL_UNION(`h`)"

    def test_hll_union_agg(self):
        sql = func.hll_union_agg(col("h")).to_sql()
        assert sql == "HLL_UNION_AGG(`h`)"

    def test_approx_count_distinct(self):
        sql = func.approx_count_distinct("uid").to_sql()
        assert sql == "APPROX_COUNT_DISTINCT(`uid`)"

    def test_array_agg(self):
        sql = func.array_agg(col("v")).to_sql()
        assert sql == "ARRAY_AGG(`v`)"

    def test_bm25(self):
        sql = func.bm25(col("content"), "query").to_sql()
        assert sql == "BM25(`content`, 'query')"


class TestCatalogTableScan:
    def test_catalog_qualified_name(self):
        compiler = SQLCompiler()
        scan = TableScan("orders", database="mydb", catalog="hive_catalog")
        sql = compiler.compile(scan)
        assert sql == "SELECT * FROM `hive_catalog`.`mydb`.`orders`"

    def test_no_catalog(self):
        compiler = SQLCompiler()
        scan = TableScan("orders", database="mydb")
        sql = compiler.compile(scan)
        assert sql == "SELECT * FROM `mydb`.`orders`"


class TestWithBm25:
    def test_with_bm25_sql(self):
        compiler = SQLCompiler()
        scan = TableScan("docs", columns=["id", "content"])
        plan = Projection(scan, [
            ColumnRef("id"),
            ColumnRef("content"),
            # Simulate with_bm25
            __import__("starrocks.plan.expr", fromlist=["Alias"]).Alias(
                __import__("starrocks.plan.expr", fromlist=["FunctionCall"]).FunctionCall(
                    "BM25", (ColumnRef("content"), __import__("starrocks.plan.expr", fromlist=["Literal"]).Literal("query"))
                ),
                "score"
            ),
        ])
        sql = compiler.compile(plan)
        assert "BM25" in sql
        assert "AS `score`" in sql
