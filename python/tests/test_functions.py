"""Unit tests for functions module (no StarRocks connection)."""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from starrocks.column import col
from starrocks.dataframe import Window
from starrocks import functions as func


class TestStringFunctions:
    def test_concat(self):
        sql = func.concat(col("a"), func.lit(" "), col("b")).to_sql()
        assert sql == "CONCAT(`a`, ' ', `b`)"

    def test_upper(self):
        assert func.upper(col("name")).to_sql() == "UPPER(`name`)"

    def test_lower(self):
        assert func.lower(col("name")).to_sql() == "LOWER(`name`)"

    def test_length(self):
        assert func.length(col("name")).to_sql() == "LENGTH(`name`)"

    def test_substring(self):
        sql = func.substring(col("name"), 1, 3).to_sql()
        assert sql == "SUBSTRING(`name`, 1, 3)"

    def test_trim(self):
        assert func.trim(col("name")).to_sql() == "TRIM(`name`)"

    def test_replace(self):
        sql = func.replace(col("name"), "old", "new").to_sql()
        assert sql == "REPLACE(`name`, 'old', 'new')"


class TestDateFunctions:
    def test_year(self):
        assert func.year(col("dt")).to_sql() == "YEAR(`dt`)"

    def test_month(self):
        assert func.month(col("dt")).to_sql() == "MONTH(`dt`)"

    def test_day(self):
        assert func.day(col("dt")).to_sql() == "DAY(`dt`)"

    def test_date_trunc(self):
        sql = func.date_trunc("month", col("dt")).to_sql()
        assert sql == "DATE_TRUNC('month', `dt`)"

    def test_now(self):
        assert func.now().to_sql() == "NOW()"

    def test_current_date(self):
        assert func.current_date().to_sql() == "CURRENT_DATE()"


class TestMathFunctions:
    def test_abs(self):
        assert func.abs(col("x")).to_sql() == "ABS(`x`)"

    def test_round(self):
        assert func.round(col("x"), 2).to_sql() == "ROUND(`x`, 2)"

    def test_ceil(self):
        assert func.ceil(col("x")).to_sql() == "CEIL(`x`)"

    def test_floor(self):
        assert func.floor(col("x")).to_sql() == "FLOOR(`x`)"


class TestConditionalFunctions:
    def test_when_otherwise(self):
        sql = func.when(col("a") > 1, "yes").otherwise("no").to_sql()
        assert sql == "CASE WHEN `a` > 1 THEN 'yes' ELSE 'no' END"

    def test_when_multi(self):
        sql = (func.when(col("a") > 10, "high")
                   .when(col("a") > 5, "mid")
                   .otherwise("low")).to_sql()
        assert "WHEN `a` > 10 THEN 'high'" in sql
        assert "WHEN `a` > 5 THEN 'mid'" in sql
        assert "ELSE 'low'" in sql

    def test_when_no_else(self):
        sql = func.when(col("a") > 1, "yes").end().to_sql()
        assert "ELSE" not in sql
        assert sql == "CASE WHEN `a` > 1 THEN 'yes' END"

    def test_coalesce(self):
        sql = func.coalesce(col("a"), func.lit(0)).to_sql()
        assert sql == "COALESCE(`a`, 0)"

    def test_if(self):
        sql = func.if_(col("a") > 1, "yes", "no").to_sql()
        assert sql == "IF(`a` > 1, 'yes', 'no')"


class TestWindowFunctions:
    def test_row_number(self):
        w = Window.partition_by("dept").order_by("salary")
        sql = func.row_number().over(w).to_sql()
        assert sql == "ROW_NUMBER() OVER (PARTITION BY `dept` ORDER BY `salary`)"

    def test_rank(self):
        w = Window.partition_by("dept").order_by("salary")
        sql = func.rank().over(w).to_sql()
        assert sql == "RANK() OVER (PARTITION BY `dept` ORDER BY `salary`)"

    def test_dense_rank(self):
        w = Window.partition_by("dept").order_by("salary")
        sql = func.dense_rank().over(w).to_sql()
        assert sql == "DENSE_RANK() OVER (PARTITION BY `dept` ORDER BY `salary`)"

    def test_lag(self):
        w = Window.partition_by("dept").order_by("salary")
        sql = func.lag(col("salary"), 1, 0).over(w).to_sql()
        assert sql == "LAG(`salary`, 1, 0) OVER (PARTITION BY `dept` ORDER BY `salary`)"

    def test_lead(self):
        w = Window.order_by("id")
        sql = func.lead(col("val")).over(w).to_sql()
        assert sql == "LEAD(`val`, 1) OVER (ORDER BY `id`)"

    def test_window_order_only(self):
        w = Window.order_by("id")
        sql = func.row_number().over(w).to_sql()
        assert sql == "ROW_NUMBER() OVER (ORDER BY `id`)"

    def test_window_partition_only(self):
        w = Window.partition_by("dept")
        sql = func.row_number().over(w).to_sql()
        assert sql == "ROW_NUMBER() OVER (PARTITION BY `dept`)"

    def test_window_with_col_expr(self):
        w = Window.partition_by(col("dept")).order_by(col("salary").desc())
        sql = func.row_number().over(w).to_sql()
        assert sql == "ROW_NUMBER() OVER (PARTITION BY `dept` ORDER BY `salary` DESC)"


class TestAggregates:
    def test_sum(self):
        assert func.sum("revenue").to_sql() == "SUM(`revenue`)"

    def test_count_star(self):
        assert func.count("*").to_sql() == "COUNT(*)"

    def test_count_distinct(self):
        assert func.count_distinct("uid").to_sql() == "COUNT(DISTINCT `uid`)"

    def test_lit(self):
        assert func.lit(42).to_sql() == "42"
        assert func.lit("hello").to_sql() == "'hello'"
