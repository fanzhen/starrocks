"""E2E integration tests (require a running StarRocks instance).

Set SR_HOST / SR_PORT environment variables to point to your cluster.
Skip with: pytest -k 'not integration'
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

SR_HOST = os.getenv("SR_HOST", "127.0.0.1")
SR_PORT = int(os.getenv("SR_PORT", "9030"))

needs_sr = pytest.mark.skipif(
    os.getenv("SR_SKIP_INTEGRATION", "0") == "1",
    reason="SR_SKIP_INTEGRATION is set",
)


@pytest.fixture(scope="module")
def session():
    from starrocks import Session

    s = Session(host=SR_HOST, port=SR_PORT, user="root", database="test_dataframe")
    s.execute("CREATE DATABASE IF NOT EXISTS test_dataframe")
    s.execute("DROP TABLE IF EXISTS test_dataframe.t1")
    s.execute("""
        CREATE TABLE test_dataframe.t1 (
            id INT, name VARCHAR(50), value DOUBLE
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
    """)
    s.execute("INSERT INTO test_dataframe.t1 VALUES (1,'alice',10.5),(2,'bob',20.3),(3,'charlie',30.1)")
    yield s
    s.execute("DROP TABLE IF EXISTS test_dataframe.t1")
    s.close()


@needs_sr
class TestIntegration:
    def test_table_schema(self, session):
        df = session.table("t1")
        assert len(df.columns) == 3
        assert "id" in df.columns

    def test_filter_to_sql(self, session):
        from starrocks import col

        df = session.table("t1")
        sql = df.filter(col("value") > 15).to_sql()
        assert "WHERE" in sql

    def test_select_to_sql(self, session):
        df = session.table("t1")
        sql = df.select("id", "name").to_sql()
        assert "`id`" in sql and "`name`" in sql

    def test_show(self, session):
        from starrocks import col

        df = session.table("t1")
        df.filter(col("value") > 15).show()  # should not raise

    def test_to_pandas(self, session):
        from starrocks import col

        df = session.table("t1")
        pdf = df.filter(col("value") > 15).to_pandas()
        assert len(pdf) == 2
        assert set(pdf["name"]) == {"bob", "charlie"}

    def test_count(self, session):
        df = session.table("t1")
        assert df.count() == 3

    def test_explain(self, session):
        df = session.table("t1")
        plan = df.explain()
        assert len(plan) > 0

    def test_first(self, session):
        df = session.table("t1")
        row = df.first()
        assert row is not None
        assert "id" in row

    def test_chain(self, session):
        from starrocks import col

        df = session.table("t1")
        sql = df.filter(col("value") > 15).select("id", "name").to_sql()
        assert "WHERE" in sql and "`id`" in sql
