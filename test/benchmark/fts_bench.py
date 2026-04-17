#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
FTS Benchmark: tantivy vs CLucene vs no-index on 100K English rows.

Produces a CSV file with raw measurements. Use fts_report.py to generate
the comparison report and PASS/FAIL verdicts.

Usage:
    python3 fts_bench.py --host 127.0.0.1 --port 9030 --http-port 8030 \
        --be-http-port 8040 --rows 100000 --output bench_results.csv

Requirements:
    pip install pymysql requests
"""

import argparse
import csv
import os
import random
import statistics
import subprocess
import sys
import tempfile
import time

try:
    import pymysql
except ImportError:
    sys.exit("pymysql is required: pip install pymysql")

try:
    import requests
except ImportError:
    sys.exit("requests is required: pip install requests")


DB = "test_db"

_SIZE_UNITS = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}


def _parse_size_str(s):
    """Parse '4.348 MB' → bytes (int)."""
    parts = s.strip().split()
    if len(parts) == 2:
        try:
            return int(float(parts[0]) * _SIZE_UNITS.get(parts[1].upper(), 1))
        except (ValueError, TypeError):
            pass
    return 0
TABLES = {
    "bench_tantivy": 'INDEX idx_c (content) USING GIN ("parser"="standard", "imp_lib"="tantivy")',
    "bench_clucene": 'INDEX idx_c (content) USING GIN ("parser"="standard", "imp_lib"="clucene")',
    "bench_noidx": None,
}
SEED_WORDS = [
    "database", "search", "engine", "query", "index", "full", "text",
    "performance", "system", "data", "analytics", "storage", "memory",
    "network", "cluster", "partition", "column", "table", "schema",
    "optimizer", "planner", "executor", "scanner", "filter", "aggregate",
    "join", "sort", "limit", "offset", "transaction", "snapshot",
    "compaction", "replication", "backup", "restore", "monitor",
    "metric", "latency", "throughput", "benchmark", "workload",
]
QUERY_ROUNDS = 5


def get_conn(args):
    return pymysql.connect(
        host=args.host, port=args.port, user="root", database=DB,
        autocommit=True, connect_timeout=10, read_timeout=300,
    )


def run_sql(conn, sql):
    try:
        conn.ping(reconnect=True)
    except Exception:
        pass
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def ensure_db(conn):
    with conn.cursor() as cur:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB}")


def ensure_gin_enabled(conn):
    run_sql(conn, 'ADMIN SET FRONTEND CONFIG ("enable_experimental_gin" = "true")')


def drop_tables(conn):
    for tbl in TABLES:
        run_sql(conn, f"DROP TABLE IF EXISTS {DB}.{tbl}")


def create_tables(conn):
    for tbl, idx_clause in TABLES.items():
        idx = f", {idx_clause}" if idx_clause else ""
        ddl = (
            f"CREATE TABLE {DB}.{tbl} "
            f"(id BIGINT, content VARCHAR(65535){idx}) "
            f"DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 8 "
            f'PROPERTIES("replication_num"="1")'
        )
        run_sql(conn, ddl)


def generate_data(num_rows, path):
    rng = random.Random(42)
    with open(path, "w") as f:
        for i in range(num_rows):
            k = rng.randint(8, 20)
            sentence = " ".join(rng.choices(SEED_WORDS, k=k))
            f.write(f"{i}\t{sentence}\n")


def stream_load(args, table, data_path):
    """Load data via Stream Load HTTP API, return elapsed seconds."""
    url = f"http://{args.host}:{args.http_port}/api/{DB}/{table}/_stream_load"
    label = f"bench_{table}_{int(time.time())}"
    t0 = time.monotonic()
    cmd = [
        "curl", "-s", "--location-trusted",
        "-u", "root:",
        "-H", f"label:{label}",
        "-H", "column_separator:\\t",
        "-T", data_path,
        url,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    t1 = time.monotonic()
    try:
        import json
        body = json.loads(result.stdout)
        if body.get("Status") != "Success":
            print(f"  WARNING: stream load {table} status={body.get('Status')}: {body.get('Message', '')}", file=sys.stderr)
    except Exception:
        print(f"  WARNING: stream load {table} response: {result.stdout[:200]}", file=sys.stderr)
    return t1 - t0


def median_latency_ms(conn, sql, rounds=QUERY_ROUNDS):
    """Run sql `rounds` times, return median wall-clock latency in ms.
    Returns -1 if all attempts fail."""
    times = []
    for _ in range(rounds):
        try:
            t0 = time.monotonic()
            run_sql(conn, sql)
            t1 = time.monotonic()
            times.append((t1 - t0) * 1000)
        except Exception as e:
            print(f"  WARN: query failed: {e}", file=sys.stderr)
    if not times:
        return -1.0
    return statistics.median(times)


def get_be_mem_bytes(args):
    """Fetch BE process memory via /metrics (Prometheus format)."""
    import re
    url = f"http://{args.host}:{args.be_http_port}/metrics"
    try:
        resp = requests.get(url, timeout=10)
        m = re.search(r"starrocks_be_process_mem_bytes\s+(\d+)", resp.text)
        if m:
            return int(m.group(1))
    except Exception:
        pass
    return -1


def run_perf_record(args, duration_s=10):
    """Start perf record on BE for `duration_s` seconds. Returns output path."""
    perf_data = "/tmp/perf_tantivy.data"
    be_pid_cmd = f"pgrep -f starrocks_be | head -1"
    result = subprocess.run(
        ["ssh", "-i", os.path.expanduser("~/.ssh/my_ecs.pem"),
         f"root@{args.remote_host}",
         f"docker exec sr-dev bash -c '{be_pid_cmd}'"],
        capture_output=True, text=True, timeout=10,
    )
    be_pid = result.stdout.strip()
    if not be_pid:
        return None, "Could not find BE pid"

    subprocess.Popen(
        ["ssh", "-i", os.path.expanduser("~/.ssh/my_ecs.pem"),
         f"root@{args.remote_host}",
         f"docker exec sr-dev bash -c 'perf record -g -p {be_pid} -o {perf_data} -- sleep {duration_s}'"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    return perf_data, be_pid


def get_perf_report(args, perf_data):
    """Fetch perf report top-20 from remote."""
    result = subprocess.run(
        ["ssh", "-i", os.path.expanduser("~/.ssh/my_ecs.pem"),
         f"root@{args.remote_host}",
         f"docker exec sr-dev bash -c 'perf report -i {perf_data} --stdio --no-children 2>/dev/null | head -50'"],
        capture_output=True, text=True, timeout=30,
    )
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description="FTS Benchmark: tantivy vs CLucene")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9030)
    parser.add_argument("--http-port", type=int, default=8030, dest="http_port")
    parser.add_argument("--be-http-port", type=int, default=8040, dest="be_http_port")
    parser.add_argument("--remote-host", default="", dest="remote_host",
                        help="SSH host for perf (e.g. 8.217.233.254); skip perf if empty")
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--output", default="bench_results.csv")
    parser.add_argument("--mem-loops", type=int, default=1000, dest="mem_loops")
    parser.add_argument("--skip-perf", action="store_true", dest="skip_perf")
    args = parser.parse_args()

    conn = get_conn(args)
    ensure_db(conn)
    ensure_gin_enabled(conn)

    results = []

    # --- Setup ---
    print("=== Setup: creating tables ===")
    drop_tables(conn)
    create_tables(conn)

    # --- TC1: Write 100K rows ---
    print(f"=== TC1: INSERT {args.rows} rows ===")
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
        data_path = tmp.name
    generate_data(args.rows, data_path)
    print(f"  Data generated: {data_path} ({args.rows} rows)")

    write_times = {}
    for tbl in TABLES:
        elapsed = stream_load(args, tbl, data_path)
        write_times[tbl] = elapsed
        print(f"  {tbl}: {elapsed:.2f}s")
    os.unlink(data_path)

    results.append(("write_tantivy_s", f"{write_times['bench_tantivy']:.3f}"))
    results.append(("write_clucene_s", f"{write_times['bench_clucene']:.3f}"))
    results.append(("write_noidx_s", f"{write_times['bench_noidx']:.3f}"))

    print("  Waiting for data visible...")
    for attempt in range(6):
        time.sleep(5)
        try:
            cnt = run_sql(conn, f"SELECT COUNT(*) FROM {DB}.bench_noidx")
            if cnt and cnt[0][0] > 0:
                print(f"  Data visible after {(attempt + 1) * 5}s: {cnt[0][0]} rows")
                break
        except Exception:
            pass
    else:
        print("  WARNING: data not visible after 30s, proceeding anyway")

    # --- TC2: Index size ---
    print("=== TC2: Index size ===")
    for tbl in TABLES:
        try:
            cnt = run_sql(conn, f"SELECT COUNT(*) FROM {DB}.{tbl}")
            print(f"  {tbl} rowcount={cnt[0][0]}")
            results.append((f"rowcount_{tbl}", str(cnt[0][0])))
        except Exception as e:
            print(f"  {tbl} COUNT failed: {e}")
            results.append((f"rowcount_{tbl}", "0"))
        try:
            rows = run_sql(conn, f"SHOW DATA FROM {DB}.{tbl}")
            if rows:
                print(f"  {tbl} SHOW DATA: {rows[0]}")
                results.append((f"showdata_{tbl}", str(rows[0])))
                size_str = rows[0][2]
                data_bytes = _parse_size_str(size_str)
                results.append((f"data_bytes_{tbl}", str(data_bytes)))
        except Exception as e:
            results.append((f"data_bytes_{tbl}", "0"))
            print(f"  {tbl} SHOW DATA failed: {e}")

    # --- TC3: MATCH_ANY high selectivity (~50%) ---
    print("=== TC3: MATCH_ANY high selectivity ===")
    # "database" appears frequently in our synthetic data
    sql_t = f"SELECT COUNT(*) FROM {DB}.bench_tantivy WHERE content MATCH_ANY 'database performance'"
    sql_c = f"SELECT COUNT(*) FROM {DB}.bench_clucene WHERE content MATCH_ANY 'database performance'"
    p50_t3 = median_latency_ms(conn, sql_t)
    p50_c3 = median_latency_ms(conn, sql_c)
    print(f"  tantivy={p50_t3:.1f}ms, clucene={p50_c3:.1f}ms")
    results.append(("match_any_high_sel_tantivy_ms", f"{p50_t3:.1f}"))
    results.append(("match_any_high_sel_clucene_ms", f"{p50_c3:.1f}"))

    # --- TC4: MATCH_ANY low selectivity (<1%) ---
    print("=== TC4: MATCH_ANY low selectivity ===")
    sql_t = f"SELECT COUNT(*) FROM {DB}.bench_tantivy WHERE content MATCH_ANY 'xyzzy'"
    sql_c = f"SELECT COUNT(*) FROM {DB}.bench_clucene WHERE content MATCH_ANY 'xyzzy'"
    p50_t4 = median_latency_ms(conn, sql_t)
    p50_c4 = median_latency_ms(conn, sql_c)
    print(f"  tantivy={p50_t4:.1f}ms, clucene={p50_c4:.1f}ms")
    results.append(("match_any_low_sel_tantivy_ms", f"{p50_t4:.1f}"))
    results.append(("match_any_low_sel_clucene_ms", f"{p50_c4:.1f}"))

    # --- TC5: MATCH_PHRASE ---
    # NOTE: CLucene MATCH_PHRASE has a known crash in DefaultSkipListReader::readSkipData
    # (SIGSEGV, null pointer in PhraseScorer). Only benchmark tantivy for MATCH_PHRASE.
    print("=== TC5: MATCH_PHRASE ===")
    sql_t = f"SELECT COUNT(*) FROM {DB}.bench_tantivy WHERE content MATCH_PHRASE 'full text search'"
    p50_t5 = median_latency_ms(conn, sql_t)
    print(f"  tantivy={p50_t5:.1f}ms, clucene=SKIP (CLucene MATCH_PHRASE crash: SIGSEGV in PhraseScorer)")
    results.append(("match_phrase_tantivy_ms", f"{p50_t5:.1f}"))
    results.append(("match_phrase_clucene_ms", "-1"))
    results.append(("match_phrase_clucene_note", "SKIP: CLucene SIGSEGV in DefaultSkipListReader::readSkipData"))

    # --- TC6: BM25 + ORDER BY + LIMIT 10 ---
    print("=== TC6: BM25 latency ===")
    sql_bm25 = (
        f"SELECT id, BM25(content, 'database') AS s FROM {DB}.bench_tantivy "
        f"WHERE content MATCH_ANY 'database' ORDER BY s DESC LIMIT 10"
    )
    sql_match = f"SELECT COUNT(*) FROM {DB}.bench_tantivy WHERE content MATCH_ANY 'database'"
    p50_bm25 = median_latency_ms(conn, sql_bm25)
    p50_match = median_latency_ms(conn, sql_match)
    print(f"  bm25={p50_bm25:.1f}ms, match_any={p50_match:.1f}ms")
    results.append(("bm25_tantivy_ms", f"{p50_bm25:.1f}"))
    results.append(("match_any_tantivy_baseline_ms", f"{p50_match:.1f}"))

    # --- TC7: Memory leak check ---
    print(f"=== TC7: Memory leak check ({args.mem_loops} loops) ===")
    mem_before = get_be_mem_bytes(args)
    leak_sql = f"SELECT COUNT(*) FROM {DB}.bench_tantivy WHERE content MATCH_ANY 'database'"
    for i in range(args.mem_loops):
        try:
            run_sql(conn, leak_sql)
        except Exception as e:
            if (i + 1) % 100 == 0:
                print(f"  WARN at loop {i+1}: {e}", file=sys.stderr)
        if (i + 1) % 200 == 0:
            print(f"  ... {i + 1}/{args.mem_loops}")
    mem_after = get_be_mem_bytes(args)
    mem_delta_mb = (mem_after - mem_before) / (1024 * 1024) if mem_before > 0 and mem_after > 0 else -1
    print(f"  mem_before={mem_before}, mem_after={mem_after}, delta={mem_delta_mb:.1f}MB")
    results.append(("mem_before_bytes", str(mem_before)))
    results.append(("mem_after_bytes", str(mem_after)))
    results.append(("mem_delta_mb", f"{mem_delta_mb:.1f}"))

    # --- TC8: perf hot function analysis ---
    perf_report_text = ""
    if not args.skip_perf and args.remote_host:
        print("=== TC8: perf hot function analysis ===")
        perf_data, be_pid = run_perf_record(args, duration_s=10)
        if perf_data:
            print(f"  perf recording (10s) on BE pid={be_pid}, running queries...")
            for i in range(50):
                run_sql(conn, f"SELECT COUNT(*) FROM {DB}.bench_tantivy WHERE content MATCH_ANY 'database performance'")
            time.sleep(12)
            perf_report_text = get_perf_report(args, perf_data)
            print(perf_report_text)
        else:
            print(f"  SKIP: {be_pid}")
            perf_report_text = f"SKIP: {be_pid}"
    else:
        print("=== TC8: perf SKIPPED (no --remote-host or --skip-perf) ===")
        perf_report_text = "SKIPPED"

    results.append(("perf_report", perf_report_text.replace("\n", "\\n")[:2000]))

    # --- Write CSV ---
    with open(args.output, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["metric", "value"])
        for k, v in results:
            writer.writerow([k, v])
    print(f"\n=== Results written to {args.output} ===")

    # --- Cleanup ---
    print("=== Cleanup ===")
    drop_tables(conn)
    conn.close()


if __name__ == "__main__":
    main()
