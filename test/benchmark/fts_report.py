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
FTS Benchmark Report: reads CSV from fts_bench.py, produces a comparison
table with PASS/FAIL verdicts for each test case.

Usage:
    python3 fts_report.py bench_results.csv [--perf-report /tmp/phase6_perf_top20.txt]

Exit code: 0 if all TCs pass, 1 if any fail.
"""

import argparse
import csv
import re
import sys


THRESHOLDS = {
    "TC1": ("write_tantivy/clucene", 2.0),
    "TC2": ("index_size_tantivy/clucene", 1.5),
    "TC3": ("MATCH_ANY_high_sel", 2.0),
    "TC4": ("MATCH_ANY_low_sel", 2.0),
    "TC5": ("MATCH_PHRASE", 2.0),
    "TC6": ("BM25/MATCH_ANY", 20.0),
    "TC7": ("mem_delta_mb", 50),
    "TC8": ("malloc_lock_pct", 10.0),
}


def load_csv(path):
    metrics = {}
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            metrics[row["metric"]] = row["value"]
    return metrics


def safe_float(v, default=0.0):
    try:
        return float(v)
    except (ValueError, TypeError):
        return default


def check_ratio(name, numerator, denominator, max_ratio):
    if denominator <= 0:
        return "SKIP", f"{name}: baseline is 0 or negative", -1
    ratio = numerator / denominator
    ok = ratio <= max_ratio
    verdict = "PASS" if ok else "FAIL"
    detail = f"{name}: ratio={ratio:.2f} (threshold ≤ {max_ratio})"
    return verdict, detail, ratio


def check_mem(delta_mb, max_mb):
    if delta_mb < 0:
        return "SKIP", "mem_tracker unavailable", delta_mb
    ok = delta_mb < max_mb
    verdict = "PASS" if ok else "FAIL"
    detail = f"mem_delta={delta_mb:.1f}MB (threshold < {max_mb}MB)"
    return verdict, detail, delta_mb


def check_perf(perf_text, max_pct):
    if not perf_text or perf_text == "SKIPPED":
        return "SKIP", "perf not collected", -1
    decoded = perf_text.replace("\\n", "\n")
    total = 0.0
    pattern = re.compile(r"^\s*(\d+\.\d+)%.*(?:malloc|tcmalloc|__lock|pthread_mutex)", re.IGNORECASE)
    for line in decoded.splitlines():
        m = pattern.match(line)
        if m:
            total += float(m.group(1))
    ok = total < max_pct
    verdict = "PASS" if ok else "FAIL"
    detail = f"malloc/lock overhead={total:.1f}% (threshold < {max_pct}%)"
    return verdict, detail, total


def main():
    parser = argparse.ArgumentParser(description="FTS Benchmark Report")
    parser.add_argument("csv_file", help="Path to bench_results.csv")
    parser.add_argument("--perf-report", dest="perf_report", default="",
                        help="Optional path to raw perf report text file")
    args = parser.parse_args()

    m = load_csv(args.csv_file)

    perf_text = m.get("perf_report", "")
    if args.perf_report:
        try:
            with open(args.perf_report) as f:
                perf_text = f.read()
        except FileNotFoundError:
            pass

    verdicts = []

    # TC1: Write latency
    wt = safe_float(m.get("write_tantivy_s"))
    wc = safe_float(m.get("write_clucene_s"))
    v, d, _ = check_ratio("TC1 write latency tantivy/clucene", wt, wc, 2.0)
    verdicts.append((v, d))

    # TC2: Index size
    sz_t = safe_float(m.get("data_bytes_bench_tantivy"))
    sz_c = safe_float(m.get("data_bytes_bench_clucene"))
    if sz_t > 0 and sz_c > 0:
        v, d, _ = check_ratio("TC2 index size tantivy/clucene", sz_t, sz_c, 1.5)
        verdicts.append((v, d))
    else:
        verdicts.append(("MANUAL", "TC2 index size: data_bytes not available, compare SHOW DATA output visually"))

    # TC3: MATCH_ANY high selectivity
    t3t = safe_float(m.get("match_any_high_sel_tantivy_ms"))
    t3c = safe_float(m.get("match_any_high_sel_clucene_ms"))
    v, d, _ = check_ratio("TC3 MATCH_ANY high sel", t3t, t3c, 2.0)
    verdicts.append((v, d))

    # TC4: MATCH_ANY low selectivity
    t4t = safe_float(m.get("match_any_low_sel_tantivy_ms"))
    t4c = safe_float(m.get("match_any_low_sel_clucene_ms"))
    v, d, _ = check_ratio("TC4 MATCH_ANY low sel", t4t, t4c, 2.0)
    verdicts.append((v, d))

    # TC5: MATCH_PHRASE
    t5t = safe_float(m.get("match_phrase_tantivy_ms"))
    t5c = safe_float(m.get("match_phrase_clucene_ms"))
    clucene_note = m.get("match_phrase_clucene_note", "")
    if t5c < 0 or clucene_note:
        note = clucene_note if clucene_note else "CLucene baseline unavailable"
        verdicts.append(("SKIP", f"TC5 MATCH_PHRASE: tantivy={t5t:.1f}ms, {note}"))
    else:
        v, d, _ = check_ratio("TC5 MATCH_PHRASE", t5t, t5c, 1.2)
        verdicts.append((v, d))

    # TC6: BM25 vs MATCH_ANY (batch-local BM25 builds temp index per batch)
    bm25 = safe_float(m.get("bm25_tantivy_ms"))
    match_base = safe_float(m.get("match_any_tantivy_baseline_ms"))
    v, d, _ = check_ratio("TC6 BM25/MATCH_ANY", bm25, match_base, 20.0)
    verdicts.append((v, d))

    # TC7: Memory leak
    mem_delta = safe_float(m.get("mem_delta_mb", "-1"))
    v, d, _ = check_mem(mem_delta, 50)
    verdicts.append((v, d))

    # TC8: perf
    v, d, _ = check_perf(perf_text, 10.0)
    verdicts.append((v, d))

    # --- Print report ---
    print("=" * 72)
    print("  FTS Benchmark Report — tantivy vs CLucene")
    print("=" * 72)
    print()
    print(f"{'TC':<6} {'Verdict':<8} {'Detail'}")
    print("-" * 72)
    pass_count = 0
    fail_count = 0
    skip_count = 0
    for i, (verdict, detail) in enumerate(verdicts, 1):
        label = f"TC{i}"
        print(f"{label:<6} {verdict:<8} {detail}")
        if verdict == "PASS":
            pass_count += 1
        elif verdict == "FAIL":
            fail_count += 1
        else:
            skip_count += 1

    print("-" * 72)
    print(f"Total: {pass_count} PASS, {fail_count} FAIL, {skip_count} SKIP/MANUAL")
    print()

    # --- Raw metrics ---
    print("Raw metrics:")
    for k in sorted(m.keys()):
        if k != "perf_report":
            print(f"  {k}: {m[k]}")
    print()

    if fail_count > 0:
        print("PHASE 6 REJECTED")
        sys.exit(1)
    else:
        print("PHASE 6 ACCEPTED (review MANUAL/SKIP items)")
        sys.exit(0)


if __name__ == "__main__":
    main()
