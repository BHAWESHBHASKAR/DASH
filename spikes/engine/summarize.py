#!/usr/bin/env python3
"""Print compact tables from results/*.jsonl. Usage: summarize.py B|D|C|E|deletes"""
import json
import os
import sys

R = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")


def rows(name):
    with open(os.path.join(R, name)) as f:
        return [json.loads(l) for l in f if l.startswith("{")]


what = sys.argv[1]
if what == "B":
    for d in rows("B_filtered.jsonl"):
        print(d["mode"][:5], d["selectivity"], int(d["avg_allowed"]), d["method"], d.get("ef", d.get("oversample", "")),
              d["recall_at_10"], d["lat"]["p50_us"], d["lat"]["p99_us"])
elif what == "D":
    for d in rows("D_concurrency.jsonl"):
        if "config" in d:
            print(d["config"], d["query_threads"], d["qps"], d["lat"]["p50_us"], d["lat"]["p95_us"], d["lat"]["p99_us"],
                  d.get("insert_rate_achieved", ""), d.get("insert_lat", {}).get("p99_us", ""))
        else:
            print({k: v for k, v in d.items() if k.startswith("recall") or k == "peak_rss_mb"})
elif what == "C":
    for d in rows("C_text.jsonl"):
        ph = d["phase"]
        if ph.startswith("query_s1"):
            print(ph, d["mix"], d["terms"], "ram", [d["tantivy_ram"][k] for k in ("p50_us", "p95_us", "p99_us")],
                  "mmap", [d["tantivy_mmap"][k] for k in ("p50_us", "p95_us", "p99_us")],
                  "repo", [d["repo"][k] for k in ("p50_us", "p95_us", "p99_us")], "cand", d["repo_avg_candidates"],
                  "ovl", round(d["top10_overlap_tantivy_vs_repo"], 3))
        elif ph.startswith("query_s2"):
            print(ph, d["mix"], d["terms"], "filterterm", [d["tantivy_filter_term_one_index"][k] for k in ("p50_us", "p95_us", "p99_us")],
                  "pertenant", [d["tantivy_index_per_tenant"][k] for k in ("p50_us", "p95_us", "p99_us")],
                  "repo", [d["repo_per_tenant"][k] for k in ("p50_us", "p95_us", "p99_us")],
                  "ovl ft/pt", round(d["top10_overlap_filterterm_vs_pertenant"], 3), "pt/repo", round(d["top10_overlap_pertenant_vs_repo"], 3))
        else:
            print({k: v for k, v in d.items() if k not in ("vocab", "zipf_s", "tenants")})
elif what == "E":
    for d in rows("E_wal.jsonl"):
        lt = d.get("commit_ack_latency") or d.get("ack_latency")
        print(d["kind"], d.get("mode", d.get("producers")), d["record_bytes"], d.get("batch", ""), d["records_per_s"],
              lt["p50_us"], lt["p99_us"], d.get("avg_batch", ""), d.get("fsyncs_per_s", ""))
elif what == "deletes":
    for d in rows("deletes.jsonl"):
        print(d["step"], d["size"], round(d["memory_usage_mb"], 1), json.dumps(d["detail"]))
