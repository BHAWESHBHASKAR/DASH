# ADR 0003: Storage engine indexes (vector, text, filtering, durability)

Status: Proposed. Date: 2026-10-09. Phase: P2 engine spike. Supports ADR-03 and ADR-04 of the
[master plan](../plans/2026-10-09-production-readiness-master-plan.md).

All numbers below come from `spikes/engine/results/*.jsonl` (raw JSON lines, one object per
measurement). Nothing here is extrapolated unless it is labelled "extrapolated" with its formula.

## 1. Context

The master plan (ADR-04) proposes an LSM-style engine: per-tenant memtable, immutable segments
with a `usearch` HNSW (quantized, filtered search by predicate) and a `tantivy` text index,
arc-swap snapshots, WAL v2 with group commit. Before building it, the spike measures the
candidate libraries against the code that exists today (`pkg/store`: `ann.rs` multi-level
HNSW-style graph and the `HashMap<token, HashSet<claim_id>>` inverted index with BM25 scoring in
`pkg/ranking`).

Code review finding (confirmed by measurement, section 3.1): the in-repo ANN insert
(`add_vector_index_entry` -> `select_ann_neighbors`) scans every vector in the store per level
(O(N) per level, O(N^2) build) and picks the nearest M neighbours with no HNSW diversity
heuristic. `usearch` is declared in the workspace but not used by any source file (claims ledger C31).

### 1.1 Environment and method

- Hardware: 4 vCPU Intel Xeon @ 2.10 GHz (AVX2, AVX-512 incl. fp16, AMX flags present; usearch picked the
  "skylake" kernel), 16 GB RAM (`MemTotal 16480952 kB`), Linux 6.18 (Firecracker-style microVM), ext4 on
  a virtio disk. The box is shared with other work, so latencies carry noise (see caveats). Full record:
  `spikes/engine/results/env.txt`.
- Toolchain: rustc 1.97, g++ 13.3, usearch 2.26.4, tantivy 0.26.2. Built with
  `RUSTFLAGS="-C target-cpu=native"`, release, `debug=0`, `strip`.
- Vector data: synthetic clustered Gaussian mixture, 384-d, L2-normalised (cosine == dot).
  `x = normalize(c_k + s1*B*z + s2*g)`, 256 uniform clusters (c_k random unit vectors), z ~ N(0, I_32)
  through a random 384x32 basis (|s1*B*z| ~ 0.6), g ~ N(0, I_384) (|s2*g| ~ 0.15). Seed 42; point i is a
  pure function of (seed, chunk of 1024), so results do not depend on thread count. 1,000 held-out
  queries come from a disjoint RNG stream. Ground truth = exact brute force (tiled SIMD-friendly f32
  dot loop), top-10, computed once per N. Metric for usearch: cosine; M=16, ef_construction=128.
- Latency: per query, single thread, 1,000 queries, hdrhistogram (microseconds). QPS at 4 threads: rayon pool,
  4,000 query executions. Peak RSS = `VmHWM`; "RSS above data" subtracts the RSS measured after generating
  the f32 dataset (the benchmark data itself is not counted). Each (kind, N, threads) is a separate process.
- Reproduce: `spikes/engine/run.sh build && spikes/engine/run.sh env && spikes/engine/run.sh A` (then `B`,
  `C`, `D`, `deletes`, `E`; `spikes/engine/summarize.py B|C|D|E|deletes` prints compact tables). Commands used
  for the committed result files are listed in section 9.

## 2. Targets (master plan section 1.3) vs what this box can show

The plan's targets are for 16 vCPU / 64 GB / NVMe, 10M claims, 768-d. This spike used 4 vCPU, up to
500k vectors, 384-d. Assessment:

| Target | Verdict on this box |
|---|---|
| Vector recall@10 >= 0.95 vs exact | Achievable: usearch f32/f16 reach it at ef=64..128 up to 500k; i8 + exact f32 rerank of 50 reaches 0.983 at ef=128 and 0.998 at ef=256 at 500k (tables 3.2). The in-repo ANN does not (section 3.1). |
| Hybrid query p50 < 15 ms, p99 < 50 ms at 500 QPS, top_k=10, tenant+time filter | Components are far inside the budget at 200k-500k: vector (i8+rerank, ef=128) p50 0.48 ms / p99 1.0 ms at 500k; filtered search 0.3-0.6 ms p50; tantivy BM25 p50 0.06-0.3 ms; memtable merge adds ~0.3-0.4 ms. A full hybrid pipeline (fusion, evidence scoring) was not built, so the end-to-end number is **not judged**. 500 QPS needs roughly one core of vector search (single-thread QPS 1,700-3,000). 10M x 768-d **cannot be judged** here (no extrapolation made). |
| Sustained ingest >= 5,000 claims/s with group commit; durable-ack p99 < 20 ms | Achievable for the log: pipelined group commit reached 37,800-55,000 records/s with ack p99 4.0-4.6 ms (64 producers), and 3,400 records/s with a single synchronous producer (p99 1.4 ms). Index insert into a memtable ran at the paced 2,000/s with insert p99 0.25-1 ms. Caveat: fsync here is ~150-200 us, which is faster than typical NVMe fsync under a real power-fail guarantee (virtualised disk); re-measure on target hardware. 5,000/s of index inserts into usearch was not measured (only 2,000/s). |
| Cold start < 30 s (mmap segments + WAL tail) | `view` (mmap) of a 500k x 384 f32 index takes 45 ms (i8: 51 ms); with page cache dropped 73-109 ms plus slower first queries (table 3.3). 10M cannot be judged; WAL-tail replay not measured. |
| Cluster failover / replication lag | Not in scope; **cannot be judged**. |

## 3. Measurements

### 3.1 A. Vector index

Exact scan baseline (f32 flat scan, one thread per query; QPS(4t) = four concurrent single-thread scans):

| N | p50 | p95 | p99 | QPS 1t | QPS 4t |
|---|---|---|---|---|---|
| 50k | 4.4 ms | 11.0 ms | 15.4 ms | 167 | 634 |
| 200k | 31.9 ms | 40.3 ms | 45.5 ms | 30 | 118 |
| 500k | 80.2 ms | 110 ms | 110 ms* | 12 | 44 |

(*p99 110.0 ms from `results/A_vector.jsonl`; 200 queries timed per N.)

**In-repo ANN** (through `InMemoryStore::ingest_bundle` + `upsert_claim_vector`, queried with
`ann_vector_top_candidates`; defaults M=12/6, 4 levels; ef set via the tuning config). Build time is
cumulative, single-threaded, excluding evaluation time:

| N | 1k | 2k | 5k | 10k | 12.5k | 15k | 17.5k | 20k |
|---|---|---|---|---|---|---|---|---|
| cumulative build | 1 s | 2 s | 12 s | 60 s | 94 s | 142 s | 222 s | 321 s |

The run stopped at the 300 s rule: 17.5k vectors finished within 5 minutes (222 s); 20k took 321 s. Growth is
super-linear (5k -> 10k -> 20k: 12 -> 60 -> 321 s, roughly N^2.2). Extrapolated with t ~ 8.0e-7 * N^2 s (fit
at N=20k: 321 s / 4e8): ~2,000 s at 50k, ~32,000 s (about 9 h) at 200k, ~2e5 s at 500k. These three numbers are
extrapolated and only indicate order of magnitude. Peak RSS above data at 20k: 77 MB.

Recall@10 of the in-repo ANN (1,000 queries, same dataset):

| N | ef=32 | ef=64 | ef=128 | ef=256 | p50 at ef=128 |
|---|---|---|---|---|---|
| 2k (clustered) | 0.786 | 0.918 | 0.977 | 0.995 | 0.77 ms |
| 5k (clustered) | 0.151 | 0.151 | 0.151 | 0.151 | 0.10 ms |
| 10k (clustered) | 0.158 | 0.158 | 0.158 | 0.158 | 0.24 ms |
| 20k (clustered) | 0.151 | 0.151 | 0.151 | 0.151 | 0.49 ms |
| 5k (control: 1 cluster) | 0.676 | 0.845 | 0.940 | 0.981 | 1.07 ms |
| 10k (control: 1 cluster) | 0.597 | 0.784 | 0.902 | 0.962 | 1.30 ms |

On clustered data the in-repo graph is effectively disconnected beyond ~2k vectors: recall does not change
with ef because the greedy search never leaves the entry point's cluster (neighbour lists are "nearest M",
so no cross-cluster edges survive pruning). On the unclustered control it works but needs ef=256 for 0.96 at 10k
(M=12 plus a 4-level hash-assigned hierarchy). Conclusion: the in-repo structure is not a viable base for the target scale.

**usearch** (M=16, ef_construction=128, ef_search sweep; 1t/4t = build threads). "i8+rr50" = i8 index search for
50 candidates then exact f32 rerank to the top 10 (f32 vectors come from RAM here; in the engine they come from the row store).
Build times for 1 thread and 4 threads (single runs, shared box):

| N | f32 1t | f32 4t | f16 1t | f16 4t | i8 1t | i8 4t |
|---|---|---|---|---|---|---|
| 50k | 32.0 s | 17.7 s | 17.5 s | 5.6 s | 11.7 s | 4.1 s |
| 200k | 144.6 s | 79.8 s | 79.4 s | 22.2 s | 56.3 s | 16.4 s |
| 500k | 476.5 s | 124.5 s | 313.1 s | 82.0 s | 197.7 s | 57.5 s |

Peak RSS above the dataset, index file size and index memory (`memory_usage`):

| N | f32 RSS / file / mem | f16 RSS / file / mem | i8 RSS / file / mem |
|---|---|---|---|
| 50k | 86 / 80 / 144 MB | 49 / 44 / 80 MB | 31 / 25 / 48 MB |
| 200k | 342 / 321 / 562 MB | 196 / 175 / 306 MB | 123 / 102 / 178 MB |
| 500k | 851 / 803 / 1140 MB | 484 / 437 / 628 MB | 301 / 254 / 372 MB |

(i8 does not include the f32 rows needed for rerank: 0.77 GB at 500k.)

Recall@10 and single-thread latency by ef (4-thread-built index; QPS 4t in the last column set):

| kind | N | ef | recall@10 | p50 | p95 | p99 | QPS 1t | QPS 4t |
|---|---|---|---|---|---|---|---|---|
| f32 | 50k | 32 | 0.9963 | 141 us | 196 us | 221 us | 6,882 | 11,942 |
| f32 | 200k | 32 | 0.9609 | 322 us | 590 us | 796 us | 2,738 | 8,207 |
| f32 | 200k | 64 | 0.9982 | 578 us | 1,012 us | 1,580 us | 1,611 | 4,696 |
| f32 | 500k | 64 | 0.9478 | 733 us | 1,275 us | 1,762 us | 1,209 | 4,998 |
| f32 | 500k | 128 | 0.9847 | 1,040 us | 1,745 us | 2,209 us | 879 | 2,850 |
| f32 | 500k | 256 | 0.9987 | 1,492 us | 2,441 us | 3,707 us | 607 | 2,340 |
| f16 | 50k | 32 | 0.9967 | 106 us | 171 us | 436 us | 7,903 | 31,868 |
| f16 | 200k | 64 | 0.9979 | 310 us | 645 us | 2,601 us | 2,419 | 9,571 |
| f16 | 500k | 128 | 0.9814 | 695 us | 1,499 us | 3,951 us | 1,144 | 4,864 |
| f16 | 500k | 256 | 0.9976 | 973 us | 1,837 us | 3,233 us | 877 | 3,581 |
| i8 (no rerank) | 50k | 128 | 0.9414 | 198 us | 395 us | 2,115 us | 3,973 | 16,589 |
| i8 (no rerank) | 200k | 128 | 0.9299 | 265 us | 461 us | 599 us | 3,341 | 12,245 |
| i8 (no rerank) | 500k | 512 | 0.9184 | 977 us | 1,899 us | 3,065 us | 853 | 3,041 |
| i8+rr50 | 50k | 50 | 0.9994 | 108 us | 181 us | 235 us | 8,380 | 26,454 |
| i8+rr50 | 200k | 50 | 0.9894 | 208 us | 408 us | 868 us | 3,990 | 15,351 |
| i8+rr50 | 200k | 128 | 0.9998 | 305 us | 495 us | 625 us | 3,015 | 9,184 |
| i8+rr50 | 500k | 64 | 0.9321 | 330 us | 588 us | 733 us | 2,692 | 7,913 |
| i8+rr50 | 500k | 128 | 0.9829 | 484 us | 850 us | 1,021 us | 1,834 | 5,851 |
| i8+rr50 | 500k | 256 | 0.9978 | 706 us | 1,361 us | 2,185 us | 1,192 | 3,991 |

Smallest tested ef reaching recall@10 >= 0.95 (tested ef = 32, 64, 128, 256, 512; i8+rr50 rows use ef = max(ef, 50)):

| N | f32 | f16 | i8 (no rerank) | i8 + rr50 |
|---|---|---|---|---|
| 50k | 32 | 32 | never (plateau 0.941) | 50 |
| 200k | 64 | 64 | never (plateau 0.930) | 50 |
| 500k | 128 | 128 | never (plateau 0.918) | 128 |

Plain i8 never reaches 0.95 on this data (quantisation error caps recall at 0.92-0.94); exact rerank removes the cap.
Build thread scaling 1 -> 4 threads: f32 3.8x at 500k, 1.8x at 200k, 1.8x at 50k; i8 3.4x at 500k (the 200k f32 figure is probably
disturbed by other load on the shared box). Recall of a 1-thread build at ef=128 matches the 4-thread build within 0.003
(`results/A_vector.jsonl`, `eval=short` rows).

### 3.2 usearch load vs view, and cold cache

| N / kind | load (copy to RAM) | view (mmap) | view, caches dropped | first-200-query p50 after cold view | warm p50 on viewed index (ef=128) |
|---|---|---|---|---|---|
| 200k f32 | 0.36 s | 21 ms | 36 ms | 1,569 us | 641 us |
| 500k f32 | 0.75 s | 45 ms | 109 ms | 2,713 us | 1,109 us |
| 500k f16 | 0.44 s | 39 ms | 68 ms | 1,560 us | 616 us |
| 500k i8 | 0.29 s | 51 ms | 74 ms | 739 us | 502 us |

(`drop_caches` was writable on this box; the cold numbers are one run each.)

### 3.3 B. Filtered search (N = 200k, i8 index + rerank 50, 300 held-out queries, single thread)

Two filter models, defined in `spikes/engine/src/filtered.rs`: uncorrelated (random subset; a time-range-like predicate)
and correlated (allowed rows are the query's own cluster first, then neighbouring clusters; an entity-like predicate;
the query's cluster is always allowed, which flatters post-filtering). Methods: `HNSW filtered` = `filtered_search` with a
bitmap predicate; `post-filter xO` = unfiltered search for 10*O results, then filter; `exact` = exact f32 scan of the allowed id list
(id list materialisation excluded; 100 timed queries). Recall is against exact filtered top-10. Latencies are p50 (p99 in the raw file).

Uncorrelated filter:

| selectivity (allowed) | exact scan | HNSW filtered ef=64 | HNSW filtered ef=256 | post-filter x10 | post-filter x100 |
|---|---|---|---|---|---|
| 100% (200,000) | 31.6 ms, R 1.0 | 0.30 ms, R 0.996 | 0.49 ms, R 1.0 | 0.33 ms, R 1.0 | 1.9 ms, R 1.0 |
| 10% (19,884) | 2.0 ms, R 1.0 | 0.58 ms, R 1.0 | 5.1 ms, R 1.0 | 0.32 ms, R 0.886 | 1.6 ms, R 1.0 |
| 3% (5,995) | 0.62 ms, R 1.0 | 3.9 ms, R 1.0 | 16.2 ms, R 1.0 | 0.28 ms, R 0.296 | 1.5 ms, R 1.0 |
| 1% (2,065) | 0.25 ms, R 1.0 | 13.3 ms, R 0.993 | 41.7 ms, R 1.0 | 0.38 ms, R 0.10 | 1.6 ms, R 0.844 |
| 0.3% (613) | 0.03 ms, R 1.0 | 36.6 ms, R 0.996 | 93.6 ms, R 1.0 | 0.47 ms, R 0.03 | 1.8 ms, R 0.310 |
| 0.1% (194) | 0.01 ms, R 1.0 | 80.3 ms, R 1.0 | 148.7 ms, R 1.0 | 0.45 ms, R 0.010 | 1.8 ms, R 0.096 |

Post-filter x1000 (10,000 fetched): ~31-34 ms p50 at every selectivity; recall 1.0 down to 3%, 0.996 at 1%, 0.982 at 0.3%, 0.822 at 0.1%.

Correlated filter (HNSW filtered ef=64 stays at 0.45-0.61 ms p50 with recall >= 0.9993 at every selectivity; ef=256 spiked to
155 ms p50 at 0.1%, a pathological case, see raw file):

| selectivity (allowed) | exact scan | HNSW filtered ef=64 | post-filter x10 |
|---|---|---|---|
| 100% | 30.6 ms | 0.49 ms, R 0.996 | 0.54 ms, R 1.0 |
| 10% (20,046) | 4.5 ms | 0.48 ms, R 0.9993 | 0.60 ms, R 1.0 |
| 1% (2,012) | 0.53 ms | 0.45 ms, R 0.9993 | 0.54 ms, R 1.0 |
| 0.1% (198) | 0.068 ms | 0.61 ms, R 1.0 | 0.55 ms, R 0.9997 |

Reading: filtered HNSW cost explodes when the allowed set is a small random slice of the graph (uncorrelated), because the graph is
traversed through non-matching nodes; exact scan is linear in the allowed count (~0.16 ms per 1,000 candidates here). Post-filter with modest oversampling collapses below ~10% selectivity.

Recommended selection rule (measured crossover, uncorrelated worst case): let A = |allowed set| from the roaring pre-filter.
A <= ~8,000: exact f32 scan of the allowed ids (<= ~1.2 ms; exact, so recall 1.0). A > ~8,000: `filtered_search` with ef=64 and rerank 50
(0.3-0.6 ms at 10%-100%). Never use plain post-filtering unless selectivity >= 30% (then x10 oversampling is fine and cheaper). The
crossover moves with N and hardware: on this box exact scan costs ~0.16 ms per 1,000 allowed ids and HNSW filtered ef=64 crosses it at A ~ 6k-20k
(2.0 ms vs 0.58 ms at 19.9k; 0.62 ms vs 3.9 ms at 6k). The engine must make the threshold a tuned setting, and the planner should use the
allowed count from the bitmap, not the nominal selectivity.

### 3.4 C. Text index (100,000 docs; the brief proposed 200k, reduced to fit the time budget)

Corpus: Zipf (s=1.0) over a 50k vocabulary, 30-60 tokens per doc (4,501,475 tokens), seed 42, 20 tenants (doc mod 20).
Queries: OR queries of 1/3/6 terms drawn from a random document ("natural", Zipf-weighted so head terms dominate) or restricted to term
rank >= 100 ("midtail", i.e. head terms treated as stop words). tantivy: 1,000 queries per cell (single thread, after warm-up); in-repo: the first
100 of the same queries (it is slow). In-repo search = `InMemoryStore::retrieve` (union of posting lists, then BM25 + lexical overlap +
confidence scoring of every candidate; its score is a composite, not pure BM25). tantivy = BM25 (k1=1.2, b=0.75 equivalents) over `WithFreqs`
postings, no stored fields, doc id in a fast field.

Build (100k docs): tantivy RAM 1 thread 1.57 s, 4 threads 0.48 s (+0.21 s force-merge to 1 segment), mmap directory 4 threads 0.50 s (+0.23 s merge);
index size 7 MB (RAM `space_usage` and on-disk directory both 7 MB). In-repo store ingest of the same docs: 5.97 s single-tenant (5.43 s with 20 tenants);
the in-repo index also holds a token vector per claim, process peak RSS 772 MB (not isolated, includes earlier tantivy builds).

Single-tenant query latency (p50 / p95 / p99):

| mix, terms | tantivy RAM | tantivy mmap | in-repo | in-repo avg candidates | top-10 overlap tantivy vs in-repo |
|---|---|---|---|---|---|
| natural, 1 | 74 / 584 / 624 us | 85 / 628 / 1421 us | 39 / 1207 / 1493 ms | 12,108 | 0.927 |
| natural, 3 | 162 / 752 / 1552 us | 153 / 724 / 1621 us | 582 / 1317 / 1378 ms | 46,727 | 0.932 |
| natural, 6 | 303 / 946 / 2137 us | 294 / 824 / 1756 us | 1331 / 1487 / 1589 ms | 78,653 | 0.928 |
| midtail, 1 | 61 / 82 / 102 us | 61 / 91 / 154 us | 17.5 / 47.7 / 57.9 ms | 642 | 0.905 |
| midtail, 3 | 83 / 145 / 193 us | 83 / 149 / 211 us | 36.4 / 75.7 / 86.1 ms | 1,857 | 0.963 |
| midtail, 6 | 147 / 292 / 465 us | 157 / 353 / 513 us | 66.4 / 137.7 / 185.5 ms | 3,595 | 0.962 |

(Natural 1-term in-repo p50 is 39 ms but p95 1.2 s: it depends on whether the term is a head term. The tantivy p95/p99 spikes of 0.6-2 ms
at 100k are likely shared-box noise: p50 is 60-300 us.) RAM vs mmap top-10 overlap is ~1.0 (the same index). Overlap of 0.90-0.96 shows
the in-repo composite score ranks differently from pure BM25; it is a ranking change, not an error measure, and must be handled in the
fusion/evidence layer (ADR-08), not assumed equal.

Tenant filter (20 tenants x 5,000 docs): (a) one tantivy index with a `tenant` term (ConstScore Must clause), (b) one tantivy index per tenant,
(c) in-repo (native per-tenant maps). p50 / p95 / p99:

| mix, terms | (a) filter term | (b) per-tenant index | (c) in-repo | overlap (a) vs (b) | overlap (b) vs (c) |
|---|---|---|---|---|---|
| natural, 1 | 101 / 239 / 270 us | 14 / 41 / 59 us | 2.2 / 56.4 / 61.0 ms | 0.98 | 0.988 |
| natural, 3 | 510 / 1053 / 1813 us | 42 / 119 / 345 us | 35.1 / 76.9 / 94.9 ms | 0.972 | 0.965 |
| natural, 6 | 785 / 1208 / 1499 us | 71 / 150 / 253 us | 63.8 / 82.0 / 91.3 ms | 0.943 | 0.95 |
| midtail, 1 | 57 / 125 / 142 us | 11 / 17 / 23 us | 1.0 / 2.4 / 3.1 ms | 0.985 | 0.996 |
| midtail, 3 | 129 / 210 / 263 us | 20 / 31 / 46 us | 2.3 / 4.8 / 7.8 ms | 0.945 | 0.988 |
| midtail, 6 | 190 / 277 / 567 us | 33 / 65 / 114 us | 3.6 / 7.4 / 8.7 ms | 0.904 | 0.984 |

Per-tenant index is 5-10x faster than a filter term and matches ADR-06 (tenant = physical partition). Building 20 per-tenant indexes
took 1.45 s total (1 thread each) vs 0.48 s for one 4-thread index. Per-tenant BM25 statistics (idf, avgdl) also differ from global ones, which
accounts for part of the (a)/(b) overlap gap (0.90-0.985).

### 3.5 D. Concurrency model (N_big = 200k, i8 + rr50, ef=128; memtable usearch f32 or flat; each cell is an 8 s run)

Recall of the merged result against exact over big + 5,000-vector memtable: big only 0.9758 (misses memtable hits), big + memtable merged 0.9998, single
index containing everything 0.9998.

Throughput and latency (all-query-threads, one shared immutable big segment via `ArcSwap<Snapshot>`):

| config | query threads | QPS | p50 | p95 | p99 | insert achieved / insert p99 |
|---|---|---|---|---|---|---|
| big segment only | 1 | 2,669 | 316 us | 577 us | 1,315 us | |
| big segment only | 2 | 4,923 | 407 us | 631 us | 850 us | |
| big segment only | 4 | 10,858 | 355 us | 573 us | 786 us | |
| big + static 5k HNSW memtable | 1 / 2 / 4 | 1,208 / 2,189 / 4,189 | 742 / 823 / 986 us | 1,120 / 1,268 / 1,256 us | 2,753 / 1,658 / 1,664 us | |
| big + static 5k flat (exact) memtable | 1 / 2 / 4 | 1,392 / 2,591 / 5,190 | 664 / 679 / 705 us | 928 / 1,097 / 1,083 us | 1,582 / 1,554 / 1,401 us | |
| single index with all 205k vectors | 1 / 2 / 4 | 3,000 / 5,724 / 10,193 | 298 / 298 / 374 us | 496 / 554 / 611 us | 709 / 770 / 845 us | |
| big + HNSW memtable (rotating at 4,000) + inserter | 1 | 1,467 | 622 us | 1,051 us | 1,729 us | 1,994/s, 904 us |
| same | 2 | 3,242 | 572 us | 943 us | 1,240 us | 1,992/s, 895 us |
| same | 3 | 5,339 | 514 us | 873 us | 1,109 us | 1,994/s, 997 us |
| same | 4 | 4,845 | 664 us | 1,266 us | 5,015 us | 1,933/s, 4,555 us |
| big + flat memtable (rotating at 4,000) + inserter | 1 | 1,887 | 505 us | 797 us | 1,281 us | 1,999/s, 254 us |
| same | 2 | 3,242 | 562 us | 979 us | 1,826 us | 2,000/s, 540 us |
| same | 3 | 4,816 | 568 us | 978 us | 1,877 us | 2,000/s, 663 us |
| same | 4 | 5,916 | 642 us | 1,010 us | 1,337 us | 1,999/s, 493 us |

Findings: (1) query QPS scales 2,669 -> 10,858 from 1 to 4 threads on the shared immutable segment (4.07x) with no lock; the arc-swap
snapshot load is not measurable next to the search cost. (2) The merge cost is real: adding a 5k-vector memtable roughly doubles per-query latency
(p50 316 -> 742 us with an HNSW memtable, 664 us with exact scan) because the memtable search costs about as much as the big-segment search;
a single index holding the same vectors costs 298 us. A small exact (flat) memtable is cheaper than a small HNSW and cheaper to insert (insert p99
0.25-0.66 ms vs 0.9-4.6 ms). The memtable should therefore stay small (the 5,000-vector test is the upper end for flat scan), be flat up to a threshold,
and be flushed to a segment promptly. (3) With a 2,000 vectors/s inserter the target rate was held (1,933-2,000/s). The inserter and 4 query
threads oversubscribe 4 cores: with the HNSW memtable the 4-thread run had p99 5.0 ms and insert p99 4.6 ms; the flat memtable did not show this
at 4 threads (p99 1.3 ms), but one run each on a shared box is not conclusive. Reserve a core for ingest/flush/compaction.

### 3.6 usearch delete/update semantics (N = 50k f32, ef=128; `results/deletes.jsonl`)

- `remove(key)` takes ~0.2 us per key (9,986 keys, 2 ms total): it marks the slot free; `size()` drops immediately, `contains()` is false, no deleted key was
  returned in 1,000 queries (0 leaks).
- Recall@10 after deleting 20% at random, against exact over the remaining set: 0.9994 (baseline 0.9993), unchanged at ef=256.
- Memory and the saved file do not shrink after deletions (`memory_usage` 144.5 MB before and after; file 80.3 MB); `compact()` took 0.93 s and did not reduce
  `memory_usage` or recall in this test (0.9994). Reclaiming space therefore needs a segment rewrite (compaction in the engine), not index-level compaction.
- `add` of a live key fails ("Duplicate keys not allowed"); update = `remove` + `add`: 200 of 200 updated vectors were found at top-2 for their new vector.
  Re-adding 9,986 removed keys succeeded (0 errors) and reused slots (`size` 50,000, memory unchanged), 5.8 s (~580 us per add, single thread).
- Churn: 5 rounds of remove 20% + re-add: recall 0.9996, 0.9997, 0.9987, 0.9998, 0.9988 (no systematic drift in 5 rounds). Capacity must be
  reserved ahead of inserts (the test reserved +25%).
- Only f32 was run (the i8 variant of this experiment was not run for lack of time budget); the i8/f16 behaviour is assumed equal but **not measured**.

### 3.7 E. Durability (ext4 on virtio disk, `std::fs` write + `sync_data`, 3 s per cell; `results/E_wal.jsonl`)

Each row is one commit per batch; ack latency of every record in a batch equals the commit latency. "prealloc" overwrites a pre-zeroed 128 MB file (no size
change, so no metadata flush).

| record | batch | append+sync_data records/s | commit p50 / p99 | prealloc+sync_data records/s | commit p50 / p99 | no fsync records/s |
|---|---|---|---|---|---|---|
| 256 B | 1 | 5,085 | 158 / 719 us | 4,941 | 164 / 823 us | 1,929,958 |
| 256 B | 16 | 61,497 | 204 / 1,070 us | 82,540 | 164 / 694 us | 11,032,149 |
| 256 B | 128 | 338,130 | 305 / 1,765 us | 551,204 | 191 / 824 us | 19,736,947 |
| 4 KiB | 1 | 2,792 | 288 / 1,560 us | 3,738 | 184 / 1,338 us | 590,822 |
| 4 KiB | 16 | 24,804 | 438 / 3,883 us | 55,418 | 235 / 983 us | 1,391,945 |
| 4 KiB | 128 | 89,256 | 1,224 / 5,099 us | 141,494 | 813 / 2,667 us | 1,553,404 |

`sync_all` per record: 4,851 rec/s (256 B), 3,815 rec/s (4 KiB), p99 0.97 / 1.2 ms (same as `sync_data` within noise).

Pipelined group commit (P producer threads, each waiting for its ack; one committer coalesces whatever is queued, max 512, one `sync_data` per batch, append mode):

| producers | 256 B records/s | avg batch | fsync/s | ack p50 / p99 | 4 KiB records/s | avg batch | ack p50 / p99 |
|---|---|---|---|---|---|---|---|
| 1 | 3,420 | 1.0 | 3,420 | 220 / 1,434 us | 3,569 | 1.0 | 235 / 906 us |
| 4 | 7,833 | 2.2 | 3,526 | 415 / 2,719 us | 6,906 | 2.3 | 477 / 2,969 us |
| 16 | 21,329 | 12.3 | 1,739 | 581 / 3,419 us | 24,667 | 12.9 | 547 / 2,291 us |
| 64 | 55,004 | 60.7 | 906 | 1,002 / 4,009 us | 37,819 | 60.4 | 1,520 / 4,583 us |

Caveat: an fsync here costs 0.15-0.3 ms, which is far lower than a durable NVMe flush commonly is (0.5-several ms); this virtual disk may acknowledge from
the host cache. The shape (throughput scales with batch size; one fsync per batch) transfers, the absolute values do not.

## 4. What the numbers say

- The in-repo ANN is O(N^2) to build (17.5k vectors in 222 s; roughly 9 h for 200k, extrapolated), and on clustered data its recall collapses to 0.15 beyond a few thousand vectors. It cannot meet recall@10 >= 0.95 at any scale in the plan.
- usearch meets recall@10 >= 0.95 on 50k-500k with f32/f16 at ef=32..128 and with i8 + exact rerank, at p50 0.1-1.0 ms and 1,200-8,400 QPS per thread. Builds: 500k in 58-125 s with 4 threads.
- Memory: f32 index costs 1.7 KB/vector (RSS 851 MB per 500k), f16 0.97 KB, i8 0.60 KB (+1.5 KB if the f32 rows must stay in RAM for rerank; they can live in the mmap'd row store).
- tantivy delivers BM25 top-10 in 0.06-0.3 ms p50 at 100k docs (natural and midtail 1-6 terms) against 17 ms - 1.3 s for the in-repo path on the same queries.
- Concurrency design works: lock-free snapshot reads scale 4x on 4 threads; the memtable + big segment merge costs ~2x latency at 5k memtable vectors, so keep memtables small and flat.
- Group commit lifts durable-ack throughput from ~3.5-5k records/s (1 fsync per record) to 37-55k records/s at 64 concurrent writers with p99 ack ~4-4.6 ms.

## 5. Decision

1. **Vector index: adopt `usearch` HNSW** (Apache-2.0, C++ via the `usearch` crate) as the segment vector index. Do not repair the in-repo ANN: the missing neighbour-diversity heuristic and the O(N) insert are the core of its design; rebuilding it equals reimplementing HNSW. Retain a pure-Rust fallback only as an exact flat scan (also needed for the memtable and small allowed sets).
2. **Quantization default: i8 index + exact f32 rerank (rerank width 50, ef 64-128)** for segments, f32 rows kept in the row store (mmap). It matched f32 recall (0.983 at ef=128, 0.998 at ef=256 at 500k) at about one third of the f32 index memory (372 vs 1,140 MB `memory_usage` at 500k) and about 0.45-0.5 ms p50 at 500k. Plain i8 (recall 0.92-0.94) must not be the default. f16 is the fallback when rerank rows cannot be read cheaply (recall equals f32, 0.57x memory). Binary quantization was not tested.
3. **Filtered search rule**: per segment, compute the allowed set size A from roaring bitmaps (tenant is a physical partition, so only time/entity/metadata filters remain). If A <= ~8,000 (tunable): exact scan of the allowed rows. Otherwise: `filtered_search` with predicate, ef=64, rerank 50. Allow post-filter x10 only when selectivity >= 30%.
4. **Text index: adopt `tantivy`** (MIT), one index per tenant segment (not a tenant filter term inside a shared index), BM25 with `WithFreqs` postings and an id fast field. Replace the `HashMap<String, HashSet<String>>` inverted index and its union-everything candidate generation. Rank-fusion layer must be re-calibrated: top-10 overlap with the in-repo composite score is only 0.90-0.96.
5. **Concurrency**: memtable (flat exact scan up to ~5k vectors, then flush) + immutable usearch segments behind `ArcSwap`, one query = parallel search of all segments + merge by score. Confirmed viable; reserve CPU for ingest/flush/compaction.
6. **WAL**: group commit with a dedicated committer coalescing queued frames (one `sync_data` per batch, no timer needed); preallocate/overwrite segment files where possible (4 KiB x 16: 55k vs 25k records/s).
7. **Deletes/updates**: tombstone in the live-docs bitmap and remove from usearch only as an optimisation; reclaim space by segment compaction. `remove`+`add` is the update path.
8. **Lance as segment format** (ADR-04 alternative): **not evaluated** in this spike.

## 6. Risks

- **C++ build dependency**: `usearch` compiles C++ (cxx bridge) on every build; needs g++/clang in CI and in container build stages. The release spike build was 3 min 17 s cold (with tantivy and the store). Mitigate with a prebuilt-dependency cache layer, pinned compiler in the builder image, and a CI job that builds on the target musl/glibc images.
- **Stability**: `usearch` crate versions move quickly (the repo lockfile has 2.26.0; the spike used 2.26.4). Pin the exact version; segment files embed the usearch version (header) so an upgrade needs a read-compat test (not tested here).
- **Delete/update semantics**: soft delete only in this test (memory and file do not shrink; `compact()` showed no effect); duplicate keys rejected; capacity must be reserved before inserts; only f32 was tested. Persisted deleted slots cost recall/latency over time (5 churn rounds were clean) - revalidate at the real churn rate.
- **Concurrent insert + search** with 4 query threads and one inserter on 4 cores gave p99 5 ms and insert p99 4.6 ms for the HNSW memtable.
- **License**: usearch Apache-2.0, tantivy MIT (both from their `Cargo.toml`); both compatible with the repo's Apache/MIT posture; add to `docs/supply-chain.md` and `cargo deny` allow-list when adopted.
- **Binary size**: the stripped spike binary (store + usearch + tantivy + rayon) is 11.7 MB; the individual contribution of each library was not isolated.
- **Measurement risk**: shared 4-vCPU box with other work running; one run per cell; the virtual disk's fsync is faster than typical media; 10M x 768-d, 16 vCPU and NVMe are not tested; the text experiment is 100k docs, not 200k.

## 7. Caveats on method

- The synthetic vectors are low-intrinsic-dimension clusters; real embeddings may be harder (lower recall at the same ef) or easier. Re-run on a real embedding set before fixing ef defaults.
- In-repo ANN numbers use `ann_vector_top_candidates`, with `search_expansion_*` pinned to ef. The control (1 cluster) isolates the clustered-data failure.
- The first 100-200 queries are used for exact-scan and in-repo text latencies; histograms for slow cells (in-repo natural queries) have few samples, so p95/p99 there are indicative only.
- Section B used 300 queries and only the i8 index; section D used one i8 index at ef=128.

## 8. Follow-up work for the engine implementation

1. `dash-index` crate: wrappers for usearch (build/view/save, i8+rerank, filtered search with predicate) and tantivy (per-segment build, BM25 top-k, fast-field ids); keep the C++ toolchain pinned in the builder image and add a CI job.
2. Flat memtable with exact scan and a flush threshold (start 5k vectors); segment flush builds i8 usearch + tantivy + roaring in the background; arc-swap publish.
3. Planner: allowed-set size from roaring bitmaps drives exact vs filtered HNSW (threshold config, default 8k); post-filter only at >= 30% selectivity.
4. Per-tenant segment layout (tenant = partition, ADR-06); per-tenant tantivy index; lazy load via `view` (mmap).
5. WAL v2 committer: group commit coalescing, preallocated segment files, `sync_data`; benchmark on target NVMe and with real power-fail semantics.
6. Rank fusion calibration against tantivy BM25 and vector scores (overlap with the old scorer is 0.90-0.96); evaluation set of real queries.
7. Re-run this spike on target hardware (16 vCPU, NVMe, 768-d, 1M and 10M vectors) and with real embeddings, then update section 1.3 targets accordingly; evaluate Lance as segment format and binary quantization.
8. Delete path: live-docs bitmap, compaction to reclaim space, version-compat test for persisted usearch files, i8/f16 delete tests.
9. Remove `pkg/store/src/ann.rs` and the O(N) neighbour scan once shadow reads agree; update claims ledger C31.

## 9. Reproduction

```
git checkout p2/engine-spike
spikes/engine/run.sh build && spikes/engine/run.sh env
spikes/engine/run.sh A                       # results/A_vector.jsonl (exact, in-repo ANN, usearch f32/f16/i8, N=50k/200k/500k)
engine-spike filter kind=i8 n=200000 nq=300  # results/B_filtered.jsonl
engine-spike text docs=100000 qn=1000 qn_repo=100          # results/C_text.jsonl
engine-spike conc kind=i8 n=200000 ef=128 dur=8 mem=5000 rate=2000 rotate_cap=4000   # results/D_concurrency.jsonl
engine-spike deletes kind=f32 n=50000        # results/deletes.jsonl
engine-spike wal secs=3                      # results/E_wal.jsonl
```

(`engine-spike` = `$CARGO_TARGET_DIR/release/engine-spike`, `SPIKE_SCRATCH` holds temporary index files.) The A, B, D, deletes, C and E result files were
produced with exactly these arguments except that `run.sh B` and `run.sh C` default to larger parameters than the committed runs (B was run with kind=i8 and nq=300 only,
C with 100k docs).
