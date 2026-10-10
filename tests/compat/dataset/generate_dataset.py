#!/usr/bin/env python3
"""Writes the compatibility dataset (ingest, delete and retrieve requests).

The dataset is the INPUT of scripts/compat/generate_fixtures.sh: the same
requests are sent to every release whose state is captured as a fixture, so
fixtures of different releases describe the same logical data. Do not change
it casually: existing fixtures were generated from the committed files, and
tests compare their results with what the old binaries answered.

Usage: python3 generate_dataset.py <output dir>
"""

import json
import math
import sys

DIM = 8


def vec(seed):
    return [round(math.sin(seed * 1.7 + i * 0.9) + 0.1 * i, 4) for i in range(DIM)]


TOPICS = [
    ("Company X acquired Company Y in 2025", ["Company X", "Company Y"], "factual", 1736035200),
    ("Company Y reported record revenue in Q3", ["Company Y"], "factual", 1727740800),
    ("Regulators opened an inquiry into the X-Y merger", ["Company X", "Company Y", "Regulator"], "temporal", 1738368000),
    ("Analysts expect Company X shares to rise", ["Company X"], "prediction", None),
    ("The merger will reduce competition in the region", ["Company X", "Region"], "opinion", None),
    ("Company Z denied any involvement in the deal", ["Company Z"], "factual", 1736121600),
    ("Supply shortages caused the Q2 delay", ["Company Y"], "causal", 1719792000),
    ("Escapes:\ttab, newline\nand backslash \\ survive", ["Escape Test"], None, None),
    ("Unicode: café – 東京 \U0001F680 claims", ["Unicode"], "factual", 1700000000),
    ("Company X appointed a new chief executive", ["Company X"], "factual", 1740787200),
    ("Company Y plans to open three new plants", ["Company Y"], "prediction", 1751328000),
    ("The acquisition price was 4.2 billion dollars", ["Company X", "Company Y"], "factual", 1736035200),
]

RELATIONS = ["supports", "contradicts", "refines", "duplicates", "depends_on"]


def ingest(cid, tenant, text, ents, ctype, t, seed, evidence, edges, emb=True, valid=None):
    claim = {
        "claim_id": cid,
        "tenant_id": tenant,
        "canonical_text": text,
        "confidence": round(0.55 + (seed % 9) * 0.05, 2),
        "entities": ents,
    }
    if ctype:
        claim["claim_type"] = ctype
    if t is not None:
        claim["event_time_unix"] = t
    if valid:
        claim["valid_from"], claim["valid_to"] = valid
    body = {"claim": claim, "evidence": evidence, "edges": edges}
    if emb:
        body["claim_embedding"] = vec(seed)
    return body


def ev(eid, cid, src, stance, quality, **extra):
    item = {
        "evidence_id": eid,
        "claim_id": cid,
        "source_id": src,
        "stance": stance,
        "source_quality": quality,
    }
    item.update(extra)
    return item


def ingest_requests():
    reqs = []
    for i, (text, ents, ctype, t) in enumerate(TOPICS):
        cid = f"a-c{i + 1:02d}"
        evidence = [
            ev(
                f"a-e{i + 1:02d}-1", cid, f"news://wire/{i + 1}", "supports", 0.9,
                chunk_id=f"chunk-{i}", span_start=10 * i, span_end=10 * i + 42,
                doc_id=f"doc-{i}", extraction_model="extractor-v1",
                ingested_at=1736035200000 + i,
            )
        ]
        if i % 3 == 1:
            evidence.append(ev(f"a-e{i + 1:02d}-2", cid, f"blog://critic/{i}", "contradicts", 0.4))
        if i % 4 == 2:
            evidence.append(ev(f"a-e{i + 1:02d}-3", cid, f"forum://thread/{i}", "neutral", 0.2))
        edges = []
        if i >= 1:
            edges.append({
                "edge_id": f"a-g{i + 1:02d}",
                "from_claim_id": cid,
                "to_claim_id": f"a-c{i:02d}",
                "relation": RELATIONS[i % 5],
                "strength": round(0.5 + (i % 5) * 0.1, 2),
            })
        valid = (t, t + 86400 * 365) if (t and i % 2 == 0) else None
        reqs.append({
            "method": "POST",
            "path": "/v1/ingest",
            "body": ingest(cid, "tenant-a", text, ents, ctype, t, i + 1, evidence, edges, valid=valid),
        })
    # Update of an existing claim: new text, one evidence row replaced, one added.
    reqs.append({
        "method": "POST",
        "path": "/v1/ingest",
        "body": ingest(
            "a-c04", "tenant-a", "Analysts now expect Company X shares to fall", ["Company X"],
            "prediction", None, 4,
            [ev("a-e04-1", "a-c04", "news://wire/4", "supports", 0.95),
             ev("a-e04-9", "a-c04", "news://wire/4b", "supports", 0.7)],
            [],
        ),
    })
    # Atomic batch with a commit id.
    items = []
    for j in range(3):
        cid = f"a-b{j + 1}"
        items.append(ingest(
            cid, "tenant-a", f"Batch claim {j + 1} about Company Y logistics",
            ["Company Y", "Logistics"], "factual", 1745000000 + j, 20 + j,
            [ev(f"a-be{j + 1}", cid, f"report://logistics/{j}",
                "supports" if j != 1 else "contradicts", 0.8)],
            [],
        ))
    reqs.append({
        "method": "POST",
        "path": "/v1/ingest/batch",
        "body": {"commit_id": "compat-batch-1", "items": items},
    })
    # A second tenant whose id has an underscore: its legacy segment
    # directory name differs from the current one.
    for k in range(4):
        cid = f"b-c{k + 1}"
        edges = []
        if k:
            edges.append({
                "edge_id": f"b-g{k + 1}", "from_claim_id": cid, "to_claim_id": "b-c1",
                "relation": "supports", "strength": 0.9,
            })
        reqs.append({
            "method": "POST",
            "path": "/v1/ingest",
            "body": ingest(
                cid, "tenant_b", f"Tenant B fact number {k + 1} about Widget Co", ["Widget Co"],
                "factual", 1710000000 + k * 1000, 40 + k,
                [ev(f"b-e{k + 1}", cid, f"news://b/{k}", "supports", round(0.6 + k * 0.1, 2))],
                edges,
            ),
        })
    # Claims whose embeddings are generated by the service's default
    # provider (their own tenant: the provider's dimension is not 8).
    for n, text in enumerate(["Company X opened an office in Berlin",
                              "Widget Co moved its headquarters to Lisbon"]):
        cid = f"h-c{n + 1}"
        reqs.append({
            "method": "POST",
            "path": "/v1/ingest",
            "body": ingest(
                cid, "tenant-hash", text, ["Company X" if n == 0 else "Widget Co"],
                "factual", 1742000000 + n, 99,
                [ev(f"h-e{n + 1}", cid, f"news://hash/{n}", "supports", 0.85)], [], emb=False,
            ),
        })
    return reqs


def retrieve_requests():
    def rq(tenant, query, **extra):
        body = {"tenant_id": tenant, "query": query}
        body.update(extra)
        return {"method": "POST", "path": "/v1/retrieve", "body": body}

    return [
        rq("tenant-a", "Company X acquired Company Y", top_k=5, query_embedding=vec(1)),
        rq("tenant-a", "revenue", top_k=3, query_embedding=vec(2), stance_mode="support_only"),
        rq("tenant-a", "merger regulators", top_k=10, query_embedding=vec(3), return_graph=True),
        rq("tenant-a", "Company Y", top_k=5, query_embedding=vec(11), entity_filters=["Company Y"]),
        rq("tenant-a", "events in 2025", top_k=5, query_embedding=vec(5),
           time_range={"from_unix": 1735689600, "to_unix": 1767225600}),
        rq("tenant-a", "Batch claim logistics", top_k=4, query_embedding=vec(21)),
        rq("tenant_b", "Widget Co fact", top_k=4, query_embedding=vec(41)),
        rq("tenant-a", "Company X acquired Company Y in 2025", top_k=5),
        rq("tenant-a", "café 東京 unicode", top_k=3),
        rq("tenant-a", "Analysts expect shares", top_k=3, stance_mode="support_only"),
        rq("tenant-hash", "office in Berlin", top_k=2),
    ]


def delete_requests():
    """Sent after the ingest requests, only to releases that have deletes
    (an older release answers 404 and the step is recorded as skipped)."""
    return [
        {"method": "DELETE", "path": "/v1/claims/a-c06?tenant_id=tenant-a"},
        {"method": "DELETE", "path": "/v1/evidence/a-e02-2?tenant_id=tenant-a"},
        {"method": "DELETE", "path": "/v1/tenants/tenant-hash"},
    ]


def write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def main():
    out = sys.argv[1] if len(sys.argv) > 1 else "."
    write_jsonl(f"{out}/ingest.jsonl", ingest_requests())
    write_jsonl(f"{out}/retrieve.jsonl", retrieve_requests())
    write_jsonl(f"{out}/deletes.jsonl", delete_requests())


if __name__ == "__main__":
    main()
