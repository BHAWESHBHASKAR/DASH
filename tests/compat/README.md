# Upgrade compatibility fixtures and tests

`dash-compat` starts the current code on state written by earlier releases and
checks that it loads, serves the same answers, migrates forward, and that the
documented downgrade rules hold. The operator-facing guide is
[`docs/operations/upgrades.md`](../../docs/operations/upgrades.md).

```
tests/compat/
  dataset/            the requests every release is fed (input, do not edit casually)
    generate_dataset.py   writes ingest.jsonl, deletes.jsonl, retrieve.jsonl
    placements.csv        control-plane placement file
  fixtures/<label>/   what one release produced for the dataset
    FIXTURE.txt           label, git ref and commit, date, generator
    state/                ingestion state: ingest.wal, ingest.wal.snapshot,
                          ingest.wal.gen (0.3+), ingest.wal.vindex (0.3+),
                          ingest.redb.gz, segments/, audit-ingestion.jsonl,
                          audit-retrieval.jsonl
    http/                 the release's answers to ingest, delete and
                          retrieve requests (JSON lines, in dataset order)
    replication/          the WAL frame (from_offset=0) and the export frame
                          the release served on /internal/replication/*
    control-plane/        lease file(s), persisted placement state, answers
  expected/<label>.json  by-design differences between the current answers
                          and the release's recorded ones, each with a reason
  src/lib.rs            fixture registry (FIXTURES), helpers, and oracles of
                          older releases' readers (old_readers)
  tests/*.rs            the tests, one file per format
```

## Fixtures

| Label | Release | Generated from |
|---|---|---|
| `v0.2-main-ae86667` | 0.2.x, the last release before the hardening work | `origin/main` at `ae86667` |
| `v0.3.0-dev` | 0.3.0 (unreleased) as of this branch | the hardening branch, see `FIXTURE.txt`; replace with the tagged 0.3.0 fixture at release |

## How a fixture is generated

```bash
scripts/compat/generate_fixtures.sh --ref <git ref> --label <label>
```

1. `git worktree add --detach` of the ref in a temporary directory.
2. `cargo build -p ingestion -p retrieval -p control-plane --bins` there,
   with a target directory of its own inside the temporary directory (a
   different source tree never shares a target directory with the working
   tree).
3. `scripts/compat/run_scenario.py` against those binaries:
   - ingestion with a WAL, the redb mirror, a segment directory, an audit
     log, an API key and `DASH_CHECKPOINT_MAX_WAL_RECORDS=50`, so the final
     state is a snapshot plus a WAL tail; every request of
     `dataset/ingest.jsonl`, then `dataset/deletes.jsonl` (skipped and
     recorded as such when the release answers 404: no deletes);
   - the replication frames, read from the running leader;
   - ingestion stopped with SIGTERM, its state copied (segment files no
     manifest references are dropped: they are garbage left for the prune
     grace period);
   - retrieval started on a scratch copy of that state (with the segment
     directory) and every request of `dataset/retrieve.jsonl`;
   - the control plane with `dataset/placements.csv`, a persisted state path
     and a lease; its files and answers.
4. `ingest.redb` is gzipped (redb preallocates), `FIXTURE.txt` written, and
   the worktree and build output deleted.

`--bin-dir <dir>` skips steps 1-2 and uses binaries you already built (the
`v0.3.0-dev` fixture was produced that way from this branch's build).
Re-running the scenario on the same binaries gives the same data and answers;
only timestamps (audit records, batch commit times, lease expiry) and the
tier a segment lands in differ.

## Adding a release

See the release checklist in `docs/operations/upgrades.md`: generate the
fixture from the tag, register it in `FIXTURES`, explain any intended answer
changes in `expected/`, and document new formats.
