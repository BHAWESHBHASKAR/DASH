#!/usr/bin/env bash
# P2 engine spike runner. Everything is sequential (the box has 4 cores; experiments must not overlap).
#
#   spikes/engine/run.sh build        # release build of the spike
#   spikes/engine/run.sh env          # record hardware / versions into results/env.txt
#   spikes/engine/run.sh A            # vector index (exact, in-repo ANN, usearch f32/f16/i8)
#   spikes/engine/run.sh B            # filtered vector search
#   spikes/engine/run.sh C            # text index
#   spikes/engine/run.sh D            # memtable + immutable segment concurrency
#   spikes/engine/run.sh deletes      # usearch remove()/re-add semantics
#   spikes/engine/run.sh E            # WAL fsync / group commit costs
#   spikes/engine/run.sh clean        # delete scratch data and the target dir
#
# Disk is tight on the reference box: the target dir and scratch dir live under
# /home/user/targets/spike and are removed by `clean`.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/user/targets/spike}"
export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
export RUSTFLAGS="${RUSTFLAGS:--C target-cpu=native}"
export SPIKE_SCRATCH="${SPIKE_SCRATCH:-/home/user/targets/spike/scratch}"
BIN="$CARGO_TARGET_DIR/release/engine-spike"
OUT="$HERE/results"
mkdir -p "$OUT" "$SPIKE_SCRATCH"

run() { # run <outfile> <args...>
  local f="$1"; shift
  echo ">>> engine-spike $*  ->  $f" >&2
  "$BIN" "$@" >> "$OUT/$f"
}

free_gb() { df -BG --output=avail / | tail -1 | tr -dc 0-9; }

case "${1:-all}" in
  build)
    (cd "$HERE" && cargo build --release)
    ;;
  env)
    {
      echo "date: $(date -u +%FT%TZ)"
      echo "nproc: $(nproc)"
      grep -m1 'model name' /proc/cpuinfo
      grep -m1 flags /proc/cpuinfo | tr ' ' '\n' | grep -E '^(avx2|avx512f|avx512_fp16|f16c|fma|amx_tile)$' | tr '\n' ' '; echo
      grep -E 'MemTotal|MemAvailable' /proc/meminfo
      uname -sr
      df -T / | tail -1
      rustc --version; cargo --version
      g++ --version | head -1
      "$BIN" info
      echo "RUSTFLAGS=$RUSTFLAGS"
      echo "git: $(git -C "$HERE" rev-parse HEAD)"
    } > "$OUT/env.txt" 2>&1
    ;;
  A)
    : > "$OUT/A_vector.jsonl"
    for n in 50000 200000 500000; do run A_vector.jsonl vec kind=exact n=$n; done
    # in-repo ANN: stop when cumulative build time exceeds 300 s; evaluate at several N
    run A_vector.jsonl vec kind=repo n=60000 cutoff=300 eval_at=2000,5000,10000,20000
    # control: same code on an unclustered (single Gaussian) dataset
    run A_vector.jsonl vec kind=repo n=10000 cutoff=300 eval_at=5000,10000 clusters=1
    for kind in f32 f16 i8; do
      for n in 50000 200000 500000; do
        keep=0; [ "$n" = 200000 ] && keep=1
        run A_vector.jsonl vec kind=$kind n=$n threads=4 eval=full keep=$keep
        run A_vector.jsonl vec kind=$kind n=$n threads=1 eval=short keep=0
        echo "free GB: $(free_gb)" >&2
      done
    done
    ;;
  B)
    : > "$OUT/B_filtered.jsonl"
    run B_filtered.jsonl filter kind=i8 n=200000 nq=500
    run B_filtered.jsonl filter kind=f32 n=200000 nq=500
    ;;
  C)
    : > "$OUT/C_text.jsonl"
    run C_text.jsonl text docs=200000 qn=1000 qn_repo=100
    ;;
  D)
    : > "$OUT/D_concurrency.jsonl"
    run D_concurrency.jsonl conc kind=i8 n=200000 ef="${EF:-128}" dur=8 mem=5000 rate=2000 rotate_cap=4000
    ;;
  deletes)
    : > "$OUT/deletes.jsonl"
    run deletes.jsonl deletes kind=f32 n=50000
    run deletes.jsonl deletes kind=i8 n=50000
    ;;
  E)
    : > "$OUT/E_wal.jsonl"
    run E_wal.jsonl wal secs=3
    ;;
  clean)
    rm -rf "$SPIKE_SCRATCH" "$CARGO_TARGET_DIR"
    ;;
  all)
    "$0" build; "$0" env; "$0" A; "$0" B; "$0" C; "$0" D; "$0" deletes; "$0" E
    ;;
  *)
    echo "usage: $0 build|env|A|B|C|D|deletes|E|clean|all" >&2; exit 2
    ;;
esac
