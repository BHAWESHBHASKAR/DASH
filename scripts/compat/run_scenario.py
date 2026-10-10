#!/usr/bin/env python3
"""Runs the compatibility scenario against one release's binaries.

Called by scripts/compat/generate_fixtures.sh; it can also be run directly
against binaries that are already built:

    python3 -I scripts/compat/run_scenario.py \
        --bin-dir <dir with ingestion, retrieval, control-plane> \
        --dataset tests/compat/dataset --out <fixture dir>

Steps (every one is recorded in <out>):

1. Start the ingestion service with a WAL, the redb mirror, a segment
   directory, an audit log and a checkpoint threshold low enough that the
   final state is a snapshot plus a WAL tail. Send every request of
   dataset/ingest.jsonl and record the answers (http/ingest-responses.jsonl).
2. Record the leader's replication frames as served on the wire
   (replication/wal-frame.txt, replication/export-frame.txt).
3. Stop ingestion (SIGTERM) and copy its state (state/).
4. Start the retrieval service on a scratch copy of that state, send every
   request of dataset/retrieve.jsonl and record the answers
   (http/retrieve-responses.jsonl) and its audit log.
5. Start the control plane with a placement file, a persisted state path
   and a lease; record the lease and state files (control-plane/).
"""

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

INGEST_KEY = "compat-ingest-key-0123456789abcdef0123"
RETRIEVE_KEY = "compat-retrieve-key-0123456789abcdef01"
REPLICATION_TOKEN = "compat-replication-token-0123456789ab"
CONTROL_TOKEN = "compat-control-token-0123456789abcdef"
CHECKPOINT_MAX_WAL_RECORDS = "50"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def http(method, url, body=None, headers=None, timeout=30):
    data = None
    hdrs = dict(headers or {})
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        hdrs["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8")
    except urllib.error.HTTPError as err:
        return err.code, err.read().decode("utf-8")


def http_half_close(method, url, headers=None, timeout=10):
    """HTTP/1.1 request that half-closes the socket after sending: some
    releases' control plane read the request until EOF."""
    hostport, _, path = url[len("http://"):].partition("/")
    host, _, port = hostport.partition(":")
    lines = [f"{method} /{path} HTTP/1.1", f"Host: {hostport}", "Connection: close",
             "Content-Length: 0"]
    lines += [f"{k}: {v}" for k, v in (headers or {}).items()]
    raw = ("\r\n".join(lines) + "\r\n\r\n").encode()
    with socket.create_connection((host, int(port)), timeout=timeout) as sock:
        sock.sendall(raw)
        sock.shutdown(socket.SHUT_WR)
        chunks = []
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
    text = b"".join(chunks).decode("utf-8")
    head, _, body = text.partition("\r\n\r\n")
    return int(head.split()[1]), body


def wait_health(base, proc, what, request=None):
    request = request or http
    deadline = time.time() + 60
    while time.time() < deadline:
        if proc.poll() is not None:
            raise SystemExit(f"{what} exited early with {proc.returncode}")
        try:
            status, _ = request("GET", base + "/health", timeout=2)
            if status == 200:
                return
        except OSError:
            # Not accepting connections yet; keep polling until the deadline.
            pass
        time.sleep(0.1)
    raise SystemExit(f"{what} did not become healthy")


def stop(proc):
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def base_env():
    env = {k: v for k, v in os.environ.items() if not k.startswith(("DASH_", "EME_"))}
    env["RUST_LOG"] = env.get("RUST_LOG", "warn")
    return env


def load_jsonl(path):
    with open(path, encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def parse_body(text):
    try:
        return json.loads(text)
    except ValueError:
        return text


def run_ingestion(bin_dir, dataset, out, work, logs):
    state = os.path.join(work, "ingest-state")
    os.makedirs(state)
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    env = base_env()
    env.update({
        "DASH_INGEST_BIND": f"127.0.0.1:{port}",
        "DASH_INGEST_WAL_PATH": os.path.join(state, "ingest.wal"),
        "DASH_INGEST_PERSISTENCE_PATH": os.path.join(state, "ingest.redb"),
        "DASH_INGEST_SEGMENT_DIR": os.path.join(state, "segments"),
        "DASH_INGEST_AUDIT_LOG_PATH": os.path.join(state, "audit-ingestion.jsonl"),
        "DASH_CHECKPOINT_MAX_WAL_RECORDS": CHECKPOINT_MAX_WAL_RECORDS,
        "DASH_INGEST_API_KEY": INGEST_KEY,
        # Releases with roles: the tenant delete needs admin.
        "DASH_INGEST_API_KEY_DEFAULT_ROLES": "admin",
        "DASH_INGEST_REPLICATION_TOKEN": REPLICATION_TOKEN,
        "DASH_INGEST_HTTP_WORKERS": "2",
    })
    with open(os.path.join(logs, "ingestion.log"), "w") as log:
        proc = subprocess.Popen([os.path.join(bin_dir, "ingestion")], env=env, stdout=log,
                                stderr=subprocess.STDOUT, cwd=work)
    try:
        wait_health(base, proc, "ingestion")
        responses = []
        for index, req in enumerate(load_jsonl(os.path.join(dataset, "ingest.jsonl"))):
            status, text = http(req["method"], base + req["path"], req["body"],
                                {"x-api-key": INGEST_KEY})
            if status != 200:
                raise SystemExit(f"ingest request {index} failed: {status} {text}")
            responses.append({"index": index, "status": status, "body": parse_body(text)})
        write_jsonl(os.path.join(out, "http", "ingest-responses.jsonl"), responses)
        # Deletes exist only in releases that have them: an older release
        # answers the first one with 404 and the step is skipped.
        responses = []
        for index, req in enumerate(load_jsonl(os.path.join(dataset, "deletes.jsonl"))):
            status, text = http(req["method"], base + req["path"], None,
                                {"x-api-key": INGEST_KEY})
            if index == 0 and status in (404, 405):
                responses = [{"index": 0, "status": status, "skipped": "deletes unsupported"}]
                break
            if status != 200:
                raise SystemExit(f"delete request {index} failed: {status} {text}")
            responses.append({"index": index, "status": status, "body": parse_body(text)})
        write_jsonl(os.path.join(out, "http", "delete-responses.jsonl"), responses)
        token = {"x-replication-token": REPLICATION_TOKEN}
        for name, path in (("wal-frame.txt", "/internal/replication/wal?from_offset=0&max_records=512"),
                           ("export-frame.txt", "/internal/replication/export")):
            status, text = http("GET", base + path, headers=token)
            if status != 200:
                raise SystemExit(f"replication {path} failed: {status} {text}")
            with open(os.path.join(out, "replication", name), "w", encoding="utf-8") as handle:
                handle.write(text)
    finally:
        stop(proc)
    return state


def copy_state(src, dst):
    os.makedirs(dst, exist_ok=True)
    for name in sorted(os.listdir(src)):
        path = os.path.join(src, name)
        if os.path.isdir(path):
            shutil.copytree(path, os.path.join(dst, name))
        else:
            shutil.copy2(path, os.path.join(dst, name))


def drop_unreferenced_segment_files(segments):
    """Removes segment files no manifest references. Releases that prune
    with a grace period leave the files of superseded manifests behind for a
    while; they are garbage and only make the fixture bigger."""
    if not os.path.isdir(segments):
        return
    for tenant in sorted(os.listdir(segments)):
        tenant_dir = os.path.join(segments, tenant)
        manifest = os.path.join(tenant_dir, "segments.manifest")
        if not os.path.isfile(manifest):
            continue
        with open(manifest, encoding="utf-8") as handle:
            rows = [line.rstrip("\n").split("\t") for line in handle][1:]
        live = {row[2] for row in rows if len(row) >= 3}
        for name in os.listdir(tenant_dir):
            if name.endswith(".seg") and name not in live:
                os.remove(os.path.join(tenant_dir, name))


def run_retrieval(bin_dir, dataset, out, work, state, logs):
    scratch = os.path.join(work, "retrieve-state")
    copy_state(state, scratch)
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    env = base_env()
    env.update({
        "DASH_RETRIEVAL_BIND": f"127.0.0.1:{port}",
        "DASH_RETRIEVAL_WAL_PATH": os.path.join(scratch, "ingest.wal"),
        "DASH_RETRIEVAL_PERSISTENCE_PATH": os.path.join(scratch, "retrieval.redb"),
        "DASH_RETRIEVAL_SEGMENT_DIR": os.path.join(scratch, "segments"),
        "DASH_RETRIEVAL_AUDIT_LOG_PATH": os.path.join(scratch, "audit-retrieval.jsonl"),
        "DASH_RETRIEVAL_API_KEY": RETRIEVE_KEY,
        "DASH_RETRIEVAL_HTTP_WORKERS": "2",
    })
    with open(os.path.join(logs, "retrieval.log"), "w") as log:
        proc = subprocess.Popen([os.path.join(bin_dir, "retrieval")], env=env, stdout=log,
                                stderr=subprocess.STDOUT, cwd=work)
    try:
        wait_health(base, proc, "retrieval")
        responses = []
        for index, req in enumerate(load_jsonl(os.path.join(dataset, "retrieve.jsonl"))):
            status, text = http(req["method"], base + req["path"], req["body"],
                                {"x-api-key": RETRIEVE_KEY})
            responses.append({"index": index, "status": status, "body": parse_body(text)})
        write_jsonl(os.path.join(out, "http", "retrieve-responses.jsonl"), responses)
    finally:
        stop(proc)
    shutil.copy2(os.path.join(scratch, "audit-retrieval.jsonl"),
                 os.path.join(out, "state", "audit-retrieval.jsonl"))


def run_control_plane(bin_dir, dataset, out, work, logs):
    cp = os.path.join(work, "control-plane")
    os.makedirs(cp)
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    env = base_env()
    env.update({
        "DASH_CONTROL_PLANE_BIND": f"127.0.0.1:{port}",
        "DASH_CONTROL_PLANE_NODE_ID": "cp-old-1",
        "DASH_CONTROL_PLANE_STATE_PATH": os.path.join(cp, "placement-state.csv"),
        "DASH_CONTROL_PLANE_LEASE_PATH": os.path.join(cp, "leader.lease"),
        "DASH_CONTROL_PLANE_LEASE_DURATION_MS": "2000",
        "DASH_CONTROL_PLANE_LEASE_RENEWAL_MS": "500",
        "DASH_CONTROL_PLANE_TOKEN": CONTROL_TOKEN,
        "DASH_ROUTER_PLACEMENT_FILE": os.path.join(dataset, "placements.csv"),
    })
    with open(os.path.join(logs, "control-plane.log"), "w") as log:
        proc = subprocess.Popen([os.path.join(bin_dir, "control-plane")], env=env, stdout=log,
                                stderr=subprocess.STDOUT, cwd=work)
    try:
        wait_health(base, proc, "control-plane", http_half_close)
        auth = {"Authorization": f"Bearer {CONTROL_TOKEN}"}
        deadline = time.time() + 30
        leader = None
        while time.time() < deadline:
            status, text = http_half_close("GET", base + "/v1/control-plane/leader", auth)
            if status == 200 and parse_body(text).get("is_leader"):
                leader = parse_body(text)
                break
            time.sleep(0.2)
        if leader is None:
            raise SystemExit("control plane never became leader")
        status, text = http_half_close("GET", base + "/v1/control-plane/placement", auth)
        write_jsonl(os.path.join(out, "control-plane", "responses.jsonl"), [
            {"route": "GET /v1/control-plane/leader", "status": 200, "body": leader},
            {"route": "GET /v1/control-plane/placement", "status": status, "body": parse_body(text)},
        ])
    finally:
        stop(proc)
    for name in sorted(os.listdir(cp)):
        path = os.path.join(cp, name)
        if os.path.isfile(path):
            shutil.copy2(path, os.path.join(out, "control-plane", name))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin-dir", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    bin_dir = os.path.abspath(args.bin_dir)
    dataset = os.path.abspath(args.dataset)
    out = os.path.abspath(args.out)
    for sub in ("state", "http", "replication", "control-plane"):
        os.makedirs(os.path.join(out, sub), exist_ok=True)
    work = tempfile.mkdtemp(prefix="dash-compat-")
    logs = os.path.join(work, "logs")
    os.makedirs(logs)
    try:
        state = run_ingestion(bin_dir, dataset, out, work, logs)
        copy_state(state, os.path.join(out, "state"))
        drop_unreferenced_segment_files(os.path.join(out, "state", "segments"))
        run_retrieval(bin_dir, dataset, out, work, state, logs)
        run_control_plane(bin_dir, dataset, out, work, logs)
    except BaseException:
        print(f"scenario failed; logs kept in {logs}", file=sys.stderr)
        raise
    shutil.rmtree(work)


if __name__ == "__main__":
    main()
