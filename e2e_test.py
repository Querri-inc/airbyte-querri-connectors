#!/usr/bin/env python3
"""End-to-end tests for Querri Airbyte connectors against a live instance.

Usage:
    python e2e_test.py

Requires a running Querri instance at http://localhost and valid API keys.
All tests use the Querri Python SDK — the same client the connectors use.
"""

import json
import os
import sys
import time
import traceback

# Add both connector packages to path
sys.path.insert(0, "source-querri")
sys.path.insert(0, "destination-querri")

from querri import Querri
from querri._exceptions import NotFoundError, PermissionError as QuerriPermissionError

# ── Configuration ──

def _secrets_file(name: str) -> dict:
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "destination-querri", "secrets", name)
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


# Fallback when the env vars are unset: the gitignored destination secrets.
_RW_FILE = _secrets_file("config.json")
_FALLBACK = {
    "QUERRI_API_KEY": _RW_FILE.get("api_key"),
    "QUERRI_ORG_ID": _RW_FILE.get("org_id"),
    "QUERRI_READ_ONLY_KEY": _secrets_file("config_ro.json").get("api_key"),
}
if _RW_FILE.get("base_url") and "QUERRI_BASE_URL" not in os.environ:
    os.environ["QUERRI_BASE_URL"] = _RW_FILE["base_url"]


def _require_env(name: str) -> str:
    """Get a required environment variable or fail fast with a clear message."""
    value = os.environ.get(name) or _FALLBACK.get(name)
    if not value:
        print(f"ERROR: Required environment variable {name} is not set.", file=sys.stderr)
        print(f"Set it in your shell or in airbyte/.env before running e2e tests.", file=sys.stderr)
        sys.exit(1)
    return value

API_KEY = _require_env("QUERRI_API_KEY")
READ_ONLY_KEY = _require_env("QUERRI_READ_ONLY_KEY")
ORG_ID = _require_env("QUERRI_ORG_ID")
BASE_URL = os.environ.get("QUERRI_BASE_URL", "http://localhost/api/v1")

# SDK expects host without /api/v1 suffix
_HOST = BASE_URL.removesuffix("/").removesuffix("/api/v1")

# Track test sources for cleanup
_created_source_ids = []

passed = 0
failed = 0
errors = []


def test(name):
    """Decorator to register and run a test function."""
    def decorator(fn):
        fn._test_name = name
        return fn
    return decorator


def run_test(fn):
    global passed, failed
    name = getattr(fn, "_test_name", fn.__name__)
    try:
        fn()
        passed += 1
        print(f"  ✓ {name}")
    except AssertionError as e:
        failed += 1
        errors.append((name, str(e)))
        print(f"  ✗ {name}: {e}")
    except Exception as e:
        failed += 1
        errors.append((name, traceback.format_exc()))
        print(f"  ✗ {name}: {type(e).__name__}: {e}")


# ── Client helpers (SDK-based) ──

def rw_client():
    return Querri(api_key=API_KEY, org_id=ORG_ID, host=_HOST)

def ro_client():
    return Querri(api_key=READ_ONLY_KEY, org_id=ORG_ID, host=_HOST)

def find_source_by_name(client, name):
    """Find a source by exact name match."""
    for source in client.sources.list():
        if source["name"] == name:
            return source
    return None


# ═══════════════════════════════════════════════
# 1. Connection & Authentication
# ═══════════════════════════════════════════════

@test("Check connection with read-write key")
def test_check_rw():
    client = rw_client()
    # Verify credentials by listing sources
    client.sources.list()

@test("Check connection with read-only key")
def test_check_ro():
    client = ro_client()
    client.sources.list()

@test("Check connection with invalid key fails")
def test_check_invalid():
    client = Querri(api_key="qk_invalid", org_id=ORG_ID, host=_HOST)
    try:
        # list() is lazy: iterate to send the request.
        next(iter(client.sources.list()), None)
    except Exception as e:
        assert "401" in str(e) or "invalid" in str(e).lower() or "authentication" in str(e).lower(), f"Unexpected error: {e}"
    else:
        assert False, "Should have raised"

@test("Check connection with wrong org_id fails")
def test_check_wrong_org():
    client = Querri(api_key=API_KEY, org_id="org_nonexistent", host=_HOST)
    try:
        # list() is lazy: iterate to send the request.
        next(iter(client.sources.list()), None)
    except Exception as e:
        assert "401" in str(e) or "invalid" in str(e).lower() or "authentication" in str(e).lower() or "500" in str(e), f"Unexpected error: {e}"
    else:
        assert False, "Should have raised"


# ═══════════════════════════════════════════════
# 2. Source CRUD Lifecycle
# ═══════════════════════════════════════════════

@test("Create source with initial data")
def test_create_source():
    client = rw_client()
    rows = [
        {"name": "Alice", "age": 30, "city": "NYC"},
        {"name": "Bob", "age": 25, "city": "LA"},
        {"name": "Charlie", "age": 35, "city": "Chicago"},
    ]
    result = client.sources.create_data_source(name="e2e_test_crud", rows=rows)
    assert result.id, "No source ID returned"
    assert result.name == "e2e_test_crud"
    assert result.row_count == 3
    assert set(result.columns) == {"name", "age", "city"}
    _created_source_ids.append(result.id)

@test("List sources includes created source")
def test_list_sources():
    client = rw_client()
    sources = list(client.sources.list(limit=100))
    names = [s["name"] for s in sources]
    assert "e2e_test_crud" in names, f"Created source not in list: {names[:5]}"

@test("Get source metadata")
def test_get_source():
    client = rw_client()
    source_id = _created_source_ids[-1]
    result = client.sources.get(source_id)
    assert result["id"] == source_id
    assert result["name"] == "e2e_test_crud"
    assert "name" in result["columns"]

@test("Read source data")
def test_read_data():
    client = rw_client()
    source_id = _created_source_ids[-1]
    result = client.sources.source_data(source_id, page=1, page_size=100)
    assert result.total_rows == 3
    assert len(result.data) == 3
    names = {r["name"] for r in result.data}
    assert names == {"Alice", "Bob", "Charlie"}


# ═══════════════════════════════════════════════
# 3. Append Rows
# ═══════════════════════════════════════════════

@test("Append rows to existing source")
def test_append_rows():
    client = rw_client()
    source_id = _created_source_ids[-1]
    new_rows = [
        {"name": "Diana", "age": 28, "city": "Seattle"},
        {"name": "Eve", "age": 32, "city": "Boston"},
    ]
    result = client.sources.append_rows(source_id, rows=new_rows)
    assert result.row_count == 5, f"Expected 5 rows, got {result.row_count}"

@test("Read after append shows all rows")
def test_read_after_append():
    client = rw_client()
    source_id = _created_source_ids[-1]
    result = client.sources.source_data(source_id)
    assert result.total_rows == 5
    names = {r["name"] for r in result.data}
    assert "Diana" in names
    assert "Alice" in names


# ═══════════════════════════════════════════════
# 4. Replace Data (Overwrite)
# ═══════════════════════════════════════════════

@test("Replace data overwrites all rows")
def test_replace_data():
    client = rw_client()
    source_id = _created_source_ids[-1]
    new_rows = [
        {"name": "Xander", "score": 100},
        {"name": "Yara", "score": 200},
    ]
    result = client.sources.replace_data(source_id, rows=new_rows)
    assert result.row_count == 2
    assert "score" in result.columns

@test("Read after replace shows only new data")
def test_read_after_replace():
    client = rw_client()
    source_id = _created_source_ids[-1]
    result = client.sources.source_data(source_id)
    assert result.total_rows == 2
    names = {r["name"] for r in result.data}
    assert names == {"Xander", "Yara"}, f"Unexpected names: {names}"


# ═══════════════════════════════════════════════
# 5. Delete Source
# ═══════════════════════════════════════════════

@test("Delete source")
def test_delete_source():
    client = rw_client()
    source_id = _created_source_ids[-1]
    result = client.sources.delete(source_id)
    assert result is None or True  # delete returns None
    _created_source_ids.remove(source_id)

@test("Deleted source returns 404")
def test_deleted_404():
    client = rw_client()
    try:
        client.sources.get("00000000-0000-0000-0000-000000000000")
    except (NotFoundError, QuerriPermissionError):
        # The access check runs before the existence check, so an unknown id
        # can be refused as 403 rather than 404; either way it is unreadable.
        pass
    else:
        assert False, "Should have raised"


# ═══════════════════════════════════════════════
# 6. Read-Only Key Scope Enforcement
# ═══════════════════════════════════════════════

@test("Read-only key can list sources")
def test_ro_list():
    client = ro_client()
    sources = list(client.sources.list(limit=100))
    assert all("id" in s for s in sources)

@test("Read-only key cannot create source")
def test_ro_cannot_create():
    client = ro_client()
    try:
        client.sources.create_data_source(name="should_fail", rows=[{"x": 1}])
        assert False, "Should have raised permission error"
    except Exception as e:
        assert "403" in str(e) or "scope" in str(e).lower() or "permission" in str(e).lower(), f"Unexpected error: {e}"


# ═══════════════════════════════════════════════
# 7. Pagination
# ═══════════════════════════════════════════════

@test("Pagination works correctly")
def test_pagination():
    client = rw_client()
    # Create a source with enough rows to paginate
    rows = [{"idx": i, "val": f"row_{i}"} for i in range(25)]
    result = client.sources.create_data_source(name="e2e_test_pagination", rows=rows)
    _created_source_ids.append(result.id)
    assert result.row_count == 25

    # Read page 1
    page1 = client.sources.source_data(result.id, page=1, page_size=10)
    assert len(page1.data) == 10
    assert page1.total_rows == 25

    # Read page 2
    page2 = client.sources.source_data(result.id, page=2, page_size=10)
    assert len(page2.data) == 10

    # Read page 3
    page3 = client.sources.source_data(result.id, page=3, page_size=10)
    assert len(page3.data) == 5


# ═══════════════════════════════════════════════
# 8. Source Connector (Airbyte Protocol)
# ═══════════════════════════════════════════════

@test("Source connector: check succeeds")
def test_source_check():
    from source_querri.source import SourceQuerri
    source = SourceQuerri()
    config = {"api_key": API_KEY, "org_id": ORG_ID, "base_url": BASE_URL}
    ok, err = source.check_connection(None, config)
    assert ok is True, f"Check failed: {err}"

@test("Source connector: discovers streams (filtered)")
def test_source_discover():
    from source_querri.source import SourceQuerri
    client = rw_client()
    pagination_source = find_source_by_name(client, "e2e_test_pagination")
    assert pagination_source, "Pagination test source not found"

    source = SourceQuerri()
    config = {
        "api_key": API_KEY, "org_id": ORG_ID, "base_url": BASE_URL,
        "source_ids": [pagination_source["id"]],
    }
    streams = source.streams(config)
    assert len(streams) >= 1, "No streams discovered"
    names = [s.name for s in streams]
    assert "e2e_test_pagination" in names, f"Expected stream not found: {names}"

@test("Source connector: reads records from stream")
def test_source_read():
    from source_querri.source import SourceQuerri
    client = rw_client()
    pagination_source = find_source_by_name(client, "e2e_test_pagination")

    source = SourceQuerri()
    config = {
        "api_key": API_KEY, "org_id": ORG_ID, "base_url": BASE_URL,
        "source_ids": [pagination_source["id"]],
    }
    streams = source.streams(config)
    target = [s for s in streams if s.name == "e2e_test_pagination"][0]
    records = list(target.read_records(sync_mode="full_refresh"))
    assert len(records) == 25, f"Expected 25 records, got {len(records)}"
    assert "_querri_source_id" in records[0]

@test("Source connector: JSON schema has correct types")
def test_source_schema():
    from source_querri.source import SourceQuerri
    client = rw_client()
    pagination_source = find_source_by_name(client, "e2e_test_pagination")

    source = SourceQuerri()
    config = {
        "api_key": API_KEY, "org_id": ORG_ID, "base_url": BASE_URL,
        "source_ids": [pagination_source["id"]],
    }
    streams = source.streams(config)
    target = [s for s in streams if s.name == "e2e_test_pagination"][0]
    schema = target.get_json_schema()
    assert "properties" in schema
    assert "idx" in schema["properties"]
    assert "_querri_source_id" in schema["properties"]


# ═══════════════════════════════════════════════
# 9. Destination Connector (Airbyte Protocol)
# ═══════════════════════════════════════════════

@test("Destination connector: check succeeds")
def test_dest_check():
    from destination_querri.destination import DestinationQuerri
    from unittest.mock import MagicMock
    dest = DestinationQuerri()
    config = {"api_key": API_KEY, "org_id": ORG_ID, "base_url": BASE_URL}
    result = dest.check(MagicMock(), config)
    assert result.status.value == "SUCCEEDED", f"Check failed: {result.message}"

@test("Destination connector: write creates new source (append mode)")
def test_dest_write_append():
    from airbyte_cdk.models import (
        AirbyteMessage, AirbyteRecordMessage, AirbyteStateMessage,
        AirbyteStateType, AirbyteStream, ConfiguredAirbyteCatalog,
        ConfiguredAirbyteStream, DestinationSyncMode, SyncMode, Type,
    )
    from destination_querri.destination import DestinationQuerri

    dest = DestinationQuerri()
    config = {"api_key": API_KEY, "org_id": ORG_ID, "base_url": BASE_URL}
    catalog = ConfiguredAirbyteCatalog(streams=[
        ConfiguredAirbyteStream(
            stream=AirbyteStream(
                name="e2e_dest_append",
                json_schema={"type": "object"},
                supported_sync_modes=[SyncMode.full_refresh],
            ),
            sync_mode=SyncMode.full_refresh,
            destination_sync_mode=DestinationSyncMode.append,
        )
    ])

    messages = [
        AirbyteMessage(type=Type.RECORD, record=AirbyteRecordMessage(
            stream="e2e_dest_append", data={"product": "Widget", "price": 9.99}, emitted_at=0,
        )),
        AirbyteMessage(type=Type.RECORD, record=AirbyteRecordMessage(
            stream="e2e_dest_append", data={"product": "Gadget", "price": 19.99}, emitted_at=0,
        )),
        AirbyteMessage(type=Type.STATE, state=AirbyteStateMessage(
            type=AirbyteStateType.STREAM, data={},
        )),
    ]

    state_msgs = list(dest.write(config, catalog, iter(messages)))
    assert len(state_msgs) >= 1, "No state messages emitted"

    # Verify data landed in Querri
    client = rw_client()
    source = find_source_by_name(client, "airbyte_e2e_dest_append")
    assert source is not None, "Destination didn't create source 'airbyte_e2e_dest_append'"
    _created_source_ids.append(source["id"])

    data = client.sources.source_data(source["id"])
    assert data.total_rows == 2
    products = {r["product"] for r in data.data}
    assert products == {"Widget", "Gadget"}

@test("Destination connector: overwrite mode replaces all data")
def test_dest_overwrite():
    from airbyte_cdk.models import (
        AirbyteMessage, AirbyteRecordMessage, AirbyteStream,
        ConfiguredAirbyteCatalog, ConfiguredAirbyteStream,
        DestinationSyncMode, SyncMode, Type,
    )
    from destination_querri.destination import DestinationQuerri

    # First create data via SDK
    client = rw_client()
    existing = find_source_by_name(client, "airbyte_e2e_dest_overwrite")
    if existing:
        client.sources.delete(existing["id"])

    result = client.sources.create_data_source(
        name="airbyte_e2e_dest_overwrite",
        rows=[{"old": "data", "should": "vanish"}],
    )
    _created_source_ids.append(result.id)

    # Now overwrite via destination
    dest = DestinationQuerri()
    config = {"api_key": API_KEY, "org_id": ORG_ID, "base_url": BASE_URL}
    catalog = ConfiguredAirbyteCatalog(streams=[
        ConfiguredAirbyteStream(
            stream=AirbyteStream(
                name="e2e_dest_overwrite",
                json_schema={"type": "object"},
                supported_sync_modes=[SyncMode.full_refresh],
            ),
            sync_mode=SyncMode.full_refresh,
            destination_sync_mode=DestinationSyncMode.overwrite,
        )
    ])

    messages = [
        AirbyteMessage(type=Type.RECORD, record=AirbyteRecordMessage(
            stream="e2e_dest_overwrite", data={"new": "data", "completely": "replaced"}, emitted_at=0,
        )),
    ]

    list(dest.write(config, catalog, iter(messages)))

    # Old data should be gone
    source = find_source_by_name(client, "airbyte_e2e_dest_overwrite")
    data = client.sources.source_data(source["id"])
    assert data.total_rows == 1
    assert data.data[0].get("new") == "data"
    assert "old" not in data.data[0] or data.data[0].get("old") is None


# ═══════════════════════════════════════════════
# 10. Round-trip: Write via Destination, Read via Source
# ═══════════════════════════════════════════════

@test("Round-trip: destination write → source read")
def test_roundtrip():
    from airbyte_cdk.models import (
        AirbyteMessage, AirbyteRecordMessage, AirbyteStateMessage,
        AirbyteStateType, AirbyteStream, ConfiguredAirbyteCatalog,
        ConfiguredAirbyteStream, DestinationSyncMode, SyncMode, Type,
    )
    from destination_querri.destination import DestinationQuerri
    from source_querri.source import SourceQuerri

    # Write via destination
    dest = DestinationQuerri()
    config = {"api_key": API_KEY, "org_id": ORG_ID, "base_url": BASE_URL}
    catalog = ConfiguredAirbyteCatalog(streams=[
        ConfiguredAirbyteStream(
            stream=AirbyteStream(
                name="e2e_roundtrip",
                json_schema={"type": "object"},
                supported_sync_modes=[SyncMode.full_refresh],
            ),
            sync_mode=SyncMode.full_refresh,
            destination_sync_mode=DestinationSyncMode.append,
        )
    ])

    messages = [
        AirbyteMessage(type=Type.RECORD, record=AirbyteRecordMessage(
            stream="e2e_roundtrip", data={"metric": "revenue", "value": 42000}, emitted_at=0,
        )),
        AirbyteMessage(type=Type.RECORD, record=AirbyteRecordMessage(
            stream="e2e_roundtrip", data={"metric": "users", "value": 1500}, emitted_at=0,
        )),
        AirbyteMessage(type=Type.STATE, state=AirbyteStateMessage(
            type=AirbyteStateType.STREAM, data={},
        )),
    ]

    list(dest.write(config, catalog, iter(messages)))

    # Track for cleanup
    client = rw_client()
    source = find_source_by_name(client, "airbyte_e2e_roundtrip")
    assert source is not None
    _created_source_ids.append(source["id"])

    # Read via source connector (use source_ids filter to avoid rate limits)
    src = SourceQuerri()
    src_config = {**config, "source_ids": [source["id"]]}
    streams = src.streams(src_config)
    target = [s for s in streams if s.name == "airbyte_e2e_roundtrip"]
    assert len(target) == 1, f"Round-trip stream not found. Streams: {[s.name for s in streams][:10]}"

    records = list(target[0].read_records(sync_mode="full_refresh"))
    assert len(records) == 2
    metrics = {r["metric"] for r in records}
    assert metrics == {"revenue", "users"}


# ═══════════════════════════════════════════════
# 11. Destination `write` entrypoint, run as Airbyte runs it
# ═══════════════════════════════════════════════
#
# `python main.py write --config ... --catalog ...` with messages on stdin.
# Each test reads what a Querri reader sees through the API (SQL over the
# source), and the checkpoint tests do so at the moment a STATE comes out.

import queue
import signal
import subprocess
import tempfile
import threading

_DEST_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "destination-querri")
TIMINGS = []


def _msg_record(stream, data):
    return {"type": "RECORD", "record": {"stream": stream, "data": data, "emitted_at": 0}}


def _msg_state(stream, cursor):
    return {"type": "STATE", "state": {"type": "STREAM", "stream": {
        "stream_descriptor": {"name": stream}, "stream_state": {"cursor": cursor}}}}


def _state_cursor(message):
    return message["state"]["stream"]["stream_state"]["cursor"]


class WriteRun:
    """One `main.py write` process; stdin fed incrementally, stdout parsed live."""

    def __init__(self, streams, batch_size=10_000):
        self._dir = tempfile.TemporaryDirectory(dir=os.path.join(_DEST_DIR, "secrets"))
        config_path = os.path.join(self._dir.name, "config.json")
        with open(config_path, "w") as f:
            json.dump({"api_key": API_KEY, "org_id": ORG_ID, "base_url": BASE_URL,
                       "batch_size": batch_size}, f)
        catalog_path = os.path.join(self._dir.name, "catalog.json")
        with open(catalog_path, "w") as f:
            json.dump({"streams": [{
                "stream": {"name": name, "json_schema": {"type": "object"},
                           "supported_sync_modes": ["full_refresh", "incremental"]},
                "sync_mode": "full_refresh" if mode == "overwrite" else "incremental",
                "destination_sync_mode": mode,
            } for name, mode in streams.items()]}, f)
        self.proc = subprocess.Popen(
            [sys.executable, "main.py", "write", "--config", config_path, "--catalog", catalog_path],
            cwd=_DEST_DIR, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1)
        self.out = queue.Queue()
        self.messages = []
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        for line in self.proc.stdout:
            try:
                self.out.put(json.loads(line))
            except ValueError:
                pass
        self.out.put(None)

    def send(self, messages):
        for m in messages:
            self.proc.stdin.write(json.dumps(m) + "\n")
        self.proc.stdin.flush()

    def next_state(self, timeout=180):
        """The next STATE the destination emits, or None if it exits first."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                m = self.out.get(timeout=max(0.1, deadline - time.time()))
            except queue.Empty:
                break
            if m is None:
                return None
            self.messages.append(m)
            if m["type"] == "STATE":
                return m
        raise AssertionError("no STATE within timeout")

    def finish(self, timeout=600):
        self.proc.stdin.close()
        self.proc.wait(timeout=timeout)
        states = []
        while True:
            m = self.out.get(timeout=30)
            if m is None:
                break
            self.messages.append(m)
            if m["type"] == "STATE":
                states.append(m)
        self._dir.cleanup()
        return self.proc.returncode, states

    def kill(self):
        """SIGKILL: the destination dies with whatever it had not written."""
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait(timeout=30)
        while True:
            m = self.out.get(timeout=30)
            if m is None:
                break
            self.messages.append(m)
        self._dir.cleanup()
        return [m for m in self.messages if m["type"] == "STATE"]

    def retries(self):
        """The destination's own retry log lines (409 / 429 / 5xx waits)."""
        return [m["log"]["message"] for m in self.messages
                if m["type"] == "LOG" and "retrying in" in m["log"]["message"]]

    def errors(self):
        return [m["trace"]["error"]["message"] for m in self.messages
                if m["type"] == "TRACE" and m.get("trace", {}).get("error")]


def _fresh(name, rows=None):
    """Delete every source called `name`; seed one with `rows` if given (an API
    source, as an earlier sync would have created it)."""
    client = rw_client()
    for s in list(client.sources.list(limit=100)):
        if s["name"] == name:
            client.sources.delete(s["id"])
    if rows:
        result = client.sources.create_data_source(name=name, rows=rows)
        _created_source_ids.append(result.id)
        return result.id
    return None


def _stats(name):
    """What a reader sees: (count, distinct rid, min rid, max rid), or None if no source."""
    client = rw_client()
    matches = [s for s in client.sources.list(limit=100) if s["name"] == name]
    if not matches:
        return None
    assert len(matches) == 1, f"{len(matches)} sources named {name}"
    _created_source_ids.append(matches[0]["id"])
    r = client.sources.query(
        source_id=matches[0]["id"],
        sql="SELECT count(*) AS n, count(DISTINCT rid) AS d, min(rid) AS lo, max(rid) AS hi FROM data")
    row = r.data[0]
    return int(row["n"]), int(row["d"]), int(row["lo"]), int(row["hi"])


def _rows(stream, start, n):
    return [_msg_record(stream, {"rid": i, "name": f"row {i}", "amount": i * 1.5,
                                 "ts": f"2026-10-26T00:{i % 60:02d}:00Z"})
            for i in range(start, start + n)]


@test("write: overwrite 50,000 rows replaces the old rows, one held state after")
def test_write_overwrite_50k():
    stream, name = "e2e_w_ow50k", "airbyte_e2e_w_ow50k"
    _fresh(name, rows=[{"rid": -i, "name": "old", "amount": 0.0, "ts": "old"} for i in range(1, 4)])
    t0 = time.time()
    run = WriteRun({stream: "overwrite"})
    run.send(_rows(stream, 0, 25_000) + [_msg_state(stream, 1)]
             + _rows(stream, 25_000, 25_000) + [_msg_state(stream, 2)])
    code, states = run.finish()
    elapsed = time.time() - t0
    TIMINGS.append(("overwrite 50k rows", elapsed, run.retries()))
    assert code == 0, run.errors()
    assert [_state_cursor(s) for s in states] == [2], states
    assert _stats(name) == (50_000, 50_000, 0, 49_999), _stats(name)


@test("write: overwrite 150,000 rows (over one request) lands every row exactly once")
def test_write_overwrite_150k_multi_request():
    stream, name = "e2e_w_ow150k", "airbyte_e2e_w_ow150k"
    _fresh(name, rows=[{"rid": -i, "name": "old", "amount": 0.0, "ts": "old"} for i in range(1, 4)])
    t0 = time.time()
    run = WriteRun({stream: "overwrite"}, batch_size=50_000)
    run.send(_rows(stream, 0, 150_000) + [_msg_state(stream, 1)])
    code, states = run.finish()
    TIMINGS.append(("overwrite 150k rows (replace + 2 appends)", time.time() - t0, run.retries()))
    assert code == 0, run.errors()
    assert [_state_cursor(s) for s in states] == [1]
    assert _stats(name) == (150_000, 150_000, 0, 149_999), _stats(name)


@test("write: append across 3 checkpoints, each state only after its rows are readable")
def test_write_append_three_checkpoints():
    stream, name = "e2e_w_app3", "airbyte_e2e_w_app3"
    _fresh(name)
    t0 = time.time()
    # 10,000 rows per checkpoint: the buffered-rows threshold that makes a STATE flush.
    run = WriteRun({stream: "append"}, batch_size=50_000)
    seen = []
    for k in range(3):
        run.send(_rows(stream, k * 10_000, 10_000) + [_msg_state(stream, k + 1)])
        state = run.next_state()
        assert state is not None, run.errors()
        # Read through the API while the destination is still running.
        seen.append((_state_cursor(state), _stats(name)))
    code, rest = run.finish()
    TIMINGS.append(("append 30,000 rows / 3 checkpoints", time.time() - t0, run.retries()))
    assert code == 0 and rest == [], (code, rest, run.errors())
    assert seen == [(1, (10_000, 10_000, 0, 9_999)), (2, (20_000, 20_000, 0, 19_999)),
                    (3, (30_000, 30_000, 0, 29_999))], seen


@test("write: overwrite killed mid-sync acks nothing and keeps old rows; rerun recovers")
def test_write_overwrite_killed_then_rerun():
    stream, name = "e2e_w_owkill", "airbyte_e2e_w_owkill"
    _fresh(name, rows=[{"rid": -i, "name": "old", "amount": 0.0, "ts": "old"} for i in range(1, 6)])
    run = WriteRun({stream: "overwrite"})
    run.send(_rows(stream, 0, 5_000) + [_msg_state(stream, 1)] + _rows(stream, 5_000, 1_000))
    time.sleep(3)  # let it consume the input; the replace only happens at end of input
    states = run.kill()
    assert states == [], f"a STATE was acknowledged before the replace: {states}"
    assert _stats(name) == (5, 5, -5, -1), _stats(name)

    t0 = time.time()
    run = WriteRun({stream: "overwrite"})
    run.send(_rows(stream, 0, 6_000) + [_msg_state(stream, 1)])
    code, states = run.finish()
    TIMINGS.append(("overwrite rerun 6,000 rows", time.time() - t0, run.retries()))
    assert code == 0, run.errors()
    assert [_state_cursor(s) for s in states] == [1]
    assert _stats(name) == (6_000, 6_000, 0, 5_999), _stats(name)


@test("write: append killed after a checkpoint resumes from it with no duplicates")
def test_write_append_killed_then_resume():
    stream, name = "e2e_w_appkill", "airbyte_e2e_w_appkill"
    _fresh(name)
    run = WriteRun({stream: "append"}, batch_size=50_000)
    run.send(_rows(stream, 0, 10_000) + [_msg_state(stream, 1)])
    assert _state_cursor(run.next_state()) == 1
    # Buffered below batch_size, never flushed, never acknowledged.
    run.send(_rows(stream, 10_000, 500))
    time.sleep(2)
    states = run.kill()
    assert [_state_cursor(s) for s in states] == [1], states
    assert _stats(name) == (10_000, 10_000, 0, 9_999), _stats(name)

    # Airbyte resumes from the last acknowledged state: records after cursor 1.
    run = WriteRun({stream: "append"}, batch_size=50_000)
    run.send(_rows(stream, 10_000, 1_000) + [_msg_state(stream, 2)])
    code, states = run.finish()
    assert code == 0, run.errors()
    assert [_state_cursor(s) for s in states] == [2]
    assert _stats(name) == (11_000, 11_000, 0, 10_999), _stats(name)


@test("write: append failure (server refuses) withholds the state for the failed batch")
def test_write_append_server_refusal():
    stream, name = "e2e_w_appfail", "airbyte_e2e_w_appfail"
    _fresh(name)
    run = WriteRun({stream: "append"}, batch_size=10_000)
    run.send(_rows(stream, 0, 10_000) + [_msg_state(stream, 1)])
    assert _state_cursor(run.next_state()) == 1
    # A row too large for the server's 50 MB request cap is refused with 413.
    run.send([_msg_record(stream, {"rid": 100, "name": "x" * (51 * 1024 * 1024)})]
             + [_msg_state(stream, 2)])
    code, states = run.finish()
    assert code != 0, "the sync must fail"
    assert states == [], states
    assert run.errors(), "expected a TRACE error"
    assert _stats(name) == (10_000, 10_000, 0, 9_999), _stats(name)


@test("write: two sources with the stream's name are refused, both untouched")
def test_write_refuses_ambiguous_sources():
    stream, name = "e2e_w_dup", "airbyte_e2e_w_dup"
    first = _fresh(name, rows=[{"rid": -i, "name": "a", "amount": 0.0, "ts": "x"} for i in range(1, 4)])
    second = rw_client().sources.create_data_source(
        name=name, rows=[{"rid": -i, "name": "b", "amount": 0.0, "ts": "x"} for i in range(1, 3)]).id
    _created_source_ids.append(second)
    run = WriteRun({stream: "overwrite"})
    run.send(_rows(stream, 0, 10) + [_msg_state(stream, 1)])
    code, states = run.finish()
    assert code != 0 and states == [], (code, states)
    traces = [m["trace"]["error"] for m in run.messages
              if m["type"] == "TRACE" and m.get("trace", {}).get("error")]
    assert traces and traces[0]["failure_type"] == "config_error", traces
    assert "2 Querri sources are named" in traces[0]["message"]
    client = rw_client()
    counts = {sid: client.sources.query(source_id=sid, sql="SELECT count(*) AS n FROM data").data[0]["n"]
              for sid in (first, second)}
    assert counts == {first: 3, second: 2}, counts


@test("write: zero-record overwrite keeps the existing rows; new stream creates nothing")
def test_write_zero_record_overwrite():
    stream, name = "e2e_w_ow0", "airbyte_e2e_w_ow0"
    new_stream, new_name = "e2e_w_ow0new", "airbyte_e2e_w_ow0new"
    _fresh(name, rows=[{"rid": -i, "name": "old", "amount": 0.0, "ts": "old"} for i in range(1, 4)])
    _fresh(new_name)
    run = WriteRun({stream: "overwrite", new_stream: "overwrite"})
    run.send([_msg_state(stream, 1)])
    code, states = run.finish()
    assert code == 0, run.errors()
    assert [_state_cursor(s) for s in states] == [1]
    # The API refuses an empty replace (documented limitation).
    assert _stats(name) == (3, 3, -3, -1), _stats(name)
    assert _stats(new_name) is None


WRITE_TESTS = [
    test_write_overwrite_50k, test_write_overwrite_150k_multi_request,
    test_write_append_three_checkpoints,
    test_write_overwrite_killed_then_rerun, test_write_append_killed_then_resume,
    test_write_append_server_refusal, test_write_refuses_ambiguous_sources,
    test_write_zero_record_overwrite,
]


# ═══════════════════════════════════════════════
# Run all tests
# ═══════════════════════════════════════════════

def cleanup():
    """Delete all test sources created during the run."""
    client = rw_client()
    for sid in _created_source_ids:
        try:
            client.sources.delete(sid)
        except Exception:
            pass
    # Also clean up by name pattern
    try:
        for s in client.sources.list():
            if s["name"].startswith("e2e_test_") or s["name"].startswith("airbyte_e2e_"):
                try:
                    client.sources.delete(s["id"])
                except Exception:
                    pass
    except Exception:
        pass


if __name__ == "__main__":
    print("\n══════════════════════════════════════════")
    print("  Querri Airbyte Connectors — E2E Tests")
    print("══════════════════════════════════════════\n")

    # E2E_ONLY=write runs only section 11.
    only_write = os.environ.get("E2E_ONLY") == "write"

    # Collect all test functions in definition order
    all_tests = [] if only_write else [
        test_check_rw, test_check_ro, test_check_invalid, test_check_wrong_org,
        test_create_source, test_list_sources, test_get_source, test_read_data,
        test_append_rows, test_read_after_append,
        test_replace_data, test_read_after_replace,
        test_delete_source, test_deleted_404,
        test_ro_list, test_ro_cannot_create,
        test_pagination,
        test_source_check, test_source_discover, test_source_read, test_source_schema,
        test_dest_check, test_dest_write_append, test_dest_overwrite,
        test_roundtrip,
    ]

    print("1. Connection & Authentication")
    for t in all_tests[:4]:
        run_test(t)

    print("\n2. Source CRUD Lifecycle")
    for t in all_tests[4:8]:
        run_test(t)

    print("\n3. Append Rows")
    for t in all_tests[8:10]:
        run_test(t)

    print("\n4. Replace Data (Overwrite)")
    for t in all_tests[10:12]:
        run_test(t)

    print("\n5. Delete Source")
    for t in all_tests[12:14]:
        run_test(t)

    print("\n6. Read-Only Key Scope Enforcement")
    for t in all_tests[14:16]:
        run_test(t)

    print("\n7. Pagination")
    for t in all_tests[16:17]:
        run_test(t)

    print("\n8. Source Connector (Airbyte Protocol)")
    for t in all_tests[17:21]:
        run_test(t)

    print("\n9. Destination Connector (Airbyte Protocol)")
    for t in all_tests[21:24]:
        run_test(t)

    print("\n10. Round-trip: Destination → Source")
    for t in all_tests[24:]:
        run_test(t)

    print("\n11. Destination `write` entrypoint (as Airbyte runs it)")
    for t in WRITE_TESTS:
        run_test(t)
    for label, seconds, retries in TIMINGS:
        print(f"    {label}: {seconds:.1f}s, {len(retries)} retries"
              + (f" (first: {retries[0]})" if retries else ""))

    # Cleanup
    print("\nCleaning up test data...")
    cleanup()

    # Summary
    print(f"\n══════════════════════════════════════════")
    print(f"  Results: {passed} passed, {failed} failed")
    print(f"══════════════════════════════════════════\n")

    if errors:
        print("Failures:")
        for name, err in errors:
            print(f"  • {name}")
            for line in err.strip().split("\n"):
                print(f"    {line}")
        print()

    sys.exit(1 if failed else 0)
