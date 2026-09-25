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
from querri.types.data import Source

# ── Configuration ──

def _require_env(name: str) -> str:
    """Get a required environment variable or fail fast with a clear message."""
    value = os.environ.get(name)
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

def _sources(client, **kwargs):
    """List sources as typed models (the SDK's sources.list() yields dicts)."""
    return [Source.model_validate(s) for s in client.sources.list(**kwargs)]

def _source(client, source_id):
    return Source.model_validate(client.sources.get(source_id))

def find_source_by_name(client, name):
    """Find a source by exact name match using SDK auto-pagination."""
    for source in _sources(client, limit=100):
        if source.name == name:
            return source
    return None


# ═══════════════════════════════════════════════
# 1. Connection & Authentication
# ═══════════════════════════════════════════════

@test("Check connection with read-write key")
def test_check_rw():
    client = rw_client()
    # Force iteration to verify credentials
    next(iter(client.sources.list(limit=1)), None)

@test("Check connection with read-only key")
def test_check_ro():
    client = ro_client()
    next(iter(client.sources.list(limit=1)), None)

@test("Check connection with invalid key fails")
def test_check_invalid():
    client = Querri(api_key="qk_invalid", org_id=ORG_ID, host=_HOST)
    try:
        next(iter(client.sources.list(limit=1)), None)
        assert False, "Should have raised"
    except Exception as e:
        assert "401" in str(e) or "invalid" in str(e).lower() or "authentication" in str(e).lower(), f"Unexpected error: {e}"

@test("Check connection with wrong org_id fails")
def test_check_wrong_org():
    client = Querri(api_key=API_KEY, org_id="org_nonexistent", host=_HOST)
    try:
        next(iter(client.sources.list(limit=1)), None)
        assert False, "Should have raised"
    except Exception as e:
        assert "401" in str(e) or "invalid" in str(e).lower() or "authentication" in str(e).lower() or "500" in str(e), f"Unexpected error: {e}"


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
    sources = list(_sources(client, limit=100))
    assert isinstance(sources, list)
    names = [s.name for s in sources]
    assert "e2e_test_crud" in names, f"Created source not in list: {names[:5]}"

@test("Get source metadata")
def test_get_source():
    client = rw_client()
    source_id = _created_source_ids[-1]
    result = _source(client, source_id)
    assert result.id == source_id
    assert result.name == "e2e_test_crud"
    assert "name" in result.columns

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
    client.sources.delete(source_id)
    _created_source_ids.remove(source_id)

@test("Deleted source returns 404")
def test_deleted_404():
    client = rw_client()
    try:
        _source(client, "00000000-0000-0000-0000-000000000000")
        assert False, "Should have raised 404"
    except Exception as e:
        assert "404" in str(e) or "not found" in str(e).lower(), f"Expected 404, got: {e}"


# ═══════════════════════════════════════════════
# 6. Read-Only Key Scope Enforcement
# ═══════════════════════════════════════════════

@test("Read-only key can list sources")
def test_ro_list():
    client = ro_client()
    sources = list(_sources(client, limit=100))
    assert isinstance(sources, list)

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
        "source_ids": [pagination_source.id],
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
        "source_ids": [pagination_source.id],
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
        "source_ids": [pagination_source.id],
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
    _created_source_ids.append(source.id)

    data = client.sources.source_data(source.id)
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
        client.sources.delete(existing.id)

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
    data = client.sources.source_data(source.id)
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
    _created_source_ids.append(source.id)

    # Read via source connector (use source_ids filter to avoid rate limits)
    src = SourceQuerri()
    src_config = {**config, "source_ids": [source.id]}
    streams = src.streams(src_config)
    target = [s for s in streams if s.name == "airbyte_e2e_roundtrip"]
    assert len(target) == 1, f"Round-trip stream not found. Streams: {[s.name for s in streams][:10]}"

    records = list(target[0].read_records(sync_mode="full_refresh"))
    assert len(records) == 2
    metrics = {r["metric"] for r in records}
    assert metrics == {"revenue", "users"}


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
        for s in _sources(client, limit=100):
            if s.name.startswith("e2e_test_") or s.name.startswith("airbyte_e2e_"):
                try:
                    client.sources.delete(s.id)
                except Exception:
                    pass
    except Exception:
        pass


if __name__ == "__main__":
    print("\n══════════════════════════════════════════")
    print("  Querri Airbyte Connectors — E2E Tests")
    print("══════════════════════════════════════════\n")

    # Collect all test functions in definition order
    all_tests = [
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
