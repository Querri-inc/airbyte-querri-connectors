"""Unit tests for the Querri destination connector.

The SDK client is replaced by ``FakeQuerri``, an in-memory model of the Querri
public API's source writes: the same SDK result types and exception classes, the
server's refusals (an empty ``rows``, an append to a source with no data, more
than 100,000 rows in one request), and injectable faults. The tests read the
fake's tables, which is what a Querri reader would see, rather than asserting
which methods were called.

The state tests drive ``write()`` lazily and, at the moment each STATE comes
out, compare the fake's committed rows with every record sent before it: that is
the Airbyte destination contract.
"""

import json
import logging
from typing import Any, Dict, List

import httpx
import pytest
from airbyte_cdk.models import (
    AirbyteGlobalState,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStream,
    AirbyteStreamState,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    Status,
    StreamDescriptor,
    SyncMode,
    Type,
)
from airbyte_cdk.utils.traced_exception import AirbyteTracedException
from querri._exceptions import (
    APIError,
    AuthenticationError,
    ConflictError,
    NotFoundError,
    PermissionError as QuerriPermissionError,
    ServerError,
    ValidationError,
)
from querri.types.data import DataWriteResult, Source

from destination_querri import destination as dest_module
from destination_querri.destination import DestinationQuerri, _validate_base_url

SERVER_MAX_ROWS = 100_000


# ── Fake Querri API ──


class FakeSources:
    def __init__(self, api: "FakeQuerri"):
        self.api = api

    def list(self, limit: int = 25):
        self.api.calls.append(("list",))
        self.api._maybe_fail("list")
        # The server sorts newest first.
        return [
            {"id": sid, "name": t["name"], "materialized": t["rows"] is not None}
            for sid, t in sorted(self.api.tables.items(), key=lambda kv: -kv[1]["updated"])
        ]

    @staticmethod
    def _check_rows(rows):
        if not rows:
            raise ValidationError("rows: List should have at least 1 item", status=422)
        if len(rows) > SERVER_MAX_ROWS:
            raise ValidationError("too many rows", status=400)

    def create_data_source(self, *, name: str, rows: List[Dict[str, Any]]):
        self.api.calls.append(("create", name, len(rows)))
        self.api._maybe_fail("create")
        self._check_rows(rows)
        sid = self.api.seed(name, rows)
        return Source(id=sid, name=name, row_count=len(rows))

    def append_rows(self, source_id: str, *, rows: List[Dict[str, Any]]):
        self.api.calls.append(("append", source_id, len(rows)))
        self.api._maybe_fail("append")
        self._check_rows(rows)
        table = self.api._table(source_id)
        if table["rows"] is None:
            raise ValidationError("Source has no existing data to append to.", status=400, code="no_data")
        table["rows"].extend(rows)
        table["updated"] = self.api._tick()
        return DataWriteResult(id=source_id, name=table["name"], row_count=len(table["rows"]))

    def replace_data(self, source_id: str, *, rows: List[Dict[str, Any]]):
        self.api.calls.append(("replace", source_id, len(rows)))
        self.api._maybe_fail("replace")
        if not self.api.write_scope:
            raise QuerriPermissionError("Missing scope", status=403, code="insufficient_scope")
        self._check_rows(rows)
        table = self.api._table(source_id)
        table["rows"] = list(rows)
        table["updated"] = self.api._tick()
        return DataWriteResult(id=source_id, name=table["name"], row_count=len(rows))


class FakeQuerri:
    def __init__(self):
        self.tables: Dict[str, Dict[str, Any]] = {}
        self.calls: List[tuple] = []
        # op -> exceptions raised by its successive calls (None = succeed)
        self.faults: Dict[str, List[Any]] = {}
        self.write_scope = True
        self._clock = 0
        self.sources = FakeSources(self)

    def _tick(self) -> int:
        self._clock += 1
        return self._clock

    def _maybe_fail(self, op: str):
        queue = self.faults.get(op)
        if queue:
            exc = queue.pop(0)
            if exc is not None:
                raise exc

    def _table(self, source_id: str):
        if source_id not in self.tables:
            raise NotFoundError(f"Source {source_id} not found.", status=404)
        return self.tables[source_id]

    def seed(self, name: str, rows, materialized: bool = True) -> str:
        sid = f"src-{len(self.tables) + 1}"
        self.tables[sid] = {"name": name, "rows": list(rows) if materialized else None,
                            "updated": self._tick()}
        return sid

    def rows_of(self, name: str):
        matches = [t for t in self.tables.values() if t["name"] == name]
        assert len(matches) == 1, f"{len(matches)} sources named {name}"
        return matches[0]["rows"]

    def writes(self):
        return [c for c in self.calls if c[0] in ("create", "replace", "append")]


# ── Message helpers ──


def _catalog(streams: Dict[str, DestinationSyncMode]) -> ConfiguredAirbyteCatalog:
    return ConfiguredAirbyteCatalog(streams=[
        ConfiguredAirbyteStream(
            stream=AirbyteStream(name=name, json_schema={"type": "object"},
                                 supported_sync_modes=[SyncMode.full_refresh]),
            sync_mode=SyncMode.full_refresh,
            destination_sync_mode=mode,
        )
        for name, mode in streams.items()
    ])


def _record(stream: str, data: Dict[str, Any]) -> AirbyteMessage:
    return AirbyteMessage(type=Type.RECORD,
                          record=AirbyteRecordMessage(stream=stream, data=data, emitted_at=0))


def _rows(stream: str, start: int, n: int) -> List[AirbyteMessage]:
    return [_record(stream, {"id": i}) for i in range(start, start + n)]


def _stream_state(stream: str, cursor: int) -> AirbyteMessage:
    return AirbyteMessage(type=Type.STATE, state=AirbyteStateMessage(
        type=AirbyteStateType.STREAM,
        stream=AirbyteStreamState(stream_descriptor=StreamDescriptor(name=stream),
                                  stream_state=AirbyteStateBlob(cursor=cursor))))


def _global_state(cursor: int) -> AirbyteMessage:
    return AirbyteMessage(type=Type.STATE, state=AirbyteStateMessage(
        type=AirbyteStateType.GLOBAL,
        global_=AirbyteGlobalState(shared_state=AirbyteStateBlob(cursor=cursor), stream_states=[])))


def _cursor(message: AirbyteMessage) -> int:
    state = message.state
    if state.type == AirbyteStateType.STREAM:
        return state.stream.stream_state.cursor
    return state.global_.shared_state.cursor


def _stream_of(message: AirbyteMessage) -> str:
    return message.state.stream.stream_descriptor.name


CONFIG = {"api_key": "qk_test", "org_id": "org_1", "base_url": "http://localhost/api/v1"}


@pytest.fixture
def fake(monkeypatch):
    api = FakeQuerri()
    monkeypatch.setattr(DestinationQuerri, "_make_client", staticmethod(lambda config: api))
    monkeypatch.setattr(DestinationQuerri, "_sleep", staticmethod(lambda s: None))
    return api


def drive(api: FakeQuerri, catalog, messages, config=None):
    """Run write(); at each STATE it emits, snapshot what readers see.

    Returns (states, checkpoints, error); each checkpoint is
    (state, records sent before it per stream, rows per source name).
    """
    sent: Dict[str, List[Dict[str, Any]]] = {}

    def feed():
        for m in messages:
            if m.type == Type.RECORD:
                sent.setdefault(m.record.stream, []).append(m.record.data)
            yield m

    checkpoints = []
    error = None
    try:
        for out in DestinationQuerri().write(config or CONFIG, catalog, feed()):
            assert out.type == Type.STATE
            snapshot = {t["name"]: list(t["rows"] or []) for t in api.tables.values()}
            checkpoints.append((out, {k: list(v) for k, v in sent.items()}, snapshot))
    except Exception as e:  # noqa: BLE001 — returned for the test to assert on
        error = e
    return [c[0] for c in checkpoints], checkpoints, error


# ── check() ──


class TestCheck:
    def test_success_lists_and_probes_write_scope(self, fake):
        # The probe's unknown id is refused (404): the scope is present.
        result = DestinationQuerri().check(logging.getLogger(), CONFIG)
        assert result.status == Status.SUCCEEDED
        assert ("replace", dest_module._PROBE_SOURCE_ID, 1) in fake.calls
        assert fake.tables == {}

    def test_missing_write_scope_fails(self, fake):
        fake.write_scope = False
        result = DestinationQuerri().check(logging.getLogger(), CONFIG)
        assert result.status == Status.FAILED
        assert "data:write" in result.message

    def test_auth_failure(self, fake):
        fake.faults["list"] = [AuthenticationError("bad key", status=401)]
        result = DestinationQuerri().check(logging.getLogger(), CONFIG)
        assert result.status == Status.FAILED
        assert "Invalid API key or org_id" in result.message

    def test_http_non_local_host_refused(self):
        result = DestinationQuerri().check(
            logging.getLogger(), {**CONFIG, "base_url": "http://example.com/api/v1"})
        assert result.status == Status.FAILED
        assert "https" in result.message


class TestBaseUrl:
    @pytest.mark.parametrize("url,host", [
        ("https://app.querri.com/api/v1", "https://app.querri.com"),
        ("https://acme.querri.com/api/v1/", "https://acme.querri.com"),
        ("http://localhost/api/v1", "http://localhost"),
        ("http://host.docker.internal:8080/api/v1", "http://host.docker.internal:8080"),
    ])
    def test_allowed(self, url, host):
        assert _validate_base_url(url) == host

    @pytest.mark.parametrize("url", ["http://app.querri.com/api/v1", "ftp://x/api/v1", "localhost"])
    def test_refused(self, url):
        with pytest.raises(Exception):
            _validate_base_url(url)

    def test_spec_pattern_matches_the_validator(self):
        import re
        pattern = DestinationQuerri().spec(logging.getLogger()).connectionSpecification[
            "properties"]["base_url"]["pattern"]
        for url in ["https://app.querri.com/api/v1", "http://localhost/api/v1",
                    "http://host.docker.internal:8080/api/v1"]:
            assert re.match(pattern, url), url
        for url in ["http://app.querri.com/api/v1", "http://localhost.evil.com/api/v1"]:
            assert not re.match(pattern, url), url


# ── Catalog and spec ──


class TestCatalog:
    def test_dedup_mode_refused(self, fake):
        _, _, error = drive(fake, _catalog({"users": DestinationSyncMode.append_dedup}), [])
        assert isinstance(error, AirbyteTracedException)
        assert fake.tables == {}

    def test_spec_offers_only_overwrite_and_append(self):
        spec = DestinationQuerri().spec(logging.getLogger())
        assert {m.value for m in spec.supported_destination_sync_modes} == {"overwrite", "append"}


# ── State ordering: append ──


class TestAppendState:
    def test_state_only_after_its_records_are_written(self, fake):
        msgs = (_rows("users", 0, 5) + [_stream_state("users", 1)]
                + _rows("users", 5, 7) + [_stream_state("users", 2)]
                + _rows("users", 12, 3) + [_stream_state("users", 3)])
        states, checkpoints, error = drive(fake, _catalog({"users": DestinationSyncMode.append}), msgs)
        assert error is None
        assert [_cursor(s) for s in states] == [1, 2, 3]
        for _, sent, snapshot in checkpoints:
            assert snapshot["airbyte_users"] == sent["users"]

    def test_failed_flush_withholds_the_state(self, fake):
        fake.seed("airbyte_users", [{"id": -1}])
        fake.faults["append"] = [None, ServerError("boom", status=500)]
        msgs = (_rows("users", 0, 2) + [_stream_state("users", 1)]
                + _rows("users", 2, 2) + [_stream_state("users", 2)])
        states, _, error = drive(fake, _catalog({"users": DestinationSyncMode.append}), msgs)
        assert isinstance(error, ServerError)
        assert [_cursor(s) for s in states] == [1]

    def test_append_500_is_not_resent(self, fake):
        # A 5xx on POST may have been applied; resending could duplicate rows.
        fake.seed("airbyte_users", [{"id": -1}])
        fake.faults["append"] = [ServerError("boom", status=500)]
        _, _, error = drive(fake, _catalog({"users": DestinationSyncMode.append}), _rows("users", 0, 3))
        assert isinstance(error, ServerError)
        assert [c[0] for c in fake.writes()] == ["append"]

    def test_append_timeout_is_not_resent(self, fake):
        fake.seed("airbyte_users", [{"id": -1}])
        timeout = APIError("timed out", status=0)
        timeout.__cause__ = httpx.ReadTimeout("read")
        fake.faults["append"] = [timeout]
        _, _, error = drive(fake, _catalog({"users": DestinationSyncMode.append}), _rows("users", 0, 1))
        assert error is timeout
        assert [c[0] for c in fake.writes()] == ["append"]

    def test_append_refused_connection_is_resent(self, fake):
        fake.seed("airbyte_users", [{"id": -1}])
        refused = APIError("refused", status=0)
        refused.__cause__ = httpx.ConnectError("refused")
        fake.faults["append"] = [refused]
        _, _, error = drive(fake, _catalog({"users": DestinationSyncMode.append}), _rows("users", 0, 1))
        assert error is None
        assert fake.rows_of("airbyte_users") == [{"id": -1}, {"id": 0}]

    def test_conflict_is_retried(self, fake):
        fake.seed("airbyte_users", [{"id": -1}])
        fake.faults["append"] = [ConflictError("sync_in_progress", status=409)] * 2
        states, _, error = drive(fake, _catalog({"users": DestinationSyncMode.append}),
                                 _rows("users", 0, 2) + [_stream_state("users", 1)])
        assert error is None and len(states) == 1
        assert fake.rows_of("airbyte_users") == [{"id": -1}, {"id": 0}, {"id": 1}]

    def test_batch_size_bounds_buffer(self, fake):
        _, _, error = drive(fake, _catalog({"users": DestinationSyncMode.append}),
                            _rows("users", 0, 25), config={**CONFIG, "batch_size": 10})
        assert error is None
        assert [c[2] for c in fake.writes()] == [10, 10, 5]
        assert fake.rows_of("airbyte_users") == [{"id": i} for i in range(25)]

    def test_unmaterialized_source_first_write_is_replace(self, fake):
        fake.seed("airbyte_users", [], materialized=False)
        _, _, error = drive(fake, _catalog({"users": DestinationSyncMode.append}), _rows("users", 0, 2))
        assert error is None
        assert fake.rows_of("airbyte_users") == [{"id": 0}, {"id": 1}]


# ── State ordering: overwrite ──


class TestOverwriteState:
    def test_states_held_until_replace_succeeds(self, fake):
        fake.seed("airbyte_items", [{"old": True}])
        msgs = (_rows("items", 0, 3) + [_stream_state("items", 1)]
                + _rows("items", 3, 3) + [_stream_state("items", 2)])
        states, checkpoints, error = drive(fake, _catalog({"items": DestinationSyncMode.overwrite}), msgs)
        assert error is None
        # Only the latest state, and only once the table holds all six rows.
        assert [_cursor(s) for s in states] == [2]
        _, sent, snapshot = checkpoints[0]
        assert snapshot["airbyte_items"] == sent["items"]

    def test_failed_final_replace_emits_no_state_and_keeps_old_rows(self, fake):
        fake.seed("airbyte_items", [{"old": True}])
        fake.faults["replace"] = [ValidationError("bad rows", status=400)]
        msgs = (_rows("items", 0, 3) + [_stream_state("items", 1)]
                + _rows("items", 3, 3) + [_stream_state("items", 2)])
        states, _, error = drive(fake, _catalog({"items": DestinationSyncMode.overwrite}), msgs)
        assert isinstance(error, ValidationError)
        assert states == []
        assert fake.rows_of("airbyte_items") == [{"old": True}]

    def test_replace_5xx_is_retried(self, fake):
        # PUT is idempotent: a resend after a 5xx cannot duplicate rows.
        fake.seed("airbyte_items", [{"old": True}])
        fake.faults["replace"] = [ServerError("boom", status=502)]
        states, _, error = drive(fake, _catalog({"items": DestinationSyncMode.overwrite}),
                                 _rows("items", 0, 2) + [_stream_state("items", 1)])
        assert error is None and len(states) == 1
        assert fake.rows_of("airbyte_items") == [{"id": 0}, {"id": 1}]

    def test_rerun_after_failure_recovers(self, fake):
        fake.seed("airbyte_items", [{"old": True}])
        fake.faults["replace"] = [ValidationError("bad", status=400)]
        msgs = _rows("items", 0, 4) + [_stream_state("items", 1)]
        catalog = _catalog({"items": DestinationSyncMode.overwrite})
        assert drive(fake, catalog, msgs)[0] == []
        states, _, error = drive(fake, catalog, msgs)
        assert error is None and [_cursor(s) for s in states] == [1]
        assert fake.rows_of("airbyte_items") == [{"id": i} for i in range(4)]

    def test_zero_records_keeps_rows_and_acks(self, fake):
        fake.seed("airbyte_items", [{"old": True}])
        states, _, error = drive(fake, _catalog({"items": DestinationSyncMode.overwrite}),
                                 [_stream_state("items", 1)])
        assert error is None and len(states) == 1
        # The API refuses an empty replace, so the old rows stay (documented).
        assert fake.rows_of("airbyte_items") == [{"old": True}]
        assert fake.writes() == []

    def test_zero_records_new_stream_creates_nothing(self, fake):
        _, _, error = drive(fake, _catalog({"items": DestinationSyncMode.overwrite}), [])
        assert error is None and fake.tables == {}

    def test_new_stream_is_created(self, fake):
        states, _, error = drive(fake, _catalog({"items": DestinationSyncMode.overwrite}),
                                 _rows("items", 0, 3) + [_stream_state("items", 1)])
        assert error is None and len(states) == 1
        assert fake.rows_of("airbyte_items") == [{"id": i} for i in range(3)]

    def test_fits_one_request_is_one_put(self, fake, monkeypatch):
        monkeypatch.setattr(dest_module, "MAX_BATCH_SIZE", 10)
        sid = fake.seed("airbyte_items", [{"old": True}])
        _, _, error = drive(fake, _catalog({"items": DestinationSyncMode.overwrite}),
                            _rows("items", 0, 10), config={**CONFIG, "batch_size": 3})
        assert error is None
        # batch_size does not split an overwrite that fits in one atomic PUT.
        assert fake.writes() == [("replace", sid, 10)]

    def test_over_cap_is_replace_then_appends(self, fake, monkeypatch):
        monkeypatch.setattr(dest_module, "MAX_BATCH_SIZE", 10)
        sid = fake.seed("airbyte_items", [{"old": True}])
        _, _, error = drive(fake, _catalog({"items": DestinationSyncMode.overwrite}),
                            _rows("items", 0, 25), config={**CONFIG, "batch_size": 10})
        assert error is None
        assert fake.writes() == [("replace", sid, 10), ("append", sid, 10), ("append", sid, 5)]
        assert fake.rows_of("airbyte_items") == [{"id": i} for i in range(25)]

    def test_byte_cap_splits_requests(self, fake, monkeypatch):
        monkeypatch.setattr(dest_module, "MAX_REQUEST_BYTES", 200)
        fake.seed("airbyte_items", [{"old": True}])
        msgs = [_record("items", {"id": i, "pad": "x" * 50}) for i in range(10)]
        _, _, error = drive(fake, _catalog({"items": DestinationSyncMode.overwrite}), msgs)
        assert error is None
        writes = fake.writes()
        assert len(writes) > 1 and writes[0][0] == "replace"
        row_bytes = len(json.dumps({"id": 0, "pad": "x" * 50}, separators=(",", ":"))) + 1
        assert all(c[2] * row_bytes <= 200 for c in writes)
        assert len(fake.rows_of("airbyte_items")) == 10

    def test_rows_are_spooled_not_held(self, fake):
        writer = dest_module._OverwriteWriter(
            dest_module._StreamConfig("s", "airbyte_s", DestinationSyncMode.overwrite),
            dest_module._Writer(fake), 10)
        for i in range(100):
            writer.add({"id": i})
        assert writer.count == 100
        assert not any(isinstance(v, list) for v in vars(writer).values())
        writer.finalize()
        writer.close()
        assert fake.rows_of("airbyte_s") == [{"id": i} for i in range(100)]


# ── Interleaved streams ──


class TestInterleaved:
    def test_mixed_modes_per_stream_state(self, fake):
        fake.seed("airbyte_items", [{"old": True}])
        catalog = _catalog({"events": DestinationSyncMode.append, "items": DestinationSyncMode.overwrite})
        msgs = (_rows("events", 0, 2) + _rows("items", 0, 2)
                + [_stream_state("events", 1), _stream_state("items", 1)]
                + _rows("items", 2, 2) + _rows("events", 2, 2)
                + [_stream_state("items", 2), _stream_state("events", 2)])
        states, checkpoints, error = drive(fake, catalog, msgs)
        assert error is None
        # events states pass through once events is flushed; items waits for its replace.
        assert [(_stream_of(s), _cursor(s)) for s in states] == [
            ("events", 1), ("events", 2), ("items", 2)]
        for state, sent, snapshot in checkpoints:
            name = _stream_of(state)
            assert snapshot[f"airbyte_{name}"] == sent[name]
        # Readers saw the old items table until the replace landed.
        assert checkpoints[0][2]["airbyte_items"] == [{"old": True}]

    def test_global_state_waits_for_overwrite(self, fake):
        catalog = _catalog({"events": DestinationSyncMode.append, "items": DestinationSyncMode.overwrite})
        msgs = (_rows("events", 0, 2) + _rows("items", 0, 2) + [_global_state(1)]
                + _rows("events", 2, 1) + [_global_state(2)])
        states, checkpoints, error = drive(fake, catalog, msgs)
        assert error is None
        assert [_cursor(s) for s in states] == [2]
        _, sent, snapshot = checkpoints[0]
        assert snapshot["airbyte_events"] == sent["events"]
        assert snapshot["airbyte_items"] == sent["items"]

    def test_global_state_append_only_passes_through(self, fake):
        msgs = _rows("events", 0, 2) + [_global_state(1)] + _rows("events", 2, 2) + [_global_state(2)]
        states, checkpoints, error = drive(fake, _catalog({"events": DestinationSyncMode.append}), msgs)
        assert error is None and [_cursor(s) for s in states] == [1, 2]
        for _, sent, snapshot in checkpoints:
            assert snapshot["airbyte_events"] == sent["events"]

    def test_overwrite_failure_after_append_states(self, fake):
        # The append stream's states were durable; the overwrite's never come out.
        fake.seed("airbyte_items", [{"old": True}])
        fake.faults["replace"] = [ValidationError("bad", status=400)]
        catalog = _catalog({"events": DestinationSyncMode.append, "items": DestinationSyncMode.overwrite})
        msgs = (_rows("events", 0, 2) + _rows("items", 0, 2)
                + [_stream_state("events", 1), _stream_state("items", 1)])
        states, _, error = drive(fake, catalog, msgs)
        assert isinstance(error, ValidationError)
        assert [_stream_of(s) for s in states] == ["events"]
        assert fake.rows_of("airbyte_items") == [{"old": True}]


# ── Source resolution ──


class TestResolution:
    def test_list_failure_fails_the_sync_instead_of_duplicating(self, fake):
        fake.seed("airbyte_users", [{"id": -1}])
        fake.faults["list"] = [ValidationError("bad", status=400)]
        _, _, error = drive(fake, _catalog({"users": DestinationSyncMode.append}), _rows("users", 0, 1))
        assert isinstance(error, ValidationError)
        assert len(fake.tables) == 1

    def test_list_5xx_is_retried(self, fake):
        fake.seed("airbyte_users", [{"id": -1}])
        fake.faults["list"] = [ServerError("boom", status=503)]
        _, _, error = drive(fake, _catalog({"users": DestinationSyncMode.append}), _rows("users", 0, 1))
        assert error is None
        assert fake.rows_of("airbyte_users") == [{"id": -1}, {"id": 0}]

    def test_duplicate_names_write_most_recent(self, fake):
        fake.seed("airbyte_users", [{"id": "older"}])
        newer = fake.seed("airbyte_users", [{"id": "newer"}])
        _, _, error = drive(fake, _catalog({"users": DestinationSyncMode.overwrite}), _rows("users", 0, 1))
        assert error is None
        assert fake.tables[newer]["rows"] == [{"id": 0}]

    def test_custom_prefix(self, fake):
        drive(fake, _catalog({"users": DestinationSyncMode.append}), _rows("users", 0, 1),
              config={**CONFIG, "source_name_prefix": "ab_"})
        assert fake.rows_of("ab_users") == [{"id": 0}]

    def test_unconfigured_stream_ignored(self, fake):
        _, _, error = drive(fake, _catalog({"users": DestinationSyncMode.append}),
                            _rows("users", 0, 1) + _rows("other", 0, 1))
        assert error is None
        assert [t["name"] for t in fake.tables.values()] == ["airbyte_users"]


# ── Entrypoint ──


class TestEntrypoint:
    """The CLI Airbyte runs. It once used the source launcher, which has no
    `write` command, so every sync failed before reading a message."""

    def _run(self, tmp_path, *args, stdin=""):
        import subprocess
        import sys
        from pathlib import Path

        main = Path(__file__).resolve().parents[1] / "main.py"
        return subprocess.run([sys.executable, str(main), *args], input=stdin,
                              capture_output=True, text=True, timeout=60)

    def test_write_command_is_accepted(self, tmp_path):
        config = tmp_path / "config.json"
        # http on a non-local host: refused before any request is made.
        config.write_text(json.dumps({**CONFIG, "base_url": "http://example.com/api/v1"}))
        catalog = tmp_path / "catalog.json"
        catalog.write_text(json.dumps({"streams": [{
            "stream": {"name": "s", "json_schema": {"type": "object"},
                       "supported_sync_modes": ["full_refresh"]},
            "sync_mode": "full_refresh", "destination_sync_mode": "append"}]}))
        result = self._run(tmp_path, "write", "--config", str(config), "--catalog", str(catalog))
        assert "invalid choice" not in result.stderr
        traces = [json.loads(line) for line in result.stdout.splitlines()
                  if line.startswith("{") and '"TRACE"' in line]
        assert traces and traces[0]["trace"]["error"]["failure_type"] == "config_error"
        assert "https" in traces[0]["trace"]["error"]["message"]

    def test_spec_command(self, tmp_path):
        result = self._run(tmp_path, "spec")
        specs = [json.loads(line) for line in result.stdout.splitlines() if '"SPEC"' in line]
        assert specs[0]["spec"]["supported_destination_sync_modes"] == ["overwrite", "append"]


class TestRetryDelay:
    def test_rate_limit_waits_what_the_server_asked(self):
        from querri._exceptions import RateLimitError
        e = RateLimitError("slow down", status=429, retry_after=60.0)
        assert dest_module._retry_delay(e, 0) == 60.0
