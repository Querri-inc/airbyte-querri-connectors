"""Unit tests for the Querri destination connector (SDK-based)."""

from unittest.mock import MagicMock, call, patch

import pytest

from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
    Type,
)
from querri.types.data import DataWriteResult, Source
from querri._exceptions import (
    AuthenticationError,
    NotFoundError,
    RateLimitError,
    ServerError,
)

from destination_querri.destination import (
    DEFAULT_BATCH_SIZE,
    MAX_BATCH_SIZE,
    DestinationQuerri,
    _StreamConfig,
)


# ── Helpers ──


def _mock_client():
    """Create a MagicMock Querri SDK client with sensible defaults."""
    client = MagicMock()
    client.sources.list.return_value = []
    client.sources.create_data_source.return_value = Source(
        id="new-src", name="test", columns=["col"], row_count=1,
    )
    client.sources.append_rows.return_value = DataWriteResult(
        id="src-1", name="test", columns=["col"], row_count=1,
    )
    client.sources.replace_data.return_value = DataWriteResult(
        id="src-1", name="test", columns=["col"], row_count=1,
    )
    return client


def _source_dict(id="src-1", name="airbyte_users", columns=None, row_count=0):
    return {"id": id, "name": name, "columns": columns or [], "row_count": row_count}


def _catalog(streams_config):
    """Build a ConfiguredAirbyteCatalog from {name: DestinationSyncMode} dict."""
    configured_streams = []
    for name, mode in streams_config.items():
        configured_streams.append(
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name=name,
                    json_schema={"type": "object"},
                    supported_sync_modes=[SyncMode.full_refresh],
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=mode,
            )
        )
    return ConfiguredAirbyteCatalog(streams=configured_streams)


def _record(stream, data):
    return AirbyteMessage(
        type=Type.RECORD,
        record=AirbyteRecordMessage(stream=stream, data=data, emitted_at=0),
    )


def _state(data=None):
    return AirbyteMessage(
        type=Type.STATE,
        state=AirbyteStateMessage(type=AirbyteStateType.STREAM, data=data or {}),
    )


# ── Connection Check ──


class TestDestinationCheck:
    def test_check_success(self):
        dest = DestinationQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1"}
        client = _mock_client()

        with patch.object(dest, "_make_client", return_value=client):
            result = dest.check(MagicMock(), config)

        assert result.status.value == "SUCCEEDED"
        client.sources.list.assert_called_once()

    def test_check_auth_failure(self):
        dest = DestinationQuerri()
        config = {"api_key": "qk_bad", "org_id": "org-1"}
        client = _mock_client()
        client.sources.list.side_effect = AuthenticationError(
            "Invalid API key", status=401
        )

        with patch.object(dest, "_make_client", return_value=client):
            result = dest.check(MagicMock(), config)

        assert result.status.value == "FAILED"
        assert "Invalid API key or org_id" in result.message

    def test_check_server_error(self):
        dest = DestinationQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1"}
        client = _mock_client()
        client.sources.list.side_effect = ServerError(
            "Internal error", status=500
        )

        with patch.object(dest, "_make_client", return_value=client):
            result = dest.check(MagicMock(), config)

        assert result.status.value == "FAILED"
        assert "Internal error" in result.message


# ── Source Resolution ──


class TestSourceResolution:
    def test_resolves_existing_source_dict(self):
        dest = DestinationQuerri()
        client = _mock_client()
        client.sources.list.return_value = [
            _source_dict(id="src-1", name="airbyte_users"),
        ]

        configs = {
            "users": _StreamConfig(
                stream_name="users",
                querri_source_name="airbyte_users",
                sync_mode=DestinationSyncMode.append,
            )
        }

        dest._resolve_sources(client, configs)
        assert configs["users"].querri_source_id == "src-1"

    def test_marks_missing_source_for_creation(self):
        dest = DestinationQuerri()
        client = _mock_client()
        client.sources.list.return_value = []

        configs = {
            "users": _StreamConfig(
                stream_name="users",
                querri_source_name="airbyte_users",
                sync_mode=DestinationSyncMode.append,
            )
        }

        dest._resolve_sources(client, configs)
        assert configs["users"].querri_source_id is None

    def test_resolution_failure_continues_gracefully(self):
        """If list_sources() fails, resolution proceeds with empty list."""
        dest = DestinationQuerri()
        client = _mock_client()
        client.sources.list.side_effect = ServerError("Timeout", status=503)

        configs = {
            "users": _StreamConfig(
                stream_name="users",
                querri_source_name="airbyte_users",
                sync_mode=DestinationSyncMode.append,
            )
        }

        dest._resolve_sources(client, configs)
        assert configs["users"].querri_source_id is None

    def test_resolves_multiple_streams(self):
        dest = DestinationQuerri()
        client = _mock_client()
        client.sources.list.return_value = [
            _source_dict(id="src-1", name="airbyte_users"),
            _source_dict(id="src-2", name="airbyte_orders"),
        ]

        configs = {
            "users": _StreamConfig("users", "airbyte_users", DestinationSyncMode.append),
            "orders": _StreamConfig("orders", "airbyte_orders", DestinationSyncMode.overwrite),
            "events": _StreamConfig("events", "airbyte_events", DestinationSyncMode.append),
        }

        dest._resolve_sources(client, configs)
        assert configs["users"].querri_source_id == "src-1"
        assert configs["orders"].querri_source_id == "src-2"
        assert configs["events"].querri_source_id is None


# ── Flush Logic ──


class TestFlushStream:
    def test_overwrite_calls_replace(self):
        dest = DestinationQuerri()
        client = _mock_client()

        cfg = _StreamConfig("users", "airbyte_users", DestinationSyncMode.overwrite)
        cfg.querri_source_id = "src-1"

        records = [{"name": "Alice"}, {"name": "Bob"}]
        dest._flush_stream(client, cfg, records, is_overwrite=True)

        client.sources.replace_data.assert_called_once_with("src-1", rows=records)
        client.sources.append_rows.assert_not_called()

    def test_append_calls_append_rows(self):
        dest = DestinationQuerri()
        client = _mock_client()

        cfg = _StreamConfig("users", "airbyte_users", DestinationSyncMode.append)
        cfg.querri_source_id = "src-1"

        records = [{"name": "Alice"}]
        dest._flush_stream(client, cfg, records, is_overwrite=False)

        client.sources.append_rows.assert_called_once_with("src-1", rows=records)
        client.sources.replace_data.assert_not_called()

    def test_creates_source_when_missing(self):
        dest = DestinationQuerri()
        client = _mock_client()

        cfg = _StreamConfig("users", "airbyte_users", DestinationSyncMode.append)
        # querri_source_id is None — source doesn't exist

        records = [{"name": "Alice"}]
        dest._flush_stream(client, cfg, records, is_overwrite=False)

        client.sources.create_data_source.assert_called_once_with(
            name="airbyte_users", rows=records
        )
        assert cfg.querri_source_id == "new-src"

    def test_empty_records_noop(self):
        dest = DestinationQuerri()
        client = _mock_client()

        cfg = _StreamConfig("users", "airbyte_users", DestinationSyncMode.append)
        cfg.querri_source_id = "src-1"

        dest._flush_stream(client, cfg, [], is_overwrite=False)

        client.sources.append_rows.assert_not_called()
        client.sources.replace_data.assert_not_called()
        client.sources.create_data_source.assert_not_called()

    def test_overwrite_with_no_source_creates(self):
        """Overwrite mode with no existing source — creates instead of replacing."""
        dest = DestinationQuerri()
        client = _mock_client()

        cfg = _StreamConfig("users", "airbyte_users", DestinationSyncMode.overwrite)
        # No querri_source_id

        records = [{"name": "Alice"}]
        dest._flush_stream(client, cfg, records, is_overwrite=True)

        # Should create, not replace (no source_id to replace)
        client.sources.create_data_source.assert_called_once()
        client.sources.replace_data.assert_not_called()

    def test_append_batches_over_max_batch_size(self):
        """Records exceeding MAX_BATCH_SIZE are split into batches."""
        dest = DestinationQuerri()
        client = _mock_client()

        cfg = _StreamConfig("users", "airbyte_users", DestinationSyncMode.append)
        cfg.querri_source_id = "src-1"

        # MAX_BATCH_SIZE + 5 records
        records = [{"i": i} for i in range(MAX_BATCH_SIZE + 5)]
        dest._flush_stream(client, cfg, records, is_overwrite=False)

        assert client.sources.append_rows.call_count == 2
        first_call = client.sources.append_rows.call_args_list[0]
        second_call = client.sources.append_rows.call_args_list[1]
        assert len(first_call.kwargs["rows"]) == MAX_BATCH_SIZE
        assert len(second_call.kwargs["rows"]) == 5

    def test_create_with_overflow_appends_rest(self):
        """Create with >MAX_BATCH_SIZE records creates first batch, appends rest."""
        dest = DestinationQuerri()
        client = _mock_client()

        cfg = _StreamConfig("big", "airbyte_big", DestinationSyncMode.append)
        # No source_id — will create

        records = [{"i": i} for i in range(MAX_BATCH_SIZE + 10)]
        dest._flush_stream(client, cfg, records, is_overwrite=False)

        # Should create with first MAX_BATCH_SIZE, then append the rest
        client.sources.create_data_source.assert_called_once()
        create_rows = client.sources.create_data_source.call_args.kwargs["rows"]
        assert len(create_rows) == MAX_BATCH_SIZE

        client.sources.append_rows.assert_called_once()
        append_rows = client.sources.append_rows.call_args.kwargs["rows"]
        assert len(append_rows) == 10

    def test_exact_max_batch_size_no_split(self):
        """Exactly MAX_BATCH_SIZE records should be a single call."""
        dest = DestinationQuerri()
        client = _mock_client()

        cfg = _StreamConfig("users", "airbyte_users", DestinationSyncMode.append)
        cfg.querri_source_id = "src-1"

        records = [{"i": i} for i in range(MAX_BATCH_SIZE)]
        dest._flush_stream(client, cfg, records, is_overwrite=False)

        client.sources.append_rows.assert_called_once()
        assert len(client.sources.append_rows.call_args.kwargs["rows"]) == MAX_BATCH_SIZE


# ── Write Flow ──


class TestWriteFlow:
    def test_append_mode_flushes_on_state(self):
        dest = DestinationQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1", "batch_size": 100}
        catalog = _catalog({"users": DestinationSyncMode.append})

        messages = [
            _record("users", {"name": "Alice"}),
            _record("users", {"name": "Bob"}),
            _state(),
        ]

        client = _mock_client()
        client.sources.list.return_value = [
            _source_dict(id="src-1", name="airbyte_users"),
        ]

        with patch.object(dest, "_make_client", return_value=client):
            state_msgs = list(dest.write(config, catalog, iter(messages)))

        assert len(state_msgs) == 1
        client.sources.append_rows.assert_called()

    def test_overwrite_mode_waits_for_all_records(self):
        dest = DestinationQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1", "batch_size": 100}
        catalog = _catalog({"users": DestinationSyncMode.overwrite})

        messages = [
            _record("users", {"name": "Alice"}),
            _state(),
            _record("users", {"name": "Bob"}),
        ]

        client = _mock_client()
        client.sources.list.return_value = [
            _source_dict(id="src-1", name="airbyte_users"),
        ]

        with patch.object(dest, "_make_client", return_value=client):
            list(dest.write(config, catalog, iter(messages)))

        # Overwrite flushes at the end with ALL records
        client.sources.replace_data.assert_called_once()
        call_rows = client.sources.replace_data.call_args.kwargs["rows"]
        assert len(call_rows) == 2

    def test_auto_creates_source_dict(self):
        dest = DestinationQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1"}
        catalog = _catalog({"orders": DestinationSyncMode.append})

        messages = [_record("orders", {"amount": 100})]

        client = _mock_client()
        client.sources.list.return_value = []

        with patch.object(dest, "_make_client", return_value=client):
            list(dest.write(config, catalog, iter(messages)))

        client.sources.create_data_source.assert_called_once_with(
            name="airbyte_orders", rows=[{"amount": 100}]
        )

    def test_custom_prefix(self):
        dest = DestinationQuerri()
        config = {
            "api_key": "qk_test", "org_id": "org-1",
            "source_name_prefix": "zapier_",
        }
        catalog = _catalog({"contacts": DestinationSyncMode.append})

        messages = [_record("contacts", {"name": "Alice"})]

        client = _mock_client()
        client.sources.list.return_value = []

        with patch.object(dest, "_make_client", return_value=client):
            list(dest.write(config, catalog, iter(messages)))

        client.sources.create_data_source.assert_called_once()
        assert client.sources.create_data_source.call_args.kwargs["name"] == "zapier_contacts"

    def test_batch_size_triggers_mid_sync_flush(self):
        """When buffer hits batch_size, it flushes immediately for append mode."""
        dest = DestinationQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1", "batch_size": 2}
        catalog = _catalog({"users": DestinationSyncMode.append})

        messages = [
            _record("users", {"name": "Alice"}),
            _record("users", {"name": "Bob"}),   # batch_size=2, should trigger flush
            _record("users", {"name": "Charlie"}),
        ]

        client = _mock_client()
        client.sources.list.return_value = [
            _source_dict(id="src-1", name="airbyte_users"),
        ]

        with patch.object(dest, "_make_client", return_value=client):
            list(dest.write(config, catalog, iter(messages)))

        # First flush at batch_size=2, second flush at end with remaining 1
        assert client.sources.append_rows.call_count == 2

    def test_batch_size_capped_at_max(self):
        """batch_size over MAX_BATCH_SIZE is capped."""
        dest = DestinationQuerri()
        config = {
            "api_key": "qk_test", "org_id": "org-1",
            "batch_size": MAX_BATCH_SIZE + 50_000,
        }
        catalog = _catalog({"users": DestinationSyncMode.append})

        # Just verify the write doesn't crash — the cap is applied internally
        messages = [_record("users", {"name": "Alice"})]
        client = _mock_client()
        client.sources.list.return_value = [
            _source_dict(id="src-1", name="airbyte_users"),
        ]

        with patch.object(dest, "_make_client", return_value=client):
            list(dest.write(config, catalog, iter(messages)))

        client.sources.append_rows.assert_called()

    def test_state_before_records_is_noop(self):
        """STATE message before any records should yield state without errors."""
        dest = DestinationQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1"}
        catalog = _catalog({"users": DestinationSyncMode.append})

        messages = [_state(), _record("users", {"name": "Alice"})]

        client = _mock_client()
        client.sources.list.return_value = [
            _source_dict(id="src-1", name="airbyte_users"),
        ]

        with patch.object(dest, "_make_client", return_value=client):
            state_msgs = list(dest.write(config, catalog, iter(messages)))

        assert len(state_msgs) == 1
        # The final record is flushed at end
        client.sources.append_rows.assert_called_once()

    def test_multiple_states_flush_between(self):
        """Each STATE flushes append buffers — multiple states work correctly."""
        dest = DestinationQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1"}
        catalog = _catalog({"users": DestinationSyncMode.append})

        messages = [
            _record("users", {"name": "Alice"}),
            _state(),
            _record("users", {"name": "Bob"}),
            _state(),
        ]

        client = _mock_client()
        client.sources.list.return_value = [
            _source_dict(id="src-1", name="airbyte_users"),
        ]

        with patch.object(dest, "_make_client", return_value=client):
            state_msgs = list(dest.write(config, catalog, iter(messages)))

        assert len(state_msgs) == 2
        # Two flushes from STATE messages (1 record each)
        assert client.sources.append_rows.call_count == 2

    def test_multiple_streams_mixed_modes(self):
        """Append and overwrite streams in same sync."""
        dest = DestinationQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1"}
        catalog = _catalog({
            "users": DestinationSyncMode.append,
            "metrics": DestinationSyncMode.overwrite,
        })

        messages = [
            _record("users", {"name": "Alice"}),
            _record("metrics", {"value": 42}),
            _state(),
            _record("users", {"name": "Bob"}),
            _record("metrics", {"value": 99}),
        ]

        client = _mock_client()
        client.sources.list.return_value = [
            _source_dict(id="src-1", name="airbyte_users"),
            _source_dict(id="src-2", name="airbyte_metrics"),
        ]

        with patch.object(dest, "_make_client", return_value=client):
            state_msgs = list(dest.write(config, catalog, iter(messages)))

        # Append stream: flushed at STATE (Alice), then at end (Bob)
        assert client.sources.append_rows.call_count == 2

        # Overwrite stream: all records flushed at end
        client.sources.replace_data.assert_called_once()
        replace_rows = client.sources.replace_data.call_args.kwargs["rows"]
        assert len(replace_rows) == 2

    def test_unconfigured_stream_records_ignored(self):
        """Records for a stream not in the catalog are silently dropped."""
        dest = DestinationQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1"}
        catalog = _catalog({"users": DestinationSyncMode.append})

        messages = [
            _record("users", {"name": "Alice"}),
            _record("unknown_stream", {"data": "ignored"}),
        ]

        client = _mock_client()
        client.sources.list.return_value = [
            _source_dict(id="src-1", name="airbyte_users"),
        ]

        with patch.object(dest, "_make_client", return_value=client):
            list(dest.write(config, catalog, iter(messages)))

        # Only the "users" record should be written
        client.sources.append_rows.assert_called_once()
        rows = client.sources.append_rows.call_args.kwargs["rows"]
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"


# ── Error Handling ──


class TestErrorHandling:
    def test_create_source_failure_raises(self):
        dest = DestinationQuerri()
        client = _mock_client()
        client.sources.create_data_source.side_effect = ServerError(
            "Internal error", status=500
        )

        cfg = _StreamConfig("users", "airbyte_users", DestinationSyncMode.append)
        # No source_id → will try to create

        with pytest.raises(ServerError):
            dest._flush_stream(client, cfg, [{"name": "Alice"}], is_overwrite=False)

    def test_append_failure_raises(self):
        dest = DestinationQuerri()
        client = _mock_client()
        client.sources.append_rows.side_effect = RateLimitError(
            "Rate limited", status=429, retry_after=60.0
        )

        cfg = _StreamConfig("users", "airbyte_users", DestinationSyncMode.append)
        cfg.querri_source_id = "src-1"

        with pytest.raises(RateLimitError):
            dest._flush_stream(client, cfg, [{"name": "Alice"}], is_overwrite=False)

    def test_replace_failure_raises(self):
        dest = DestinationQuerri()
        client = _mock_client()
        client.sources.replace_data.side_effect = NotFoundError(
            "Source not found", status=404
        )

        cfg = _StreamConfig("users", "airbyte_users", DestinationSyncMode.overwrite)
        cfg.querri_source_id = "src-1"

        with pytest.raises(NotFoundError):
            dest._flush_stream(client, cfg, [{"name": "Alice"}], is_overwrite=True)


# ── Flush Append Buffers ──


class TestFlushAppendBuffers:
    def test_only_flushes_append_mode(self):
        """Overwrite buffers are NOT flushed during STATE checkpoints."""
        dest = DestinationQuerri()
        client = _mock_client()

        configs = {
            "append_stream": _StreamConfig(
                "append_stream", "airbyte_append_stream", DestinationSyncMode.append
            ),
            "overwrite_stream": _StreamConfig(
                "overwrite_stream", "airbyte_overwrite_stream", DestinationSyncMode.overwrite
            ),
        }
        configs["append_stream"].querri_source_id = "src-1"
        configs["overwrite_stream"].querri_source_id = "src-2"

        buffers = {
            "append_stream": [{"a": 1}],
            "overwrite_stream": [{"b": 2}],
        }

        dest._flush_append_buffers(client, configs, buffers, DEFAULT_BATCH_SIZE)

        # Append stream flushed
        client.sources.append_rows.assert_called_once()
        assert buffers["append_stream"] == []

        # Overwrite stream NOT flushed
        client.sources.replace_data.assert_not_called()
        assert buffers["overwrite_stream"] == [{"b": 2}]

    def test_empty_buffers_noop(self):
        dest = DestinationQuerri()
        client = _mock_client()

        configs = {
            "users": _StreamConfig("users", "airbyte_users", DestinationSyncMode.append),
        }
        configs["users"].querri_source_id = "src-1"

        buffers = {"users": []}

        dest._flush_append_buffers(client, configs, buffers, DEFAULT_BATCH_SIZE)
        client.sources.append_rows.assert_not_called()
