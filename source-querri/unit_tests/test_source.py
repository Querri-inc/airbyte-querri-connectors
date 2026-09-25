"""Unit tests for the Querri source connector (SDK-based)."""

from unittest.mock import MagicMock, patch

import pytest

from querri.types.data import DataPage, Source
from querri._exceptions import (
    AuthenticationError,
    NotFoundError,
    RateLimitError,
    ServerError,
)

from source_querri.source import SourceQuerri
from source_querri.streams import (
    PAGE_SIZE,
    QUERRI_TYPE_MAP,
    QuerriSourceStream,
    _sanitize_stream_name,
)


# ── Helpers ──


def _mock_client(**overrides):
    """Create a MagicMock Querri SDK client with sensible defaults."""
    client = MagicMock()
    client.sources.list.return_value = []
    client.sources.get.return_value = Source(
        id="src-1", name="Test", columns=["col"], row_count=0
    )
    client.sources.source_data.return_value = DataPage(data=[], total_rows=0)
    for k, v in overrides.items():
        attr = getattr(client.sources, k, None)
        if attr is not None:
            if callable(v) and not isinstance(v, MagicMock):
                setattr(client.sources, k, v)
            else:
                attr.return_value = v
    return client


def _source(id="src-1", name="Test", columns=None, column_types=None,
            row_count=0, updated_at=None, access_controlled=None):
    """Build a Source model for mocking."""
    return Source(
        id=id,
        name=name,
        columns=columns or [],
        column_types=column_types,
        row_count=row_count,
        updated_at=updated_at,
        access_controlled=access_controlled,
    )


def _stream(client=None, source_id="src-1", source_name="Test",
            columns=None, column_types=None, row_count=0, updated_at=None):
    """Build a QuerriSourceStream with defaults."""
    return QuerriSourceStream(
        client=client or _mock_client(),
        source_id=source_id,
        source_name=source_name,
        columns=columns or [],
        column_types=column_types or {},
        row_count=row_count,
        updated_at=updated_at,
    )


# ── Stream Name Sanitization ──


class TestStreamNameSanitization:
    def test_basic_name(self):
        assert _sanitize_stream_name("My Source") == "my_source"

    def test_special_characters(self):
        assert _sanitize_stream_name("CRM Contacts (v2)") == "crm_contacts_v2"

    def test_unicode(self):
        assert _sanitize_stream_name("日本語テスト") == "unnamed_source"

    def test_multiple_spaces(self):
        assert _sanitize_stream_name("a   b   c") == "a_b_c"

    def test_empty_name(self):
        assert _sanitize_stream_name("") == "unnamed_source"

    def test_already_valid(self):
        assert _sanitize_stream_name("valid_name_123") == "valid_name_123"

    def test_leading_trailing_special(self):
        assert _sanitize_stream_name("---hello---") == "hello"

    def test_all_special_chars(self):
        assert _sanitize_stream_name("@#$%^&*") == "unnamed_source"


# ── Source Connection Check ──


class TestSourceConnectionCheck:
    def test_check_success(self):
        source = SourceQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1"}
        client = _mock_client()

        with patch.object(SourceQuerri, "_make_client", return_value=client):
            ok, err = source.check_connection(MagicMock(), config)

        assert ok is True
        assert err is None
        client.sources.list.assert_called_once_with(limit=1)

    def test_check_auth_failure(self):
        source = SourceQuerri()
        config = {"api_key": "qk_bad", "org_id": "org-1"}
        client = _mock_client()
        client.sources.list.side_effect = AuthenticationError(
            "Invalid API key", status=401
        )

        with patch.object(SourceQuerri, "_make_client", return_value=client):
            ok, err = source.check_connection(MagicMock(), config)

        assert ok is False
        assert "Invalid API key or org_id" in err

    def test_check_server_error(self):
        source = SourceQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1"}
        client = _mock_client()
        client.sources.list.side_effect = ServerError(
            "Internal error", status=500
        )

        with patch.object(SourceQuerri, "_make_client", return_value=client):
            ok, err = source.check_connection(MagicMock(), config)

        assert ok is False
        assert "Internal error" in err


# ── Stream Discovery ──


class TestStreamDiscovery:
    def test_discovers_streams(self):
        source = SourceQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1"}

        mock_sources = [
            _source(id="src-1", name="Contacts", columns=["name"]),
            _source(id="src-2", name="Deals", columns=["amount"]),
        ]
        schema_1 = _source(
            id="src-1", name="Contacts", columns=["name", "email"],
            column_types={"name": "string", "email": "string"},
            row_count=10, updated_at="2026-01-01T00:00:00",
        )
        schema_2 = _source(
            id="src-2", name="Deals", columns=["amount", "stage"],
            column_types={"amount": "number", "stage": "string"},
            row_count=5, updated_at="2026-01-02T00:00:00",
        )

        client = _mock_client()
        client.sources.list.return_value = mock_sources
        client.sources.get.side_effect = [schema_1, schema_2]

        with patch.object(SourceQuerri, "_make_client", return_value=client):
            streams = source.streams(config)

        assert len(streams) == 2
        assert streams[0].name == "contacts"
        assert streams[1].name == "deals"

    def test_empty_source_list(self):
        source = SourceQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1"}
        client = _mock_client()
        client.sources.list.return_value = []

        with patch.object(SourceQuerri, "_make_client", return_value=client):
            streams = source.streams(config)

        assert streams == []

    def test_filters_by_source_ids(self):
        source = SourceQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1", "source_ids": ["src-1"]}

        mock_sources = [
            _source(id="src-1", name="Contacts"),
            _source(id="src-2", name="Deals"),
        ]
        schema = _source(
            id="src-1", name="Contacts", columns=["name"],
            column_types={"name": "string"}, row_count=10,
        )

        client = _mock_client()
        client.sources.list.return_value = mock_sources
        client.sources.get.return_value = schema

        with patch.object(SourceQuerri, "_make_client", return_value=client):
            streams = source.streams(config)

        assert len(streams) == 1
        assert streams[0].name == "contacts"
        # get_source should only be called once (for the filtered source)
        client.sources.get.assert_called_once_with("src-1")

    def test_filter_with_nonexistent_ids(self):
        """Filter with IDs that don't exist in list_sources — should return empty."""
        source = SourceQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1",
                  "source_ids": ["src-nonexistent"]}

        client = _mock_client()
        client.sources.list.return_value = [
            _source(id="src-1", name="Contacts"),
        ]

        with patch.object(SourceQuerri, "_make_client", return_value=client):
            streams = source.streams(config)

        assert streams == []
        client.sources.get.assert_not_called()

    def test_partial_discovery_skips_failed_schemas(self):
        """If get_source fails for one source, others are still discovered."""
        source = SourceQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1"}

        mock_sources = [
            _source(id="src-1", name="Good"),
            _source(id="src-2", name="Bad"),
            _source(id="src-3", name="Also Good"),
        ]
        schema_1 = _source(
            id="src-1", name="Good", columns=["a"],
            column_types={"a": "string"}, row_count=1,
        )
        schema_3 = _source(
            id="src-3", name="Also Good", columns=["b"],
            column_types={"b": "integer"}, row_count=2,
        )

        client = _mock_client()
        client.sources.list.return_value = mock_sources
        client.sources.get.side_effect = [
            schema_1,
            NotFoundError("Not found", status=404),
            schema_3,
        ]

        with patch.object(SourceQuerri, "_make_client", return_value=client):
            streams = source.streams(config)

        assert len(streams) == 2
        assert streams[0].name == "good"
        assert streams[1].name == "also_good"

    def test_column_types_none_defaults_to_empty(self):
        """When column_types is None from the API, connector uses empty dict."""
        source = SourceQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1"}

        client = _mock_client()
        client.sources.list.return_value = [
            _source(id="src-1", name="NoTypes"),
        ]
        client.sources.get.return_value = _source(
            id="src-1", name="NoTypes", columns=["a", "b"],
            column_types=None, row_count=5,
        )

        with patch.object(SourceQuerri, "_make_client", return_value=client):
            streams = source.streams(config)

        assert len(streams) == 1
        schema = streams[0].get_json_schema()
        # All columns should default to string when column_types is empty
        assert schema["properties"]["a"] == {"type": ["null", "string"]}
        assert schema["properties"]["b"] == {"type": ["null", "string"]}


# ── Pagination (list_sources via SDK) ──


class TestPagination:
    def test_single_page(self):
        source = SourceQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1"}

        sources = [_source(id=f"src-{i}", name=f"S{i}") for i in range(5)]
        schemas = [
            _source(id=f"src-{i}", name=f"S{i}", columns=["x"],
                    column_types={"x": "string"})
            for i in range(5)
        ]

        client = _mock_client()
        client.sources.list.return_value = sources
        client.sources.get.side_effect = schemas

        with patch.object(SourceQuerri, "_make_client", return_value=client):
            streams = source.streams(config)

        assert len(streams) == 5

    def test_sources_called_without_limit(self):
        """streams() calls sources() without limit to get all pages."""
        source = SourceQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1"}

        client = _mock_client()
        client.sources.list.return_value = []

        with patch.object(SourceQuerri, "_make_client", return_value=client):
            source.streams(config)

        # Connector calls sources() with no limit arg to get all pages
        client.sources.list.assert_called_once_with(limit=100)


# ── Stream Schema ──


class TestStreamSchema:
    def test_json_schema_generation(self):
        stream = _stream(
            columns=["name", "score", "active"],
            column_types={"name": "string", "score": "integer", "active": "boolean"},
        )
        schema = stream.get_json_schema()
        props = schema["properties"]

        assert props["name"] == {"type": ["null", "string"]}
        assert props["score"] == {"type": ["null", "integer"]}
        assert props["active"] == {"type": ["null", "boolean"]}
        assert "_querri_source_id" in props
        assert "_querri_source_name" in props

    def test_unknown_type_defaults_to_string(self):
        stream = _stream(
            columns=["weird_col"],
            column_types={"weird_col": "unknown_type_xyz"},
        )
        schema = stream.get_json_schema()
        assert schema["properties"]["weird_col"] == {"type": ["null", "string"]}

    def test_all_querri_type_mappings(self):
        """Every entry in QUERRI_TYPE_MAP produces the correct JSON schema."""
        columns = list(QUERRI_TYPE_MAP.keys())
        column_types = {col: col for col in columns}
        stream = _stream(columns=columns, column_types=column_types)
        schema = stream.get_json_schema()

        for col_type, expected in QUERRI_TYPE_MAP.items():
            assert schema["properties"][col_type] == expected, \
                f"Failed for type '{col_type}'"

    def test_column_without_type_defaults_to_string(self):
        """Column present in columns list but missing from column_types dict."""
        stream = _stream(
            columns=["has_type", "no_type"],
            column_types={"has_type": "integer"},
        )
        schema = stream.get_json_schema()
        assert schema["properties"]["has_type"] == {"type": ["null", "integer"]}
        assert schema["properties"]["no_type"] == {"type": ["null", "string"]}

    def test_empty_columns(self):
        stream = _stream(columns=[], column_types={})
        schema = stream.get_json_schema()
        # Only metadata fields
        assert "_querri_source_id" in schema["properties"]
        assert "_querri_source_name" in schema["properties"]
        assert len(schema["properties"]) == 2

    def test_date_and_datetime_formats(self):
        stream = _stream(
            columns=["d", "dt", "ts"],
            column_types={"d": "date", "dt": "datetime", "ts": "timestamp"},
        )
        schema = stream.get_json_schema()
        assert schema["properties"]["d"]["format"] == "date"
        assert schema["properties"]["dt"]["format"] == "date-time"
        assert schema["properties"]["ts"]["format"] == "date-time"


# ── Stream Reading ──


class TestStreamReading:
    def test_reads_all_pages(self):
        client = _mock_client()
        client.sources.source_data.side_effect = [
            DataPage(
                data=[{"name": f"row-{i}"} for i in range(10)],
                total_rows=15, page=1, page_size=10,
            ),
            DataPage(
                data=[{"name": f"row-{i}"} for i in range(10, 15)],
                total_rows=15, page=2, page_size=10,
            ),
        ]
        client.sources.get.return_value = _source(updated_at="2026-03-08T00:00:00")

        stream = _stream(
            client=client, columns=["name"], column_types={"name": "string"},
            row_count=15,
        )

        records = list(stream.read_records(sync_mode="full_refresh"))
        assert len(records) == 15
        assert records[0]["_querri_source_id"] == "src-1"
        assert records[0]["_querri_source_name"] == "Test"

    def test_reads_empty_source(self):
        client = _mock_client()
        client.sources.source_data.return_value = DataPage(
            data=[], total_rows=0, page=1, page_size=PAGE_SIZE,
        )
        client.sources.get.return_value = _source(updated_at=None)

        stream = _stream(client=client, row_count=0)
        records = list(stream.read_records(sync_mode="full_refresh"))
        assert records == []

    def test_reads_partial_last_page(self):
        client = _mock_client()
        client.sources.source_data.side_effect = [
            DataPage(data=[{"v": i} for i in range(3)], total_rows=3, page=1, page_size=PAGE_SIZE),
        ]
        client.sources.get.return_value = _source(updated_at="2026-01-01T00:00:00")

        stream = _stream(client=client, columns=["v"], row_count=3)
        records = list(stream.read_records(sync_mode="full_refresh"))
        assert len(records) == 3

    def test_error_mid_pagination_stops_gracefully(self):
        """If a page fetch fails, reading stops but doesn't crash."""
        client = _mock_client()
        client.sources.source_data.side_effect = [
            DataPage(data=[{"v": i} for i in range(10)], total_rows=20, page=1, page_size=10),
            ServerError("Connection reset", status=502),
        ]
        client.sources.get.return_value = _source(updated_at="2026-01-01T00:00:00")

        stream = _stream(client=client, columns=["v"], row_count=20)
        records = list(stream.read_records(sync_mode="full_refresh"))
        # Should have the first 10 rows, then stop on error
        assert len(records) == 10

    def test_enriches_with_metadata(self):
        client = _mock_client()
        client.sources.source_data.return_value = DataPage(
            data=[{"col": "val"}], total_rows=1, page=1, page_size=PAGE_SIZE,
        )
        client.sources.get.return_value = _source(updated_at="2026-01-01T00:00:00")

        stream = _stream(
            client=client, source_id="src-42", source_name="My Source",
            columns=["col"], row_count=1,
        )
        records = list(stream.read_records(sync_mode="full_refresh"))
        assert records[0]["_querri_source_id"] == "src-42"
        assert records[0]["_querri_source_name"] == "My Source"


# ── Incremental Sync ──


class TestIncrementalSync:
    def test_skips_unchanged_source(self):
        client = _mock_client()
        client.sources.get.return_value = _source(updated_at="2026-01-01T00:00:00")

        stream = _stream(client=client)
        stream.state = {"updated_at": "2026-06-01T00:00:00"}

        records = list(stream.read_records(
            sync_mode="incremental",
            stream_state={"updated_at": "2026-06-01T00:00:00"},
        ))
        assert len(records) == 0
        client.sources.source_data.assert_not_called()

    def test_reads_when_source_updated(self):
        """Source updated_at is newer than cursor — should read data."""
        client = _mock_client()
        # First call: incremental check returns newer timestamp
        # Second call: after reading, update cursor
        client.sources.get.return_value = _source(updated_at="2026-07-01T00:00:00")
        client.sources.source_data.return_value = DataPage(
            data=[{"v": 1}], total_rows=1, page=1, page_size=PAGE_SIZE,
        )

        stream = _stream(client=client, columns=["v"], row_count=1)
        stream.state = {"updated_at": "2026-06-01T00:00:00"}

        records = list(stream.read_records(
            sync_mode="incremental",
            stream_state={"updated_at": "2026-06-01T00:00:00"},
        ))
        assert len(records) == 1

    def test_equal_timestamps_skip(self):
        """Equal updated_at means no change — skip (uses <=)."""
        client = _mock_client()
        client.sources.get.return_value = _source(updated_at="2026-06-01T00:00:00")

        stream = _stream(client=client)
        stream.state = {"updated_at": "2026-06-01T00:00:00"}

        records = list(stream.read_records(
            sync_mode="incremental",
            stream_state={"updated_at": "2026-06-01T00:00:00"},
        ))
        assert len(records) == 0

    def test_no_cursor_value_does_full_read(self):
        """If no previous cursor, should do a full read even in incremental mode."""
        client = _mock_client()
        client.sources.source_data.return_value = DataPage(
            data=[{"v": 1}], total_rows=1, page=1, page_size=PAGE_SIZE,
        )
        client.sources.get.return_value = _source(updated_at="2026-01-01T00:00:00")

        stream = _stream(client=client, columns=["v"], row_count=1)
        # No state set — cursor_value is None

        records = list(stream.read_records(
            sync_mode="incremental",
            stream_state=None,
        ))
        assert len(records) == 1

    def test_cursor_updated_after_read(self):
        """After reading, the cursor should be updated to the latest updated_at."""
        client = _mock_client()
        client.sources.source_data.return_value = DataPage(
            data=[{"v": 1}], total_rows=1, page=1, page_size=PAGE_SIZE,
        )
        client.sources.get.return_value = _source(updated_at="2026-07-15T12:00:00")

        stream = _stream(client=client, columns=["v"], row_count=1)

        list(stream.read_records(sync_mode="full_refresh"))
        assert stream.state["updated_at"] == "2026-07-15T12:00:00"

    def test_cursor_fallback_on_fetch_failure(self):
        """If _get_current_updated_at fails, falls back to cached value."""
        client = _mock_client()
        client.sources.source_data.return_value = DataPage(
            data=[{"v": 1}], total_rows=1, page=1, page_size=PAGE_SIZE,
        )
        # First call succeeds (incremental check), second fails (cursor update)
        client.sources.get.side_effect = [
            ServerError("Timeout", status=503),
        ]

        stream = _stream(
            client=client, columns=["v"], row_count=1,
            updated_at="2026-05-01T00:00:00",
        )

        list(stream.read_records(sync_mode="full_refresh"))
        # Falls back to the initial updated_at
        assert stream.state["updated_at"] == "2026-05-01T00:00:00"

    def test_state_property(self):
        stream = _stream(source_id="src-1")
        stream.state = {"updated_at": "2026-03-08T12:00:00", "source_id": "src-1"}
        assert stream.state["updated_at"] == "2026-03-08T12:00:00"
        assert stream.state["source_id"] == "src-1"

    def test_state_with_no_cursor(self):
        """State property returns empty string for cursor when not set."""
        stream = _stream()
        state = stream.state
        assert state["updated_at"] == ""

    def test_stream_state_without_cursor_key(self):
        """Setting state with a dict missing the cursor field sets None."""
        stream = _stream()
        stream.state = {"something_else": "value"}
        assert stream._cursor_value is None


# ── Error Handling ──


class TestErrorHandling:
    def test_auth_error_in_discovery(self):
        source = SourceQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1"}
        client = _mock_client()
        client.sources.list.side_effect = AuthenticationError(
            "Invalid API key", status=401
        )

        with patch.object(SourceQuerri, "_make_client", return_value=client):
            with pytest.raises(AuthenticationError):
                source.streams(config)

    def test_rate_limit_error_in_discovery(self):
        source = SourceQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1"}
        client = _mock_client()
        client.sources.list.side_effect = RateLimitError(
            "Rate limited", status=429, retry_after=30.0
        )

        with patch.object(SourceQuerri, "_make_client", return_value=client):
            with pytest.raises(RateLimitError) as exc_info:
                source.streams(config)
            assert exc_info.value.retry_after == 30.0

    def test_not_found_during_read(self):
        """404 during source_data read stops gracefully."""
        client = _mock_client()
        client.sources.source_data.side_effect = NotFoundError(
            "Source not found", status=404
        )
        client.sources.get.return_value = _source(updated_at=None)

        stream = _stream(client=client, row_count=10)
        records = list(stream.read_records(sync_mode="full_refresh"))
        assert records == []
