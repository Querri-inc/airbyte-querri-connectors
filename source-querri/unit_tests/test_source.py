"""Unit tests for the Querri source connector."""

from unittest.mock import MagicMock, patch

import pytest

from source_querri.client import QuerriClient
from source_querri.source import SourceQuerri
from source_querri.streams import QuerriSourceStream, _sanitize_stream_name


# ── Client Tests ──


class TestQuerriClient:
    def test_headers_set_correctly(self):
        client = QuerriClient(
            api_key="qk_test123", org_id="org-1", base_url="https://api.test.com/v1"
        )
        assert client.session.headers["Authorization"] == "Bearer qk_test123"
        assert client.session.headers["X-Tenant-ID"] == "org-1"

    def test_url_construction(self):
        client = QuerriClient(
            api_key="qk_x", org_id="o", base_url="https://api.test.com/v1/"
        )
        assert client._url("/data/sources") == "https://api.test.com/v1/data/sources"

    def test_url_no_trailing_slash(self):
        client = QuerriClient(
            api_key="qk_x", org_id="o", base_url="https://api.test.com/v1"
        )
        assert client._url("/data/sources") == "https://api.test.com/v1/data/sources"


# ── Stream Name Sanitization ──


class TestStreamNameSanitization:
    def test_basic_name(self):
        assert _sanitize_stream_name("My Source") == "my_source"

    def test_special_characters(self):
        assert _sanitize_stream_name("CRM Contacts (v2)") == "crm_contacts_v2"

    def test_unicode(self):
        assert _sanitize_stream_name("日本語テスト") == "unnamed_source"  # Non-ascii stripped → fallback

    def test_multiple_spaces(self):
        assert _sanitize_stream_name("a   b   c") == "a_b_c"

    def test_empty_name(self):
        assert _sanitize_stream_name("") == "unnamed_source"

    def test_already_valid(self):
        assert _sanitize_stream_name("valid_name_123") == "valid_name_123"


# ── Source Connection Check ──


class TestSourceConnectionCheck:
    def test_check_success(self):
        source = SourceQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1"}

        with patch.object(QuerriClient, "check_connection", return_value=True):
            ok, err = source.check_connection(MagicMock(), config)

        assert ok is True
        assert err is None

    def test_check_failure(self):
        source = SourceQuerri()
        config = {"api_key": "qk_bad", "org_id": "org-1"}

        with patch.object(
            QuerriClient,
            "check_connection",
            side_effect=Exception("401 Unauthorized"),
        ):
            ok, err = source.check_connection(MagicMock(), config)

        assert ok is False
        assert "401" in err


# ── Stream Discovery ──


class TestStreamDiscovery:
    def test_discovers_streams(self):
        source = SourceQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1"}

        mock_sources = [
            {"id": "src-1", "name": "Contacts", "columns": ["name"], "row_count": 10},
            {"id": "src-2", "name": "Deals", "columns": ["amount"], "row_count": 5},
        ]

        mock_schema_1 = {
            "id": "src-1",
            "name": "Contacts",
            "columns": ["name", "email"],
            "column_types": {"name": "string", "email": "string"},
            "row_count": 10,
            "updated_at": "2026-01-01T00:00:00",
        }
        mock_schema_2 = {
            "id": "src-2",
            "name": "Deals",
            "columns": ["amount", "stage"],
            "column_types": {"amount": "number", "stage": "string"},
            "row_count": 5,
            "updated_at": "2026-01-02T00:00:00",
        }

        with patch.object(QuerriClient, "list_sources", return_value=mock_sources), \
             patch.object(
                 QuerriClient,
                 "get_source",
                 side_effect=[mock_schema_1, mock_schema_2],
             ):
            streams = source.streams(config)

        assert len(streams) == 2
        assert streams[0].name == "contacts"
        assert streams[1].name == "deals"

    def test_filters_by_source_ids(self):
        source = SourceQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1", "source_ids": ["src-1"]}

        mock_sources = [
            {"id": "src-1", "name": "Contacts", "columns": ["name"], "row_count": 10},
            {"id": "src-2", "name": "Deals", "columns": ["amount"], "row_count": 5},
        ]

        mock_schema = {
            "id": "src-1",
            "name": "Contacts",
            "columns": ["name"],
            "column_types": {"name": "string"},
            "row_count": 10,
        }

        with patch.object(QuerriClient, "list_sources", return_value=mock_sources), \
             patch.object(QuerriClient, "get_source", return_value=mock_schema):
            streams = source.streams(config)

        assert len(streams) == 1
        assert streams[0].name == "contacts"


# ── Stream Schema ──


class TestStreamSchema:
    def test_json_schema_generation(self):
        client = MagicMock()
        stream = QuerriSourceStream(
            client=client,
            source_id="src-1",
            source_name="My Source",
            columns=["name", "score", "active"],
            column_types={"name": "string", "score": "integer", "active": "boolean"},
        )

        schema = stream.get_json_schema()
        props = schema["properties"]

        assert props["name"] == {"type": ["null", "string"]}
        assert props["score"] == {"type": ["null", "integer"]}
        assert props["active"] == {"type": ["null", "boolean"]}
        assert "_querri_source_id" in props

    def test_unknown_type_defaults_to_string(self):
        client = MagicMock()
        stream = QuerriSourceStream(
            client=client,
            source_id="src-1",
            source_name="Test",
            columns=["weird_col"],
            column_types={"weird_col": "unknown_type_xyz"},
        )

        schema = stream.get_json_schema()
        assert schema["properties"]["weird_col"] == {"type": ["null", "string"]}


# ── Stream Reading ──


class TestStreamReading:
    def test_reads_all_pages(self):
        client = MagicMock()
        client.read_source_data = MagicMock(
            side_effect=[
                {
                    "data": [{"name": f"row-{i}"} for i in range(10)],
                    "total_rows": 15,
                    "page": 1,
                    "page_size": 10,
                },
                {
                    "data": [{"name": f"row-{i}"} for i in range(10, 15)],
                    "total_rows": 15,
                    "page": 2,
                    "page_size": 10,
                },
            ]
        )
        client.get_source = MagicMock(
            return_value={"updated_at": "2026-03-08T00:00:00"}
        )

        stream = QuerriSourceStream(
            client=client,
            source_id="src-1",
            source_name="Test",
            columns=["name"],
            column_types={"name": "string"},
            row_count=15,
        )

        records = list(stream.read_records(sync_mode="full_refresh"))
        assert len(records) == 15
        assert records[0]["_querri_source_id"] == "src-1"

    def test_incremental_skips_unchanged(self):
        client = MagicMock()
        client.get_source = MagicMock(
            return_value={"updated_at": "2026-01-01T00:00:00"}
        )

        stream = QuerriSourceStream(
            client=client,
            source_id="src-1",
            source_name="Test",
            columns=["name"],
            column_types={},
        )
        # Set state to a later timestamp
        stream.state = {"updated_at": "2026-06-01T00:00:00"}

        records = list(
            stream.read_records(
                sync_mode="incremental",
                stream_state={"updated_at": "2026-06-01T00:00:00"},
            )
        )
        assert len(records) == 0
        # read_source_data should NOT be called
        client.read_source_data.assert_not_called()

    def test_state_property(self):
        client = MagicMock()
        stream = QuerriSourceStream(
            client=client,
            source_id="src-1",
            source_name="Test",
            columns=[],
            column_types={},
        )

        stream.state = {"updated_at": "2026-03-08T12:00:00", "source_id": "src-1"}
        assert stream.state["updated_at"] == "2026-03-08T12:00:00"
        assert stream.state["source_id"] == "src-1"
