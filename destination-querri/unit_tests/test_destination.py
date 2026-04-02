"""Unit tests for the Querri destination connector."""

from unittest.mock import MagicMock, call, patch

import pytest

from destination_querri._client import QuerriClient
from destination_querri.destination import DestinationQuerri, _StreamConfig


def _mock_client(**overrides):
    """Create a MagicMock QuerriClient with sensible defaults."""
    client = MagicMock(spec=QuerriClient)
    client.check_connection.return_value = True
    client.list_sources.return_value = []
    client.create_source.return_value = {"id": "new-src", "row_count": 1}
    client.append_rows.return_value = {"row_count": 1}
    client.replace_data.return_value = {"row_count": 1}
    for k, v in overrides.items():
        setattr(client, k, v if callable(v) else MagicMock(return_value=v))
    return client


# ── Connection Check ──


class TestDestinationCheck:
    def test_check_success(self):
        dest = DestinationQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1"}
        mock = _mock_client()

        with patch.object(dest, "_make_client", return_value=mock):
            result = dest.check(MagicMock(), config)

        assert result.status.value == "SUCCEEDED"

    def test_check_failure(self):
        dest = DestinationQuerri()
        config = {"api_key": "qk_bad", "org_id": "org-1"}
        mock = _mock_client()
        mock.check_connection.side_effect = Exception("403 Forbidden")

        with patch.object(dest, "_make_client", return_value=mock):
            result = dest.check(MagicMock(), config)

        assert result.status.value == "FAILED"
        assert "403" in result.message


# ── Source Resolution ──


class TestSourceResolution:
    def test_resolves_existing_source(self):
        dest = DestinationQuerri()
        client = _mock_client()
        client.list_sources.return_value = [
            {"id": "src-1", "name": "airbyte_users"},
        ]

        from airbyte_cdk.models import DestinationSyncMode

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
        client.list_sources.return_value = []

        from airbyte_cdk.models import DestinationSyncMode

        configs = {
            "users": _StreamConfig(
                stream_name="users",
                querri_source_name="airbyte_users",
                sync_mode=DestinationSyncMode.append,
            )
        }

        dest._resolve_sources(client, configs)
        assert configs["users"].querri_source_id is None


# ── Flush Logic ──


class TestFlushStream:
    def test_overwrite_calls_replace(self):
        dest = DestinationQuerri()
        client = _mock_client()

        from airbyte_cdk.models import DestinationSyncMode

        cfg = _StreamConfig(
            stream_name="users",
            querri_source_name="airbyte_users",
            sync_mode=DestinationSyncMode.overwrite,
        )
        cfg.querri_source_id = "src-1"

        records = [{"name": "Alice"}, {"name": "Bob"}]
        dest._flush_stream(client, cfg, records, is_overwrite=True)

        client.replace_data.assert_called_once_with("src-1", records)
        client.append_rows.assert_not_called()

    def test_append_calls_append_rows(self):
        dest = DestinationQuerri()
        client = _mock_client()

        from airbyte_cdk.models import DestinationSyncMode

        cfg = _StreamConfig(
            stream_name="users",
            querri_source_name="airbyte_users",
            sync_mode=DestinationSyncMode.append,
        )
        cfg.querri_source_id = "src-1"

        records = [{"name": "Alice"}]
        dest._flush_stream(client, cfg, records, is_overwrite=False)

        client.append_rows.assert_called_once_with("src-1", records)
        client.replace_data.assert_not_called()

    def test_creates_source_when_missing(self):
        dest = DestinationQuerri()
        client = _mock_client()

        from airbyte_cdk.models import DestinationSyncMode

        cfg = _StreamConfig(
            stream_name="users",
            querri_source_name="airbyte_users",
            sync_mode=DestinationSyncMode.append,
        )
        # No querri_source_id = source doesn't exist yet

        records = [{"name": "Alice"}]
        dest._flush_stream(client, cfg, records, is_overwrite=False)

        client.create_source.assert_called_once_with("airbyte_users", records)
        assert cfg.querri_source_id == "new-src"

    def test_empty_records_noop(self):
        dest = DestinationQuerri()
        client = _mock_client()

        from airbyte_cdk.models import DestinationSyncMode

        cfg = _StreamConfig(
            stream_name="users",
            querri_source_name="airbyte_users",
            sync_mode=DestinationSyncMode.append,
        )
        cfg.querri_source_id = "src-1"

        dest._flush_stream(client, cfg, [], is_overwrite=False)

        client.append_rows.assert_not_called()
        client.replace_data.assert_not_called()
        client.create_source.assert_not_called()


# ── Write Flow ──


class TestWriteFlow:
    def _make_catalog(self, streams):
        from airbyte_cdk.models import (
            AirbyteStream,
            ConfiguredAirbyteCatalog,
            ConfiguredAirbyteStream,
            DestinationSyncMode,
            SyncMode,
        )

        configured_streams = []
        for name, mode in streams.items():
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

    def _make_record(self, stream: str, data: dict):
        from airbyte_cdk.models import AirbyteMessage, AirbyteRecordMessage, Type

        return AirbyteMessage(
            type=Type.RECORD,
            record=AirbyteRecordMessage(stream=stream, data=data, emitted_at=0),
        )

    def _make_state(self, data: dict = None):
        from airbyte_cdk.models import (
            AirbyteMessage,
            AirbyteStateMessage,
            AirbyteStateType,
            Type,
        )

        return AirbyteMessage(
            type=Type.STATE,
            state=AirbyteStateMessage(type=AirbyteStateType.STREAM, data=data or {}),
        )

    def test_append_mode_flushes_on_state(self):
        from airbyte_cdk.models import DestinationSyncMode

        dest = DestinationQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1", "batch_size": 100}
        catalog = self._make_catalog({"users": DestinationSyncMode.append})

        messages = [
            self._make_record("users", {"name": "Alice"}),
            self._make_record("users", {"name": "Bob"}),
            self._make_state(),
        ]

        mock = _mock_client()
        mock.list_sources.return_value = [{"id": "src-1", "name": "airbyte_users"}]

        with patch.object(dest, "_make_client", return_value=mock):
            state_msgs = list(dest.write(config, catalog, iter(messages)))

        assert len(state_msgs) == 1
        mock.append_rows.assert_called()

    def test_overwrite_mode_waits_for_all_records(self):
        from airbyte_cdk.models import DestinationSyncMode

        dest = DestinationQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1", "batch_size": 100}
        catalog = self._make_catalog({"users": DestinationSyncMode.overwrite})

        messages = [
            self._make_record("users", {"name": "Alice"}),
            self._make_state(),
            self._make_record("users", {"name": "Bob"}),
        ]

        mock = _mock_client()
        mock.list_sources.return_value = [{"id": "src-1", "name": "airbyte_users"}]

        with patch.object(dest, "_make_client", return_value=mock):
            list(dest.write(config, catalog, iter(messages)))

        mock.replace_data.assert_called_once()
        args = mock.replace_data.call_args
        assert len(args[0][1]) == 2

    def test_auto_creates_source(self):
        from airbyte_cdk.models import DestinationSyncMode

        dest = DestinationQuerri()
        config = {"api_key": "qk_test", "org_id": "org-1"}
        catalog = self._make_catalog({"orders": DestinationSyncMode.append})

        messages = [self._make_record("orders", {"amount": 100})]

        mock = _mock_client()
        mock.list_sources.return_value = []

        with patch.object(dest, "_make_client", return_value=mock):
            list(dest.write(config, catalog, iter(messages)))

        mock.create_source.assert_called_once_with("airbyte_orders", [{"amount": 100}])

    def test_custom_prefix(self):
        from airbyte_cdk.models import DestinationSyncMode

        dest = DestinationQuerri()
        config = {
            "api_key": "qk_test",
            "org_id": "org-1",
            "source_name_prefix": "zapier_",
        }
        catalog = self._make_catalog({"contacts": DestinationSyncMode.append})

        messages = [self._make_record("contacts", {"name": "Alice"})]

        mock = _mock_client()
        mock.list_sources.return_value = []

        with patch.object(dest, "_make_client", return_value=mock):
            list(dest.write(config, catalog, iter(messages)))

        mock.create_source.assert_called_once()
        assert mock.create_source.call_args[0][0] == "zapier_contacts"
