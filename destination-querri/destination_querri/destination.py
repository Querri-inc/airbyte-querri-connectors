"""Querri destination connector — writes data to Querri via the Data API.

Supports two sync modes:
- overwrite: Replace all data in the target source (PUT /data/sources/{id}/data)
- append: Add rows incrementally (POST /data/sources/{id}/rows)

Auto-creates Querri sources for new streams that don't have a matching source.
"""

import logging
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Mapping, Optional

from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    ConfiguredAirbyteCatalog,
    DestinationSyncMode,
    Status,
    Type,
)

# Import the shared client from the sibling package, or inline it
import sys
import os

# Add the source connector's parent to path so we can reuse the client
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "source-querri"))

try:
    from source_querri.client import QuerriClient
except ImportError:
    # Fallback: inline minimal client if source package not available
    from destination_querri._client import QuerriClient

logger = logging.getLogger("airbyte")

DEFAULT_BATCH_SIZE = 10_000
MAX_BATCH_SIZE = 100_000


class DestinationQuerri(Destination):
    """Writes Airbyte records to Querri data sources."""

    def check(
        self, logger: logging.Logger, config: Mapping[str, Any]
    ) -> AirbyteConnectionStatus:
        """Verify API key has write access."""
        try:
            client = self._make_client(config)
            client.check_connection()

            # Verify write access by attempting to list (needs data:read)
            # The actual write scope is verified on first write operation
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(
                status=Status.FAILED,
                message=f"Connection failed: {repr(e)}",
            )

    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[AirbyteMessage]:
        """Write records to Querri, yielding state messages as checkpoints.

        Flow:
        1. Resolve each stream to a Querri source (by name, create if missing)
        2. For overwrite streams: buffer ALL records, then replace on flush
        3. For append streams: buffer records, flush in batches
        4. Yield state messages after successful flushes
        """
        client = self._make_client(config)
        prefix = config.get("source_name_prefix", "airbyte_")
        batch_size = min(config.get("batch_size", DEFAULT_BATCH_SIZE), MAX_BATCH_SIZE)

        # Build stream config map
        stream_configs: Dict[str, _StreamConfig] = {}
        for configured_stream in configured_catalog.streams:
            stream_name = configured_stream.stream.name
            sync_mode = configured_stream.destination_sync_mode
            querri_name = f"{prefix}{stream_name}"

            stream_configs[stream_name] = _StreamConfig(
                stream_name=stream_name,
                querri_source_name=querri_name,
                sync_mode=sync_mode,
            )

        # Resolve existing Querri sources
        self._resolve_sources(client, stream_configs)

        # For overwrite mode, we need to collect ALL records before writing
        # For append mode, we flush in batches
        buffers: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        for message in input_messages:
            if message.type == Type.STATE:
                # Flush append-mode buffers before checkpointing
                self._flush_append_buffers(client, stream_configs, buffers, batch_size)
                yield message

            elif message.type == Type.RECORD:
                stream_name = message.record.stream
                if stream_name not in stream_configs:
                    logger.warning(f"Received record for unconfigured stream: {stream_name}")
                    continue

                record = message.record.data
                buffers[stream_name].append(record)

                # For append mode, flush when buffer is full
                cfg = stream_configs[stream_name]
                if (
                    cfg.sync_mode != DestinationSyncMode.overwrite
                    and len(buffers[stream_name]) >= batch_size
                ):
                    self._flush_stream(
                        client, cfg, buffers[stream_name], is_overwrite=False
                    )
                    buffers[stream_name] = []

        # Final flush — overwrite streams write everything now
        for stream_name, records in buffers.items():
            if not records:
                continue
            cfg = stream_configs[stream_name]
            is_overwrite = cfg.sync_mode == DestinationSyncMode.overwrite
            self._flush_stream(client, cfg, records, is_overwrite=is_overwrite)

    def _resolve_sources(
        self,
        client: QuerriClient,
        stream_configs: Dict[str, "_StreamConfig"],
    ):
        """Map each stream to an existing Querri source by name, or mark for creation."""
        try:
            existing_sources = client.list_sources()
        except Exception as e:
            logger.warning(f"Failed to list sources for resolution: {e}")
            existing_sources = []

        name_to_source = {s.get("name"): s for s in existing_sources}

        for cfg in stream_configs.values():
            existing = name_to_source.get(cfg.querri_source_name)
            if existing:
                cfg.querri_source_id = existing.get("id")
                logger.info(
                    f"Stream '{cfg.stream_name}' → existing source "
                    f"'{cfg.querri_source_name}' ({cfg.querri_source_id})"
                )
            else:
                logger.info(
                    f"Stream '{cfg.stream_name}' → will create source "
                    f"'{cfg.querri_source_name}'"
                )

    def _flush_stream(
        self,
        client: QuerriClient,
        cfg: "_StreamConfig",
        records: List[Dict[str, Any]],
        is_overwrite: bool,
    ):
        """Write records to Querri for a single stream."""
        if not records:
            return

        source_id = cfg.querri_source_id

        if source_id and is_overwrite:
            # Replace all data
            logger.info(
                f"Replacing {len(records)} rows in source "
                f"'{cfg.querri_source_name}' ({source_id})"
            )
            result = client.replace_data(source_id, records)
            logger.info(f"Replaced → {result.get('row_count', '?')} rows")

        elif source_id:
            # Append rows in batches
            logger.info(
                f"Appending {len(records)} rows to source "
                f"'{cfg.querri_source_name}' ({source_id})"
            )
            # Split into sub-batches if needed (API limit: 100k per request)
            for i in range(0, len(records), MAX_BATCH_SIZE):
                batch = records[i : i + MAX_BATCH_SIZE]
                result = client.append_rows(source_id, batch)
                logger.info(
                    f"Appended batch ({len(batch)} rows) → "
                    f"total: {result.get('row_count', '?')}"
                )

        else:
            # Source doesn't exist — create it with the data
            logger.info(
                f"Creating source '{cfg.querri_source_name}' "
                f"with {len(records)} rows"
            )
            # Create requires rows, split if over the limit
            first_batch = records[:MAX_BATCH_SIZE]
            result = client.create_source(cfg.querri_source_name, first_batch)
            cfg.querri_source_id = result.get("id")
            logger.info(
                f"Created source {cfg.querri_source_id} "
                f"with {result.get('row_count', '?')} rows"
            )

            # Append remaining rows if we had more than MAX_BATCH_SIZE
            remaining = records[MAX_BATCH_SIZE:]
            if remaining:
                for i in range(0, len(remaining), MAX_BATCH_SIZE):
                    batch = remaining[i : i + MAX_BATCH_SIZE]
                    client.append_rows(cfg.querri_source_id, batch)
                    logger.info(f"Appended overflow batch ({len(batch)} rows)")

    def _flush_append_buffers(
        self,
        client: QuerriClient,
        stream_configs: Dict[str, "_StreamConfig"],
        buffers: Dict[str, List[Dict[str, Any]]],
        batch_size: int,
    ):
        """Flush only append-mode stream buffers (not overwrite, which waits for all data)."""
        for stream_name, records in list(buffers.items()):
            if not records:
                continue
            cfg = stream_configs.get(stream_name)
            if cfg and cfg.sync_mode != DestinationSyncMode.overwrite:
                self._flush_stream(client, cfg, records, is_overwrite=False)
                buffers[stream_name] = []

    @staticmethod
    def _make_client(config: Mapping[str, Any]) -> QuerriClient:
        return QuerriClient(
            api_key=config["api_key"],
            org_id=config["org_id"],
            base_url=config.get("base_url", "https://app.querri.com/api/v1"),
        )


class _StreamConfig:
    """Internal state for a configured stream during a sync."""

    def __init__(
        self,
        stream_name: str,
        querri_source_name: str,
        sync_mode: DestinationSyncMode,
    ):
        self.stream_name = stream_name
        self.querri_source_name = querri_source_name
        self.sync_mode = sync_mode
        self.querri_source_id: Optional[str] = None
