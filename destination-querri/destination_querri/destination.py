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
from querri import Querri
from querri._exceptions import (
    AuthenticationError,
    ConfigError,
    NotFoundError,
    PermissionError as QuerriPermissionError,
    RateLimitError,
)

logger = logging.getLogger("airbyte")


def _format_error(e: Exception) -> str:
    """Format a Querri SDK exception into a user-friendly Airbyte error message."""
    if isinstance(e, AuthenticationError):
        return "Invalid API key or org_id. Check your Querri credentials."
    if isinstance(e, QuerriPermissionError):
        return "API key lacks permission. Ensure it has data:write scope."
    if isinstance(e, RateLimitError):
        return f"Rate limited by Querri API. Retry after {getattr(e, 'retry_after', 'a few')} seconds."
    if isinstance(e, NotFoundError):
        return f"Resource not found: {e}"
    if isinstance(e, ConfigError):
        return f"Configuration error: {e}"
    return str(e)

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
            # Force iteration to trigger the HTTP call (SyncCursorPage is lazy)
            next(iter(client.data.sources(limit=1)), None)
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(
                status=Status.FAILED,
                message=_format_error(e),
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
        client: Querri,
        stream_configs: Dict[str, "_StreamConfig"],
    ):
        """Map each stream to an existing Querri source by name, or mark for creation."""
        try:
            # Auto-paginating: iterates all pages
            existing_sources = list(client.data.sources())
        except Exception as e:
            logger.warning(f"Failed to list sources for resolution: {e}")
            existing_sources = []

        name_to_source = {s.name: s for s in existing_sources}

        for cfg in stream_configs.values():
            existing = name_to_source.get(cfg.querri_source_name)
            if existing:
                cfg.querri_source_id = existing.id
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
        client: Querri,
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
            result = client.data.replace_data(source_id, rows=records)
            logger.info(f"Replaced → {result.row_count} rows")

        elif source_id:
            # Append rows in batches
            logger.info(
                f"Appending {len(records)} rows to source "
                f"'{cfg.querri_source_name}' ({source_id})"
            )
            # Split into sub-batches if needed (API limit: 100k per request)
            for i in range(0, len(records), MAX_BATCH_SIZE):
                batch = records[i : i + MAX_BATCH_SIZE]
                result = client.data.append_rows(source_id, rows=batch)
                logger.info(
                    f"Appended batch ({len(batch)} rows) → "
                    f"total: {result.row_count}"
                )

        else:
            # Source doesn't exist — create it with the data
            logger.info(
                f"Creating source '{cfg.querri_source_name}' "
                f"with {len(records)} rows"
            )
            # Create requires rows, split if over the limit
            first_batch = records[:MAX_BATCH_SIZE]
            result = client.data.create_source(
                name=cfg.querri_source_name, rows=first_batch
            )
            cfg.querri_source_id = result.id
            logger.info(
                f"Created source {cfg.querri_source_id} "
                f"with {result.row_count} rows"
            )

            # Append remaining rows if we had more than MAX_BATCH_SIZE
            remaining = records[MAX_BATCH_SIZE:]
            if remaining:
                for i in range(0, len(remaining), MAX_BATCH_SIZE):
                    batch = remaining[i : i + MAX_BATCH_SIZE]
                    client.data.append_rows(cfg.querri_source_id, rows=batch)
                    logger.info(f"Appended overflow batch ({len(batch)} rows)")

    def _flush_append_buffers(
        self,
        client: Querri,
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
    def _make_client(config: Mapping[str, Any]) -> Querri:
        base_url = config.get("base_url", "https://app.querri.com/api/v1")
        # SDK expects host without /api/v1 suffix
        host = base_url.removesuffix("/").removesuffix("/api/v1")
        return Querri(
            api_key=config["api_key"],
            org_id=config["org_id"],
            host=host,
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
