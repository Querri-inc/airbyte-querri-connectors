"""Querri source connector — reads data from Querri sources via the Data API."""

import logging
from typing import Any, List, Mapping, Optional, Tuple

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from querri import Querri
from querri._exceptions import (
    AuthenticationError,
    ConfigError,
    NotFoundError,
    PermissionError as QuerriPermissionError,
    RateLimitError,
)

from source_querri.streams import QuerriSourceStream

logger = logging.getLogger("airbyte")


def _format_error(e: Exception) -> str:
    """Format a Querri SDK exception into a user-friendly Airbyte error message."""
    if isinstance(e, AuthenticationError):
        return "Invalid API key or org_id. Check your Querri credentials."
    if isinstance(e, QuerriPermissionError):
        return "API key lacks permission. Ensure it has data:read scope."
    if isinstance(e, RateLimitError):
        return f"Rate limited by Querri API. Retry after {getattr(e, 'retry_after', 'a few')} seconds."
    if isinstance(e, NotFoundError):
        return f"Resource not found: {e}"
    if isinstance(e, ConfigError):
        return f"Configuration error: {e}"
    return str(e)


class SourceQuerri(AbstractSource):
    """Reads data sources from Querri.

    Each Querri data source becomes an Airbyte stream. Supports both
    full refresh and incremental sync (using source updated_at as cursor).
    """

    def check_connection(
        self, logger: logging.Logger, config: Mapping[str, Any]
    ) -> Tuple[bool, Optional[Any]]:
        """Verify the API key and org_id are valid by listing sources."""
        try:
            client = self._make_client(config)
            # Force iteration to trigger the HTTP call (SyncCursorPage is lazy)
            next(iter(client.data.sources(limit=1)), None)
            return True, None
        except Exception as e:
            return False, _format_error(e)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """Discover Querri sources and return them as Airbyte streams.

        Each source in Querri becomes a separate Airbyte stream with its
        column schema dynamically derived from the source metadata.
        """
        client = self._make_client(config)

        # Auto-paginating: iterates all pages
        all_sources = list(client.data.sources())

        # Optionally filter to specific source IDs
        filter_ids = config.get("source_ids", [])
        if filter_ids:
            all_sources = [s for s in all_sources if s.id in filter_ids]

        streams = []
        for source in all_sources:
            # Fetch full schema for each source (includes column_types)
            try:
                schema = client.data.source(source.id)
            except Exception as e:
                logger.warning(
                    f"Skipping source {source.id}: failed to get schema: {e}"
                )
                continue

            streams.append(
                QuerriSourceStream(
                    client=client,
                    source_id=schema.id,
                    source_name=schema.name,
                    columns=schema.columns,
                    column_types=schema.column_types or {},
                    row_count=schema.row_count or 0,
                    updated_at=schema.updated_at,
                )
            )

        logger.info(f"Discovered {len(streams)} Querri source streams")
        return streams

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
