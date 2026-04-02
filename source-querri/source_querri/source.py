"""Querri source connector — reads data from Querri sources via the Data API."""

import logging
from typing import Any, List, Mapping, Optional, Tuple

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from source_querri.client import QuerriClient
from source_querri.streams import QuerriSourceStream

logger = logging.getLogger("airbyte")


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
            client.check_connection()
            return True, None
        except Exception as e:
            return False, f"Connection failed: {repr(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """Discover Querri sources and return them as Airbyte streams.

        Each source in Querri becomes a separate Airbyte stream with its
        column schema dynamically derived from the source metadata.
        """
        client = self._make_client(config)
        sources = client.list_sources()

        # Optionally filter to specific source IDs
        filter_ids = config.get("source_ids", [])
        if filter_ids:
            sources = [s for s in sources if s.get("id") in filter_ids]

        streams = []
        for source in sources:
            source_id = source.get("id")
            if not source_id:
                continue

            # Fetch full schema for each source
            try:
                schema = client.get_source(source_id)
            except Exception as e:
                logger.warning(
                    f"Skipping source {source_id}: failed to get schema: {e}"
                )
                continue

            streams.append(
                QuerriSourceStream(
                    client=client,
                    source_id=source_id,
                    source_name=schema.get("name", source_id),
                    columns=schema.get("columns", []),
                    column_types=schema.get("column_types", {}),
                    row_count=schema.get("row_count", 0),
                    updated_at=schema.get("updated_at"),
                )
            )

        logger.info(f"Discovered {len(streams)} Querri source streams")
        return streams

    @staticmethod
    def _make_client(config: Mapping[str, Any]) -> QuerriClient:
        return QuerriClient(
            api_key=config["api_key"],
            org_id=config["org_id"],
            base_url=config.get("base_url", "https://app.querri.com/api/v1"),
        )
