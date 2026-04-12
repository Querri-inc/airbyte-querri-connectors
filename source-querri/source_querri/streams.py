"""Airbyte stream implementation for a single Querri data source."""

import logging
import re
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional

from airbyte_cdk.sources.streams import Stream
from querri import Querri

logger = logging.getLogger("airbyte")

PAGE_SIZE = 10_000

# Map Querri column types to JSON schema types
QUERRI_TYPE_MAP = {
    "string": {"type": ["null", "string"]},
    "integer": {"type": ["null", "integer"]},
    "float": {"type": ["null", "number"]},
    "number": {"type": ["null", "number"]},
    "boolean": {"type": ["null", "boolean"]},
    "date": {"type": ["null", "string"], "format": "date"},
    "datetime": {"type": ["null", "string"], "format": "date-time"},
    "timestamp": {"type": ["null", "string"], "format": "date-time"},
}


def _sanitize_stream_name(name: str) -> str:
    """Convert a Querri source name into a valid Airbyte stream name.

    Airbyte stream names should be lowercase alphanumeric + underscores.
    """
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    sanitized = re.sub(r"_+", "_", sanitized).strip("_").lower()
    return sanitized or "unnamed_source"


class QuerriSourceStream(Stream):
    """Stream that reads all data from a single Querri source.

    Supports:
    - Full refresh: reads all rows with pagination
    - Incremental: tracks source updated_at to skip unchanged sources
    """

    cursor_field = "updated_at"
    primary_key = None  # Querri sources don't have a guaranteed PK

    def __init__(
        self,
        client: Querri,
        source_id: str,
        source_name: str,
        columns: List[str],
        column_types: Dict[str, str],
        row_count: int = 0,
        updated_at: Optional[str] = None,
    ):
        super().__init__()
        self._client = client
        self._source_id = source_id
        self._source_name = source_name
        self._columns = columns
        self._column_types = column_types
        self._row_count = row_count
        self._updated_at = updated_at
        self._cursor_value: Optional[str] = None

    @property
    def name(self) -> str:
        return _sanitize_stream_name(self._source_name)

    @property
    def source_defined_cursor(self) -> bool:
        return True

    @property
    def supports_incremental(self) -> bool:
        return True

    # ── State management (incremental) ──

    @property
    def state(self) -> MutableMapping[str, Any]:
        return {
            self.cursor_field: self._cursor_value or "",
            "source_id": self._source_id,
        }

    @state.setter
    def state(self, value: MutableMapping[str, Any]):
        self._cursor_value = value.get(self.cursor_field)

    # ── Schema ──

    def get_json_schema(self) -> Mapping[str, Any]:
        """Build JSON schema from the Querri source's column metadata."""
        properties = {}
        for col in self._columns:
            querri_type = self._column_types.get(col, "string")
            properties[col] = QUERRI_TYPE_MAP.get(
                querri_type, {"type": ["null", "string"]}
            )

        # Add Querri metadata fields
        properties["_querri_source_id"] = {"type": "string"}
        properties["_querri_source_name"] = {"type": "string"}

        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": properties,
        }

    # ── Data reading ──

    def read_records(
        self,
        sync_mode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        """Read all rows from the Querri source with pagination.

        For incremental syncs, we check if the source has been updated
        since our last cursor value. If not, we skip reading entirely.
        """
        # Incremental: skip if source hasn't changed
        if stream_state and self._cursor_value:
            current_updated_at = self._get_current_updated_at()
            if current_updated_at and current_updated_at <= self._cursor_value:
                logger.info(
                    f"Source {self._source_id} unchanged "
                    f"(last: {self._cursor_value}, current: {current_updated_at}), skipping"
                )
                return

        page = 1
        total_yielded = 0

        while True:
            try:
                result = self._client.sources.source_data(
                    self._source_id, page=page, page_size=PAGE_SIZE
                )
            except Exception as e:
                logger.error(
                    f"Failed to read page {page} of source {self._source_id}: {e}"
                )
                break

            rows = result.data
            if not rows:
                break

            for row in rows:
                # Enrich with source metadata
                row["_querri_source_id"] = self._source_id
                row["_querri_source_name"] = self._source_name
                yield row
                total_yielded += 1

            total_rows = result.total_rows or 0
            if total_yielded >= total_rows:
                break

            page += 1

        # Update cursor
        new_updated_at = self._get_current_updated_at()
        if new_updated_at:
            self._cursor_value = new_updated_at

        logger.info(
            f"Read {total_yielded} rows from source "
            f"{self._source_name} ({self._source_id})"
        )

    def _get_current_updated_at(self) -> Optional[str]:
        """Fetch the current updated_at from source metadata."""
        try:
            source = self._client.sources.get(self._source_id)
            return source.get("updated_at")
        except Exception:
            return self._updated_at
