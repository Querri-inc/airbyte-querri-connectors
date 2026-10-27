"""Querri destination connector: writes Airbyte records to Querri sources.

Sync modes:
- overwrite: replace all rows of the target source (PUT /sources/{id}/data)
- append: add rows (POST /sources/{id}/rows)

Deduplicated modes are not offered: the Querri API has no upsert endpoint.

Each stream is written to the source named ``<prefix><stream>``, created on the
first write when it does not exist yet. An existing source with that name is
written to only if it is an API source (see ``_resolve_sources``).

State semantics (the Airbyte destination contract: a STATE is emitted only after
every record received before it is durably written):

- append: a STATE is held until every append buffer is flushed and the server
  has answered success. A STATE triggers that flush only when STATE_FLUSH_ROWS
  rows are buffered or STATE_FLUSH_SECONDS have passed (or nothing is
  buffered); otherwise it waits for a later one, or the end. Held states leave in
  arrival order, so a source that checkpoints every few records does not turn
  into one write request per checkpoint.
- overwrite: the source keeps its old rows until the sync's final replace, so
  nothing before that replace is durable. Every STATE that covers an overwrite
  stream (its own per-stream STATE, or any GLOBAL/LEGACY STATE when the catalog
  has an overwrite stream) is held back. After input ends and every final
  replace has succeeded, the latest held STATE of each kind is emitted. If a
  replace fails the write raises, Airbyte marks the attempt failed, and no state
  covering it was ever acknowledged, so the retry starts that stream over.

Memory: append holds at most one batch. Overwrite spools its rows to a temp file
and reads them back one request at a time. The server caps a request at 100,000
rows and 50 MB; a stream that fits in one request is replaced atomically, a
larger one is replaced by a PUT of the first chunk then appends of the rest (see
``_OverwriteWriter``).
"""

import json
import logging
import tempfile
import time
from collections import OrderedDict
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Tuple
from urllib.parse import urlparse

import httpx
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteStateType,
    ConfiguredAirbyteCatalog,
    DestinationSyncMode,
    FailureType,
    Status,
    Type,
)
from airbyte_cdk.utils.traced_exception import AirbyteTracedException
from querri import (
    APIError,
    AuthenticationError,
    ConfigError,
    ConflictError,
    NotFoundError,
    PermissionError as QuerriPermissionError,
    Querri,
    RateLimitError,
)

logger = logging.getLogger("airbyte")

DEFAULT_BASE_URL = "https://app.querri.com/api/v1"
DEFAULT_BATCH_SIZE = 10_000
# Server limits per write request (server-api public_api/routes/sources.py).
MAX_BATCH_SIZE = 100_000
SERVER_MAX_PAYLOAD_BYTES = 50 * 1024 * 1024
# Headroom under the server's cap for the JSON envelope and our size estimate.
MAX_REQUEST_BYTES = 40 * 1024 * 1024

# Hosts allowed over plain http, for local testing only.
LOCAL_HTTP_HOSTS = {"localhost", "127.0.0.1", "host.docker.internal"}

# The server's limit on a source name (CreateDataSourceRequest.name).
MAX_SOURCE_NAME = 200

# A STATE flushes the append buffers (and is then emitted) only once this many
# rows are buffered or this long has passed since the last flush; until then it
# is held. A STATE arriving with nothing buffered is emitted at once. Either way
# a STATE leaves only after every record before it is written.
STATE_FLUSH_ROWS = 10_000
STATE_FLUSH_SECONDS = 30.0

# A source this destination may write to: one created through the API's rows
# endpoints, as the destination creates them. An upload, a View or a connector's
# source that shares the name is never written to. (A stronger marker, the
# source's notes, needs admin:sources:write to set, which a data:write key lacks.)
WRITABLE_SERVICE = "api"

# Retry policy. 429 and the two transient 409s (the source is being synced or
# written) mean the server did not apply the request, so any request may be
# resent. Any other 409 (e.g. managed_by_sites) is permanent. A 5xx or a timeout
# may have been applied: only an idempotent GET, PUT or PATCH is resent.
TRANSIENT_CONFLICT_CODES = {"write_in_progress", "sync_in_progress"}
MAX_ATTEMPTS = 8
CONFLICT_RETRY_SECONDS = 10.0
MAX_BACKOFF_SECONDS = 30.0
MAX_RETRY_AFTER_SECONDS = 300.0

# An id that never names a real source, for the write-scope probe in check().
_PROBE_SOURCE_ID = "00000000-0000-0000-0000-000000000000"


def _format_error(e: Exception) -> str:
    """Format a Querri SDK exception into a user-friendly Airbyte error message."""
    if isinstance(e, AuthenticationError):
        return "Invalid API key or org_id. Check your Querri credentials."
    if isinstance(e, QuerriPermissionError):
        return "API key lacks permission. Ensure it has the data:read and data:write scopes."
    if isinstance(e, RateLimitError):
        return f"Rate limited by Querri API. Retry after {getattr(e, 'retry_after', 'a few')} seconds."
    if isinstance(e, NotFoundError):
        return f"Resource not found: {e}"
    if isinstance(e, ConfigError):
        return f"Configuration error: {e}"
    return str(e)


def _validate_base_url(base_url: str) -> str:
    """Return the SDK host for ``base_url``; https only, except local test hosts."""
    parsed = urlparse(base_url)
    if parsed.scheme not in ("https", "http") or not parsed.hostname:
        raise ConfigError(f"base_url must be an http(s) URL, got {base_url!r}")
    if parsed.scheme == "http" and parsed.hostname not in LOCAL_HTTP_HOSTS:
        raise ConfigError(
            "base_url must use https. Plain http is allowed only for "
            f"{', '.join(sorted(LOCAL_HTTP_HOSTS))} (local testing)."
        )
    # The SDK takes the host without the /api/v1 suffix.
    return base_url.rstrip("/").removesuffix("/api/v1")


def _row_bytes(record: Mapping[str, Any]) -> int:
    return len(json.dumps(record, default=str, separators=(",", ":"))) + 1


def _is_retryable(e: Exception, idempotent: bool) -> bool:
    if isinstance(e, ConflictError):
        return e.code in TRANSIENT_CONFLICT_CODES
    if isinstance(e, RateLimitError):
        return True
    if not isinstance(e, APIError):
        return False
    if e.status == 0:
        # The SDK wraps transport faults as status 0. A refused connection never
        # reached the server; a timeout may have been applied.
        return idempotent or isinstance(e.__cause__, httpx.ConnectError)
    return idempotent and e.status >= 500


def _retry_delay(e: Exception, attempt: int) -> float:
    if isinstance(e, RateLimitError) and getattr(e, "retry_after", None):
        # Waiting less than the server asked only spends an attempt on another 429.
        return min(float(e.retry_after), MAX_RETRY_AFTER_SECONDS)
    if isinstance(e, ConflictError):
        return CONFLICT_RETRY_SECONDS
    return min(2.0**attempt, MAX_BACKOFF_SECONDS)


class _Writer:
    """Calls the Querri API with the destination's retry policy."""

    def __init__(self, client: Querri, sleep: Callable[[float], None] = time.sleep):
        self.client = client
        self._sleep = sleep

    def call(self, what: str, idempotent: bool, fn: Callable[[], Any]) -> Any:
        for attempt in range(MAX_ATTEMPTS):
            try:
                return fn()
            except Exception as e:  # noqa: BLE001 — classified below
                if attempt + 1 >= MAX_ATTEMPTS or not _is_retryable(e, idempotent):
                    raise
                delay = _retry_delay(e, attempt)
                logger.warning(f"{what} failed ({_format_error(e)}); retrying in {delay:.0f}s")
                self._sleep(delay)

    def list_sources(self) -> List[Dict[str, Any]]:
        # Auto-paginating, newest first (the server sorts by updated_at desc).
        return self.call("List sources", True, lambda: list(self.client.sources.list(limit=100)))

    def create(self, name: str, rows: List[Dict[str, Any]]) -> str:
        result = self.call(
            f"Create source '{name}'", False,
            lambda: self.client.sources.create_data_source(name=name, rows=rows))
        logger.info(f"Created source '{name}' ({result.id}) with {result.row_count} rows")
        return result.id

    def append(self, source_id: str, rows: List[Dict[str, Any]]) -> None:
        result = self.call(
            f"Append to {source_id}", False,
            lambda: self.client.sources.append_rows(source_id, rows=rows))
        logger.info(f"Appended {len(rows)} rows to {source_id} → {result.row_count} total")

    def replace(self, source_id: str, rows: List[Dict[str, Any]]) -> None:
        result = self.call(
            f"Replace {source_id}", True,
            lambda: self.client.sources.replace_data(source_id, rows=rows))
        logger.info(f"Replaced {source_id} with {len(rows)} rows → {result.row_count} total")


def _is_writable(source: Mapping[str, Any]) -> bool:
    return source.get("service") == WRITABLE_SERVICE and not source.get("connector_id")


def _chunks(rows: Iterable[Tuple[Dict[str, Any], int]], max_rows: int) -> Iterator[List[Dict[str, Any]]]:
    """Group (row, size) pairs into request bodies under the row and byte caps."""
    chunk: List[Dict[str, Any]] = []
    size = 0
    for row, row_size in rows:
        if chunk and (len(chunk) >= max_rows or size + row_size > MAX_REQUEST_BYTES):
            yield chunk
            chunk, size = [], 0
        chunk.append(row)
        size += row_size
    if chunk:
        yield chunk


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
        # False for a source that exists but has no data yet (an append would 400).
        self.materialized = True

    @property
    def is_overwrite(self) -> bool:
        return self.sync_mode == DestinationSyncMode.overwrite


class _AppendWriter:
    """Buffers one batch in memory; flush() makes every buffered row durable."""

    def __init__(self, cfg: _StreamConfig, writer: _Writer, batch_size: int):
        self.cfg = cfg
        self.writer = writer
        self.batch_size = batch_size
        self.rows: List[Dict[str, Any]] = []
        self.bytes = 0

    def add(self, record: Dict[str, Any]) -> None:
        self.rows.append(record)
        self.bytes += _row_bytes(record)
        if len(self.rows) >= self.batch_size or self.bytes >= MAX_REQUEST_BYTES:
            self.flush()

    def flush(self) -> None:
        if not self.rows:
            return
        cfg = self.cfg
        for chunk in _chunks(((r, _row_bytes(r)) for r in self.rows), self.batch_size):
            if cfg.querri_source_id is None:
                cfg.querri_source_id = self.writer.create(cfg.querri_source_name, chunk)
            elif not cfg.materialized:
                self.writer.replace(cfg.querri_source_id, chunk)
            else:
                self.writer.append(cfg.querri_source_id, chunk)
            cfg.materialized = True
        self.rows, self.bytes = [], 0


class _OverwriteWriter:
    """Spools a full-refresh stream to disk; finalize() replaces the source.

    A stream that fits in one request (<= MAX_BATCH_SIZE rows and
    MAX_REQUEST_BYTES) is one PUT: readers see the old rows until it lands, then
    the new rows, never a mix. A larger stream is a PUT of the first chunk then
    appends of the rest, because the server has no staged replace: while those
    requests run readers see a partial table, and a failure part-way leaves it
    partial. The sync then fails with no state acknowledged, and the next attempt
    replaces the whole table again.
    """

    def __init__(self, cfg: _StreamConfig, writer: _Writer):
        self.cfg = cfg
        self.writer = writer
        self.count = 0
        self.bytes = 0
        self._spool = tempfile.TemporaryFile(mode="w+", encoding="utf-8")

    def add(self, record: Dict[str, Any]) -> None:
        line = json.dumps(record, default=str, separators=(",", ":"))
        self._spool.write(line)
        self._spool.write("\n")
        self.count += 1
        self.bytes += len(line) + 1

    def _read_back(self) -> Iterator[Tuple[Dict[str, Any], int]]:
        self._spool.seek(0)
        for line in self._spool:
            yield json.loads(line), len(line)

    def finalize(self) -> None:
        cfg = self.cfg
        if self.count == 0:
            # The API refuses an empty replace (rows needs at least one item),
            # so an empty full refresh cannot empty the source.
            if cfg.querri_source_id:
                logger.warning(
                    f"Stream '{cfg.stream_name}' sent 0 records; the Querri API cannot "
                    f"empty a source, so '{cfg.querri_source_name}' keeps its previous rows."
                )
            else:
                logger.info(f"Stream '{cfg.stream_name}' sent 0 records; no source created.")
            return
        single = self.count <= MAX_BATCH_SIZE and self.bytes <= MAX_REQUEST_BYTES
        if not single:
            logger.warning(
                f"Stream '{cfg.stream_name}' has {self.count} rows ({self.bytes} bytes), more "
                f"than one request holds ({MAX_BATCH_SIZE} rows / {MAX_REQUEST_BYTES} bytes). "
                "Replacing in several requests: readers see a partial table until the last one lands."
            )
        # The server's max per chunk, not batch_size: fewer requests mean a shorter
        # partial window and fewer view-refresh cascades.
        for i, chunk in enumerate(_chunks(self._read_back(), MAX_BATCH_SIZE)):
            if i > 0:
                self.writer.append(cfg.querri_source_id, chunk)
            elif cfg.querri_source_id is None:
                cfg.querri_source_id = self.writer.create(cfg.querri_source_name, chunk)
            else:
                self.writer.replace(cfg.querri_source_id, chunk)

    def close(self) -> None:
        self._spool.close()


def _state_key(message: AirbyteMessage) -> Tuple[str, ...]:
    state = message.state
    if state.type == AirbyteStateType.STREAM and state.stream:
        desc = state.stream.stream_descriptor
        return ("stream", desc.namespace or "", desc.name)
    if state.type == AirbyteStateType.GLOBAL:
        return ("global",)
    return ("legacy",)


class DestinationQuerri(Destination):
    """Writes Airbyte records to Querri data sources."""

    # Indirections so tests can skip retry waits and control the flush clock.
    _sleep = staticmethod(time.sleep)
    _now = staticmethod(time.monotonic)

    def check(
        self, logger: logging.Logger, config: Mapping[str, Any]
    ) -> AirbyteConnectionStatus:
        """Verify the credentials can list sources and hold the data:write scope."""
        try:
            prefix = config.get("source_name_prefix", "airbyte_")
            if len(prefix) >= MAX_SOURCE_NAME:
                raise ConfigError(
                    f"source_name_prefix is {len(prefix)} characters; with the stream name it "
                    f"must stay under Querri's {MAX_SOURCE_NAME}-character source name limit.")
            client = self._make_client(config)
            # Force iteration to trigger the HTTP call (SyncCursorPage is lazy)
            next(iter(client.sources.list(limit=1)), None)
            self._check_write_scope(client)
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(
                status=Status.FAILED,
                message=_format_error(e),
            )

    @staticmethod
    def _check_write_scope(client: Querri) -> None:
        """Replace a source id that no source has.

        The scope is checked before the source is looked up, so a key without
        data:write is refused with ``insufficient_scope``; with the scope the
        server refuses the unknown id instead, and nothing is written either way.
        """
        try:
            client.sources.replace_data(_PROBE_SOURCE_ID, rows=[{"probe": 1}])
        except APIError as e:
            if e.code == "insufficient_scope":
                raise QuerriPermissionError(
                    "API key lacks the data:write scope.", status=e.status, code=e.code
                ) from e
            if isinstance(e, AuthenticationError) or e.status == 0 or e.status >= 500:
                raise

    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[AirbyteMessage]:
        """Write records to Querri, yielding each STATE once it is durable.

        See the module docstring for the state contract per sync mode.
        """
        try:
            client = self._make_client(config)
        except ConfigError as e:
            raise AirbyteTracedException(
                message=str(e), failure_type=FailureType.config_error) from e
        writer = _Writer(client, sleep=self._sleep)
        prefix = config.get("source_name_prefix", "airbyte_")
        batch_size = max(1, min(int(config.get("batch_size", DEFAULT_BATCH_SIZE)), MAX_BATCH_SIZE))

        stream_configs: Dict[str, _StreamConfig] = {}
        for configured_stream in configured_catalog.streams:
            stream_name = configured_stream.stream.name
            sync_mode = configured_stream.destination_sync_mode
            if sync_mode not in (DestinationSyncMode.overwrite, DestinationSyncMode.append):
                raise AirbyteTracedException(
                    message=f"Stream '{stream_name}': sync mode {sync_mode.value} is not "
                    "supported. Use Overwrite or Append.",
                    failure_type=FailureType.config_error,
                )
            if stream_name in stream_configs:
                raise AirbyteTracedException(
                    message=f"Two streams are named '{stream_name}' (different namespaces); "
                    "both would write the same Querri source.",
                    failure_type=FailureType.config_error,
                )
            source_name = f"{prefix}{stream_name}"
            if len(source_name) > MAX_SOURCE_NAME:
                raise AirbyteTracedException(
                    message=f"Stream '{stream_name}' would be written to a source named "
                    f"'{source_name[:40]}...' ({len(source_name)} characters); Querri allows "
                    f"{MAX_SOURCE_NAME}. Shorten the prefix or rename the stream.",
                    failure_type=FailureType.config_error,
                )
            stream_configs[stream_name] = _StreamConfig(
                stream_name=stream_name,
                querri_source_name=source_name,
                sync_mode=sync_mode,
            )

        self._resolve_sources(writer, stream_configs)

        appenders: Dict[str, _AppendWriter] = {}
        overwriters: Dict[str, _OverwriteWriter] = {}
        for name, cfg in stream_configs.items():
            if cfg.is_overwrite:
                overwriters[name] = _OverwriteWriter(cfg, writer)
            else:
                appenders[name] = _AppendWriter(cfg, writer, batch_size)

        # States held until every overwrite stream is replaced; latest per key.
        deferred: "OrderedDict[Tuple[str, ...], AirbyteMessage]" = OrderedDict()
        # States waiting for the next append flush, in arrival order.
        pending: List[AirbyteMessage] = []
        last_flush = self._now()
        warned_global = False

        def flush_appends() -> None:
            for appender in appenders.values():
                appender.flush()

        try:
            for message in input_messages:
                if message.type == Type.RECORD:
                    stream_name = message.record.stream
                    target = appenders.get(stream_name) or overwriters.get(stream_name)
                    if target is None:
                        logger.warning(f"Received record for unconfigured stream: {stream_name}")
                        continue
                    target.add(message.record.data)

                elif message.type == Type.STATE:
                    if self._covers_overwrite(message, overwriters):
                        if message.state.type != AirbyteStateType.STREAM and not warned_global:
                            warned_global = True
                            logger.warning(
                                "The source sends GLOBAL state and the catalog has an Overwrite "
                                "stream, so no checkpoint is acknowledged until every overwrite "
                                "is written at the end of the sync. A failed sync restarts from "
                                "its beginning.")
                        key = _state_key(message)
                        deferred.pop(key, None)
                        deferred[key] = message
                        continue
                    pending.append(message)
                    buffered = sum(len(a.rows) for a in appenders.values())
                    if (buffered == 0 or buffered >= STATE_FLUSH_ROWS
                            or self._now() - last_flush >= STATE_FLUSH_SECONDS):
                        flush_appends()
                        last_flush = self._now()
                        yield from pending
                        pending.clear()

            flush_appends()
            yield from pending
            pending.clear()
            for overwriter in overwriters.values():
                overwriter.finalize()
        finally:
            for overwriter in overwriters.values():
                overwriter.close()

        if deferred:
            logger.info(f"Every overwrite stream is written; emitting {len(deferred)} held state(s)")
        yield from deferred.values()

    @staticmethod
    def _covers_overwrite(message: AirbyteMessage, overwriters: Mapping[str, _OverwriteWriter]) -> bool:
        if not overwriters:
            return False
        state = message.state
        if state.type == AirbyteStateType.STREAM and state.stream:
            return state.stream.stream_descriptor.name in overwriters
        # GLOBAL and LEGACY state cover every stream in the sync.
        return True

    @staticmethod
    def _resolve_sources(writer: _Writer, stream_configs: Dict[str, _StreamConfig]) -> None:
        """Map each stream to the existing API source with its name, or mark it
        for creation.

        Only an API source (service "api", no connector) is written to; any other
        source with the name, an upload, a View or a connector's table, refuses
        the sync rather than being overwritten, and so do two API sources with
        the name. A failed listing fails the sync: treating it as "no sources"
        would create a second source with the name.
        """
        wanted = {cfg.querri_source_name for cfg in stream_configs.values()}
        by_name: Dict[str, List[Dict[str, Any]]] = {}
        for source in writer.list_sources():
            if source.get("name") in wanted:
                by_name.setdefault(source["name"], []).append(source)

        for cfg in stream_configs.values():
            matches = by_name.get(cfg.querri_source_name, [])
            if not matches:
                logger.info(
                    f"Stream '{cfg.stream_name}' → will create source '{cfg.querri_source_name}'"
                )
                continue
            others = [s for s in matches if not _is_writable(s)]
            if others:
                kind = others[0].get("service") or "connector"
                raise AirbyteTracedException(
                    message=f"A Querri source named '{cfg.querri_source_name}' already exists "
                    f"({others[0]['id']}, a {kind} source) that this destination did not create, "
                    "so it is not written to. Rename that source or change the Source Name Prefix.",
                    failure_type=FailureType.config_error,
                )
            if len(matches) > 1:
                raise AirbyteTracedException(
                    message=f"{len(matches)} Querri sources are named '{cfg.querri_source_name}' "
                    f"({', '.join(s['id'] for s in matches)}). Delete or rename all but one.",
                    failure_type=FailureType.config_error,
                )
            existing = matches[0]
            cfg.querri_source_id = existing["id"]
            cfg.materialized = existing.get("materialized", True) is not False
            logger.info(
                f"Stream '{cfg.stream_name}' → existing source "
                f"'{cfg.querri_source_name}' ({cfg.querri_source_id})"
            )

    @staticmethod
    def _make_client(config: Mapping[str, Any]) -> Querri:
        host = _validate_base_url(config.get("base_url") or DEFAULT_BASE_URL)
        # Retries are the destination's own (_Writer): the SDK would resend a
        # timed-out append, which may already have been applied.
        return Querri(
            api_key=config["api_key"],
            org_id=config["org_id"],
            host=host,
            max_retries=0,
        )
