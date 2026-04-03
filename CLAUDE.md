# CLAUDE.md — Querri Airbyte Connectors

## Terminology

**Critical distinction** — "source" means two different things:

- **Querri source**: A dataset/table in the Querri platform (like a spreadsheet). Has columns, rows, column_types.
- **Airbyte source** (this connector): The `source-querri` package that *reads from* Querri sources and emits Airbyte records.

The destination connector *writes to* Querri sources. Both connectors use the Querri Python SDK.

## Package Structure

```
airbyte/
├── source-querri/source_querri/     # Airbyte source connector
│   ├── source.py                    # SourceQuerri — discovery + check
│   ├── streams.py                   # QuerriSourceStream — data reading
│   ├── client.py                    # Legacy client (used only by e2e tests)
│   └── spec.yaml                    # Airbyte connector spec
├── destination-querri/destination_querri/
│   ├── destination.py               # DestinationQuerri — buffered writes
│   └── spec.json                    # Airbyte connector spec
├── e2e_test.py                      # End-to-end tests (requires live instance)
├── CLAUDE.md                        # This file
└── SPEC.md                          # Full specification
```

Both connectors depend on the **Querri Python SDK** (`querri-python-sdk/`) — not raw HTTP. The SDK handles auth headers, retry with backoff, cursor pagination, and typed response models.

## Commands

```bash
# Install SDK (from repo root)
pip install -e querri-python-sdk/

# Install connectors
pip install -e airbyte/source-querri/
pip install -e airbyte/destination-querri/

# Run SDK unit tests
pytest querri-python-sdk/tests/ -v

# Run e2e tests (requires running Querri instance + env vars)
cd airbyte
QUERRI_API_KEY=qk_... QUERRI_READ_ONLY_KEY=qk_... QUERRI_ORG_ID=org_... python e2e_test.py
```

### Required Environment Variables for E2E Tests

| Variable | Required | Description |
|----------|----------|-------------|
| `QUERRI_API_KEY` | Yes | Read-write API key |
| `QUERRI_READ_ONLY_KEY` | Yes | Read-only API key (for scope tests) |
| `QUERRI_ORG_ID` | Yes | Organization ID |
| `QUERRI_BASE_URL` | No | Defaults to `http://localhost/api/v1` |

## Architecture

### Source Connector Flow

1. **Check**: `client.data.sources(limit=1)` — verifies credentials.
2. **Discover**: `list(client.data.sources())` auto-paginates all sources. For each, calls `client.data.source(id)` to get `column_types` for JSON schema generation.
3. **Read**: `QuerriSourceStream.read_records()` paginates through `client.data.source_data()` in 10k-row pages. Enriches each row with `_querri_source_id` and `_querri_source_name`.
4. **Incremental**: Tracks `updated_at` cursor per source. Skips unchanged sources.

### Destination Connector Flow

1. **Check**: `client.data.sources(limit=1)` — verifies credentials.
2. **Resolve**: Maps each Airbyte stream to an existing Querri source by name (`airbyte_` prefix).
3. **Buffer**: Collects records in memory per stream.
4. **Flush**: On state message or end-of-input:
   - **Append mode**: `client.data.append_rows()` in batches of 10k.
   - **Overwrite mode**: `client.data.replace_data()` with all buffered records.
   - **New source**: `client.data.create_source()` then append overflow.

### SDK Integration

The connectors use `from querri import Querri` with:
- `host` parameter (SDK appends `/api/v1`): strip suffix from Airbyte's `base_url` config.
- Auto-paginating `client.data.sources()` — fixes the old single-page bug.
- Typed models: `Source`, `DataWriteResult`, `DeleteResult`, `DataPage`.
- Built-in retry: 429 with Retry-After, 5xx with exponential backoff (3 retries default).
- Typed exceptions: `AuthenticationError`, `PermissionError`, `RateLimitError`, `NotFoundError`.

## API Endpoints Used

| Endpoint | SDK Method | Used By |
|----------|-----------|---------|
| `GET /data/sources` | `client.data.sources()` | Both (discovery, resolution) |
| `GET /data/sources/{id}` | `client.data.source(id)` | Source (schema + column_types) |
| `GET /data/sources/{id}/data` | `client.data.source_data(id)` | Source (read records) |
| `POST /data/sources` | `client.data.create_source()` | Destination (new sources) |
| `POST /data/sources/{id}/rows` | `client.data.append_rows()` | Destination (append mode) |
| `PUT /data/sources/{id}/data` | `client.data.replace_data()` | Destination (overwrite mode) |
| `DELETE /data/sources/{id}` | `client.data.delete_source()` | E2E tests (cleanup) |

## Common Tasks

### Adding a new stream type

1. Create a new `Stream` subclass in `streams.py`.
2. Override `get_json_schema()`, `read_records()`, and `name`.
3. Register it in `source.py`'s `streams()` method.

### Adding a new API endpoint to the SDK

Follow `querri-python-sdk/CLAUDE.md` — add types in `types/data.py`, methods in both `Data` and `AsyncData` classes in `resources/data.py`.

## Known Limitations

- **N+1 discovery**: Source connector calls `source()` per discovered source to get `column_types`. For orgs with many sources, use the `source_ids` config filter to limit scope.
- **Rate limits**: SDK retries automatically, but large orgs (>50 sources) may hit 60/min limit during discovery. Use `source_ids` filter.
