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

1. **Check**: `client.sources.list()` — verifies credentials.
2. **Discover**: `client.sources.list()` returns all sources as `List[Dict]`. For each, calls `client.sources.get(id)` to get `column_types` for JSON schema generation. Both return dicts (access via `source["id"]`, `source["name"]`).
3. **Read**: `QuerriSourceStream.read_records()` paginates through `client.sources.source_data()` in 10k-row pages. Enriches each row with `_querri_source_id` and `_querri_source_name`.
4. **Incremental**: Tracks `updated_at` cursor per source. Skips unchanged sources.

### Destination Connector Flow

1. **Check**: `client.sources.list()` — verifies credentials.
2. **Resolve**: Maps each Airbyte stream to an existing Querri source by name (`airbyte_` prefix). `list()` returns `List[Dict]` — access via `source["name"]`, `source["id"]`.
3. **Buffer**: Collects records in memory per stream.
4. **Flush**: On state message or end-of-input:
   - **Append mode**: `client.sources.append_rows()` in batches of 10k.
   - **Overwrite mode**: `client.sources.replace_data()` with all buffered records.
   - **New source**: `client.sources.create_data_source()` then append overflow.

### SDK Integration

The connectors use `from querri import Querri` with:
- `host` parameter (SDK appends `/api/v1`): strip suffix from Airbyte's `base_url` config.
- All methods via `client.sources.*` (the `client.data` namespace no longer exists).
- `list()` and `get()` return plain dicts (access via `["key"]`). `create_data_source()`, `source_data()`, `append_rows()`, `replace_data()` still return Pydantic models (`Source`, `DataPage`, `DataWriteResult`).
- Built-in retry: 429 with Retry-After, 5xx with exponential backoff (3 retries default).
- Typed exceptions: `AuthenticationError`, `PermissionError`, `RateLimitError`, `NotFoundError`.

## API Endpoints Used

| Endpoint | SDK Method | Used By |
|----------|-----------|---------|
| `GET /sources` | `client.sources.list()` | Both (discovery, resolution) |
| `GET /sources/{id}` | `client.sources.get(id)` | Source (schema + column_types) |
| `GET /sources/{id}/data` | `client.sources.source_data(id)` | Source (read records) |
| `POST /sources` | `client.sources.create_data_source()` | Destination (new sources) |
| `POST /sources/{id}/rows` | `client.sources.append_rows()` | Destination (append mode) |
| `PUT /sources/{id}/data` | `client.sources.replace_data()` | Destination (overwrite mode) |
| `DELETE /sources/{id}` | `client.sources.delete()` | E2E tests (cleanup) |

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
