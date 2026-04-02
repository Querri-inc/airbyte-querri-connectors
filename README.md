# Querri Airbyte Connectors

Airbyte source and destination connectors for the [Querri Data API](https://docs.querri.com/api/data-api).

## Connectors

### Source Connector (`source-querri`)

Reads data from Querri sources into Airbyte. Each Querri data source becomes an Airbyte stream.

**Supported sync modes:**
- Full Refresh
- Incremental (cursor: `updated_at` on source metadata)

### Destination Connector (`destination-querri`)

Writes data from Airbyte into Querri sources.

**Supported sync modes:**
- Overwrite (replaces all data via `PUT /data/sources/{id}/data`)
- Append (adds rows via `POST /data/sources/{id}/rows`)

## Prerequisites

- A Querri account with API access enabled
- A `qk_` API key created at **Settings → API Keys** in Querri
- Your Org ID from **Settings → Account**

### API Key Scopes

| Connector | Required scopes |
|-----------|----------------|
| Source (read from Querri) | `data:read` |
| Destination (write to Querri) | `data:write`, `data:read` |

The destination needs `data:read` in addition to `data:write` to verify source existence, check schemas, and list available sources.

## Quick Start

### Source Connector

```bash
cd source-querri
pip install -e .

# Check connection
python main.py check --config secrets/config.json

# Discover available streams
python main.py discover --config secrets/config.json

# Read data
python main.py read --config secrets/config.json --catalog integration_tests/configured_catalog.json
```

### Destination Connector

```bash
cd destination-querri
pip install -e .

# Check connection
python main.py check --config secrets/config.json

# Write data (pipe from a source or use test input)
cat test_messages.jsonl | python main.py write --config secrets/config.json --catalog integration_tests/configured_catalog.json
```

### Docker

```bash
# Source
cd source-querri
docker build -t airbyte/source-querri:dev .
docker run airbyte/source-querri:dev spec

# Destination
cd destination-querri
docker build -t airbyte/destination-querri:dev .
docker run airbyte/destination-querri:dev spec
```

## Configuration

Both connectors use the same connection parameters:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `api_key` | string | Yes | Your `qk_` API key |
| `org_id` | string | Yes | Your Querri organization ID (from **Settings → Account**) |
| `base_url` | string | No | API base URL (default: `https://app.querri.com/api/v1`) |

### Source-specific configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source_ids` | array | No | Limit to specific source IDs. Empty = all sources. |

### Destination-specific configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source_name_prefix` | string | No | Prefix for auto-created sources (default: `airbyte_`) |
| `batch_size` | integer | No | Rows per write request (default: 10000, max: 100000) |

### Example config.json

```json
{
  "api_key": "qk_your_key_here",
  "org_id": "org_your_org_id",
  "base_url": "https://app.querri.com/api/v1"
}
```

## How It Works

### Source: Querri → Airbyte

1. **Discover**: Calls `GET /data/sources` to list all accessible sources. Each source becomes an Airbyte stream with its column schema.
2. **Full Refresh**: Reads all data from the source via `GET /data/sources/{id}/data` with pagination (10,000 rows per page).
3. **Incremental**: Tracks `updated_at` timestamp from source metadata. On subsequent syncs, only reads sources whose `updated_at` is newer than the last sync. Re-reads the full source data when it detects changes.

### Destination: Airbyte → Querri

1. **Check**: Calls `GET /data/sources` to verify API key and connectivity.
2. **Stream mapping**: Maps each Airbyte stream to a Querri source by name (with configurable prefix).
3. **Overwrite mode**: Calls `PUT /data/sources/{id}/data` to replace all data.
4. **Append mode**: Buffers records and calls `POST /data/sources/{id}/rows` in batches.
5. **Auto-create**: If a source doesn't exist for a stream, creates it via `POST /data/sources`.

## Testing

### Unit Tests

```bash
# Source connector (18 tests)
cd source-querri && pytest -v

# Destination connector (12 tests)
cd destination-querri && pytest -v
```

### End-to-End Tests

The `e2e_test.py` script runs 26 tests against a live Querri instance, covering the full lifecycle:

```bash
# Requires a running Querri instance and valid API keys
python e2e_test.py

# Or use environment variables
QUERRI_API_KEY=qk_... QUERRI_ORG_ID=org_... QUERRI_BASE_URL=http://localhost/api/v1 python e2e_test.py
```

**E2E test coverage:**
- Connection and authentication (valid, invalid, wrong org, read-only)
- Source CRUD lifecycle (create, list, get metadata, read data, delete)
- Append rows with verification
- Replace/overwrite data with verification
- Pagination across multiple pages
- Read-only key scope enforcement
- Source connector protocol (check, discover, read, schema)
- Destination connector protocol (check, append, append-more, overwrite)
- Full round-trip (write via destination connector, read back via source connector)

## Development

```bash
# Install dev dependencies
cd source-querri && pip install -e ".[dev]"
cd destination-querri && pip install -e ".[dev]"

# Run unit tests
cd source-querri && pytest -v
cd destination-querri && pytest -v

# Run E2E tests (requires running Querri instance)
python e2e_test.py
```

## Limits

These limits come from the Querri Data API:

| Limit | Value |
|-------|-------|
| Rows per request | 100,000 |
| Payload size | 50 MB |
| Rate limit | Per-key setting (default 60/min) |
