# Querri Destination

<HideInUI>

This page contains the setup guide and reference information for the [Querri](https://querri.com) destination connector.

</HideInUI>

## Prerequisites

- A Querri account with API access
- A Querri API key with `data:read` and `data:write` scopes
- Your Querri Organization ID

The destination requires both `data:read` and `data:write` scopes. The `data:read` scope is needed to verify source existence, check schemas, and list available sources during stream-to-source resolution.

## Setup Guide

### For Airbyte Cloud

1. Log in to your [Airbyte Cloud](https://cloud.airbyte.com/workspaces) account.
2. Navigate to **Destinations** and click **+ New destination**.
3. Search for "Querri" and select it.
4. Enter your **API Key** (starts with `qk_`).
5. Enter your **Organization ID** (starts with `org_`).
6. (Optional) Enter a custom **Base URL** if using a self-hosted Querri instance.
7. (Optional) Adjust the **Source Name Prefix** (default: `airbyte_`).
8. (Optional) Adjust the **Batch Size** (default: 10,000).
9. Click **Set up destination**.

### For Airbyte Open Source

1. Navigate to **Destinations** and click **+ New destination**.
2. Search for "Querri" and select it.
3. Configure the connection:
   - **API Key**: Your `qk_` API key from Querri Settings > API Keys (requires `data:read` and `data:write` scopes)
   - **Organization ID**: Your `org_` ID from Querri Settings > Account
   - **Base URL** (optional): Default is `https://app.querri.com/api/v1`
   - **Source Name Prefix** (optional): Prefix added to Airbyte stream names when creating Querri sources (default: `airbyte_`)
   - **Batch Size** (optional): Number of rows per API write request (default: 10,000)

## Supported Sync Modes

| Feature | Supported |
|---------|-----------|
| Overwrite | Yes |
| Append | Yes |
| Append + Dedup | No |

### Overwrite Mode

Replaces all existing data in the target Querri source with the incoming records. Uses the `PUT /sources/{id}/data` endpoint. All records are buffered in memory before the replacement write to avoid partial overwrites.

### Append Mode

Adds new rows to the target Querri source without removing existing data. Uses the `POST /sources/{id}/rows` endpoint. Records are buffered and flushed in batches (controlled by the `batch_size` configuration).

## Stream-to-Source Mapping

The destination connector maps each Airbyte stream to a Querri source by name using the following convention:

```
Querri source name = {source_name_prefix}{stream_name}
```

For example, with the default prefix `airbyte_`:

| Airbyte Stream | Querri Source Name |
|----------------|--------------------|
| `users` | `airbyte_users` |
| `orders` | `airbyte_orders` |
| `page_views` | `airbyte_page_views` |

### Auto-Creation of Sources

If a Querri source does not already exist for a given stream, the connector automatically creates it via `POST /sources`. The new source's schema is inferred from the first batch of records. Subsequent batches are appended to the newly created source.

This means you do not need to pre-create Querri sources before running a sync -- the connector handles it.

## Configuration Reference

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `api_key` | string | Yes | | Your `qk_` API key from Settings > API Keys. Requires `data:write` and `data:read` scopes. |
| `org_id` | string | Yes | | Your Querri organization ID from Settings > Account. |
| `base_url` | string | No | `https://app.querri.com/api/v1` | Querri API base URL. Change only for self-hosted instances. |
| `source_name_prefix` | string | No | `airbyte_` | Prefix added to stream names when creating or resolving Querri sources. |
| `batch_size` | integer | No | `10000` | Number of rows to send per API request. Min: 1, Max: 100,000. Higher values are more efficient but use more memory. |

## API Limits

The following limits are enforced by the Querri Data API:

| Limit | Value |
|-------|-------|
| Rows per request | 100,000 |
| Payload size per request | 50 MB |
| Rate limit | Per-key setting (default 60 requests/minute) |

The default `batch_size` of 10,000 is well within these limits. Increase it for better throughput if your records are small, or decrease it if individual records are large (to stay under the 50 MB payload limit).

## Performance Considerations

### Rate Limiting

The Querri API enforces rate limits per API key (default: 60 requests/minute). The connector uses the Querri Python SDK, which automatically retries on rate limit responses (HTTP 429) with exponential backoff respecting the `Retry-After` header. Server errors (5xx) are also retried up to 3 times.

### Batch Size Tuning

- **Small records** (few columns, short strings): Increase `batch_size` toward 100,000 for fewer API calls and faster syncs.
- **Large records** (many columns, long text fields): Decrease `batch_size` to stay under the 50 MB payload limit per request.
- **Memory usage**: All records for a given stream are buffered in memory before flushing. In overwrite mode, the entire stream is buffered before writing. Plan memory accordingly for large datasets.

### Large Organizations

During stream resolution, the connector lists all existing Querri sources (with auto-pagination) to match stream names. For organizations with many existing sources, this adds a small amount of overhead at the start of each sync.

## Changelog

<details>
<summary>Expand to review</summary>

| Version | Date | Pull Request | Subject |
|---------|------|-------------|---------|
| 0.1.0 | 2026-04-03 | | Initial release: overwrite and append modes, auto-creation, SDK-based client |

</details>
