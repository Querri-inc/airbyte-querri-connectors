# Querri Source

<HideInUI>

This page contains the setup guide and reference information for the [Querri](https://querri.com) source connector.

</HideInUI>

## Prerequisites

- A Querri account with API access
- A Querri API key with `data:read` scope
- Your Querri Organization ID

## Setup Guide

### For Airbyte Cloud

1. Log in to your [Airbyte Cloud](https://cloud.airbyte.com/workspaces) account.
2. Navigate to **Sources** and click **+ New source**.
3. Search for "Querri" and select it.
4. Enter your **API Key** (starts with `qk_`).
5. Enter your **Organization ID** (starts with `org_`).
6. (Optional) Enter a custom **Base URL** if using a self-hosted Querri instance.
7. (Optional) Enter specific **Source IDs** to limit which data sources are synced.
8. Click **Set up source**.

### For Airbyte Open Source

1. Navigate to **Sources** and click **+ New source**.
2. Search for "Querri" and select it.
3. Configure the connection:
   - **API Key**: Your `qk_` API key from Querri Settings > API Keys
   - **Organization ID**: Your `org_` ID from Querri Settings > Account
   - **Base URL** (optional): Default is `https://app.querri.com/api/v1`
   - **Source IDs** (optional): Array of specific source UUIDs to sync

## Supported Sync Modes

| Feature | Supported |
|---------|-----------|
| Full Refresh - Overwrite | Yes |
| Full Refresh - Append | Yes |
| Incremental - Append | Yes |
| Incremental - Append + Dedup | No |

### Incremental Sync

The connector tracks the `updated_at` timestamp on each Querri data source. On incremental syncs, sources that haven't changed since the last sync are skipped entirely. When a source has been updated, its full data is re-read.

## Supported Streams

Each data source in your Querri organization becomes a separate Airbyte stream. Stream names are derived from the source name (lowercased, special characters replaced with underscores).

Every record includes two metadata fields:

- `_querri_source_id`: The UUID of the Querri data source
- `_querri_source_name`: The original name of the Querri data source

The JSON schema for each stream is dynamically generated from the Querri source's `column_types` metadata (retrieved via the source detail endpoint).

## Configuration Reference

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `api_key` | string | Yes | | Your `qk_` API key from Settings > API Keys. Requires `data:read` scope. |
| `org_id` | string | Yes | | Your Querri organization ID from Settings > Account. |
| `base_url` | string | No | `https://app.querri.com/api/v1` | Querri API base URL. Change only for self-hosted instances. |
| `source_ids` | array of strings | No | `[]` | Limit sync to specific Querri source UUIDs. Leave empty to sync all accessible sources. |

## Performance Considerations

### Rate Limiting

The Querri API enforces rate limits per API key (default: 60 requests/minute). The connector uses the Querri Python SDK, which automatically retries on rate limit responses (HTTP 429) with exponential backoff respecting the `Retry-After` header.

### Large Organizations

During discovery, the connector makes one API call per data source to retrieve column type metadata. For organizations with many data sources, this can be slow and may hit rate limits:

- An org with 200 sources requires approximately 202 API calls (2 list pages + 200 detail calls).
- At 60 requests/minute, discovery takes approximately 3.5 minutes.

**Recommendation**: For organizations with 50+ data sources, use the `source_ids` configuration to limit which sources are synced.

### Data Pagination

Data is read in pages of 10,000 rows per request. The connector handles pagination automatically and verifies completeness using the `total_rows` count from the API response.

## Changelog

<details>
<summary>Expand to review</summary>

| Version | Date | Pull Request | Subject |
|---------|------|-------------|---------|
| 0.1.0 | 2026-04-03 | | Initial release: full refresh, incremental sync, SDK-based client |

</details>
