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

The connector is distributed as a Docker image, `ghcr.io/querri-inc/destination-querri`, and is not in the Airbyte marketplace yet. Add it as a custom connector first: see [Querri destination: custom connector setup](../../destination-querri-setup.md).

### For Airbyte Cloud

1. Log in to your [Airbyte Cloud](https://cloud.airbyte.com/workspaces) account (custom connectors need a plan that allows them).
2. Navigate to **Destinations** and click **+ New destination**.
3. Select the Querri custom connector you added.
4. Enter your **API Key** (starts with `qk_`).
5. Enter your **Organization ID** (starts with `org_`).
6. (Optional) Enter a custom **Base URL** if using a self-hosted Querri instance.
7. (Optional) Adjust the **Source Name Prefix** (default: `airbyte_`).
8. (Optional) Adjust the **Batch Size** (default: 10,000).
9. Click **Set up destination**.

### For Airbyte Open Source

1. Navigate to **Destinations** and click **+ New destination**.
2. Select the Querri custom connector you added.
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
| Append + Dedup | No (the Querri API has no upsert endpoint, so the spec does not offer it) |

### Overwrite Mode

Replaces all rows of the target Querri source with the records of this sync (`PUT /sources/{id}/data`). Records are spooled to a temporary file on disk, not held in memory, and written once the source has sent its last record.

- **Up to 100,000 rows and about 40 MB of JSON**: one request, so the replace is atomic. Readers see the old rows until it lands and the new rows after, never a mix.
- **Larger streams**: the first chunk is a replace and the rest are appends, because the API has no staged replace. While those requests run, readers see a partial table. If one fails, the sync fails and the next attempt replaces the whole table again.
- **Zero records**: the API refuses an empty replace, so a full refresh that sends no records leaves the source's previous rows in place (and logs a warning). A stream with no existing source creates nothing.

### Append Mode

Adds rows to the target source (`POST /sources/{id}/rows`). At most one batch (`batch_size` rows, or about 40 MB) is buffered in memory.

### Checkpoints (state messages)

The destination acknowledges an Airbyte state message only once every record received before it has been written and the Querri API answered success:

- **Append**: a state is held until the buffered rows are flushed, and flushing happens when a state arrives with at least 10,000 rows buffered or 30 seconds after the last flush (a state that arrives with nothing buffered is emitted at once). The held states are then emitted in order. A source that sends a state every few records therefore costs one write per 10,000 rows or 30 seconds, not one per state. If a write fails the sync fails, and Airbyte resumes from the last acknowledged state.
- **Overwrite**: nothing is durable until the final replace, so states covering an overwrite stream are held. After every replace succeeds, the latest held state is emitted. If a replace fails, no state is acknowledged and the source keeps its old rows (for streams that fit in one request).

- **GLOBAL state with an Overwrite stream** (sources that send one state for the whole sync, typically CDC): every such state covers the overwrite stream, so none is acknowledged until the overwrite lands at the end. A failure anywhere in that sync restarts it from the beginning, and the append streams' rows from the failed attempt are written again (at least once, across the whole sync). The destination logs a warning when it sees this.

A sync that fails part-way through an append batch can write that batch again when Airbyte resumes: delivery is at least once. The destination never resends an append that may have been applied (a timeout or a 5xx); it resends only requests the server refused without applying (409 `sync_in_progress` / `write_in_progress`, 429) and connection failures.

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

### Which existing source is written to

A source with the stream's name is written to only if it is an **API source** (created through the Data API's rows endpoints, as this destination creates them, with no connector). If the name belongs to an upload, a View, a file or a connector's table, the sync fails with a configuration error instead of overwriting it, and so does a name shared by two sources. Rename the other source or change the Source Name Prefix. An API source you created yourself with the same name *is* written to (the API has no marker a `data:write` key can set to tell the two apart), so keep the `airbyte_` prefix unless you mean to target it.

Querri source names are limited to 200 characters; a prefix plus stream name over that fails the sync before anything is written.

A created source is homed in the workspace the API key is bound to. With an unbound key it lands in the **key creator's personal workspace**, invisible to teammates and other keys until shared. Note the prefix applies to every stream: a stream already named `airbyte_x` becomes `airbyte_airbyte_x` with the default prefix.

## Configuration Reference

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `api_key` | string | Yes | | Your `qk_` API key from Settings > API Keys. Requires `data:write` and `data:read` scopes. |
| `org_id` | string | Yes | | Your Querri organization ID from Settings > Account. |
| `base_url` | string | No | `https://app.querri.com/api/v1` | Querri API base URL. Must be https; plain http is accepted only for `localhost`, `127.0.0.1` and `host.docker.internal`. |
| `source_name_prefix` | string | No | `airbyte_` | Prefix added to stream names when creating or resolving Querri sources. |
| `batch_size` | integer | No | `10000` | Rows per append request. Min: 1, Max: 100,000. Overwrite streams send up to 100,000 rows per request regardless. |

## API Limits

The following limits are enforced by the Querri Data API:

| Limit | Value |
|-------|-------|
| Rows per request | 100,000 |
| Payload size per request | 50 MB |
| Rate limit | Per-key setting (default 60 requests/minute) |

The destination keeps every request under both limits (it splits by rows and by bytes), so `batch_size` only tunes append throughput.

## Performance Considerations

### Retries

The destination retries 429 (waiting the full `Retry-After`, up to 5 minutes) and 409 `sync_in_progress` / `write_in_progress` (the source is being synced or written, retried every 10 seconds) on every request; any other 409, such as a Sites-managed source, fails at once, and 5xx and timeouts on idempotent requests only (listing sources, replace). It does not resend an append after a timeout or 5xx, because that append may already have been applied.

### Batch Size Tuning

- **Small records**: increase `batch_size` toward 100,000 for fewer append requests.
- **Memory**: append holds one batch in memory; overwrite holds its rows on local disk, so the container needs free disk roughly equal to the stream's size as JSON.

### Allowed hosts

`metadata.yaml` declares `allowedHosts: app.querri.com, *.querri.com`. Airbyte applies allowed hosts from a connector's registry entry; a custom connector added by image has none, so today the destination connects to whatever `base_url` says (https, or http for local hosts only). If the connector is listed in the Airbyte registry, a Querri on a domain outside `*.querri.com` (a dedicated or self-hosted deployment) must be added to `allowedHosts` in `metadata.yaml`, or Airbyte Cloud will block the connection.

### Large Organizations

During stream resolution, the connector lists all existing Querri sources (with auto-pagination) to match stream names. For organizations with many existing sources, this adds a small amount of overhead at the start of each sync.

## Changelog

<details>
<summary>Expand to review</summary>

| Version | Date | Pull Request | Subject |
|---------|------|-------------|---------|
| 0.2.0 | 2026-10-26 | | Fix the `write` entrypoint; acknowledge state only after records are written; spool overwrite to disk; byte-aware request splitting; never resend an append that may have been applied; https required except localhost; Docker image on GHCR |
| 0.1.0 | 2026-04-03 | | Initial release: overwrite and append modes, auto-creation, SDK-based client |

</details>
