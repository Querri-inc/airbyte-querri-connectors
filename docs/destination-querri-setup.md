# Querri destination: custom connector setup

This guide gets data from any Airbyte source into Querri with the Querri destination, which is
distributed as a Docker image and added to Airbyte as a custom connector.

**Time:** about 30 to 45 minutes the first time if you are installing Airbyte, most of it waiting
for `abctl` to download and start Airbyte. If you already run Airbyte, about 10 minutes.

| Step | Time |
|------|------|
| Install Airbyte with `abctl` (skip if you have Airbyte) | 20 to 30 min |
| Create a Querri API key | 2 min |
| Add the Querri destination to Airbyte | 3 min |
| Configure the destination and a connection | 5 min |
| First sync | depends on data size |

## 1. Install Airbyte (skip if you already have it)

You need Docker running, and at least 4 CPUs and 8 GB of memory available to it.

```bash
curl -LsfS https://get.airbyte.com | bash -
abctl local install
abctl local credentials
```

`abctl local install` downloads and starts Airbyte in a local Kubernetes cluster; it is the slow
step. When it finishes, open http://localhost:8000 and sign in with the email and password that
`abctl local credentials` prints.

## 2. Create a Querri API key

1. In Querri, open **Settings → API Keys** and create a key.
2. Give it the **`data:read`** and **`data:write`** scopes. The destination lists your sources
   (read) and writes rows (write); its connection check fails if either is missing.
3. **Choose where synced tables will live.** A source the destination creates is homed in the
   workspace the key is bound to. A key with no workspace binding puts every synced source in the
   **key creator's personal workspace**, where teammates (and other keys) cannot read it until it
   is shared. To land tables in a shared workspace, create the key bound to that workspace.
4. Copy the key (it starts with `qk_`). You will not see it again.
5. Copy your **Organization ID** from **Settings → Account** (it starts with `org_`).

## 3. Add the Querri destination to Airbyte

1. In Airbyte, open **Settings → Destinations** (under Workspace) and click **+ New connector**,
   then **Add a new Docker connector**.
2. Fill in:
   - **Connector display name**: `Querri`
   - **Docker repository name**: `ghcr.io/querri-inc/destination-querri`
   - **Docker image tag**: `0.2.0`
   - **Connector documentation URL**: `https://docs.querri.com/api/data-api`
3. Click **Add**. Airbyte pulls the image and reads its spec.

## 4. Configure the destination

1. Open **Destinations → + New destination** and choose **Querri**.
2. Enter the **API Key** and **Organization ID** from step 2.
3. Leave **API Base URL** at `https://app.querri.com/api/v1` unless Querri gave you a dedicated
   URL. It must be `https`.
4. Optional: **Source Name Prefix** (default `airbyte_`): each stream is written to the Querri
   source named prefix + stream name, created on the first sync. Stream `orders` becomes
   `airbyte_orders`; a stream already named `airbyte_orders` becomes `airbyte_airbyte_orders`, so
   set the prefix to empty if your stream names already carry one. An existing source with
   exactly that name is reused, so pre-create one if you want a specific workspace or name.
5. Click **Set up destination**. Airbyte runs the connection check, which confirms the key can
   list sources and has `data:write`.

## 5. Create a connection and sync

1. Create a connection from your source to the Querri destination.
2. For each stream choose a sync mode:
   - **Full refresh | Overwrite**: each sync replaces the Querri source's rows.
   - **Incremental | Append** (or Full refresh | Append): each sync adds rows.
   - Deduplicated modes are not offered; the Querri API has no upsert.
3. Run the sync. The new sources appear in your Querri Library as `airbyte_<stream>`.

## What to expect

- **Overwrite** keeps the old rows until the sync's final write. For a stream of up to 100,000
  rows (and about 40 MB) that write is a single atomic replace. A larger stream is written in
  several requests, so while they run Querri readers see a partial table; a failure fails the sync,
  and the next sync replaces the whole table.
- **An overwrite that sends zero records leaves the previous rows in place.** The Querri API cannot
  empty a source.
- **Append** is at least once: if a sync fails mid-batch and Airbyte resumes it, that batch can be
  written twice.
- Views built on a source refresh after each successful write (Querri's view-refresh cascade).

## Troubleshooting

| Symptom | Cause |
|---------|-------|
| Check fails: "Invalid API key or org_id" | Wrong key, a revoked key, or a key from another organization. |
| Check fails: "lacks permission" | The key is missing `data:read` or `data:write`. |
| Check fails: "base_url must use https" | `http://` is accepted only for `localhost`, `127.0.0.1` and `host.docker.internal`. |
| Airbyte cannot pull the image | The GHCR package must be public, or the cluster needs pull credentials for `ghcr.io`. |
| Teammates cannot see the synced tables | The key has no workspace binding, so the sources are in the key creator's personal workspace. Share them, or use a key bound to a shared workspace. |
| Sync waits and logs "Rate limited ... retrying" | The key's request limit (default 60 per minute) is shared by everything using it. Give Airbyte its own key. |
| Sync waits and logs "retrying in 10s" | The Querri source is being synced or written by something else; the destination waits for it. |
