# SPEC.md — Querri Airbyte Connectors Hardening

> **Status**: DRAFT — Round 4 (updated for SDK migration — human D2 override)
> **Owner**: Architect
> **Last updated**: 2026-04-03

## 1. Vision

After this work, the Querri Airbyte source and destination connectors:

1. **Correctly handle the current Querri API** — no silent data loss, no model mismatches.
2. **Eliminate duplicate code** — both connectors use the Querri Python SDK, not two diverging hand-rolled HTTP clients.
3. **Have exhaustive test coverage** — unit tests with mocked HTTP, e2e tests with no hardcoded secrets.
4. **Are documented** — CLAUDE.md for developer onboarding, updated READMEs.

The connectors will be migrated to use the **Querri Python SDK** (`querri-python-sdk/`) instead of the hand-rolled `requests`-based HTTP client. This requires fixing two SDK bugs first (D3: `Source.column_types`, D4: `DataWriteResult` model). See §6 Decision: SDK Adoption for rationale.

---

## 2. Current State Assessment

### 2.1 Critical Bugs

| # | Bug | Impact | Root Cause |
|---|-----|--------|------------|
| 1 | `list_sources()` fetches only first page | Orgs with >20 sources silently lose sources from discovery and `find_source_by_name` | API now returns cursor-paginated responses (`has_more`, `next_cursor`); client calls once and returns `resp.json().get("data", [])` |
| 2 | SDK `Source` model missing `column_types` | Source connector relies on `column_types` for JSON schema generation; works only because it calls the raw API, not the SDK | SDK model drift from API |
| 3 | SDK `DataWriteResult` fields wrong | SDK has `source_id`/`rows_affected`; API returns `id`/`name`/`columns`/`row_count`/`updated_at` | SDK model drift from API |

### 2.2 Code Quality Issues

| # | Issue | Impact |
|---|-------|--------|
| 4 | Duplicate `QuerriClient` in `destination_querri/_client.py` | Bug fixes must be applied twice; they will diverge |
| 5 | `sys.path.insert()` hack in `destination.py` | Fragile import path; breaks when packages are installed independently |
| 6 | Hardcoded API keys in `e2e_test.py` (lines 25-32) | Credential exposure in source control |
| 7 | No CLAUDE.md | No developer guide for the airbyte directory |

### 2.3 What Already Works Well

- Source connector: stream discovery, full refresh + incremental sync via `updated_at` cursor, data pagination, sanitized stream names, column type mapping.
- Destination connector: overwrite + append modes, auto-creation of sources, batching with configurable size, state message passthrough.
- E2E test suite: good coverage of CRUD lifecycle, round-trip, scope enforcement, pagination.

---

## 3. End-State Architecture

### 3.1 Package Structure

```
airbyte/
├── source-querri/
│   └── source_querri/
│       ├── source.py
│       ├── streams.py
│       └── spec.yaml
├── destination-querri/
│   └── destination_querri/
│       ├── destination.py
│       └── spec.json
├── e2e_test.py
├── CLAUDE.md                       # NEW
└── README.md                       # Updated
querri-python-sdk/                  # Querri Python SDK (sibling repo)
└── querri/
    ├── _client.py                  # Querri / AsyncQuerri entry points
    ├── resources/data.py           # Data resource (sources, queries, writes)
    └── types/data.py               # Source, DataWriteResult, etc.
```

**Key change**: Both connectors depend on the **Querri Python SDK** instead of maintaining their own HTTP client code. Delete `source_querri/client.py`, `destination_querri/_client.py`, and the `sys.path.insert()` hack. Both connectors import `from querri import Querri` and use `client.data.*` methods.

**Installation**: Both `source-querri/setup.py` and `destination-querri/setup.py` declare `querri` as a dependency (installed from `querri-python-sdk/` via relative path for local dev, or from PyPI for production). Docker builds `pip install` the SDK.

**No shared client package needed** — the SDK *is* the shared client. This eliminates D5 (shared package vs synced duplication) entirely.

### 3.2 SDK Client API Surface (used by connectors)

The connectors use the SDK's `Querri` sync client. The relevant API surface from `client.data`:

```python
from querri import Querri

client = Querri(api_key="...", org_id="...", base_url="...")

# Read operations
client.data.sources(limit=100)           # Returns SyncCursorPage[Source] — auto-paginates via __iter__
client.data.source(source_id)            # Returns Source (detail with column_types after D3 fix)
client.data.source_data(source_id, page=1, page_size=10_000)  # Returns DataPage

# Write operations
client.data.create_source(name=..., rows=[...])    # Returns Source (after D4 fix)
client.data.append_rows(source_id, rows=[...])     # Returns DataWriteResult (after D4 fix)
client.data.replace_data(source_id, rows=[...])    # Returns DataWriteResult (after D4 fix)
client.data.delete_source(source_id)                # Returns Dict
```

**Connector adaptation notes**:
- `list_sources()` → `list(client.data.sources(limit=100))` — the SDK's `SyncCursorPage` handles cursor pagination automatically. This fixes Bug #1 for free.
- `find_source_by_name()` → iterate `client.data.sources()` and match by name. Auto-pagination means all pages are searched.
- `get_source()` → `client.data.source(id)` — returns a `Source` model. After D3 fix, includes `column_types`.
- `check_connection()` → try `client.data.sources(limit=1)` and catch exceptions.
- Write operations return typed models instead of raw dicts. Connector code must adapt to use model attributes (e.g., `result.id` instead of `result["id"]`).

### 3.3 Pagination Fix Detail

With the SDK migration, the pagination bug (Bug #1) is fixed by the SDK's built-in cursor pagination. The SDK's `SyncCursorPage[Source]` auto-paginates via `__iter__`:

```python
# In connector code — replaces the broken single-page list_sources()
all_sources = list(client.data.sources(limit=100))
```

The SDK's pagination (`querri/_pagination.py`) handles the `has_more`/`next_cursor` loop internally. The connector no longer needs its own pagination logic.

**Finding a source by name** also benefits automatically:
```python
def find_source_by_name(name: str) -> Optional[Source]:
    for source in client.data.sources(limit=100):
        if source.name == name:
            return source
    return None
```

This is safe because:
- The SDK requests up to 100 items per page (max allowed).
- The SDK stops when `has_more` is false.
- Lazy iteration means `find_source_by_name` short-circuits on first match.

---

## 4. API Contract Reference

These are the API response shapes the connectors must handle. All responses from the actual Querri server API (`Querri/server-api/api/public_api/`).

### 4.1 GET /sources (list)

```json
{
  "data": [{"id": "...", "name": "...", "columns": [...], "row_count": N, "access_controlled": bool, "updated_at": "..."}],
  "has_more": true|false,
  "next_cursor": "..." | null
}
```

Pagination: cursor-based, default limit 20, max limit 100. Pass `after=<cursor>` for next page.

### 4.2 GET /sources/{id} (detail)

```json
{"id": "...", "name": "...", "columns": [...], "column_types": {"col": "type"}, "row_count": N, "access_controlled": bool, "updated_at": "..."}
```

**Note**: `column_types` is only on the detail endpoint, not the list endpoint. The source connector correctly calls `get_source()` per source to get column types for schema generation.

### 4.3 GET /sources/{id}/data (read)

```json
{"data": [{...}], "total_rows": N, "page": N, "page_size": N}
```

Offset-paginated (page/page_size). The connector's `read_source_data()` handles this correctly.

### 4.4 POST /sources (create)

```json
{"id": "...", "name": "...", "columns": [...], "row_count": N, "updated_at": "..."}
```

### 4.5 POST /sources/{id}/rows (append), PUT /sources/{id}/data (replace)

```json
{"id": "...", "name": "...", "columns": [...], "row_count": N, "updated_at": "..."}
```

### 4.6 DELETE /sources/{id}

```json
{"id": "...", "deleted": true}
```

### 4.7 Auth Headers

```
Authorization: Bearer {api_key}
X-Tenant-ID: {org_id}
Content-Type: application/json
```

---

## 5. Security Properties

### 5.1 Credential Handling (SEC-1)

- **MUST**: API keys come from connector config only (Airbyte injects them as secrets).
- **MUST**: E2E test credentials come from environment variables only — no hardcoded defaults. Tests fail fast with clear error if env vars missing.
- **MUST**: The two hardcoded `qk_` keys currently in `e2e_test.py` must be removed AND rotated server-side (they are in git history forever).
- **MUST**: API keys are never logged, even at DEBUG level.
- **MUST**: `spec.yaml` and `spec.json` mark `api_key` fields with `airbyte_secret: true` (already done).
- **MUST**: Error messages must not include raw URLs or `qk_` strings. Truncate response bodies to 500 chars.
- **MUST**: Error messages put actionable guidance first, raw API detail second. Format: `"<What failed> (HTTP <status>). <Hint>. API response: <truncated>"`. Example: `"Authentication failed (HTTP 401). Check that your api_key and org_id are correct. API response: ..."`.
- **SHOULD**: A `_redact_key(text: str)` utility that replaces `qk_...` patterns with `qk_***` — used in any error path that might contain config values. This is defense-in-depth against future `logger.debug(f"Config: {config}")` calls.

### 5.2 Import Security (SEC-2)

- **MUST**: No `sys.path` manipulation. The SDK migration (§3.1) eliminates this entirely — both connectors declare `querri` as a dependency and import it normally.

### 5.3 Network Security (SEC-9)

- Default base URL is `https://app.querri.com/api/v1` (HTTPS enforced).
- **MUST**: Log a warning when `base_url` uses plain HTTP for non-localhost URLs.
- Request timeout of 60s prevents hanging connections.
- No SSL verification bypass — standard `httpx` SSL behavior (SDK default).

### 5.4 Rate Limit Handling (SEC-5)

The API enforces rate limits: 60 requests/minute per API key (configurable per key), 429 responses include `Retry-After` header and `X-RateLimit-*` headers.

- **SHOULD**: On 429 response, retry with backoff respecting `Retry-After` header.
- **SHOULD**: On 5xx responses, retry up to 3 times with exponential backoff + jitter.
- **SHOULD**: Log rate limit warnings when `X-RateLimit-Remaining` drops below 10.

Implementation: The SDK's `SyncHTTPClient` / `AsyncHTTPClient` already implements retry with exponential backoff (configured via `max_retries` in the client constructor). The connectors inherit this behavior automatically. Verify that the SDK's retry handles 429 with `Retry-After` and 5xx with backoff.

**Note**: Retries are SHOULD (not MUST) because the connector has no users today. The `source_ids` filter is the primary mitigation for large orgs. Retries are recommended for production use and should be added before the connector has real users.

**Note on discovery**: An org with 200 sources requires ~202 API calls for discovery (2 list pages + 200 `get_source()` calls). At 60/min, this takes ~3.5 minutes. The `source_ids` config filter is the recommended mitigation for large orgs.

### 5.5 Data Integrity

- Pagination fix ensures no silent data loss during discovery.
- Data read pagination in `read_records()` uses `total_rows` to verify completeness.
- Overwrite mode buffers all records before writing to avoid partial replacements.

### 5.6 .gitignore Hygiene (SEC-8)

- **MUST**: Add `.env` and `*.env` to `.gitignore`.

---

## 6. Decision: SDK Adoption

**Decision: ADOPT the Querri Python SDK.** *(Human override — reversed team's original recommendation to defer.)*

### Original Team Recommendation (rejected)

The team recommended deferring SDK adoption due to: (1) SDK model bugs (`DataWriteResult`, `Source.column_types`), (2) `httpx` vs `requests` dependency conflict risk, (3) minimal API surface (7 endpoints / 140 lines), (4) coupling risk. See DECISIONS.md D2 for full debate.

### Human's Decision and Rationale

The human approved SDK migration with the following context:
- The team is authorized to fix the SDK bugs directly (D3, D4 are now in-scope)
- The team has access to both the SDK repo (`querri-python-sdk/`) and the server API (`Querri/`)
- No external users depend on either the SDK or the connectors today — breakage risk is minimal
- SDK adoption eliminates the entire shared-client-packaging problem (D5 becomes moot)
- SDK provides auto-pagination, typed responses, retry logic, and auth handling — features the hand-rolled client would need to reimplement

### Implications

1. **D3 and D4 are now in-scope**: SDK `Source` model must add `column_types` and `access_controlled` fields. SDK `DataWriteResult` must be replaced with correct fields (`id`, `name`, `columns`, `row_count`, `updated_at`).
2. **D5 is eliminated**: No shared client package needed — the SDK is the shared dependency.
3. **Phase 2 changes**: Instead of extracting a shared `QuerriClient`, refactor both connectors to import `from querri import Querri` and use `client.data.*` methods.
4. **Dependency change**: Both connectors gain `querri` (and transitively `httpx`, `pydantic`) as dependencies. The `requests` library is no longer needed for API calls (may still be needed by Airbyte CDK).
5. **Test changes**: Unit tests must mock `httpx` (via `respx`) instead of `requests`. The SDK's test patterns (see `querri-python-sdk/CLAUDE.md`) provide the template.

---

## 7. Success Criteria

### 7.1 Functional

- [ ] `list_sources()` (via SDK auto-pagination) returns all sources for orgs with >100 sources.
- [ ] `find_source_by_name()` finds sources beyond the first page.
- [ ] Source connector discovers all streams, not just the first 20.
- [ ] Destination connector resolves all existing sources, not just the first 20.
- [ ] All 7 API endpoints work against the current API response shapes via the SDK.
- [ ] E2E tests pass against a live instance with no hardcoded credentials.

### 7.2 SDK Fixes (Prerequisites)

- [ ] SDK `Source` model includes `column_types: Optional[Dict[str, str]]` and `access_controlled: Optional[bool]` fields (D3).
- [ ] SDK `DataWriteResult` model matches actual API response: `id`, `name`, `columns`, `row_count`, `updated_at` (D4).
- [ ] SDK unit tests cover the fixed models against real API response shapes.

### 7.3 Code Quality & Security

- [ ] Both connectors use `from querri import Querri` — no hand-rolled HTTP client code.
- [ ] `source_querri/client.py` and `destination_querri/_client.py` deleted.
- [ ] No `sys.path.insert()` hacks.
- [ ] No hardcoded API keys anywhere in the codebase. Existing keys rotated.
- [ ] CLAUDE.md exists with development setup, test commands, architecture, and terminology glossary.
- [ ] `.env` and `*.env` in `.gitignore`.
- [ ] Error messages: actionable hint first, truncated, no `qk_` strings leaked.
- [ ] Airbyte CDK version pinned to tested range (not open-ended `>=1.0.0`).
- [ ] Both connectors can be `pip install`'d independently and pass their unit tests without the other present.

### 7.4 Test Coverage

- [ ] Unit tests cover pagination (SDK auto-pagination with mocked multi-page responses).
- [ ] Unit tests cover error paths (auth failure, 404, network timeout).
- [ ] E2E tests cover full CRUD lifecycle, round-trip, and scope enforcement.
- [ ] Connector unit tests mock the SDK client (not raw HTTP) for connector-level logic.

### 7.5 Out of Scope

- Adding new Airbyte sync modes (dedup, soft delete).
- Publishing connectors to the Airbyte registry.
- Client-side regex validation on `org_id`, `api_key`, `source_id` formats (SEC-4) — see §9 Rejected Requirements.
- SSRF protection / private IP range blocking on `base_url` (SEC-3) — see §9 Rejected Requirements.

---

## 8. Risks

| Risk | Mitigation |
|------|------------|
| SDK `httpx` conflicts with Airbyte CDK's `requests` | Both libraries can coexist — they are independent HTTP clients. Airbyte CDK uses `requests` internally; connectors use SDK (`httpx`) for Querri API calls only. No shared session state. |
| SDK model changes break connectors | Pin SDK version in connector `setup.py`. SDK and connectors are in the same monorepo — changes are coordinated. |
| Pagination cursor format changes | SDK treats cursor as opaque string, passes it back as-is |
| API response shape changes in future | E2E tests act as a contract test suite; SDK model tests catch regressions |
| Large org with thousands of sources causes slow discovery (N+1 problem) | `source_ids` config filter limits discovery scope; pagination uses `limit=100` (max) to minimize round-trips. This is a known performance issue: discovery calls `source()` per source for `column_types`. No fix planned — document in CLAUDE.md |
| Rate limits (60/min) slow discovery for large orgs | SDK has built-in retry with exponential backoff; document `source_ids` filter as recommended for orgs with >50 sources |
| Airbyte CDK breaking changes | Pin CDK to tested version range, not open-ended `>=1.0.0` |
| SDK bug fixes (D3/D4) don't match actual API | Verify fixes against §4 API contract reference and run e2e tests |

---

## 9. Rejected Requirements (with reasoning)

### SEC-3: SSRF Protection / base_url Validation — REJECTED

**Proposed**: Validate `base_url` against private IP ranges, enforce HTTPS for non-localhost, full SSRF protection. Later narrowed to 4-line URL scheme validation.

**Rejection reasoning**: In Airbyte, connector config is injected by the platform admin or authorized config creators. IP range blocking and DNS resolution checks add maintenance burden disproportionate to the threat in this context. The `requests` library already rejects non-HTTP schemes. The HTTPS warning (§5.3) catches the realistic misconfiguration case (missing `s` in `https`).

**Security reviewer final position**: Dropped the dispute. "7 of 9 accepted is a strong outcome." Accepted that `requests` handles non-HTTP schemes and the remaining gap (confusing error messages) is a UX improvement, not a security requirement.

**Residual risk**: The connector trusts `base_url` from config. A misconfigured URL sends the Bearer token to the wrong server. Mitigated by HTTPS warning + `requests` scheme rejection + admin-controlled config. **Revisit if connector is published to the Airbyte registry.**

### SEC-4: Client-Side Input Regex Validation — REJECTED

**Proposed**: Validate `org_id` matches `^org_[a-zA-Z0-9]+$`, `api_key` matches `^qk_[a-zA-Z0-9_-]+$`, etc.

**Rejection reasoning**: The Querri API validates all inputs server-side and returns clear error messages. Client-side regex creates a maintenance burden (format changes require connector updates) and provides minimal security benefit since the config is admin-controlled. The API is the authoritative validator. If the API rejects a malformed org_id, the connector's `_raise_for_status` surfaces the error clearly.

### SDK Migration — APPROVED (human override)

**Originally proposed by UX-lead, rejected by team, then approved by human override.**

**Original rejection reasoning**: SDK has model bugs, uses `httpx` which may conflict with Airbyte's `requests`, connector surface is only 7 endpoints / 140 lines.

**Override**: Human authorized the team to fix SDK bugs directly (D3, D4) and confirmed that no external users depend on either codebase, making this the ideal time for the migration. See §6 for full rationale.

---

## 10. CLAUDE.md Requirements

The new `airbyte/CLAUDE.md` must cover:

1. **Commands**: How to run unit tests, e2e tests, lint. Required env vars for e2e.
2. **Architecture**: Package structure, data flow (source connector discovery → streams → records; destination connector stream mapping → buffering → flush).
3. **Terminology**: "Querri source" (a dataset/table in Querri) vs "Airbyte source" (this connector). This is the #1 confusion point for new developers.
4. **API contract**: Link to §4 of this spec or summarize the key endpoints and response shapes.
5. **Common tasks**: How to add a new API endpoint to the client, how to add a new stream type.
