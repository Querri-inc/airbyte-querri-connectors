# DECISIONS.md — Querri Airbyte Connector Hardening

> **Status**: Key decisions resolved (D2-D5 decided 2026-04-03)
> **Last updated**: 2026-04-03T14:00

Decisions requiring human input before or during execution. Each includes the team's debate positions and a recommendation.

---

## D1: Rotate Exposed API Keys

**Status**: URGENT — requires IMMEDIATE human action (do not wait for any other work)

**Context**: `e2e_test.py` lines 24-32 contain two live `qk_` API keys and an org ID as hardcoded defaults. These are in git history permanently — code removal alone is insufficient.

**Keys to rotate**:
- RW key: `qk_EaBEjnx-0peH-w67PiZOn7K6tuGZWzg8pVuyvQyPoOEbNQ_ZbbbEni38NydEvUOR`
- RO key: `qk_zBENyrpIk1LZuXfdB2wADhCEj-oVttzCpbsF5UXSVlhT8__eCvKBu2xjPzxwoITW`
- Org: `org_01JBETJ7PYNGXVMXV0BD3CFNA8`

**Action required**: Key owner rotates both keys in the Querri dashboard (Settings → API Keys). Code removal is handled in Phase 0.2.

**Team consensus**: Unanimous — must rotate.

```
[ ] Decision made
[ ] Keys rotated
[ ] New keys stored in secure env vars for e2e testing

These keys were on my localhost only so no big deal. Just remove them and use the `querri` CLI to make new ones that you store in environment variables, also on localhost. 

```

---

## D2: Refactor Connectors to Use Querri Python SDK

**Status**: DECIDED — APPROVED (human override, 2026-04-03)

**Context**: The connectors use a hand-rolled `requests`-based HTTP client (~140 lines, 7 endpoints) instead of the Querri Python SDK. Should we migrate?

**Arguments FOR (UX-lead, Round 1)**:
- SDK handles auth, pagination, retries, and typed responses
- Fixes pagination + deduplication in one move
- No users today = ideal timing for rewrite
- Future API changes only need one SDK update

**Arguments AGAINST (Architect, Skeptic, PM — consensus)**:
- SDK has bugs: `DataWriteResult` has wrong fields (`source_id`/`rows_affected` vs actual `id`/`name`/`columns`/`row_count`/`updated_at`), `Source` model missing `column_types`
- SDK uses `httpx`; Airbyte CDK is built on `requests`. Incompatible connection pooling and retry semantics — not just a dependency, it changes error handling behavior
- The connector's API surface is only 7 endpoints / 140 lines — the SDK is 1500+ lines with its own maintenance burden
- SDK's `Data.sources()` returns `SyncCursorPage[Source]` (lazy iterator) — connector needs a flat list, requiring `.to_list()` + Pydantic-to-dict conversion. More code, not less
- Migrating to a buggy SDK to fix a buggy connector trades one set of bugs for another

**Recommendation**: **Defer.** Fix the raw client now. The SDK needs its own fixes first (D3, D4). Revisit when the SDK is stable and the connector has real users.

**UX-lead final position**: Accepted the rejection after reviewing dependency conflict risk.

```
[ ] Decision: Keep raw client (recommended)
[x] Decision: Migrate to SDK (requires fixing D3 + D4 first) ✓ APPROVED

You are free to to work on the SDK _and_ the public API in ../Querri as well. You're an expert in all of the above and these are underused right now so you don't have to be too worried about breaking things.

```

**Impact of approval**: D3 and D4 are now in scope. D5 (shared package) is moot — both connectors will import the SDK directly.

---

## D3: Fix SDK `Source` Model (Missing `column_types`)

**Status**: DECIDED — IN SCOPE (D2 approved, 2026-04-03)

**Context**: The API's `GET /sources/{id}` returns `column_types` (a dict mapping column names to types like `"string"`, `"integer"`, etc.). The SDK's `Source` Pydantic model does not include this field. The connector relies on `column_types` for Airbyte JSON schema generation.

**File**: `querri-python-sdk/querri/types/data.py`, line 10-17

**Impact**: If D2 is approved, this must be fixed first or the source connector cannot generate schemas.

**Recommendation**: File as a separate SDK issue regardless of D2 outcome.

```
[x] Decision: Fix as part of this project
[ ] Decision: File as separate issue (recommended)
```

---

## D4: Fix SDK `DataWriteResult` Model Mismatch

**Status**: DECIDED — IN SCOPE (D2 approved, 2026-04-03)

**Context**: The SDK's `DataWriteResult` model has `source_id: str` and `rows_affected: int`. The actual API returns `{id, name, columns, row_count, updated_at}` for create/append/replace operations. The SDK will throw Pydantic validation errors on every write operation.

**File**: `querri-python-sdk/querri/types/data.py`, line 39-42

**Impact**: If D2 is approved, this must be fixed first or the destination connector is completely broken.

**Recommendation**: File as a separate SDK issue regardless of D2 outcome.

```
[x] Decision: Fix as part of this project
[ ] Decision: File as separate issue (recommended)
```

---

## D5: Shared Client Package vs Synced Duplication

**Status**: MOOT (2026-04-03) — SDK migration eliminates the need for either approach

**Context**: The destination connector's `_client.py` is a full duplicate of the source connector's `client.py`. The destination also has a fragile `sys.path.insert()` hack to try importing from the source first. Both the hack and the duplication must be addressed.

**Option A: Shared package (`querri_airbyte_shared/`)**

*Pros*:
- Single source of truth — bug fixes, pagination, retry logic applied once
- Clean imports, no hacks
- Both connectors declare it as a local dependency

*Cons (Skeptic)*:
- Third package to manage (setup.py, pyproject.toml)
- Both connector Dockerfiles must COPY the shared package
- Airbyte connector packaging may not support shared local packages cleanly
- If someone installs just the destination via pip, the shared package won't be found unless also published
- A lot of packaging ceremony for ~140 lines that change infrequently

*Conditions for acceptance (Skeptic)*:
- Phase 2.1 timeboxed to 1 day
- Both connectors must `pip install -e .` independently without manual prerequisite steps
- Fallback to Option B if packaging proves impractical

**Option B: Synced duplication with CI check**

*Pros (Skeptic)*:
- Zero packaging overhead — each connector is fully self-contained
- Works with any Airbyte packaging convention
- Already the destination's fallback behavior
- Fix takes 3 lines: delete sys.path hack, make `_client.py` the primary import

*Cons*:
- Two files must stay in sync — CI check catches drift but doesn't prevent it
- Any fix (pagination, retry, error sanitization) must be applied to both files
- Hidden coupling: the files look independent but aren't

**Recommendation**: **Try Option A first** (timeboxed to 1 day in Phase 2.1 — half day investigation, half day implementation). If Airbyte packaging makes it impractical, fall back to Option B. The `sys.path.insert()` hack is removed in either option.

**UX-lead note on Option B**: If falling back, use a `make sync-client` target that copies from the canonical source, not just a CI lint check. A lint check is the current problem repackaged — the files already drifted despite being copies.

```
[ ] Decision: Option A — Shared package (recommended, with timebox)
[x] Decision: Option B — Synced duplication with CI check
```

---

## D6: Client-Side Input Validation (SEC-3/SEC-4)

**Status**: Resolved — team consensus, no human decision needed

**Context**: Security reviewer proposed: (a) regex validation on `org_id`, `api_key`, `source_id` formats (SEC-4), and (b) SSRF protection on `base_url` including IP range blocking and scheme enforcement (SEC-3).

**Team discussion**:
- **Security-reviewer**: Originally proposed both as HIGH severity. Narrowed SEC-3 to 4-line scheme check. Ultimately dropped both disputes: "7 of 9 accepted is a strong outcome." Accepted that `requests` already rejects non-HTTP schemes and the API validates inputs server-side.
- **Architect**: Rejected both. API is the authoritative validator. Client-side regex creates maintenance burden and false negatives (valid keys rejected by stale patterns).
- **UX-lead**: Supports rejection. "Client-side regexes create a worse DX failure mode than they prevent."
- **Skeptic**: Supports rejection. Threat model analysis is correct for admin-controlled config.

**Resolution**: No client-side config validation beyond the HTTPS warning already in SPEC §5.3.

**Residual risk** (documented per security-reviewer): The connector trusts `base_url` from Airbyte config. A misconfigured or malicious URL sends the Bearer token to the wrong server. Mitigated by: (a) HTTPS warning for non-localhost HTTP URLs (SPEC §5.3), (b) `requests` library rejecting non-HTTP schemes, (c) Airbyte config is typically admin-controlled. **Revisit if connector is published to the Airbyte registry** (broader user base, less controlled config).

**No human decision needed** — documented for the record.

---

## D7: Error Sanitization Scope

**Status**: Resolved — team consensus

**Context**: Security reviewer proposed truncation + regex redaction of `qk_` patterns in error output.

**Resolution**: Truncate response bodies to 500 chars only. Skip regex redaction.

**Rationale**: API keys appear in Authorization headers, not in response bodies. Redaction would only matter if the server reflected credentials in error responses, which would be a server bug. Truncation provides sufficient protection without added complexity.

**No human decision needed** — team reached consensus.

---

## D8: Rate Limit Retry Priority

**Status**: Resolved — SHOULD, Phase 2.2

**Context**: The pagination fix means `list_sources()` now makes multiple API calls. Orgs with 35+ sources will hit the 60 req/min rate limit during discovery. Without retries, the pagination fix only works for small orgs.

**Resolution**: Include as Phase 2.2 (SHOULD priority, not a launch blocker). Skeptic initially pushed to defer but conceded after reviewing the pagination impact.

**No human decision needed** — team reached consensus.

---

## Decision Summary

| ID | Topic | Urgency | Status | Recommendation |
|----|-------|---------|--------|----------------|
| D1 | Rotate exposed API keys | IMMEDIATE | **Needs human action** | Rotate both keys now |
| D2 | SDK adoption | Before Phase 2 | **APPROVED** (human override) | Migrate to SDK |
| D3 | Fix SDK `Source.column_types` | Phase 1 blocker | **IN SCOPE** (D2 approved) | Fix as part of this project |
| D4 | Fix SDK `DataWriteResult` | Phase 1 blocker | **IN SCOPE** (D2 approved) | Fix as part of this project |
| D5 | Shared package vs duplication | n/a | **MOOT** (SDK migration) | N/A — both connectors import SDK directly |
| D6 | Input validation scope | n/a | Resolved (consensus) | Scheme-only validation |
| D7 | Error sanitization scope | n/a | Resolved (consensus) | Truncation only, no regex |
| D8 | Rate limit retry priority | n/a | Resolved (consensus) | SHOULD, Phase 2.2 |
