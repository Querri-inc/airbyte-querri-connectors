# PROJECT.md — Querri Airbyte Connector Hardening

**Owner**: Product Manager
**Status**: COMPLETE — All phases delivered (SDK Migration Path)
**Last updated**: 2026-04-03T14:00

## Overview

Harden the Querri Airbyte source and destination connectors against the current public API. The connectors have drifted from the API and contain bugs, duplicated code, and hardcoded secrets. No external users depend on these connectors today — the goal is to get them to a state where a first user *can* succeed, not to gold-plate them.

### IMPORTANT: Scope Change (2026-04-03)

**The human overrode the team's D2 recommendation.** The connectors WILL be migrated to use the Querri Python SDK. This means:

- **D2 (SDK adoption)**: APPROVED — connectors will import and use the Querri Python SDK
- **D3 (Fix SDK `Source.column_types`)**: NOW IN SCOPE — must be fixed before source connector migration
- **D4 (Fix SDK `DataWriteResult`)**: NOW IN SCOPE — must be fixed before destination connector migration
- **D5 (Shared package)**: MOOT — both connectors import the SDK directly, eliminating the need for a shared `QuerriClient` package or vendor-copy approach

This changes the critical path: SDK bug fixes (D3, D4) must land before the connector refactors can proceed.

---

## Project-Level Acceptance Criteria (Definition of "Done")

- [x] `list_sources()` returns all sources for an org with >25 sources (via SDK's auto-pagination)
- [x] No secrets in the codebase (hardcoded keys removed, `.env` gitignored)
- [ ] Exposed API keys rotated server-side (human action — see DECISIONS.md D1)
- [x] E2E tests pass against live API with env-var-only configuration
- [x] Unit tests cover all bug fixes as regression tests
- [x] `pip install -e .` of both connectors works independently (with SDK as dependency)
- [x] CLAUDE.md enables a new developer to set up, test, and modify the connectors
- [x] Both connectors use the Querri Python SDK instead of hand-rolled HTTP clients

---

## Phase 0: Security — Human Action Required

**Goal**: Contain credential exposure already in git history.

### 0.1 Rotate exposed API keys *(HUMAN ACTION — see DECISIONS.md D1)*
- **Priority**: IMMEDIATE.
- **Status**: PENDING HUMAN ACTION
- **Issue**: `e2e_test.py` lines 24-32 contain two live `qk_` API keys and an org ID as hardcoded defaults.
- **Action**: Use `querri` CLI to create new keys, store in environment variables on localhost.

### 0.2 Remove hardcoded API keys from e2e_test.py
- **Status**: DONE (2026-04-03T14:15)
- **Files**: `airbyte/e2e_test.py` (lines 24-32)
- **Task**: Replace hardcoded API key defaults with env-var-only lookups. Fail fast with a clear error if env vars are missing.

### 0.3 Add `.env` to .gitignore
- **Status**: DONE (2026-04-03T14:15)
- **Files**: `airbyte/.gitignore` (create or update)

**Phase 0 dependencies**: None. Start immediately.

---

## Phase 1: SDK Bug Fixes (Prerequisites for Migration)

**Goal**: Fix the Querri Python SDK so it correctly models the API responses the connectors depend on.

### 1.1 Fix SDK `Source` model — add `column_types` field
- **Status**: DONE (2026-04-03T14:15)
- **Files**: `querri-python-sdk/querri/types/data.py`
- **Task**: The API's `GET /sources/{id}` returns `column_types` (dict mapping column names to types). The SDK's `Source` Pydantic model does not include this field. Add it.
- **Acceptance criteria**: `Source` model includes `column_types: Optional[Dict[str, str]]`. SDK correctly parses the field from API responses.
- **Effort**: Small

### 1.2 Fix SDK `DataWriteResult` model mismatch
- **Status**: DONE (2026-04-03T14:15)
- **Files**: `querri-python-sdk/querri/types/data.py`
- **Task**: SDK has `source_id`/`rows_affected`; API returns `id`/`name`/`columns`/`row_count`/`updated_at`. Fix the model to match actual API responses.
- **Acceptance criteria**: `DataWriteResult` fields match the API response shape. Write operations parse correctly.
- **Effort**: Small

**Phase 1 dependencies**: None. Both tasks are independent and can be parallelized. Runs in parallel with Phase 0.

**Phase 1 exit criteria**: SDK models correctly represent the API response shapes used by both connectors.

---

## Phase 2: Connector Migration to SDK

**Goal**: Replace hand-rolled `requests`-based clients with SDK imports. Eliminate duplicate code and `sys.path` hacks.

### 2.1 Refactor source connector to use Querri SDK
- **Status**: DONE (2026-04-03T14:30)
- **Files**: `source-querri/source_querri/source.py`, `source-querri/source_querri/streams.py`, `source-querri/source_querri/client.py`
- **Task**: Replace the hand-rolled `QuerriClient` with Querri SDK calls. The SDK handles auth, pagination (cursor-based), and typed responses. Remove `client.py` or replace with thin SDK wrapper.
- **Acceptance criteria**:
  - Source connector uses SDK for all API calls
  - Stream discovery works with SDK's auto-pagination (no silent data loss)
  - `column_types` correctly extracted from SDK `Source` model
  - All existing unit tests updated and passing
  - `pip install -e .` works with SDK as dependency
- **Depends on**: Phase 1.1 (SDK `Source.column_types` fix)
- **Effort**: Medium

### 2.2 Refactor destination connector to use Querri SDK
- **Status**: DONE (2026-04-03T14:30)
- **Files**: `destination-querri/destination_querri/destination.py`, `destination-querri/destination_querri/_client.py`
- **Task**: Replace duplicate `_client.py` with SDK calls. Remove `sys.path.insert()` hack. Delete `_client.py`.
- **Acceptance criteria**:
  - Destination connector uses SDK for all API calls
  - No `sys.path` manipulation
  - No duplicate client code
  - Write operations (create, append, replace) work with corrected `DataWriteResult`
  - All existing unit tests updated and passing
  - `pip install -e .` works independently
- **Depends on**: Phase 1.2 (SDK `DataWriteResult` fix), Phase 2.1 (learn from source refactor)
- **Effort**: Medium

### 2.3 Add retry logic for rate limits
- **Status**: DONE (2026-04-03T14:45) — Free with SDK migration; SDK handles 429/5xx retries natively
- **Task**: If the SDK doesn't handle retries, add retry with exponential backoff + jitter for HTTP 429 and 5xx responses. Check if SDK provides this out of the box first.
- **Depends on**: Phase 2.1 (need to know SDK's retry capabilities)
- **Effort**: Small-Medium (may be trivial if SDK handles it)

### 2.4 Improve error messages
- **Status**: DONE (2026-04-03T14:45)
- **Task**: Ensure error messages from SDK are actionable and hint-first. Add truncation. Log HTTP warning for non-HTTPS `base_url`.
- **Depends on**: Phase 2.1
- **Effort**: Small

**Phase 2 exit criteria**: Both connectors use the SDK. No duplicate client code. No `sys.path` hacks. Rate limits handled. Errors are actionable.

---

## Phase 3: Test Coverage Expansion

**Goal**: Fill remaining test gaps with the new SDK-based connectors.

### 3.1 Update unit tests for SDK-based connectors
- **Status**: DONE (2026-04-03T15:00)
- **Files**: Both unit test suites
- **Task**: Update all unit tests to work with SDK-based connectors. Mock SDK calls instead of raw HTTP. Cover pagination, error paths, batch overflow, write flows.
- **Depends on**: Phase 2.1, 2.2
- **Effort**: Medium

### 3.2 Update e2e tests for env-var config
- **Status**: DONE (2026-04-03T14:45)
- **Files**: `airbyte/e2e_test.py`
- **Task**: Verify env-var-only config works. Update any test code that assumed the old client interface.
- **Depends on**: Phase 0.2, Phase 2.1, 2.2
- **Effort**: Small

**Phase 3 exit criteria**: All tests pass against SDK-based connectors. No hardcoded credentials.

---

## Phase 4: Documentation

**Goal**: Make the connector maintainable by anyone.

### 4.1 Create CLAUDE.md
- **Status**: DONE (2026-04-03T14:45)
- **Files**: `airbyte/CLAUDE.md` (new)
- **Task**: Document dev setup, test commands, architecture (now SDK-based), terminology, common tasks.
- **Depends on**: Phase 2.1, 2.2
- **Effort**: Small

### 4.2 Update README and governance docs
- **Status**: DONE (2026-04-03T15:00)
- **Files**: Source and destination connector READMEs, SPEC.md, DECISIONS.md
- **Task**: Update to reflect SDK migration. Fix sample configs. Document SDK dependency.
- **Depends on**: Phase 2.1, 2.2, Phase 3.1
- **Effort**: Small

**Phase 4 exit criteria**: A new developer can set up, run, test, and modify the connectors using only CLAUDE.md.

---

## Decisions Required (→ DECISIONS.md)

### D1: Rotate exposed API keys
- **Status**: PENDING HUMAN ACTION
- **Action**: Use `querri` CLI to create new keys, store in env vars on localhost.
- **Urgency**: High

### D2: Refactor connectors to use Querri Python SDK
- **Status**: DECIDED — APPROVED by human (overriding team recommendation to defer)
- **Decision date**: 2026-04-03
- **Impact**: D3, D4 now in scope. D5 is moot.

### D3: Fix SDK `Source` model (missing `column_types`)
- **Status**: DECIDED — Fix as part of this project (Phase 1.1)

### D4: Fix SDK `DataWriteResult` model mismatch
- **Status**: DECIDED — Fix as part of this project (Phase 1.2)

### D5: Shared client package vs synced duplication
- **Status**: MOOT — SDK migration eliminates the need for either approach. Both connectors import the SDK directly.

---

## Risk Register

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| Exposed API keys exploited before rotation | Data access | Medium | Escalate D1 as urgent. Phase 0 runs first. |
| SDK `httpx` conflicts with Airbyte CDK's `requests` | Build/runtime failures | Medium | Test early in Phase 2.1. If conflicts arise, isolate with careful dependency pinning. |
| SDK migration takes longer than expected | Timeline overrun | Medium | Phase 2.1 is the pathfinder — learn from it before starting 2.2. |
| SDK auto-pagination behavior differs from spec | Silent data loss | Low | Unit tests verify pagination behavior explicitly. |
| Discovery performance with 200+ sources at 60 req/min rate limit | 3.5+ min discovery time | Medium | Check SDK retry capabilities. Add retry if needed (Phase 2.3). |

---

## Effort Summary

| Phase | Tasks | Estimated Effort | Parallelizable |
|-------|-------|-----------------|----------------|
| 0: Security | 3 (1 human) | Trivial (code) + human action | Yes |
| 1: SDK Bug Fixes | 2 | Small (0.5-1 day) | Yes, both |
| 2: Connector Migration | 4 | Medium (3-4 days) | Partially (2.1 before 2.2) |
| 3: Test Expansion | 2 | Small-Medium (1-2 days) | Yes |
| 4: Documentation | 2 | Small (1 day) | Yes |
| **Total** | **13** | **~6-9 days** | |

---

## Execution Order (Critical Path)

```
Phase 0.1 (human) ──────────────────────────────────────────→ (urgent)
Phase 0.2 ──┐
Phase 0.3 ──┤
Phase 1.1 ──┼──→ Phase 2.1 ──→ Phase 2.3 ──→ Phase 3 ──→ Phase 4
Phase 1.2 ──┘         │         Phase 2.4
                      ↓
                Phase 2.2 ──→ Phase 3
```

Phases 0 and 1 run in parallel. SDK bug fixes (1.1, 1.2) gate connector migration. Phase 2.1 (source connector) is the pathfinder; 2.2 (destination) follows. Phase 3 tests the final state. Phase 4 documents it.

---

## Progress Log

| Timestamp | Event |
|-----------|-------|
| 2026-04-03T14:00 | PROJECT.md revised: Human approved D2 (SDK migration). D3, D4 now in scope. D5 moot. Task list and phases restructured accordingly. |
| 2026-04-03T14:15 | Phase 0 complete (0.2, 0.3). Phase 1 complete (1.1, 1.2). Hardcoded keys replaced with `_require_env()`. `.gitignore` updated. SDK `Source` model now has `column_types` + `access_controlled`. SDK `DataWriteResult` fixed to match API shape; new `DeleteResult` model added. 221 SDK tests pass (2 pre-existing failures in unrelated `test_security.py`). Phase 2 unblocked. |
| 2026-04-03T14:20 | Architecture review complete for Phase 2. Key migration patterns documented: dict→attribute access, keyword-only args, `check_connection()` via `sources(limit=1)`, delete both old client files. Ready for implementation. |
| 2026-04-03T14:30 | Phase 2.1 and 2.2 complete. Both connectors now use Querri SDK. Source: `check_connection()` via `sources(limit=1)`, auto-paginating `list(client.data.sources())`, attribute access throughout. Destination: `sys.path.insert()` hack removed, `_client.py` deleted, all write ops use SDK. Both `setup.py` files updated (`requests` → `querri`). Note: source `client.py` kept for e2e tests (task #10 scope). Remaining Phase 2 tasks (2.3, 2.4) and Phases 3-4 now unblocked. |
| 2026-04-03T14:45 | Phase 2 COMPLETE. Retry logic free via SDK (429/5xx handled natively). Error messages: `_format_error()` maps SDK exceptions to actionable hints. Phase 3.2 (e2e) complete — env-var config verified. Phase 4.1 (CLAUDE.md) complete — covers terminology, architecture, commands, SDK integration. 10/12 tasks done. Remaining: #9 (unit tests, owned by backend-qa) and #12 (README updates, blocked on #9). |
| 2026-04-03T15:00 | Phase 3.1 (unit tests) complete by backend-qa. Phase 4.2 (README/docs) complete. Self-review caught host-stripping bug (removesuffix order) — fixed in both connectors. README updated for SDK-based architecture. PROJECT.md marked COMPLETE. All 12 tasks done. |
| 2026-04-03T15:05 | PM final review: 12/12 tasks complete. 7/8 acceptance criteria met. Only remaining action: D1 (key rotation) — requires human action, not a code task. Project delivered. |
