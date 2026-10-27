# Next Steps: Airbyte Connector Submission

**Last updated**: 2026-04-03

## Current State

Both connectors (source-querri and destination-querri) are technically ready for submission:
- SDK-based, 97 passing tests, Airbyte-standard documentation, Poetry packaging, acceptance test configs, icons, proper metadata

## Submission Paths

### Path A: Partner Certified Destination (Recommended)

Airbyte's **Partner Certified** program is the most viable route for destination connectors. As of mid-2025, Airbyte is NOT actively accepting community destination PRs — they're revamping their core Java destinations codebase. However, they have a formal partner program specifically for vendor-built destinations.

**Requirements:**
- Respond to customer inquiries within 3 business days
- 95%+ first-sync success rate and 95%+ overall sync success
- Update connector every 6 months minimum
- Emit state messages at least every 15 minutes
- Syncs must be idempotent (re-runnable without side effects)
- `AllowedHosts` declared in spec (done)
- HTTPS for all external connections (done)

**Action Items:**
1. **Contact Airbyte partnerships team** — Email partnerships@airbyte.com or fill out the partner form at https://airbyte.com/partners. Introduce Querri and express interest in the Partner Certified destination program.
2. **Join the Airbyte Contributor Slack** — https://airbyte.com/community/contributor-program. This gives direct access to the connector team and expedites reviews.
3. **File a GitHub Discussion** — Post in https://github.com/airbytehq/airbyte/discussions to announce the connector and check for interest/overlap. Tag it as a new connector proposal.
4. **Prepare SLA documentation** — Write up how Querri will meet the 3-day response, 95% success rate, and 6-month update cadence requirements.
5. **Consider adding `_airbyte_meta` columns** — For "bulk" destinations (which Querri might be classified as), Airbyte requires metadata columns: `_airbyte_raw_id`, `_airbyte_extracted_at`, `_airbyte_meta`. Evaluate whether these should be added as optional enrichment fields.

### Path B: Custom Connector Distribution (Immediate)

Users can add custom connectors to their Airbyte instances today without marketplace approval. This gives us immediate distribution.

**Action Items:**
1. **Publish Docker images**: the destination has a Dockerfile and a GHCR workflow
   (`.github/workflows/publish-destination-querri.yml`). Push a tag
   `destination-querri-v<version>` (matching pyproject.toml, metadata.yaml and the Dockerfile
   label) and it builds amd64 + arm64 and pushes `ghcr.io/querri-inc/destination-querri`.
   Make the GHCR package public after the first push, or customers cannot pull it.
   The source connector still needs the same.

2. **Custom connector setup guide**: `docs/destination-querri-setup.md`.

3. **Publish to PyPI** — This enables PyAirbyte users to `pip install source-querri destination-querri` directly. The `remoteRegistries.pypi.enabled: true` metadata is already in place.

### Path C: Community Marketplace (Source Connector Only)

Airbyte IS accepting new **source** connectors built with the Connector Builder or Python CDK. Our source connector qualifies.

**Action Items:**
1. **Submit source-querri as a community connector** — Follow https://docs.airbyte.com/platform/contributing-to-airbyte/submit-new-connector
2. **Fork the airbyte repo** and add the source connector under `airbyte-integrations/connectors/source-querri/`
3. **Run `airbyte-ci connectors test`** to validate against the full QA check suite (27 checks)
4. **Open a PR** — Airbyte reviews community source connectors within ~1 week

The destination can follow later via Path A when the partner certification is established.

## Recommended Sequence

1. **Now**: Path B — Publish Docker images + PyPI for immediate custom usage
2. **This week**: Path C — Submit source-querri as a community connector
3. **This month**: Path A — Contact Airbyte partnerships team for destination certification
4. **Ongoing**: Monitor the Airbyte community Slack for when destination contributions reopen

## Technical Gaps to Address Before Each Path

### For Path B (Custom Distribution)
- Recreate Dockerfiles (or use `airbyte-ci build`)
- Test Docker builds end-to-end
- Write custom connector setup instructions

### For Path C (Source Marketplace Submission)
- Run full `airbyte-ci connectors test` QA suite
- Ensure connector works in the official Airbyte repo directory structure
- May need to adjust import paths if the repo structure differs

### For Path A (Partner Certification)
- Evaluate `_airbyte_meta` column requirements
- Implement schema evolution (auto-add new columns)
- Add time-based state emission (every 15 min) for very large syncs
- Document idempotency guarantees
- Prepare SLA commitment

## Links

- Airbyte Partner Program: https://airbyte.com/partners
- Submit New Connector: https://docs.airbyte.com/platform/contributing-to-airbyte/submit-new-connector
- Partner Certified Destinations: https://docs.airbyte.com/platform/connector-development/partner-certified-destinations
- Connector QA Checks: https://docs.airbyte.com/contributing-to-airbyte/resources/qa-checks
- Contributor Program: https://airbyte.com/community/contributor-program
- Airbyte Community Slack: https://airbyte.com/community
