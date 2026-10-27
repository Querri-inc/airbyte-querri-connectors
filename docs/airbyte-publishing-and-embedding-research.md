# Airbyte: publishing our connectors, and embedding Airbyte connectors in Querri

**Research date:** 2026-09-23
**Scope:** `source-querri` and `destination-querri` (Python CDK, airbyte-cdk 7.x, MIT). Also covers whether Querri can run Airbyte source connectors inside its own hosted product.
**Status:** This is research, not legal advice. Every licensing conclusion below needs review by counsel before we build on it.

Conventions:
- **[Source]** means a primary source says it, and the claim is linked.
- **[Inference]** means it is my reading or extrapolation.
- Counts come from data I pulled on 2026-09-23. The method is in the [appendix](#appendix-a--method-for-the-counts) so anyone can reproduce them.

---

## TL;DR

1. **Nearly every Airbyte connector is now ELv2, not MIT.** On 2025-07-22 Airbyte relicensed its connectors from MIT to ELv2 in one bulk change ([PR #63723](https://github.com/airbytehq/airbyte/pull/63723), [blog: move-to-elv2](https://airbyte.com/blog/move-to-elv2)). I checked the metadata of the version each connector currently publishes: **637 of 658 (96.8%) are ELv2, 15 (2.3%) are MIT and 6 (0.9%) are "Airbyte Enterprise"**. The `license` field in the public registry JSON is stale and still shows about 73 MIT connectors. Don't rely on it.
2. **`airbyte-cdk` (MIT) and the Airbyte Protocol (MIT) are open. PyAirbyte is ELv2, not MIT.** PyAirbyte's [LICENSE.md](https://github.com/airbytehq/PyAirbyte/blob/main/LICENSE.md) is ELv2 and has been since 2024-02. The platform ([airbyte-platform LICENSE](https://github.com/airbytehq/airbyte-platform/blob/main/LICENSE)) is also ELv2.
3. **Embedding verdict: needs legal review. Airbyte's own published examples point toward "allowed".** Airbyte's license examples page lists this as permitted: *"Creating any kind of platform on which you offer Airbyte's connectors to your customers to bring their data in, unless you're selling an ELT/ETL solution"* ([examples](https://docs.airbyte.com/community/licenses/examples)). The FAQ also permits *"I am creating an analytics platform and I want to use Airbyte to bring data in on behalf of my customers"* ([FAQ](https://docs.airbyte.com/community/licenses/license-faq)). The risk is in how we position and expose it (see Part 2 §2.3). Get written confirmation from `license@airbyte.io`.
4. **Publishing:**
   - **Source:** Airbyte currently accepts new *Connector Builder / low-code* API connectors. New Python CDK connectors are "case-by-case", and you have to start with a GitHub Discussion.
   - **Destination:** the rule "we don't accept destination contributions" was removed on 2026-07-29. However, Airbyte bulk-closed a batch of stale new-destination PRs on 2026-07-27 with the note *"We aren't able to take a new connector forward through PR review right now."*
   - **What to do first:** self-distribute on PyPI and Docker right away, and pursue the marketplace in parallel.
5. **The `airbyte-ci` tool is gone.** It was replaced by `poe` tasks, the `airbyte-cdk` CLI (`airbyte-cdk image build` / `image test`) and `airbyte-internal-ops` (`airbyte-ops local connector qa`) ([cleanup PR #75953](https://github.com/airbytehq/airbyte/pull/75953), [connector-ci-checks.yml](https://github.com/airbytehq/airbyte/blob/master/.github/workflows/connector-ci-checks.yml)). NEXT-STEPS.md is out of date on this point.

---

## Where NEXT-STEPS.md (2026-04) is now wrong or stale

| NEXT-STEPS.md claim | Current state (2026-09-23) |
|---|---|
| "Airbyte is NOT actively accepting community destination PRs" | The explicit ban was removed on 2026-07-29 ([PR #82787](https://github.com/airbytehq/airbyte/pull/82787)). The contributing page still says *"We're not reviewing/accepting new Java connectors"* ([contributing](https://docs.airbyte.com/community/contributing-to-airbyte)). Python destinations fall under "case-by-case". |
| "Airbyte IS accepting new source connectors built with the Connector Builder or Python CDK" | The accepted list names only **Connector Builder** connectors. Anything else is *"evaluated … on a case-by-case basis. Reach out to the Airbyte team before starting"* ([contributing](https://docs.airbyte.com/community/contributing-to-airbyte)). For Python CDK connectors, the submit guide says to file a Discussion first ([submit-new-connector](https://docs.airbyte.com/platform/connector-development/submit-new-connector)). |
| "Run `airbyte-ci connectors test` … (27 checks)" | `airbyte-ci` has been removed from the docs and CI ([PR #75943](https://github.com/airbytehq/airbyte/pull/75943), [PR #75953](https://github.com/airbytehq/airbyte/pull/75953)). QA now runs through `airbyte-ops local connector qa --name <connector>` in CI ([workflow line 523](https://github.com/airbytehq/airbyte/blob/master/.github/workflows/connector-ci-checks.yml)). |
| "Airbyte reviews community source connectors within ~1 week" | No such SLA is published. Recent merged community Builder connectors took **5 weeks to 5 months**, for example source-perk (2026-02-16 → 03-25, [#73361](https://github.com/airbytehq/airbyte/pull/73361)), source-interzoid (2025-11-24 → 2026-04-23, [#69847](https://github.com/airbytehq/airbyte/pull/69847)) and source-smsmode-sms (2026-06-15 → 08-28, [#79709](https://github.com/airbytehq/airbyte/pull/79709)). A few merged within a day, such as source-ramp ([#83706](https://github.com/airbytehq/airbyte/pull/83706)). |
| "Contributor program … airbyte.com/community/contributor-program" | That page no longer describes a contributor program (checked via fetch). Treat it as defunct. The "Marketplace team" reference was removed from the submit guide on 2026-08-05 ([commit d231675](https://github.com/airbytehq/airbyte/commit/d231675698)). |
| "Cloud: Admin panel → Custom Connectors → Add [Docker]" | The Docker custom-connector guide is tagged `products: oss-*`, meaning OSS / Self-Managed only ([using-custom-connectors](https://docs.airbyte.com/platform/operator-guides/using-custom-connectors)). I found no current doc for uploading Docker images to Cloud. PyAirbyte's Cloud API says *"Only YAML connectors are currently supported. Docker connectors will raise NotImplementedError"* ([PyAirbyte cloud reference](https://github.com/airbytehq/airbyte/blob/master/docs/developers/pyairbyte/reference/airbyte/cloud/connectors.md)). **[Inference]** For Cloud, assume only Builder/YAML custom connectors, unless Airbyte sales confirms otherwise. |

---

# Part 1 — How to publish our two connectors

## 1.1 (a) Community source connector → Airbyte Marketplace / registry

**What Airbyte accepts [Source]** ([contributing](https://docs.airbyte.com/community/contributing-to-airbyte)):
- Fixes and enhancements to existing API connectors
- New streams and features for existing connectors using Connector Builder / YAML
- **New API connectors built with the Connector Builder**
- Migrations from Python to Builder / YAML
- Everything else: *"Airbyte evaluates contributions outside this scope on a case-by-case basis. Reach out to the Airbyte team before starting."*

**Two submission routes [Source]** ([submit-new-connector](https://docs.airbyte.com/platform/connector-development/submit-new-connector)):
1. **Connector Builder:** build it in the Builder UI, then click **Publish → Contribute to Marketplace**. This needs a classic GitHub PAT with `repo` + `workflow` scopes. The Builder opens the PR for you. Airbyte describes this as the fastest route to review.
2. **Custom Python CDK:**
   - First, *find or start a GitHub Discussion* in the `new-connector-request` category.
   - Then open a PR that passes the tests and includes a README, docs and an icon.
   - Reviewers check authentication, pagination, incremental syncs, a reasonable set of streams, and unit tests for any custom components.
   - Be ready to provide a **sandbox account** for CI.

**PR mechanics [Source]** ([contributing](https://docs.airbyte.com/community/contributing-to-airbyte)):
- Open the PR from a fork in a **personal** GitHub account, not an org account, with "Allow edits from maintainers" checked.
- Don't open it from the fork's default branch.
- Link an issue, or explain the problem, the solution and your design decisions.
- Follow the PR title convention.
- Sign the **CLA** on your first contribution ([license FAQ](https://docs.airbyte.com/community/licenses/license-faq)).
- Review status is tracked in a [GitHub Project](https://github.com/orgs/airbytehq/projects/108/views/4).

**Implication for `source-querri` [Inference]:**
- Our source is Python CDK, so it falls into the "case-by-case" bucket.
- The Querri API is plain REST with API-key auth and cursor pagination. That is exactly what the declarative YAML framework handles.
- **Re-implementing `source-querri` as a manifest-only (Builder) connector is the most likely path to a merge.** It also removes our dependency on the `querri` Python SDK inside the connector.
- If we keep it as Python, open the Discussion first.

**License on submission [Source]:**
- The QA check only requires *"MIT or Elv2"* ([QA checks](https://docs.airbyte.com/community/contributing-to-airbyte/resources/qa-checks)).
- A community connector merged in 2026-03 (source-perk) still carries `license: MIT` ([metadata](https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-perk/metadata.yaml)).
- **[Inference]** We can probably keep MIT, but Airbyte may ask for ELv2. The blog says *"going forward, contributions will be under ELv2"* ([blog](https://airbyte.com/blog/move-to-elv2)). Decide in advance whether we'd accept that.

## 1.2 (b) Destinations: community PRs and the Partner Certified program

**Community destination PRs [Source]:**
- On 2026-07-29 Airbyte removed the line "Contributions to destination connectors. We only accept contributions to source connectors" and generalized the accepted types to "API connectors" ([PR #82787](https://github.com/airbytehq/airbyte/pull/82787)).
- The page still warns: *"Airbyte is revamping its core Java destinations codebase. We're not reviewing/accepting new Java connectors at this time."*
- The Connector Builder and low-code CDK **cannot build destinations** ([Builder overview](https://docs.airbyte.com/platform/connector-development/connector-builder-ui/overview), [connector-development](https://docs.airbyte.com/platform/connector-development)). A new Python destination is therefore always "case-by-case".
- Evidence of actual appetite: on 2026-07-27 Airbyte closed several open new-destination PRs, including StarRocks, Dewey, NebulaGraph and Altertable. The comment on the StarRocks PR reads: *"We aren't able to take a new connector forward through PR review right now. We are closing a number of PRs that may be stale…"* ([#74743](https://github.com/airbytehq/airbyte/pull/74743)).
- **[Inference]** A cold community PR for `destination-querri` has low odds of being merged soon.

**Partner Certified destinations: requirements [Source]** ([partner-certified-destinations](https://docs.airbyte.com/platform/connector-development/partner-certified-destinations), last changed 2025-04-23):
- *Listing and SLA requirements:*
  - A public GitHub repo/project for issue tracking.
  - Monitor a Slack channel shared with Airbyte Support/Dev.
  - **3 business days** maximum to first response.
  - **≥95% first-sync success and ≥95% overall sync success** (config errors excluded).
  - Update the CDK or meet new platform requirements **at least every 6 months**.
  - HTTPS only.
- *Functional requirements:*
  - Emit state only after records are durably committed.
  - Append record counts to state.
  - **No more than 15 minutes between state messages.**
  - Idempotent, re-runnable syncs; dedupe by primary key where available.
  - `SPEC` regex validation, and useful `CHECK` errors.
  - `airbyte_secret: true` on secrets.
  - `allowedHosts` declared.
  - Data-fidelity type rules.
- *"Bulk" destinations only:* `_airbyte_raw_id`, `_airbyte_extracted_at` and `_airbyte_meta` columns, `_airbyte_meta.changes[]`, and automatic schema evolution. "Publish" (reverse-ETL/API) destinations are partly exempt. **[Inference]** Querri takes tables, so it is closer to a "bulk" destination. Expect to be asked for the metadata columns and schema evolution.

**How to apply [Source + gap]:**
- The requirements doc gives no application form.
- The general partner program has a "Become an Airbyte Partner" path at [airbyte.com/company/become-a-partner](https://airbyte.com/company/become-a-partner), linked from [airbyte.com/partners](https://airbyte.com/partners).
- I found no public statement on whether Airbyte is admitting new partner destinations in 2026. **Ask directly.**
- The "Airbyte Certified" label (`supportLevel: certified`) is used only for Airbyte-maintained connectors. All 14 certified destinations are Airbyte's own (registry data, [appendix](#appendix-a--method-for-the-counts)).

## 1.3 (c) PyPI publishing for PyAirbyte

**How Airbyte does it [Source]:**
- The QA check requires Python connectors to set `remoteRegistries.pypi.enabled: true` ([QA checks](https://docs.airbyte.com/community/contributing-to-airbyte/resources/qa-checks)).
- Airbyte's CI publishes with [`poe-tasks/publish-python-registry.sh`](https://github.com/airbytehq/airbyte/blob/master/poe-tasks/publish-python-registry.sh), using Airbyte's PyPI token.
- The package naming convention is `airbyte-source-<name>`, as in `packageName: airbyte-source-perk` ([example](https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-perk/metadata.yaml)).
- Our `metadata.yaml` has `enabled: true` but **no `packageName`**.

**Using a connector with PyAirbyte when it is not in Airbyte's registry [Source]:**
- `get_source()` raises `AirbyteConnectorNotRegisteredError` for unknown names, *unless* an explicit install method is given (`pip_url`, `local_executable`, `docker_image`, or `source_manifest`) ([PyAirbyte `_executors/util.py`](https://github.com/airbytehq/PyAirbyte/blob/main/airbyte/_executors/util.py)).
- Self-published users would run `ab.get_source("source-querri", pip_url="<our-pypi-name>", config=...)`.

**Blockers in our repo today [Source: repo files]:**
- `source-querri/pyproject.toml` declares `querri = { path = "../../querri-python-sdk", develop = true }`. A path dependency cannot be published to PyPI.
- The SDK is on PyPI as [`querri` 2.0.0](https://pypi.org/project/querri/), from [Querri-inc/querri-python](https://github.com/Querri-inc/querri-python). Switch the dependency to a version range such as `querri = "^2.0"`, then re-test. Check the destination's pyproject the same way.
- PyPI names are unclaimed right now: `source-querri`, `destination-querri`, `airbyte-source-querri` and `airbyte-destination-querri` all return 404 on PyPI.
- **[Inference]** Register `airbyte-source-querri` / `airbyte-destination-querri` ourselves now, to match Airbyte's convention. If Airbyte later merges the connector, add Airbyte as a co-maintainer so their CI can publish.

## 1.4 (d) Docker images for custom connectors (OSS/Self-Managed, and Cloud)

**Image contract [Source]** ([protocol-docker](https://docs.airbyte.com/platform/understanding-airbyte/airbyte-protocol-docker)):
- `AIRBYTE_ENTRYPOINT` env var equal to the image's `ENTRYPOINT`, using an absolute path.
- Run as non-root user `airbyte`.
- A writable `/airbyte` directory.
- Write only to directories the base image permits.
- **amd64 or multi-arch.**

**Build without a Dockerfile [Source]:**
- Airbyte's QA check says *"Python connectors must not use a Dockerfile and must declare their base image in metadata.yaml"* ([QA checks](https://docs.airbyte.com/community/contributing-to-airbyte/resources/qa-checks)).
- The `airbyte-cdk` CLI builds an image from metadata: `airbyte-cdk image build [--tag] [--no-verify] [--dockerfile]`, then `airbyte-cdk image test` ([CDK `_image.py`](https://github.com/airbytehq/airbyte-python-cdk/blob/main/airbyte_cdk/cli/airbyte_cdk/_image.py)). Install it with `uv tool install 'airbyte-cdk[dev]'` ([local development](https://docs.airbyte.com/platform/connector-development/local-connector-development)).
- **Our `baseImage` is `python-connector-base:3.0.0`, but the current version is `4.1.1`** ([Docker Hub tags](https://hub.docker.com/r/airbyte/python-connector-base/tags)). Bump it and pin a digest.
- For self-published images, use our own namespace (e.g. `querri/source-querri`), not `airbyte/…`.

**Adding the image in OSS / Self-Managed [Source]** ([using-custom-connectors](https://docs.airbyte.com/platform/operator-guides/using-custom-connectors)):
1. Push the image to a registry that the Airbyte instance can pull from.
2. In the UI, go to **Workspace settings → Sources/Destinations → New connector → Add a new Docker connector**, then enter the image name, tag and optional docs URL.
3. For `abctl`/kind, run `kind load docker-image`.
4. For private registries on Kubernetes, set `JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_SECRET`.

**Airbyte Cloud:** Docker custom connectors are unclear or unavailable (see the stale-claims table above). Builder/YAML custom connectors do work in Cloud. **[Inference]** This is another reason to build a manifest-only version of `source-querri`: it would work in Cloud as a Builder custom connector with no review needed. The destination has no Cloud self-serve route.

## 1.5 Concrete requirements checklist (for a marketplace PR)

All items below come from [QA checks](https://docs.airbyte.com/community/contributing-to-airbyte/resources/qa-checks), [testing](https://docs.airbyte.com/platform/connector-development/testing-connectors) and [metadata file](https://docs.airbyte.com/platform/connector-development/connector-metadata-file) unless noted otherwise.

| Area | Requirement | Our status |
|---|---|---|
| metadata.yaml | Valid schema; `connectorType`, `definitionId` (UUID), `dockerImageTag` (semver), `dockerRepository: airbyte/<name>`, `license` (MIT or ELv2), `supportLevel`, `documentationUrl`, `tags` (`language:*`, `cdk:*`), `registryOverrides`, `githubIssueLabel`, `icon`, `releaseStage` | Has most fields. **Missing `githubIssueLabel`, `icon`, `releaseStage` and `remoteRegistries.pypi.packageName`.** The source has no `allowedHosts`. Our `definitionId`s look hand-typed (`a3f2b1c4-d5e6-4f78-9a0b-1c2d3e4f5678`); regenerate them with `uuidgen`. |
| Versions | The version in metadata.yaml must equal the pyproject version; semver; bumped on every PR (Version Increment Check) | 0.1.0 in both |
| License | metadata.yaml must match pyproject | MIT in both |
| Packaging | Poetry; no Dockerfile; `baseImage` declared | Poetry OK; base image is stale (3.0.0) |
| Docs | `docs/integrations/<type>s/<name>.md` with the standard header order (Prerequisites, Setup guide, Supported sync modes, Supported Streams, …, Changelog); a changelog entry for each version; valid links | We have `docs/integrations/{sources,destinations}/querri.md`. They need a header-order and changelog check. |
| Assets | `icon.svg` at the connector root, square | Present; check that it is square |
| Security | HTTPS only | OK |
| Tests | Unit tests (blocking), integration tests, Connector Acceptance Tests (need sandbox creds), and the `airbyte-cdk connector test` "FAST standard tests". Community PRs without Airbyte-held credentials: integration and container tests may be non-blocking; unit tests always block ([testing](https://docs.airbyte.com/platform/connector-development/testing-connectors)) | 97 unit tests; `acceptance-test-config.yml` present |
| Local tooling | `poe` tasks (`poe test-unit-tests`, `poe check-all`), `airbyte-cdk image build/test`, `airbyte-ops local connector qa` ([local dev](https://docs.airbyte.com/platform/connector-development/local-connector-development), [poetry-connector-tasks.toml](https://github.com/airbytehq/airbyte/blob/master/poe-tasks/poetry-connector-tasks.toml)) | Not yet run |
| Certified sources only | `maxSecondsBetweenMessages` | N/A (community) |
| Review SLA | **None published** for community connectors; observed 1 day to 5 months | — |

## 1.6 Recommended order of steps

1. **Fix the packaging blockers** (about 1 day):
   - Replace the `querri` path dependency with `querri = "^2.0"`.
   - Bump `baseImage` to `python-connector-base:4.1.1@sha256:…`.
   - Add `packageName`, `githubIssueLabel`, `icon`, `releaseStage` and real UUIDs.
2. **Self-distribute now:**
   - Publish `airbyte-source-querri` / `airbyte-destination-querri` to PyPI.
   - Build multi-arch images with `airbyte-cdk image build` and push them to `docker.io/querri/*` or GHCR.
   - Publish a short "Use Querri with Airbyte OSS / PyAirbyte" guide in Querri's docs.
   - This gets us real distribution with no dependency on Airbyte.
3. **Source to the Marketplace:**
   - Port `source-querri` to a manifest-only YAML connector, using Connector Builder or hand-written YAML.
   - Submit it through the Builder's **Contribute to Marketplace** button, or as a PR.
   - Offer a sandbox Querri org and API key for their CI.
   - Once published, this also gives PyAirbyte users `get_source("source-querri")` with zero configuration, via YAML install.
4. **Destination:**
   - Open a GitHub Discussion (new-connector-request) and email partnerships through [become-a-partner](https://airbyte.com/company/become-a-partner). Ask explicitly whether they are admitting new Python partner destinations in 2026.
   - Only build the Partner Certified extras if they say yes: `_airbyte_meta` columns, schema evolution, the 15-minute state cadence, and SLA docs.
   - Meanwhile, the destination ships only through PyPI and Docker (step 2).
5. **Keep watching** [contributing](https://docs.airbyte.com/community/contributing-to-airbyte) for policy changes. It changed twice in 2026 (2026-05-04 and 2026-07-29).

---

# Part 2 — What Querri may do if we use Airbyte connectors internally

## 2.1 Licensing map

| Component | License | Evidence |
|---|---|---|
| Airbyte platform (`airbytehq/airbyte-platform`) | **ELv2** | [LICENSE](https://github.com/airbytehq/airbyte-platform/blob/main/LICENSE) |
| Connectors monorepo (`airbytehq/airbyte`) | **ELv2** at the root. Per connector, see `metadata.yaml` `license:` | [LICENSE](https://github.com/airbytehq/airbyte/blob/master/LICENSE), [licenses README](https://docs.airbyte.com/community/licenses) |
| Airbyte Protocol (`airbytehq/airbyte-protocol`) | **MIT** | [licenses README](https://docs.airbyte.com/community/licenses): *"Airbyte Protocol is open sourced and available under the MIT License"*; GitHub license API = MIT |
| Python CDK (`airbyte-cdk` on PyPI, v7.30.0) | **MIT** | [LICENSE.txt](https://github.com/airbytehq/airbyte-python-cdk/blob/main/LICENSE.txt), [pyproject `license = "MIT"`](https://github.com/airbytehq/airbyte-python-cdk/blob/main/pyproject.toml), [PyPI](https://pypi.org/project/airbyte-cdk/) |
| **PyAirbyte** (`airbyte` on PyPI, v0.69.0) | **ELv2** (not MIT) | [LICENSE.md](https://github.com/airbytehq/PyAirbyte/blob/main/LICENSE.md), added 2024-02-17 |
| Airbyte Agent SDK / agent connectors | **ELv2** | [airbyte-agent-connectors LICENSE](https://github.com/airbytehq/airbyte-agent-connectors/blob/main/LICENSE) |
| Airbyte Cloud, Enterprise, Agents (hosted) | Commercial | [licenses README](https://docs.airbyte.com/community/licenses) |

Inconsistencies to flag for counsel [Source]:
- The licenses README says *"Airbyte Connectors and everything in our public repos excluding the airbytehq/airbyte-protocol are … ELv2"* ([README](https://docs.airbyte.com/community/licenses)). The CDK repo, however, is plainly MIT.
- The FAQ still says *"Our own connectors remain open-source … The Connector Development Kit (CDK) … also remains open-source"* ([FAQ](https://docs.airbyte.com/community/licenses/license-faq)). Airbyte's own blog calls ELv2 *"source-available rather than open source by OSI definition"* ([blog](https://airbyte.com/blog/move-to-elv2)).
- The Cloud Terms of Service (effective 2025-05-19) say Airbyte Connectors are *"licensed to you pursuant to the MIT License"* ([terms](https://airbyte.com/company/terms)). That predates the July 2025 relicense.

### Connector license counts (data pulled 2026-09-23)

I read the per-version metadata published at `connectors.airbyte.com/files/metadata/airbyte/<name>/<version>/metadata.yaml` for every connector version in the OSS registry ([registry](https://connectors.airbyte.com/files/registries/v0/oss_registry.json)).

| | ELv2 | MIT | Airbyte Enterprise | Total |
|---|---|---|---|---|
| Sources | 583 (96.8%) | 13 (2.2%) | 6 (1.0%) | 602 |
| Destinations | 54 (96.4%) | 2 (3.6%) | 0 | 56 |
| **All** | **637 (96.8%)** | **15 (2.3%)** | **6 (0.9%)** | **658** |

- **MIT sources:** Acuity Scheduling, Adobe Commerce (Magento), Infor Nexus, Mantle, Metricool, Perk, QuickBooks, Shiftbase, Stigg, TickTick, plus three *certified* ones: Workday, Workday REST and ServiceNow.
  - The three certified ones are not in the public monorepo, so their source code location is unclear.
  - All the others are small, low-usage community connectors.
- **MIT destinations:** HubSpot, S3 Glue.
- **Airbyte Enterprise (commercial):** Db2, NetSuite Enterprise, Oracle, SAP HANA, SharePoint Enterprise, SharePoint Lists Enterprise.
- **Cross-check against repo HEAD:** 690 `metadata.yaml` files, of which 680 are ELv2 and 10 MIT.
- **Warning:** the aggregated registry JSON's `license` field is stale. It still reports about 73 MIT connectors, but the per-version metadata for the same versions says ELv2 (for example, source-activecampaign 0.2.14: registry = MIT, [published metadata](https://connectors.airbyte.com/files/metadata/airbyte/source-activecampaign/latest/metadata.yaml) = ELv2).
- **Older MIT versions:** *"Past contributions remain under their original license terms"* ([blog](https://airbyte.com/blog/move-to-elv2)).
  - **[Inference]** Connector source at git commits before 2025-07-22, and images built from it, are arguably still MIT.
  - They are also 14+ months stale, would miss API changes and security fixes, and would need to be forked and maintained by us.
  - This is a legal-review question, and not a practical long-term strategy.

## 2.2 What ELv2 prohibits

The license text ([ELv2](https://github.com/airbytehq/airbyte/blob/master/LICENSE)) sets three limits:
1. *"You may not provide the software to third parties as a hosted or managed service, where the service provides users with access to any substantial set of the features or functionality of the software."*
2. No circumventing license-key functionality.
3. No removing or obscuring licensing, copyright or other notices.

It also:
- requires anyone who receives a copy to get the terms;
- requires marking modified copies;
- terminates automatically on violation, with a 30-day cure after notice.

**Airbyte's interpretation [Source]** ([FAQ](https://docs.airbyte.com/community/licenses/license-faq), [examples](https://docs.airbyte.com/community/licenses/examples), [licenses README](https://docs.airbyte.com/community/licenses)):
- **Not allowed without a contract:**
  - *"Hosting Airbyte yourself and selling it as an ELT/ETL tool … a competitive alternative to Airbyte Cloud or Airbyte Enterprise."*
  - *"Selling a product that directly exposes Airbyte's UI or API."*
- **Allowed:**
  - *"Creating an analytics or attribution platform for which you want to use Airbyte to bring data in on behalf of your customers."*
  - *"Creating any kind of platform on which you offer Airbyte's connectors to your customers to bring their data in, unless you're selling an ELT/ETL solution."*
  - *"You can sell access to applications built and run using Airbyte."*
  - *"You can modify the software, integrate the variant into your app, and operate the modified app."*
- **Short version:** *"Unless you want to host Airbyte yourself and sell it as an ELT/ETL tool, or sell a product that directly exposes Airbyte's UI or API, you should be good to go."*
- The 2025 blog adds: *"The only thing ELv2 restricts is taking Airbyte's connectors and platform, packaging them up, and reselling them as a managed service under another name."* ([blog](https://airbyte.com/blog/move-to-elv2))

**Elastic's own FAQ (the license author) [Source]** ([Elastic FAQ](https://www.elastic.co/licensing/elastic-license/faq)):
- *Allowed:* *"I'm using Elasticsearch to put a search box on my cat-picture SaaS product."*
- *Not allowed:* offering it *"as a service, where my customers have direct access to substantial portions of the … APIs"*.

## 2.3 Does Querri's design fit? — **Needs legal review; the evidence leans "permitted"**

**[Inference] Why it likely fits:**
- Querri is an analytics / data-analysis SaaS. Pulling customers' data in so they can analyze it in Querri matches Airbyte's own "analytics platform … bring data in on behalf of my customers" example almost word for word.
- It also fits the "any kind of platform on which you offer Airbyte's connectors to your customers to bring their data in" example.
- Customers never see the Airbyte UI or API.

**[Inference] Where the risk is.** Put these to counsel, and ideally to Airbyte in writing:
1. **"Substantial set of the features or functionality."** The design lists *every* Airbyte source, renders each connector's `spec`, and runs `check`/`discover`/`read` on a schedule. That is essentially all of a connector's functionality exposed to paying third parties.
   - Airbyte's examples say that is fine *unless* it amounts to selling ELT/ETL.
   - The license text itself doesn't contain that carve-out; the FAQ supplies it.
   - FAQ statements are persuasive (and arguably support estoppel), but they are not the license.
2. **Positioning.** If Querri markets "600+ connectors" as a headline feature, lets customers export or forward the synced data to *other* destinations (warehouse replication), or charges per connector or row, it starts to look like "selling an ELT/ETL solution". Keep the connectors clearly subordinate to Querri's analysis product: data lands in Querri for use in Querri.
3. **Commercial signal.** Airbyte now sells exactly this use case:
   - The Airbyte Agents "Custom" plan is *"for large companies and embedded products"* and includes *"an authentication module for embedding connector authentication in your app"* ([billing](https://docs.airbyte.com/ai-agents/admin/billing)).
   - The Embedded page quotes *"custom pricing based on your customer count, sync volume, and deployment model"* ([embedded-integrations](https://airbyte.com/embedded-integrations)).
   - Airbyte could tighten its FAQ interpretation later. The 2025 relicense shows its licensing posture does change.
4. **PyAirbyte is also ELv2.** Even if we avoid the platform, using PyAirbyte as our runner is itself ELv2 use, subject to the same analysis. Running images directly, or using the MIT `airbyte-cdk`, avoids that one layer. The ELv2 connector code itself remains.
5. **Declarative manifests.** A manifest-only connector is ELv2 YAML (plus sometimes ELv2 `components.py`) executed by the MIT CDK. **[Inference]** Running it in-process with the CDK doesn't change the connector's license.
6. **Obligations we must meet anyway:**
   - Don't strip notices.
   - Mark any modifications we make to connectors.
   - Pass on the ELv2 terms if we ever *distribute* connector copies, for example in on-prem Querri. That doesn't apply to pure SaaS.

**Recommended action:**
- Email `license@airbyte.io` (the address given in the [licenses README](https://docs.airbyte.com/community/licenses)).
- Describe the exact design: an analytics SaaS, connectors run on Querri infrastructure, data lands only in Querri, no Airbyte UI or API exposed, not sold as ELT.
- Ask for written confirmation that this is the permitted "analytics platform" case, and ask what an OEM license would cost if they say no.

## 2.4 Airbyte's commercial embedding offerings

- **Airbyte Embedded** was announced 2025-04-02. It offers an embeddable widget to collect end-customer credentials, with Airbyte handling OAuth and syncing to S3 or Snowflake ([announcement](https://airbyte.com/blog/announcing-airbyte-embedded)).
  - By 2026 it has been folded into **Airbyte Agents**, launched 2026-05-04 ([blog](https://airbyte.com/blog/airbyte-agents)).
  - The docs call the widget the *"authentication module (also known as Airbyte Embedded)"*. That doc page is currently marked `draft: true` ([source](https://github.com/airbytehq/airbyte/blob/master/docs/ai-agents/interfaces/api/authentication-module.md)).
- **Pricing (Airbyte Agents) [Source]** ([billing](https://docs.airbyte.com/ai-agents/admin/billing)):
  - Free: $0, 1,000 agent operations per month.
  - Individual: $29 per month.
  - Team: $299 per month.
  - **Custom**, for embedded products: sales-assisted, and includes the auth module, SLAs and an account manager.
  - The Embedded marketing page says *"custom pricing based on your customer count, sync volume, and deployment model"* and *"OAuth and secrets handled by Airbyte"* ([embedded-integrations](https://airbyte.com/embedded-integrations)).
- **"Powered by Airbyte"** is still listed as a product ([product page](https://airbyte.com/product/powered-by-airbyte), [2023 press release](https://www.businesswire.com/news/home/20231101054340/en/Powered-by-Airbyte-Offers-Software-Makers-More-than-100-Data-Integrations)). It also appears as a support column in [connector support levels](https://docs.airbyte.com/integrations/connector-support-levels). In this model you drive Airbyte Cloud through its API, with one workspace per end customer.
- **Data replication pricing (Cloud)** ([pricing](https://airbyte.com/pricing), fetched 2026-09-23):
  - Standard from $20/month.
  - Plus from $189/month.
  - Pro and Enterprise Flex: custom pricing.
- **Other licenses:** *"If you need a different license, please get in touch with us at: contact@airbyte.io"* ([FAQ](https://docs.airbyte.com/community/licenses/license-faq)). No public OEM price list exists.

## 2.5 Running connectors without the Airbyte platform — supported and documented

- **Direct Docker [Source]** ([protocol-docker](https://docs.airbyte.com/platform/understanding-airbyte/airbyte-protocol-docker)):
  ```
  docker run --rm -i <image> spec
  docker run --rm -i <image> check    --config config.json
  docker run --rm -i <image> discover --config config.json
  docker run --rm -i <image> read     --config config.json --catalog catalog.json [--state state.json]
  ```
  - STDIO/JSONL is the default (`DATA_CHANNEL_MEDIUM=STDIO`).
  - There is also a newer *socket mode* (Unix sockets + protobuf, 4–10× faster) that the platform selects automatically. **[Inference]** We can stay on STDIO.
- **PyAirbyte [Source]** ([README](https://github.com/airbytehq/PyAirbyte/blob/main/README.md), [`get_source`](https://github.com/airbytehq/PyAirbyte/blob/main/airbyte/sources/util.py)):
  - Install modes are `source_manifest` (download the YAML and run it in-process), `pip_url` (a Python venv via `uv`), `docker_image`, and `local_executable`.
  - With no install mode given, it follows the registry's default install type: YAML → Python → Docker ([executor logic](https://github.com/airbytehq/PyAirbyte/blob/main/airbyte/_executors/util.py)).
  - PyAirbyte FAQ: it *"does not have orchestration or scheduling capabilities"*. We would supply those ourselves (our Redis queue).
  - It sends telemetry to Segment unless `DO_NOT_TRACK` is set ([telemetry.py](https://github.com/airbytehq/PyAirbyte/blob/main/airbyte/_util/telemetry.py)). **Set `DO_NOT_TRACK=1` in a multi-tenant SaaS.**
- **CDK only (MIT) [Source]:** `airbyte-cdk` ships a `source-declarative-manifest` CLI with `check`/`discover`/`read` plus `--manifest-path`, `--components-path` and `--state` ([README](https://github.com/airbytehq/airbyte-python-cdk/blob/main/airbyte_cdk/cli/source_declarative_manifest/README.md), [pyproject scripts](https://github.com/airbytehq/airbyte-python-cdk/blob/main/pyproject.toml)). Manifests can be downloaded from `connectors.airbyte.com/files/metadata/airbyte/<name>/<version>/manifest.yaml` (the URL pattern PyAirbyte uses).

## 2.6 OAuth

- **We cannot use Airbyte's OAuth apps.**
  - In Airbyte Cloud, *"client credentials are securely managed by Airbyte"*.
  - In Self-Managed Airbyte, *"for security reasons, you must configure your own client credentials"*.
  - Marketplace and custom connectors always need your own ([OAuth doc](https://docs.airbyte.com/platform/using-airbyte/oauth)).
  - **[Inference]** Airbyte's client secrets are not published, and using another company's OAuth app would violate every provider's developer terms. We need our own OAuth app per provider, and many providers require app review or verification (Google, Salesforce, HubSpot, Meta and others).
- **Who runs the flow.** The consent-URL / code-exchange / refresh dance is done by the **Airbyte platform**, not by the connector (*"Airbyte handles OAuth flow coordination: generating consent URLs, managing state parameters, token exchange, refresh token management"*, [declarative OAuth](https://docs.airbyte.com/platform/connector-development/config-based/advanced-topics/oauth)). Without the platform, **Querri must implement the OAuth dance itself**. The connector just receives `client_id`, `client_secret` and `refresh_token` (or `access_token`) in its config and refreshes tokens at runtime.
- **How specs expose OAuth.** `spec.advanced_auth` contains:
  - `auth_flow_type: oauth2.0`;
  - `predicate_key` / `predicate_value`, which pick the OAuth branch of the `credentials` oneOf;
  - `oauth_config_specification.complete_oauth_server_input_specification`, meaning our `client_id`/`client_secret`;
  - `complete_oauth_output_specification`, which lists the tokens and their `path_in_connector_config` for injection into config.
  - Newer connectors also include `oauth_connector_input_specification`, which declares `consent_url`, `access_token_url`, params, scopes and `extract_output` as templates. For example, Asana has `consent_url: https://app.asana.com/-/oauth_authorize?{{client_id_param}}&…` (registry spec).
  - **[Inference]** That declarative block is enough for Querri to build *one* generic OAuth handler. The legacy (non-declarative) flows live in the ELv2 platform's Java code, so we'd write those per provider.
- **Counts from the OSS registry specs (heuristic, 602 sources):**
  - **54** declare `advanced_auth` (OAuth).
    - **29** of them use declarative OAuth (`oauth_connector_input_specification`): Asana, GitHub, Gmail, Gong, GA4, Google Drive, Google Search Console, Google Sheets, Greenhouse, Harvest, HubSpot, Intercom, Jira, Linear, LinkedIn Ads, Mailchimp, Microsoft SharePoint, Monday, Notion, Salesforce, SharePoint Enterprise, Slack, TickTick, YouTube Data, the four Zendesk connectors and ZohoCRM.
    - About **33 of the 54** also offer a non-OAuth option (API key or PAT) in the same `credentials` oneOf.
  - The other 25 OAuth connectors use the legacy flow: Airtable, Amazon Ads, Amazon Seller Partner, Bing Ads, Facebook Marketing, Facebook Pages, Instagram, Pinterest, Shopify, Snapchat, TikTok Marketing, Typeform, and others.
  - About **380** use only non-OAuth credentials: API key, token, basic auth, database user and password.
  - About **56** have OAuth-style fields (`client_id` / `client_secret` / `refresh_token`) without `advanced_auth`. The user pastes tokens they obtained themselves.
  - **[Inference]** OAuth-only providers with no API-key fallback that matter most for a BI product include Google Ads, GA4, Search Console, Facebook/Instagram Marketing, LinkedIn Ads, Bing Ads, TikTok, Snapchat, Amazon Ads and Amazon Seller Partner. Budget for app review for each of these.

## 2.7 Trademarks and branding

- ELv2 grants no trademark license: *"Any use of the licensor's trademarks is subject to applicable law"* and *"These terms do not imply any licenses other than those expressly granted"* ([ELv2](https://github.com/airbytehq/airbyte/blob/master/LICENSE)).
- The press kit says *"Please see our Trademark Policy before using our logo"*, but the link just points to the Terms of Service ([press](https://airbyte.com/company/press)). The ToS contains no trademark-usage section ([terms](https://airbyte.com/company/terms)).
- **I found no published Airbyte trademark policy or "Powered by Airbyte" badge rules.**
- **[Inference]**
  - Don't use "Powered by Airbyte" or the Airbyte logo without written permission.
  - A plain factual attribution in docs or legal notices ("includes connectors from the Airbyte project, licensed under ELv2") is low-risk nominative use, and it also helps satisfy ELv2's notice obligations.
  - Connector icons in the Airbyte repo (Salesforce, HubSpot and so on) are *those vendors'* trademarks. Airbyte cannot license them to us; follow each vendor's brand guidelines.
  - Referring to "Salesforce" by name to identify the integration is ordinary nominative use.

## 2.8 Alternatives if ELv2 is a blocker (licensing and connector count only)

| Option | License | Connector count | Notes |
|---|---|---|---|
| Airbyte MIT-only subset (current) | MIT | 13 sources and 2 destinations | Mostly niche. Not a viable catalog. |
| Airbyte pre-2025-07-22 versions | MIT (per "past contributions remain under original terms") | Roughly the ~500+ that existed then | Frozen and stale; we would maintain a fork. Needs legal sign-off. |
| **dlt (dlthub)** | Apache-2.0 ([dlt](https://github.com/dlt-hub/dlt), [verified-sources](https://github.com/dlt-hub/verified-sources)) | 36 verified-source directories in the repo; about 30 listed in the [docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/). A generic REST API source and a SQL (SQLAlchemy) source cover many more. | Python library, in-process, no Docker. |
| **Singer / Meltano taps** | Per tap, mixed. Meltano itself is MIT ([meltano](https://github.com/meltano/meltano)); the Meltano SDK is Apache-2.0 ([sdk](https://github.com/meltano/sdk)). Many official `singer-io` taps are **AGPL-3.0** ([tap-salesforce](https://github.com/singer-io/tap-salesforce), [tap-hubspot](https://github.com/singer-io/tap-hubspot), [tap-stripe](https://github.com/singer-io/tap-stripe)). MeltanoLabs taps are often Apache-2.0 ([tap-github](https://github.com/MeltanoLabs/tap-github)). | Meltano Hub: **631 distinct extractors** (1,049 variants) ([hub repo](https://github.com/meltano/hub)) | Quality and maintenance vary widely; each tap's license must be vetted. AGPL is a network-use copyleft risk for SaaS. |
| Write our own on the MIT `airbyte-cdk` (declarative YAML) | Ours | As many as we build | Same runtime as Airbyte manifests. We own maintenance. |
| Buy: Airbyte Agents Custom / Embedded | Commercial | 600+ | Removes the license question; adds cost and a vendor dependency. |

---

# Part 3 — Technical feasibility notes

## 3.1 Connector implementation mix (OSS registry, 2026-09-23)

| | manifest-only (YAML) | Python | Java/Kotlin |
|---|---|---|---|
| Sources (602) | **515 (85.5%)** | 63 (10.5%) | 24 (4.0%) |
| Destinations (56) | 0 | 25 | 31 |

- **Java sources** are almost all databases: BigQuery, ClickHouse, CockroachDB, Db2, DynamoDB, Elasticsearch, Kafka, MSSQL, MongoDB, MySQL, Oracle, Postgres, Redshift, SAP HANA, SFTP, SingleStore, Snowflake, Teradata, TiDB, plus two E2E test sources.
- They run on `java-connector-base` (Amazon Corretto 21) ([Dockerfile](https://github.com/airbytehq/airbyte/blob/master/docker-images/Dockerfile.java-connector-base)).
- **[Inference]** These are the heavy ones (JVM heap, CDC). They can only run as containers; PyAirbyte also uses Docker for them.
- **Python sources** (63) include the big SaaS ones: Salesforce, Shopify, Facebook Marketing, Google Ads, GitHub, Marketo, Mixpanel, S3, GCS, Google Drive, and others.
- **Custom Python inside manifests:** 56 of 513 manifest-only connector directories ship a `components.py` (repo tree count). The CDK refuses to run it unless `AIRBYTE_ENABLE_UNSAFE_CODE=true` ([custom_code_compiler.py](https://github.com/airbytehq/airbyte-python-cdk/blob/main/airbyte_cdk/sources/declarative/parsers/custom_code_compiler.py)).
  - **[Inference]** Those ~11% need a sandbox (a container) or a code review before we trust them in-process.
  - The other ~89% of manifest connectors are pure YAML and run safely in one Python process.

## 3.2 Resource footprint

- **Image sizes (Docker Hub, compressed amd64)** ([Docker Hub](https://hub.docker.com/u/airbyte)):

  | Image | Size |
  |---|---|
  | `source-declarative-manifest` 7.30 | ~286 MB |
  | `python-connector-base` 4.1.1 | ~132 MB |
  | `java-connector-base` 2.0.4 | ~227 MB |
  | `source-postgres` | ~318 MB |
  | `source-mysql` | ~312 MB |
  | `source-hubspot` | ~286 MB |
  | `source-salesforce` | ~189 MB |

  All manifest connectors share the one `source-declarative-manifest` image. The image tag pins the CDK version; the manifest is baked in.
- **Memory hints in metadata:** only a handful of connectors declare `resourceRequirements`, for example:
  - HubSpot, GA4 and Airtable: 1.5–1.6 GiB for check/discover;
  - Azure Blob Storage and SFTP Bulk: 4 GiB;
  - Visma Economic: 4 GiB (registry `resourceRequirements`).
- **Platform defaults:** Airbyte leaves CPU and memory requests/limits unset by default and lets operators set them instance-wide, per connector, or per connection ([configuring connector resources](https://docs.airbyte.com/platform/operator-guides/configuring-connector-resources)).
- **How Airbyte itself runs connectors:** as Kubernetes **pods per operation**, with a sidecar or orchestrator container. It warns that large requests (e.g. 4 CPU / 4 GiB × 4 containers) and first-time image pulls delay scheduling ([jobs](https://docs.airbyte.com/platform/understanding-airbyte/jobs)).
- **[Inference] Rough budgets:**
  - A manifest connector in-process: about 150–500 MB RSS.
  - Python connector containers: about 0.5–2 GB.
  - Java database connectors: plan 2–4 GB.

## 3.3 Execution models for Querri

| Model | Pros | Cons |
|---|---|---|
| **In-process CDK for manifest-only connectors** (MIT `airbyte-cdk`, `source-declarative-manifest` / declarative source classes; pull the manifest from the registry and pin the version) | No Docker. Fast start. Covers ~85% of sources. Easy to run serially on a Redis-queue worker. | One CDK version for all manifests: a manifest built for CDK 6.x may break on 7.x. Mitigate with a venv per CDK major. The 56 `components.py` connectors need `AIRBYTE_ENABLE_UNSAFE_CODE`, so isolate them. Tenant isolation is weaker (shared process memory and secrets). |
| **PyAirbyte** (ELv2) with a venv per Python connector | Handles install, versions and `uv`/Python pinning (`use_python`). Also does YAML. | ELv2. Telemetry (set `DO_NOT_TRACK`). Venvs are per host. Java still needs Docker. No scheduler. |
| **Kubernetes Job per sync** (`docker run`-equivalent via the protocol) | Strong isolation for secrets and tenants. Works for every language. Mirrors Airbyte's own architecture. | Pod scheduling latency. We write the orchestration (config/catalog/state files, STDOUT parsing). Needs RBAC to create Jobs. |
| **Docker-in-pod / DinD** | Simple locally | Privileged containers and security risk in a multi-tenant cluster. **[Inference]** Avoid; prefer K8s Jobs, or a rootless runtime such as Kata/gVisor if needed. |

**[Inference] Suggested hybrid:**
- A long-running worker pulls from Redis.
- It runs pure-YAML manifest connectors in-process (or in a subprocess, for crash isolation) using the MIT CDK.
- It dispatches Python, Java and `components.py` connectors as Kubernetes Jobs using the official images.
- Serial overnight execution is fine at small scale. Add per-connector concurrency limits, because each provider has its own rate limits.

## 3.4 State / checkpointing for incremental syncs

These points come from the [protocol doc, State & Checkpointing](https://docs.airbyte.com/platform/understanding-airbyte/airbyte-protocol).

- The source emits `AirbyteStateMessage`s interleaved with records. We pass the last committed state back via `read --state state.json`.
- State content is an **opaque blob** owned by the source: *"Platform & Destinations treat state as a black box."*
- **Types:**
  - `STREAM`: per stream; allows per-stream reset and parallelism. Most connectors use this.
  - `GLOBAL`: shared state plus per-stream state; used by CDC database sources.
  - `LEGACY`: deprecated.
- **Commit rule:**
  - Only persist a state message after all records *before* it are durably written to Querri.
  - Store the latest state per `(connection, stream descriptor)` for STREAM, or the whole blob for GLOBAL.
  - On failure, resume from the last committed state (partial success).
- Sources emit state even in full refresh, and don't emit duplicate consecutive states.
- **[Inference] Querri schema sketch:** `connector_state(connection_id, stream_namespace, stream_name, state_type, state_json, committed_at)`. Also keep a GLOBAL row with a null stream. Reset means deleting rows. Keep the configured catalog next to it, because state is only valid for the catalog it was produced under.

---

## Top risks

1. **ELv2 interpretation.**
   - Our design exposes most of what connectors do to paying customers.
   - Airbyte's FAQ and examples say that's fine for analytics platforms, but the binding text is the license, and Airbyte now sells an "embedded products" plan.
   - Mitigation: get written confirmation from Airbyte, position the connectors as ingestion into Querri only, and never offer data forwarding or ELT.
2. **License drift.** Airbyte already flipped roughly 99% of connectors from MIT to ELv2 in 2025, and its docs and ToS contradict each other. Pin versions, snapshot the license at the time of use, and re-check each upgrade.
3. **OAuth burden.** There is no access to Airbyte's OAuth apps, and many top BI sources are OAuth-only and need provider app review. The OAuth dance is not in the connectors; Querri must build it.
4. **Operational load.**
   - 600 connectors across 3 runtimes and many CDK versions.
   - Some need 1.5–4 GB.
   - 56 manifests execute arbitrary Python (`components.py`).
   - Community connectors come with *"No SLAs"* and *"may experience backward-incompatible, breaking changes with no notice"* ([support levels](https://docs.airbyte.com/integrations/connector-support-levels)).
   - Mitigation: curate a supported subset (for example the 62 certified sources plus the ones customers ask for) instead of listing all of them.
5. **Marketplace uncertainty for our own connectors.**
   - No review SLA exists.
   - Python sources and all new destinations are case-by-case.
   - Airbyte bulk-closed new-destination PRs in July 2026.
   - Mitigation: self-distribute via PyPI and Docker now, and port the source to YAML.

---

## Appendix A — Method for the counts

- **Registry:** `https://connectors.airbyte.com/files/registries/v0/oss_registry.json` (602 sources, 56 destinations) and `cloud_registry.json` (584 sources, 44 destinations), downloaded 2026-09-23. This gave the `language`, `supportLevel`, `spec.advanced_auth`, `resourceRequirements` and `remoteRegistries` fields.
- **License:** for each `(dockerRepository, dockerImageTag)` in the OSS registry, I fetched `https://connectors.airbyte.com/files/metadata/airbyte/<name>/<tag>/metadata.yaml` (658/658 succeeded) and tallied `data.license`, ignoring case (`Elv2` = `ELv2`).
- **Repo HEAD:** fetched all 690 `airbyte-integrations/connectors/*/metadata.yaml` files from `airbytehq/airbyte@master` (680 ELv2, 10 MIT). `components.py` count: 56 of 513 `manifest.yaml` connector directories.
- **OAuth categories** are a keyword heuristic over each `connectionSpecification`, so treat them as approximate.
- **Meltano Hub:** unique directory names under `_data/meltano/extractors/` in `meltano/hub@main`.
