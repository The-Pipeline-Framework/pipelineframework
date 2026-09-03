# TPF Author MCP Operations

This is the maintainer runbook for the local Repowise index that feeds the hosted TPF
Author MCP. Application authors do not need Repowise, Ollama, a TPF checkout, or access
to the publication infrastructure. The bridge implementation remains documented in
[`tpf-mcp-bridge/DEVELOPING.md`](https://github.com/The-Pipeline-Framework/tpf-mcp-bridge/blob/main/DEVELOPING.md).

## 1. Keep the two knowledge systems separate

| Maintainer knowledge | Author knowledge |
| --- | --- |
| Local Repowise MCP | `https://mcp.pipelineframework.org/mcp` |
| Current `main`-oriented repository intelligence | Exact released or snapshot-version evidence |
| Includes implementation details and maintainer ADRs | Includes only approved author-facing docs, APIs, examples, and the authoring skill |
| Runs on a maintainer machine | Runs as a stateless Cloudflare Worker |

The hosted service never queries Repowise. Repowise produces a commit-addressed build
input; the bridge compiles that input and exact TPF source into a versioned MCP dataset.

## 2. Local components

One configured clean `main` checkout triggers refreshes. A separate stable
`tpf-mcp-bridge` checkout owns the scripts. Its default state directory contains:

```text
<state-dir>/
├── framework/          last healthy promoted index and clean TPF clone
├── candidate/          disposable refresh candidate
├── previous-index/     rollback material, when present
├── completed-refresh.json
├── failed-refresh.json
├── refresh.log
└── refresh request and lock files
```

The primary installation currently uses
`~/.local/state/tpf-author-mcp/repowise`; the path is configurable and is not part of
the public contract. A `.repowise` directory in an ordinary development checkout is not
the publication index unless it is the installer's configured `framework/` clone.

## 3. Git-hook installation and location

Install from a stable, dependency-installed bridge checkout:

```shell
npm run install:repowise-upload-hook -- \
  --framework-dir /path/to/pipelineframework \
  --state-dir /path/to/tpf-author-mcp/repowise \
  --environment production
```

The installer adds managed blocks to the configured clone's shared Git hook directory:

```shell
git -C /path/to/pipelineframework rev-parse --path-format=absolute --git-path hooks/post-commit
git -C /path/to/pipelineframework rev-parse --path-format=absolute --git-path hooks/post-merge
git -C /path/to/pipelineframework rev-parse --path-format=absolute --git-path hooks/post-checkout
```

Hooks are not versioned. They contain absolute paths to the configured TPF checkout,
bridge checkout, Node runtime, and state directory. Reinstall them after moving or
replacing any of those paths. Each managed block requires the exact configured checkout
and `main`; shared hooks therefore ignore sibling feature worktrees.

## 4. What `git pull --ff-only` does

In the configured clean `main` checkout, a successful fast-forward pull runs the
`post-merge` hook and queues the new full commit SHA. The background refresh writes to
`<state-dir>/refresh.log`; the pull does not wait for it.

```text
git pull --ff-only
        │
        └─ post-merge ─ queue exact HEAD ─ background refresh
                                              │
                                              ├─ validate candidate
                                              ├─ promote healthy index
                                              └─ export and upload R2 input
```

A failed pull, `git fetch`, file checkout, non-`main` checkout, or sibling-worktree
operation does not prepare a new publication input. The runner is single-flight: newer
requests supersede older queued requests rather than starting competing updates.

## 5. Refresh and promotion

The runner fetches `origin/main`, rejects commits outside it, and prepares a candidate
from the last healthy index. It first attempts a model-free incremental structural
update. Promotion requires all of the following:

- candidate Git `HEAD` and Repowise `last_sync_commit` equal the requested full SHA;
- the candidate worktree is clean;
- stale-page count is zero;
- SQL/vector, SQL/FTS, and coordinator counts agree;
- Repowise's stored repository paths name the candidate correctly.

If incremental refresh fails these checks, the runner discards the candidate and tries
a genuinely empty `--no-prose --provider mock --model mock` structural rebuild. The last
healthy index remains available until a candidate passes. Refresh duration consumes
local CPU, Ollama embedding time, memory, and disk, but no external model tokens.

## 6. Export, outbox, and R2 input

After promotion, the runner exports Repowise pages and provenance into a durable local
outbox before attempting network delivery:

```text
.repowise/tpf-mcp-upload-queue/<full-commit>/
```

Successful delivery creates immutable private R2 objects:

```text
inputs/repowise/<full-commit>/wiki_pages.json
inputs/repowise/<full-commit>/manifest.json
```

Network or Cloudflare failure leaves the outbox entry for bounded retries and later Git
activity. Uploading the same checksum again is a no-op; different content for an existing
commit is rejected. Uploading this input does not activate a public MCP version.

## 7. Snapshot publication

`.github/workflows/publish-snapshots.yml` runs nightly or by manual dispatch on current
`main`. It verifies the framework, publishes the Maven `-SNAPSHOT`, then dispatches
`tpf-mcp-bridge/.github/workflows/publish-knowledge.yml` with `kind=snapshot`, the exact
version, and the exact full commit.

The bridge checks out that commit and waits up to ten minutes for only its matching R2
input. It then compiles and verifies a new immutable dataset before atomically switching
the mutable `X.Y.Z-SNAPSHOT` alias. The preceding snapshot remains active on failure.
Because Maven publication precedes the dispatch, an MCP failure does not roll Maven back.

## 8. Tagged release publication

`.github/workflows/publish.yml` is triggered by protected `v*` tags. It validates that
the tag matches the non-snapshot Maven version, deploys to Maven Central, creates the
GitHub release, then dispatches the same bridge workflow with the exact version, tag,
and full release commit. The bridge validates the exact tag checkout and publishes an
immutable release dataset.

Therefore the Repowise input path does affect tag releases, but only their final Author
MCP leg. If that leg fails, do not move the tag or repeat Maven publication. Deliver the
missing exact-commit input and rerun only the bridge workflow. A release version cannot
be republished with a different checksum.

## 9. Cloudflare ownership

R2 stores immutable raw inputs, normalized author pages, provenance, and approved source
payloads. D1 stores supported-version records, dataset aliases, document/source metadata,
and the FTS5 search index. Publication stages and verifies a complete dataset before one
D1 operation activates it.

Do not replace dataset-level FTS cleanup with per-document deletion. The FTS metadata
columns are unindexed; repeated row-by-row cleanup previously exhausted the account-wide
D1 rows-read allowance. If the allowance is exhausted, D1-backed MCP calls fail until
reset, while R2 objects remain intact.

## 10. Hosted MCP use by application authors

Codex users can register the service with:

```shell
codex mcp add tpf --url https://mcp.pipelineframework.org/mcp
```

After restarting Codex, an authoring agent should read the application's pinned TPF
version, confirm it with `tpf_versions`, search that exact version with `tpf_search`,
retrieve selected evidence with `tpf_context`, and use `tpf_source` only for precise
source verification. The Worker returns bounded evidence and performs no model calls.

## 11. Local Repowise use by maintainers

Point the repository-local Codex MCP entry at the last healthy promoted clone:

```toml
[mcp_servers.repowise]
command = "repowise"
args = ["mcp"]
cwd = "/path/to/tpf-author-mcp/repowise/framework"
startup_timeout_sec = 20
```

Restart Codex after changing this file: an existing MCP child retains its original
working directory and index. Local Repowise is for orientation, search, rationale, risk,
and graph context. The active worktree remains edit authority and must be read before
changing source. This installation intentionally has no Repowise LLM answer provider;
`degraded: no-llm-provider` is non-blocking, and Codex synthesizes retrieved evidence.

## 12. Feature worktrees

An ignored repository-local `.codex/config.toml` is not automatically copied to a new
worktree. A worktree may use the same MCP entry and the same last-healthy `framework/`
path. It must not create or refresh its own publication index.

The shared index describes current indexed `main`, not unmerged branch changes. Use it
to understand the repository, then inspect the worktree's source and diff directly. Do
not run publication `repowise update`, `init --force`, or hook installation from a
feature worktree.

## 13. Known Repowise failure modes

- Repowise 0.47 may leave hundreds of structural pages stale after an incremental
  update. Repeated `update`, `doctor --repair`, and `init --force` are not publication
  proof. The runner's validated empty-candidate fallback is the workaround.
- Repowise stores absolute repository paths. The bridge relinks and validates them when
  moving an index between candidate and healthy locations; a successful `doctor` alone
  is insufficient evidence that export used the intended checkout.
- The configured local index has real Ollama embeddings but no answer-model provider.
  `no-llm-provider` affects Repowise prose synthesis, not retrieval.
- Hooks launch background processes. In automation shells that terminate child
  processes, invoke the refresh runner in the foreground instead.

## 14. Recovery and verification

Check local state before retrying anything:

```shell
set -eu
state_dir=/path/to/tpf-author-mcp/repowise
tail -n 200 "$state_dir/refresh.log"
jq . "$state_dir/completed-refresh.json"
(cd "$state_dir/framework" && repowise doctor --no-workspace)
```

After correcting a recorded refresh failure, clear only its marker and run the bridge's
refresh command in the foreground for the exact `origin/main` commit:

```shell
set -eu
state_dir=/path/to/tpf-author-mcp/repowise
bridge_dir=/path/to/tpf-mcp-bridge
test -d "$state_dir/framework/.repowise"
rm -f "$state_dir/failed-refresh.json"
cd "$bridge_dir"
npm run refresh:repowise -- \
  --state-dir "$state_dir" \
  --commit "$(git -C "$state_dir/framework" rev-parse origin/main)"
```

Retry queued network delivery without re-indexing:

```shell
set -eu
state_dir=/path/to/tpf-author-mcp/repowise
bridge_dir=/path/to/tpf-mcp-bridge
cd "$bridge_dir"
npm run upload:repowise-input -- \
  --framework-dir "$state_dir/framework" \
  --environment production \
  --retry-only \
  --attempts 4
```

When the exact input exists, rerun `publish-knowledge.yml` in `tpf-mcp-bridge` with the
same kind, version, full commit, and release tag when applicable. Finally check
`https://mcp.pipelineframework.org/health` and use `tpf_versions` to verify the activated
version reports the intended commit.

## 15. Maintainer checklist

Do:

- use `git pull --ff-only` in the one configured clean `main` checkout;
- inspect `refresh.log`, `completed-refresh.json`, commit equality, and full doctor
  health when publication depends on a fresh input;
- keep the durable outbox and last healthy index;
- reinstall hooks after moving the TPF checkout, bridge checkout, runtime, or state;
- rerun only the failed MCP leg after Maven or GitHub publication has succeeded.

Do not:

- treat an ordinary checkout's `.repowise` directory as the publication index;
- run concurrent manual Repowise updates or refresh feature worktrees;
- use a real LLM for structural recovery;
- accept stale pages, commit mismatch, or inconsistent stores for publication;
- assume a successful pull means its asynchronous refresh and upload succeeded;
- silently substitute `latest`, another commit, or another version;
- delete the healthy index or queued input while recovering a candidate;
- repeatedly rerun high-read D1 publication statements without diagnosing them.

The design is fail-closed: local refresh, export, upload, or Cloudflare publication may
fail, but no failure may replace the last healthy local index or last active hosted MCP
dataset with mismatched or incomplete knowledge.
