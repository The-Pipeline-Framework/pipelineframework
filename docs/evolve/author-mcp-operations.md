# TPF knowledge sync operations

This is the maintainer runbook for the local knowledge preparation flow.
Application authors do not need maintainer-level index tooling for release readiness.

## 1. Scope and ownership

- **Maintainer knowledge**: local indexed source, ADRs, and historical notes used for reasoning.
- **Author knowledge**: canonical, release-oriented API/docs surface presented to end users.
- The public service is independent of local authoring workspaces and does not modify release worktrees.

## 2. Keep the two planes separate

The published service keeps user-facing metadata distinct from internal maintainer context.
Do not use application worktree local artifacts as the publication source of truth.

## 3. Local index layout

Use a stable, clean checkout as the input source and run a dedicated maintainer index
service from that checkout path.

```text
<state-dir>/
├── framework/              clean repository clone used as source-of-truth
├── candidate/              refresh candidate state
├── previous-index/         rollback material, when present
├── completed-refresh.json
├── failed-refresh.json
├── refresh.log
└── queue/
```

- Keep `state-dir` outside the worktree when possible.
- Never point publication tooling at a feature branch or feature worktree.

## 4. Change triggers

A clean `main` checkout event triggers a single-flight sync attempt:

- queue new commit SHA
- verify candidate input
- promote only when checks pass
- write stable outbox entries for bounded retries

Higher-frequency commands should not run overlapping sync operations.

## 5. Sync and promotion checks

Require all of the following before promotion:

- candidate checkout is clean
- candidate commit matches queued `HEAD`
- indexed repository paths are expected for that commit
- no stale-page or validation failures in the index state
- explicit publish checksum is present and stable

If validation fails, keep the last healthy index and retry with a clean candidate.

## 6. Sync outputs

The promoted input is written to a durable local outbox before external delivery.
Deliveries are retried with bounded backoff and are expected to be idempotent for the same
commit checksum.

## 7. Snapshot path

On snapshot publishes, the snapshot workflow publishes Maven artifacts from `main` first.
If only the knowledge sync leg fails, rerun that sync for the same `X.Y.Z-SNAPSHOT` commit only.

## 8. Release path

For tagged releases, keep Maven/GitHub release publication and downstream knowledge sync scoped
to the exact tag commit.
If the sync leg fails after a successful tagged publication,
rerun only the sync recovery path for that exact commit.
Do not rewrite the release tag or re-run Maven publication for this reason.

## 9. Hosted usage

The service endpoint remains `https://mcp.pipelineframework.org/mcp`.
For end-user workflows, rely on normal published sources (`tpf_versions`, `tpf_search`,
`tpf_context`, `tpf_source`) and do not infer release status from local index state.

## 10. Worktree safety

One configured clean `main` source should own publication preparation.
Feature worktrees and temporary checkouts should not create, refresh, or publish indexes.

## 11. Known recovery failure modes

- Queue growth or stale input indicates a partially completed refresh.
- Mismatch between requested commit and indexed paths indicates checkout drift.
- Storage failures should be retried using the existing queue rather than by forcing
  republish.

## 12. Recovery checklist

1. Inspect `refresh.log` and completed state before retry.
2. Confirm the queued commit and source checkout match.
3. Re-run only the failed sync leg.
4. Verify the active hosted commit/version after successful queue drain.
5. Confirm public endpoint health after completion.

## 13. Recovery principles

- Avoid changing the release commit to recover a sync failure.
- Avoid concurrent manual refreshes.
- Avoid deleting healthy indexes while recovery is in progress.
- Do not assume a successful pull means publish data is ready.
- Keep all retries deterministic and commit-specific.
