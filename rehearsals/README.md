# Hosted mixed-version rehearsals

Run **TPF System Tests — Full Train** from `main` with
`rehearsal_manifest=rehearsals/1052-candidate-runtime.json`. Leave `release_manifest` empty
and `search_aws_only=false`. The complete current full-train policy runs, including hosted AWS.

The rehearsal freezes a trusted train's exact candidate cohort and hydrated input artifact
IDs/digests, then replaces selected components with existing stable Central versions.
Here Runtime `26.11.1-main.67792dc6bd83` supplies the release resolver; Expansions stays
on `26.9.4`. All other candidates and source harness SHAs remain frozen. The source run
establishes input provenance only; its Search-only result is not full-train coverage.

The coordinator verifies source workflow/job provenance, artifact checksums, component
ownership, stable release tags and selected Maven files. Hydration is restored once and
shared with credential-free product shards. Expired or mismatching source artifacts fail
closed: prepare a new manifest from a fresh successful materialization rather than changing
the meaning of this one. No mutable refs or SNAPSHOTs are resolved during execution.

`tpf-full-train-resolved-set` records `rehearsal-selection.json`, including the source
artifact identities, exact mixed selection and BOM/policy hashes. This is rehearsal
evidence, **not** `release-selection.json`: it cannot approve Central publication.
The run never advances the baseline or invokes a publisher. Stable release preparation
still requires stable-only pins and its own successful selected-release Full Train.

Merging coordinator changes normally triggers Central snapshot publication. For a
no-publication rehearsal setup, validate the exact PR head first and put `[skip ci]` in
the merge commit message to suppress that push event. Then dispatch Full Train explicitly;
do not disable publishers globally. Verify that no snapshot run was created for the merge.
