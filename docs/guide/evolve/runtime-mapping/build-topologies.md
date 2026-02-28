# Build Topologies

This page explains what runtime mapping means for app developers using TPF.

For hands-on app-developer usage in the Build section, see:

- [Runtime Layouts and Build Topologies](/guide/build/runtime-layouts/)
- [Maven Migration Playbook](/guide/build/runtime-layouts/maven-migration)
- [CSV Payments Monolith Walkthrough](/guide/build/runtime-layouts/csv-payments-monolith)

Typical onboarding is:

1. Design the pipeline in Web UI.
2. Download the scaffolded app.
3. Optionally add `pipeline.runtime.yaml` to control runtime placement.

## What runtime mapping controls

- Placement of steps and synthetics into modules/runtimes.
- Validation behavior (`auto` or `strict`).
- Generated client/server wiring aligned with mapping and pipeline transport.

## What runtime mapping does not change by itself

- Your physical Maven module structure.
- Your deploy artifact structure (for example, turning a modular scaffold into one monolith artifact automatically).

If your scaffold is modular, `layout: monolith` in mapping is a logical target. A real monolith runtime requires a build that produces a monolith artifact.

## Practical usage

- If you do nothing, scaffold defaults apply (backward compatible).
- If you add `pipeline.runtime.yaml`, start with `validation: auto`.
- Move to `validation: strict` once mappings are complete.

## Choosing a layout

- `modular`: best match for current scaffold defaults.
- `pipeline-runtime`: good when your build already supports grouping steps into fewer runtime units.
- `monolith`: use when your app build is monolith-oriented.

## Recommended developer workflow

1. Keep the generated scaffold running with defaults first.
2. Add runtime mapping gradually.
3. Validate runtime shape with topology-aware tests (not only functional E2E).
4. Keep one canonical build entrypoint per layout and require it to work from a clean local Maven repository state.
