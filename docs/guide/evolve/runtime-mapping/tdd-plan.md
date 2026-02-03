# TDD Plan

This plan treats runtime mapping as a compiler feature with deterministic inputs and outputs. Start with small tests and grow coverage alongside implementation.

## Unit tests (parser and resolver)

- Parse minimal file with `version: 1` and defaults.
- Parse `layout: monolith` and enforce implied single runtime/module.
- Resolve step placement with explicit mappings.
- Resolve step placement via defaults in `validation: auto`.
- Resolve synthetic placement using canonical ids and suffixes.
- Reject ambiguous synthetic ids when multiple instances exist.
- Reject unknown steps/synthetics/modules/runtimes.

## Integration tests (annotation processor)

- Per-module AP uses global mapping and filters to current module.
- Cross-module client wiring uses the pipeline transport.
- In-module wiring remains in-process.
- `layout: monolith` collapses to single module behavior.

## Regression tests

- No mapping file present: generated outputs match current scaffold.
- Partial mapping in `validation: auto` does not change unmapped behavior.

## Suggested naming for tests

- `RuntimeMappingParserTest`
- `RuntimeMappingResolverTest`
- `RuntimeMappingValidationTest`
- `RuntimeMappingApIntegrationTest`
