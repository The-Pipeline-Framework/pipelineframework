# Docs Snapshot Process

Docs snapshots preserve public documentation for released framework versions.

## Current Sections

Future snapshots should copy the current public sections:

- `value`
- `design`
- `develop`
- `deploy`
- `operate`
- `evolve`

Historical snapshots under `docs/versions/**` remain immutable after publication.

## Validation

Use the docs build and snapshot tests before release:

```bash
npm --prefix docs test
npm --prefix docs run build
```

When cutting a version snapshot, run the snapshot command for the target release and inspect rewritten links before committing.

## Route Rule

Current docs no longer use `/guide`. Version snapshots may still contain historical `/guide` routes. Current public docs should link to `/design`, `/develop`, `/deploy`, `/operate`, `/evolve`, and `/value`.

