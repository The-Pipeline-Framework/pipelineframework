# Cache Key Strategy

This page focuses on how to design cache keys for the Search example.

## Recommended keys per step

| Step | Key strategy | Reasoning |
| --- | --- | --- |
| Crawl | `docId` | Deduplicate ingestion for the same document. |
| Parse | `docId` | Parsing should be deterministic for a raw document. |
| Tokenize | `docId` + `tokenizerVersion` | Tokenization changes frequently. |
| Index | `docId` + `indexSchemaVersion` | Schema changes require isolation. |

If you use a global `x-pipeline-version`, you can avoid embedding per-step versions in the key. If you do embed per-step versions, keep them aligned with the pipeline version to avoid confusion.

## Bad keys

- `timestamp`
- random UUIDs
- transport headers or request IDs

## Good key patterns

- `docId`
- `docId:version`
- `customerId:invoiceId`

## Key stability checklist

- Does the key remain stable for the lifetime of the record?
- Does the key change only when the business meaning changes?
- Can you reconstruct it from the domain object alone?
