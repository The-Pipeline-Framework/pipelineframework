---
search: false
---

# Cache Key Strategy

This page focuses on how to design cache keys for the Search example.

## Recommended keys per step

| Step     | Key strategy                                  | Reasoning                                                     |
|----------|-----------------------------------------------|---------------------------------------------------------------|
| Crawl    | `sourceUrl` + `fetchOptions`                  | Fetching changes with URL and request options.                |
| Parse    | `rawContentHash`                              | Parsing should be deterministic for the raw bytes.            |
| Tokenize | `contentHash` + `modelVersion`                | Tokenization changes with the model version.                  |
| Index    | `tokenizedDocHash` + `schema/indexVersion`    | Indexing must isolate schema and tokenized output revisions.  |

If you use a global `x-pipeline-version`, you can avoid embedding per-step versions in the key. If you do embed per-step versions, keep them aligned with the pipeline version to avoid confusion.

In the Search example, these are implemented as cache key strategies in each service module so the key logic can track step-specific inputs (URL options, content hashes, and model/index versions).

For crawl keys, include only request options that materially change the fetched bytes (HTTP method, Accept/Accept-Language, auth scope, cookies, client hints). Normalize and sort option values so semantically identical requests produce the same key.

To avoid re-hashing in key generators, persist content hash fields (for example `rawContentHash`, `contentHash`, `tokensHash`) alongside the step outputs and reuse them downstream.

## Bad keys

- `timestamp`
- random UUIDs
- transport headers or request IDs

## Good key patterns

- `docId`
- `docId:version`
- `customerId:invoiceId`
- `contentHash:modelVersion`

## Key stability checklist

- Does the key remain stable for the lifetime of the record?
- Does the key change only when the business meaning changes?
- Can you reconstruct it from the domain object alone?
