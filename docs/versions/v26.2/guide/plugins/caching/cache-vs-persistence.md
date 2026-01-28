---
search: false
---

# Cache vs Persistence

Caching is for fast, short-lived decisions. Persistence is for durable replay and auditability. This table clarifies when each is the right tool.

| Capability             | Cache plugin             | Persistence plugin          |
|------------------------|--------------------------|-----------------------------|
| Re-entrancy protection | Strong                   | Strong                      |
| Replay from a step     | Good with versioned keys | Best (durable storage)      |
| Cost control           | TTL / eviction           | Storage grows unless pruned |
| Cross-service sharing  | Redis backend            | Native (DB-backed)          |
| Auditability           | Limited                  | Full (history preserved)    |
| Operational complexity | Low to medium            | Medium to high              |

## Guidance

- Use cache for re-entrancy, retries, and fast guards around expensive steps.
- Use persistence when you need full replayability, lineage, and audits.
- For Search, use persistence for Crawl/Parse and cache for Tokenize/Index.
