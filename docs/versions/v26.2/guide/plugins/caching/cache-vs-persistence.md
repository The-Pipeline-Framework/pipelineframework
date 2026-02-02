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

## Durable cache (materialisation-lite)

If you want the cache to act as a durable materialisation layer (read-first, skip step on hit) without
building a dedicated indexing/read API, you can run the cache as a durable Redis store and rely on
the existing cache pre-read + skip behavior in the runner.

Recommended setup:

- Use the Redis cache provider (not in-memory or Caffeine).
- Do not set TTLs (omit `pipeline.cache.ttl` and per-step TTLs) so entries persist.
- Configure Redis durability and no-eviction in production.

Example application config:

```
pipeline.cache.provider=redis
pipeline.cache.policy=prefer-cache
# omit pipeline.cache.ttl for durable entries
```

Example Redis durability settings (illustrative):

```
appendonly yes
appendfsync everysec
save 900 1
maxmemory-policy noeviction
```

This gives you "rematerialisation-lite" behavior (durable cache + skip on hit) while keeping
persistence for audit/lineage. It is still a cache: it does not provide a query/index API over
all persisted outputs.
