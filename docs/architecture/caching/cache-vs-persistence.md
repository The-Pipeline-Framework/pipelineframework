# Cache vs Persistence

Persistence and caching solve different parts of the same application problem.

- Persistence keeps durable business state for audit, follow-on processing, and UI or API queries.
- Cache keeps expensive derived outputs close at hand so replay, recomputation, and fast reuse stay cheap.
- Together they give TPF a practical state-and-replay story without forcing teams to build separate storage and replay plumbing first.

If you want the value-led overview before the mechanics, start with [State, Replay, and Queryable Data](/value/state-replay-and-queryable-data).

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
- Use persistence when you need durable business state, replay support, traceability, audits, or UI/API query access later.
- For Search (the search indexing/reference example), use persistence for Crawl/Parse and cache for Tokenize/Index.

## Typical Combined Pattern

For many applications, the practical split looks like this:

1. Persist the business records or primary processing outputs you will need later.
2. Cache expensive downstream derived outputs that are useful to reuse or recompute selectively.
3. Replay from the durable records and use cache to avoid rerunning stable upstream work.

## Durable cache for read-first reuse

If you want the cache to act as a durable read-first layer (read-first, skip step on hit) without
building a dedicated indexing/read API, you can run the cache as a durable Redis store and rely on
the existing cache pre-read + skip behaviour in the runner.

Recommended setup:

- Use the Redis cache provider (not in-memory or Caffeine).
- Do not set TTLs (omit `pipeline.cache.ttl` and per-step TTLs) so entries persist.
- Configure Redis durability and `noeviction` in production.

Example application config:

```properties
pipeline.cache.provider=redis
pipeline.cache.policy=prefer-cache
# omit pipeline.cache.ttl for durable entries
```

Example Redis durability settings (illustrative):

`maxmemory-policy noeviction` is intentional here: it prevents Redis from evicting entries when memory is full, which protects durable-cache scenarios from silent data loss.

```properties
appendonly yes
appendfsync everysec
save 900 1
maxmemory-policy noeviction
```

This gives you durable-cache behaviour (durable cache + skip on hit) while keeping
persistence for audit history and traceability. It is still a cache: it does not provide a
query API over all persisted outputs.
