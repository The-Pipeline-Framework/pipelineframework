# Search Replay Walkthrough

This walkthrough uses the Search pipeline to show replay and versioned caching end to end.

## Pipeline shape

```mermaid
flowchart LR
  A[Crawl] --> B[Parse] --> C[Tokenize] --> D[Index]
```

The expensive stage is Crawl. We want to re-index with a new tokenizer without re-crawling.

## Search use cases

- **Normal production run**: `x-pipeline-cache-policy: prefer-cache` keeps throughput high and reuses stable outputs.
- **Deterministic replay**: `x-pipeline-cache-policy: require-cache` ensures only cached entries are used.
- **Forced rebuild**: `x-pipeline-cache-policy: cache-only` overwrites cached outputs without reads.
- **Debug or verification**: `x-pipeline-cache-policy: bypass-cache` runs the pipeline without cache I/O.

Invalidation steps are reserved for targeted corrections (bug fixes, schema changes) and only run when `x-pipeline-replay: true` is provided.

## Step 1: Choose cache keys

Define cache key strategies that emit stable keys for each output type:

```java
import java.util.Optional;

public class ParsedDocumentKeyStrategy implements CacheKeyStrategy {
    @Override
    public Optional<String> resolveKey(Object item, PipelineContext context) {
        if (!(item instanceof ParsedDocument doc) || doc.docId == null) {
            return Optional.empty();
        }
        return Optional.of(doc.getClass().getName() + ":" + doc.docId);
    }
}
```

## Step 2: Run baseline (v1)

```
x-pipeline-version: v1
x-pipeline-cache-policy: cache-only
```

This caches every stage output under:

```
v1:{Type}:{docId}
```

## Step 3: Recompute downstream while reusing cached upstream outputs

Change the tokenizer logic and reuse cached outputs from earlier steps by keeping the same version tag:

```
x-pipeline-version: v1
x-pipeline-cache-policy: prefer-cache
```

Now:
- Parse cache lookup hits `v1:{Type}:{docId}`.
- Tokenize runs with new logic.
- Index runs with new logic.
- Outputs are cached under `v1:{Type}:{docId}`.

`x-pipeline-replay` is currently propagated as a header only; it is not interpreted by the runtime.

Caching happens in the cache plugin side-effect steps, so the step services remain unchanged.

## Step 4: Fork a new version

If you want a clean namespace for a new run, bump the version tag:

```
x-pipeline-version: v2
x-pipeline-cache-policy: cache-only
```

## Manual verification (curl + Redis)

Warm the cache:

```bash
curl -k -X POST https://localhost:8443/pipeline/run \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -H 'x-pipeline-version: v4' \
  -H 'x-pipeline-cache-policy: prefer-cache' \
  -d '{"docId":"00000000-0000-0000-0000-000000000001","sourceUrl":"https://example.com"}'
```

Inspect Redis keys:

```bash
redis-cli --scan --pattern "pipeline-cache:v4:*"
```

Expected key shapes (Search example):

- `v4:org.pipelineframework.search.common.domain.RawDocument:https://example.com|method=GET|accept=text/html`
- `v4:org.pipelineframework.search.common.domain.ParsedDocument:<rawContentHash>`
- `v4:org.pipelineframework.search.common.domain.TokenBatch:<contentHash>:model=v1`
- `v4:org.pipelineframework.search.common.domain.IndexAck:<tokensHash>:schema=v1`

Force deterministic replay (should fail on cold cache, succeed on warm cache):

```bash
curl -k -X POST https://localhost:8443/pipeline/run \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -H 'x-pipeline-version: v4' \
  -H 'x-pipeline-cache-policy: require-cache' \
  -d '{"docId":"00000000-0000-0000-0000-000000000001","sourceUrl":"https://example.com"}'
```

Use invalidation only when replaying:

```bash
curl -k -X POST https://localhost:8443/pipeline/run \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -H 'x-pipeline-version: v4' \
  -H 'x-pipeline-cache-policy: prefer-cache' \
  -H 'x-pipeline-replay: true' \
  -d '{"docId":"00000000-0000-0000-0000-000000000001","sourceUrl":"https://example.com"}'
```

This intentionally misses old cache entries and recomputes the pipeline.

## Replay flow diagram

```mermaid
sequenceDiagram
  participant Client
  participant Orchestrator
  participant CachePlugin
  participant Tokenize
  participant Index

  Client->>Orchestrator: run with version v1
  Orchestrator->>CachePlugin: lookup v1:Type:docId (Parse output)
  CachePlugin-->>Orchestrator: HIT (ParsedDocument)
  Orchestrator->>Tokenize: process(ParsedDocument)
  Tokenize-->>Orchestrator: TokenBatch
  Orchestrator->>CachePlugin: store v1:Type:docId (TokenBatch)
  Orchestrator->>Index: process(TokenBatch)
  Index-->>Orchestrator: IndexAck
  Orchestrator->>CachePlugin: store v1:Type:docId (IndexAck)
```

## Header propagation diagram

```mermaid
sequenceDiagram
  participant Client
  participant Orchestrator
  participant StepA
  participant StepB

  Client->>Orchestrator: x-pipeline-version=v1<br/>x-pipeline-cache-policy=prefer-cache
  Orchestrator->>StepA: propagate headers
  StepA-->>Orchestrator: response
  Orchestrator->>StepB: propagate headers
  StepB-->>Orchestrator: response
```

## Outcome

- Old outputs remain under `v1`.
- New outputs land under `v2`.
- You can compare Index outputs across versions without re-crawling.
