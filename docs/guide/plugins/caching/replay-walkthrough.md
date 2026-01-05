# Search Replay Walkthrough

This walkthrough uses the Search pipeline to show replay and versioned caching end to end.

## Pipeline shape

```mermaid
flowchart LR
  A[Crawl] --> B[Parse] --> C[Tokenize] --> D[Index]
```

The expensive stage is Crawl. We want to re-index with a new tokenizer without re-crawling.

## Step 1: Choose cache keys

Each output type implements `CacheKey` using a stable `docId`:

```java
public class ParsedDocument implements CacheKey {
    @Override
    public String cacheKey() {
        return docId.toString();
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
v1:{docId}
```

## Step 3: Replay from Parse with new tokenizer (v2)

Change the tokenizer logic and replay from Parse onward:

```
x-pipeline-version: v2
x-pipeline-cache-policy: return-cached
x-pipeline-replay: from-parse
```

Now:
- Parse cache lookup hits `v1:{docId}` (or stored outputs).
- Tokenize runs with new logic.
- Index runs with new logic.
- Outputs are cached under `v2:{docId}`.

Caching happens in the orchestrator client step before the remote call, so the step services remain unchanged.

## Replay flow diagram

```mermaid
sequenceDiagram
  participant Client
  participant Orchestrator
  participant Cache
  participant Tokenize
  participant Index

  Client->>Orchestrator: replay from Parse (version v2)
  Orchestrator->>Cache: lookup v1:docId (Parse output)
  Cache-->>Orchestrator: HIT (ParsedDocument)
  Orchestrator->>Tokenize: process(ParsedDocument)
  Tokenize-->>Orchestrator: TokenBatch
  Orchestrator->>Cache: store v2:docId (TokenBatch)
  Orchestrator->>Index: process(TokenBatch)
  Index-->>Orchestrator: IndexAck
  Orchestrator->>Cache: store v2:docId (IndexAck)
```

## Header propagation diagram

```mermaid
sequenceDiagram
  participant Client
  participant Orchestrator
  participant StepA
  participant StepB

  Client->>Orchestrator: x-pipeline-version=v2<br/>x-pipeline-cache-policy=return-cached
  Orchestrator->>StepA: propagate headers
  StepA-->>Orchestrator: response
  Orchestrator->>StepB: propagate headers
  StepB-->>Orchestrator: response
```

## Outcome

- Old outputs remain under `v1`.
- New outputs land under `v2`.
- You can compare Index outputs across versions without re-crawling.
