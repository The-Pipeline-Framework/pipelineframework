# Search Replay Demo Runbook

Use this runbook for the "fast chunker, slow embedder" replay-viewer video.

## Story

A classic producer/consumer mismatch is that document chunking can emit work faster than an embedding model can consume it. A common answer is to add an ad hoc Redis queue or `PipeDoc` buffer between the chunker and embedder.

TPF's Search demo shows the framework-native answer instead:

1. `Tokenize Content` emits typed `TokenBatch` stream elements.
2. `Embed Content` consumes each batch one at a time and can be slowed with `search.embed.delay-ms`.
3. `Build Search Index Document` projects each `EmbeddedChunk` into a deterministic external write intent.
4. `Write Search Index Document` sends that intent through the command shell, recording the effect result.
5. `Summarize Index Writes` reduces recorded write results back into one document-level `IndexAck`.
6. Persistence/cache side effects, lineage, backpressure, command replay, and duplicate suppression are part of the runtime path rather than an external buffering convention.

## Capture

Build the modular replay images:

```bash
./examples/search/build-modular-replay-images.sh
```

Run the replay capture with a visible embed delay:

```bash
./mvnw -f examples/search/pom.xml -pl orchestrator-svc -am \
  -Dsearch.replay.synthetic-url-count=100 \
  -Dsearch.replay.embed.delay-ms=25 \
  -Dsearch.replay.embed.vector-version=demo-v1 \
  -Dtest=SearchReplayEndToEndIT \
  -Dsurefire.failIfNoSpecifiedTests=false \
  test
```

Load either generated replay through the replay viewer's Custom replay option:

- `examples/search/orchestrator-svc/target/test-e2e/replay/search-warm-cache-replay.json`
- `examples/search/orchestrator-svc/target/test-e2e/replay/search-cache-hit-replay.json`

## Demo Beats

- Start with the warm-cache dataset to show the full typed stream path.
- Pause around `Tokenize Content -> Embed Content` to call out one-to-many chunk emission followed by a slower one-to-one consumer.
- Let playback continue through `Build Search Index Document -> Write Search Index Document -> Summarize Index Writes` to show the side-effecting sink separated from the pure document-level aggregation.
- Switch to the cache-hit dataset to show replayable runtime behavior and reused upstream outputs without changing the pipeline code.

## Knobs

- `search.replay.synthetic-url-count`: number of deterministic crawl requests captured by the replay E2E.
- `search.replay.embed.delay-ms`: per-chunk embed delay used by the modular replay containers.
- `search.replay.embed.vector-version`: deterministic vector-version salt used in `EmbeddedChunk.vectorHash`.

Use higher `search.replay.embed.delay-ms` values when recording close-ups. Avoid presenting the result as a benchmark unless measured separately.

## Adjacent Capabilities

Keep this video focused on typed stream fan-out/fan-in and replay. For deferred external completion, use [Await Boundaries](../../docs/guide/development/orchestrator-runtime/await.md) and the CSV Payments replay. For runtime failure triage, use [Error Handling](../../docs/guide/operations/error-handling.md) and the item reject sink docs.
