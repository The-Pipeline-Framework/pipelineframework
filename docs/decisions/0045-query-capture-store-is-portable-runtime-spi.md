---
title: Query capture storage is portable runtime SPI
status: accepted
---

# ADR-0045: Query capture storage is portable runtime SPI

## Context

Query replay is a durable runtime boundary used by runtime hosts and connector providers. `QueryCaptureStore` and the unary and streaming values it exchanges lived in the Quarkus-backed runtime artifact even though their contracts depend only on JDK types and framework-neutral connector observations.

`QueryCaptureRecord` also carried a Jackson creator for an obsolete direct-record JSON shape. The production durable store does not persist that shape: `QueryCaptureEventCodec` owns the versioned unary and streaming event representation, validation, and legacy event decoding. Keeping the annotation on the record would make a serialization library part of the provider SPI and obscure the actual durable protocol owner.

## Decision

`pipelineframework-runtime-spi` owns `QueryCaptureStore`, `QueryCaptureRecord`, Query outcome status and failure codes, and the streaming capture request, open result, item, and writer contracts.

The SPI types retain their packages, store method signatures, constructor behavior, and query observation semantics. The unused Jackson `QueryCaptureRecord.fromJson` creator and its direct-record JSON tests are removed. The runtime implementation continues to own `QueryCaptureEventCodec`, including the versioned durable event format and compatibility decoding exercised by `QueryCaptureEventCodecTest`.

In-memory and Dynamo stores, configuration mapping, provider selection, payload codecs, telemetry, and Quarkus/CDI wiring remain in `pipelineframework`.

## Rationale

Store implementors need the Java contract and semantic capture values, not a Quarkus runtime implementation or a particular object mapper. Durable compatibility belongs to the codec that actually writes persisted events. This keeps runtime-spi free of Jackson while allowing customer runtimes and separately deployed infrastructure to implement or consume the same released Query capture contract.

## Consequences

- Query capture providers compile against runtime-spi without depending on the Quarkus runtime artifact.
- Source and binary compatibility apply to the store and value contracts; replay and serialized compatibility apply to the versioned runtime event codec.
- Direct serialization of `QueryCaptureRecord` through a generic Jackson mapper is not a TPF durable protocol.
- Runtime tests continue to prove unary and streaming replay, observation preservation, legacy event decoding, Dynamo persistence, and provider selection.
