---
search: false
---

# Logging

Use structured logging for consistent, searchable diagnostics across steps.

## MDC Context

TPF does not set MDC values automatically. MDC is `ThreadLocal`, so it does not automatically flow to new threads, `CompletableFuture` callbacks, or reactive pipelines. In Quarkus, prefer SmallRye Context Propagation for thread hops, and use explicit correlation IDs across process boundaries.

```java
try {
    MDC.put("pipelineId", pipelineId);
    MDC.put("stepName", stepName);
    processRecord(record);
} finally {
    MDC.remove("pipelineId");
    MDC.remove("stepName");
}
```

For `CompletableFuture`, capture and restore MDC around the callback. For Mutiny or Reactor pipelines, copy the MDC values into the reactive context or use your team's context-propagation bridge so the correlation values survive asynchronous execution.

## Trace Correlation

When OpenTelemetry is enabled, include trace/span IDs in both the log formatter and programmatic logs.

```properties
quarkus.log.console.format=%d{yyyy-MM-dd HH:mm:ss} %-5p traceId=%X{traceId} spanId=%X{spanId} [%c] (%t) %s%e%n
```

```java
SpanContext spanContext = Span.current().getSpanContext();
MDC.put("traceId", spanContext.getTraceId());
MDC.put("spanId", spanContext.getSpanId());
LOG.infof("Processing payment for order %s", orderId);
```

## JSON Logging

Prefer JSON logs in production to integrate with log aggregation.

```properties
quarkus.log.console.json=true
quarkus.log.console.json.pretty-print=false
```

## Log Levels

1. **DEBUG**: Development diagnostics
2. **INFO**: Business events and step completion
3. **WARN**: Recoverable issues, including retries
4. **ERROR**: Failures requiring intervention

## Logging Standards

1. Avoid logging full payloads.
2. Mask secrets and PII. Use a shared helper such as `maskPII()` instead of ad-hoc masking. Examples:
   `maskEmail("user@domain.com") -> "u***@domain.com"`
   `maskCreditCard("4111111111111111") -> "************1111"`
   `maskBearerToken("Bearer abc.def") -> "Bearer ***"`
3. Keep messages consistent across steps.
4. Avoid per-item logs in high-cardinality flows.

## Performance and sampling

Synchronous logging in hot paths adds latency and backpressure. Prefer async appenders or non-blocking writers for high-throughput flows, and sample aggressively when a step emits many near-identical log events.

- Use deterministic sampling per session when you need one complete narrative per request class.
- Use probabilistic sampling per item for firehose-style traffic.
- Rate-limit repeated failures and keep full payload dumps in offline stores rather than the hot log path.

Recommended starting defaults:

- sample 1 in 1000 successful per-item logs for very hot flows
- keep full WARN and ERROR logs
- rate-limit repeated identical errors to one log every 30 seconds
