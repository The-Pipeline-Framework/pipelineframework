---
search: false
---

# Logging

Use structured logging for consistent, searchable diagnostics across steps.

## MDC Context

Attach pipeline identifiers to MDC so logs can be correlated across services:

```java
MDC.put("pipelineId", pipelineId);
MDC.put("stepName", stepName);
```

## Trace Correlation

When OpenTelemetry is enabled, prefer including trace/span IDs in log format so APM and logs
link correctly.

## JSON Logging

Prefer JSON logs in production to integrate with log aggregation.

```properties
quarkus.log.console.json=true
quarkus.log.console.json.pretty-print=false
```

## Log Levels

1. **DEBUG**: Development diagnostics
2. **INFO**: Business events and step completion
3. **WARN**: Recoverable issues
4. **ERROR**: Failures and retries

## Logging Standards

1. Avoid logging full payloads
2. Mask secrets and PII
3. Keep messages consistent across steps
4. Avoid per-item logs in high-cardinality flows
