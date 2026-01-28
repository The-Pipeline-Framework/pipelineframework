---
search: false
---

# Security Notes

Observability data can contain sensitive information. Treat it like production data.

1. Restrict access by role
2. Avoid logging secrets, credentials, or PII
3. Encrypt telemetry in transit
4. Align retention with compliance requirements

## Access Controls

Ensure metrics and trace backends are protected with authentication and network policies. Avoid exposing telemetry endpoints publicly.

When exporting to OTLP over HTTP, prefer TLS endpoints and authenticate with headers (for example
`quarkus.otel.exporter.otlp.headers=api-key=...`).

## Data Minimization

Keep attributes and log fields minimal. Prefer identifiers over payloads.

## Redaction

If payload content must be logged, redact sensitive fields at the logger or mapper level.
