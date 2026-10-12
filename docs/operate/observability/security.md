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

Keep attributes and log fields minimal. Prefer identifiers to payloads.

## Redaction

If payload content must be logged, redact sensitive fields at the logger or mapper level.

## Owned payload HTTP boundaries

Generated payload routes require an authenticated principal and one application or host
`PayloadBoundaryAuthorizer`. Treat the tenant and scope headers as untrusted claims; the
authoriser must verify both against the principal and return the allowed owner. Downloads
compare that owner with provider-signed reference metadata before opening content. Rotate
`TPF_OBJECT_REFERENCE_HMAC_KEY` (or `tpf.object.reference.hmac-key`) with care: references
issued under an older key fail closed once that key is removed. All replicas that must read
the same references need the same Base64-encoded secret of at least 32 bytes. A missing key
uses an instance-local key, suitable only when references need not outlive that instance.
