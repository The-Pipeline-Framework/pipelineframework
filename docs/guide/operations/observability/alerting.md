# Alerting

Alerts should be actionable, low-noise, and tied to user impact.

## Principles

1. Alert on symptoms, not every error
2. Use severity levels consistently
3. Include context in the alert payload
4. Separate SLO alerts from operational alerts

## Dashboards

Pair alerts with dashboards that show step latency, throughput, and error rates.

## Common Alerts

1. Sustained step error rate above threshold
2. Step latency above SLO
3. Dead-letter queue growth
4. Orchestrator runtime failure or restart loops

## Practical Defaults

Start with:

1. Error rate > 2% for 5 minutes (warning)
2. p95 latency > 2x baseline for 10 minutes (warning)
3. DLQ > 0 with sustained growth (critical)
