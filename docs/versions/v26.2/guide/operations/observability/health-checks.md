---
search: false
---

# Health Checks

Expose liveness and readiness so orchestration platforms can manage your services safely.

## Built-in Checks

Quarkus provides default endpoints that report service health and readiness.

Common endpoints:

1. `/q/health` (aggregate)
2. `/q/health/live` (liveness)
3. `/q/health/ready` (readiness)

## Pipeline Startup Checks

The orchestrator performs startup health checks for dependent services before running a pipeline.
These are controlled by `pipeline.health.startup-timeout` and will fail startup if required services
are unhealthy.

## Custom Checks

Add checks for dependencies such as databases or external APIs.

```java
@Readiness
@ApplicationScoped
public class PaymentProviderHealthCheck implements HealthCheck {
    @Override
    public HealthCheckResponse call() {
        return HealthCheckResponse.up("payment-provider");
    }
}
```

## Design Notes

1. Keep checks fast
2. Fail readiness when critical dependencies are down
3. Use graceful degradation when possible
4. Keep startup checks aligned with pipeline dependencies
