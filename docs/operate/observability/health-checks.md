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

### Authenticated dependency probes

Registering a REST-client filter does not authenticate the separate HTTP health probe automatically.
An application may select a Quarkus `@Alternative` `HealthCheckService` bean with `@Priority` and override
`protected HttpRequest customizeHttpHealthRequest(String clientType, String clientName, HttpRequest request)`
to authenticate the probe. The default returns the request unchanged. Preserve its URI, GET method,
empty body, timeout and TLS verification. Reject unavailable credentials rather than retrying unsigned.
Requests with `Authorization`, `Cookie` or `X-API-Key` headers require HTTPS, including
a request returned by the customisation hook. The runtime rejects them before sending over HTTP.
If customisation throws, the probe fails without an unsigned retry. The normal health-status
check still requires HTTP 200; authentication must not turn HTTP 503 into
a healthy result or substitute liveness for readiness.

Strict `QUEUE_ASYNC` services must also configure their intended idempotency policy explicitly,
for example `pipeline.orchestrator.idempotency-policy=CLIENT_KEY_REQUIRED`. Authentication does
not replace that startup requirement. Deploy compatible runtime and application versions together;
the specialisation requires a runtime containing the customisation hook.

## Custom Checks

Add checks for dependencies such as databases or external APIs.

```java
@Readiness
@ApplicationScoped
public class PaymentProviderHealthCheck implements HealthCheck {
    @Inject
    PaymentProviderClient client;

    @Override
    public HealthCheckResponse call() {
        try {
            client.ping();
            return HealthCheckResponse.up("payment-provider");
        } catch (Exception error) {
            return HealthCheckResponse.down("payment-provider");
        }
    }
}
```

## Design Notes

1. Keep checks fast.
2. Fail readiness when critical dependencies are down.
3. Use graceful degradation when possible.
4. Keep startup checks aligned with pipeline dependencies.
