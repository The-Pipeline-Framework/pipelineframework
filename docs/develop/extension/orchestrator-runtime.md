# Orchestrator Runtime Extensions

The orchestrator runtime is produced during the build by the pipeline code generation phases, and you can extend the generated runtime by wrapping the generated entrypoints or by replacing the application wiring with your own host classes. Keep durable custom logic in your own classes rather than editing regenerated files directly.

## Common Extensions

1. Custom input provisioning (CLI, files, queues, or APIs)
2. Additional logging and metrics around pipeline execution
3. Retry and backoff logic tailored to your domain

## Custom Orchestrator (REST)

You can skip `OrchestratorApplication` entirely and build a custom orchestrator that drives the pipeline via REST.
See `examples/csv-payments/ui-dashboard/src/services/optimizedRestOrchestrationService.js` for a real example.

### Recommended flow

1. Initialise the generated client or REST resource wrapper.
2. Authenticate the caller and derive the correlation or idempotency identifiers you need.
3. Start the pipeline with the generated `run`, `runAsync`, or equivalent entrypoint.
4. Expose status and result endpoints if callers need polling.
5. Map transport and domain failures to stable API responses.
6. Add logging, metrics, and retry/backoff only at the orchestration boundary.

### Example flow

```java
@Path("/orders")
@ApplicationScoped
public class OrderOrchestratorResource {
    @Inject
    CheckoutOrchestratorClient client;

    @POST
    public Uni<Response> createOrder(CreateOrderRequest request) {
        return client.runAsync(request)
            .map(run -> Response.accepted(Map.of("runId", run.runId())).build());
    }

    @GET
    @Path("/{runId}")
    public Uni<Response> status(@PathParam("runId") String runId) {
        return client.status(runId)
            .map(status -> Response.ok(status).build());
    }
}
```

### Common pitfalls and best practices

- Use idempotency keys for externally retried requests.
- Keep retry/backoff at the boundary so downstream steps keep their normal semantics.
- Emit structured logs and metrics for start, completion, and failure transitions.
- Avoid editing generated files directly unless you are prepared to re-apply the change after regeneration.
