# REST Resource Extensions

Generated REST resources are standard classes. You can extend or wrap them when you need custom REST behaviour.

## Typical Extensions

1. Add custom endpoints alongside the generated `/process`
2. Override exception mapping
3. Inject additional services for cross-cutting concerns

### 1. Add a custom endpoint alongside `/process`

Use a thin wrapper resource when you want a bespoke route without replacing the generated processing entrypoint.

```java
@Path("/payments")
@ApplicationScoped
public class PaymentResourceExtension {
    @Inject
    ProcessPaymentResource generatedResource;

    @POST
    @Path("/process")
    public Uni<Response> process(PaymentRecordDto input) {
        return generatedResource.process(input);
    }

    @GET
    @Path("/health-summary")
    public Response healthSummary() {
        return Response.ok(Map.of("status", "ok")).build();
    }
}
```

### 2. Override exception mapping

Replace or extend the generated exception mapper when you need domain-specific HTTP status codes or payloads.

```java
@Provider
public class PaymentFailureMapper implements ExceptionMapper<PaymentValidationException> {
    @Override
    public Response toResponse(PaymentValidationException error) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity(Map.of("code", "payment-validation", "message", error.getMessage()))
            .build();
    }
}
```

### 3. Inject a cross-cutting service

Inject a dedicated service for audit, tracing, tenancy, or feature-flag decisions and keep the controller thin.

```java
@ApplicationScoped
public class RequestAuditService {
    public void record(String routeName, String correlationId) {
        // Send audit event or metric here.
    }
}

@Path("/payments")
@ApplicationScoped
public class PaymentResourceExtension {
    @Inject
    RequestAuditService auditService;

    @POST
    @Path("/preview")
    public Response preview(@HeaderParam("X-Correlation-ID") String correlationId) {
        auditService.record("payments-preview", correlationId);
        return Response.accepted().build();
    }
}
```

## Publish an application-owned OpenAPI contract

Generated REST resources expose typed step boundaries. When a frontend or external client should see only an
application-owned façade, add the SmallRye OpenAPI extension and activate TPF's opt-in contract filter:

```properties
mp.openapi.filter=org.pipelineframework.openapi.PublicApiContractFilter
pipeline.openapi.public-path-prefixes=/api/cases
```

The filter retains only the configured path roots and the transitive closure of schemas referenced by those
operations. Internal generated resources and unrelated runtime schemas therefore remain outside the published
contract. Multiple comma-separated roots are supported. A configured filter without a public root fails closed.

The application still owns its aggregate resources and their OpenAPI annotations. In particular, annotate sealed
or polymorphic application contracts with their concrete variants and discriminator mapping; the filter preserves
those reachable schemas but does not infer application-specific discriminator values.
