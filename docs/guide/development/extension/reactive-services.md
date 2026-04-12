# Reactive Service Extensions

Reactive service interfaces define a `process()` method. You can wrap or adapt implementations of the `process()` method without changing pipeline contracts.

## Typical Extensions

1. Add Micrometer timers or counters around `process()`
2. Validate inputs before processing
3. Map errors to domain-specific failures
4. Apply rate limiting or circuit breaking

### 1. Metrics around `process()`

Use timers and counters when you need latency and failure visibility for hot paths.

```java
@ApplicationScoped
public class MeteredPaymentService implements ReactiveService<PaymentRecord, PaymentStatus> {
    @Inject
    ProcessPaymentService delegate;
    @Inject
    MeterRegistry registry;

    @Override
    public Uni<PaymentStatus> process(PaymentRecord input) {
        Timer.Sample sample = Timer.start(registry);
        return delegate.process(input)
            .invoke(() -> sample.stop(registry.timer("tpf.step.process-payment")))
            .onFailure().invoke(error -> registry.counter("tpf.step.process-payment.failures").increment());
    }
}
```

### 2. Validate before `process()`

Validate eagerly when you want a clear domain failure before downstream transport or persistence work starts.

```java
@Override
public Uni<PaymentStatus> process(PaymentRecord input) {
    if (input.amount() <= 0) {
        return Uni.createFrom().failure(new PaymentValidationException("amount must be positive"));
    }
    return delegate.process(input);
}
```

### 3. Map exceptions to domain failures

Use explicit exception mapping when fault reporting must stay within the pipeline's business vocabulary.

```java
@Override
public Uni<PaymentStatus> process(PaymentRecord input) {
    return delegate.process(input)
        .onFailure(WebApplicationException.class)
        .transform(error -> new PaymentProviderUnavailable("provider temporarily unavailable", error));
}
```

### 4. Rate limiting or circuit breaking

Add rate limiting or circuit breaking when you need throttling or fault isolation around a fragile dependency.

```java
@Override
@CircuitBreaker(requestVolumeThreshold = 10, failureRatio = 0.5)
@Timeout(500)
public Uni<PaymentStatus> process(PaymentRecord input) {
    return delegate.process(input);
}
```

Micrometer is documented at [micrometer.io](https://micrometer.io/), and SmallRye Fault Tolerance provides the Quarkus circuit-breaker annotations documented at [quarkus.io/guides/smallrye-fault-tolerance](https://quarkus.io/guides/smallrye-fault-tolerance).
