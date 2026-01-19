# Concurrency, Error Handling, and DLQ

This guide explains the advanced features of The Pipeline Framework including concurrency control, error handling mechanisms, dead letter queue (DLQ) support, and immutable architecture patterns.

## Concurrency Control

The Pipeline Framework provides sophisticated concurrency controls to optimize performance while maintaining resource efficiency.

### Reactive Processing

The framework leverages Mutiny's reactive processing model for high-throughput, efficient concurrency:

```java
@PipelineStep(
    inputType = PaymentRecord.class,
    outputType = PaymentStatus.class,
    stepType = StepOneToOne.class,
    inboundMapper = PaymentRecordInboundMapper.class,
    outboundMapper = PaymentStatusOutboundMapper.class,
    runOnVirtualThreads = true  // Run on virtual threads for I/O-bound operations
)
@ApplicationScoped
public class ProcessPaymentService implements ReactiveService<PaymentRecord, PaymentStatus> {

    @Override
    public Uni<PaymentStatus> process(PaymentRecord paymentRecord) {
        // This will run reactively using Mutiny's event/worker thread model
        return processPaymentWithExternalService(paymentRecord);
    }

    private Uni<PaymentStatus> processPaymentWithExternalService(PaymentRecord record) {
        // Reactive processing is ideal for I/O-bound operations like HTTP calls
        return webClient.post("/process-payment")
            .sendJson(record)
            .onItem().transform(response -> {
                // Processing response reactively
                return convertResponseToPaymentStatus(response, record);
            });
    }
}
```

### Concurrency Limits

Control the maximum number of concurrent operations through backpressure and buffer settings:

```properties
# application.properties
pipeline.defaults.backpressure-buffer-capacity=1024
pipeline.defaults.backpressure-strategy=BUFFER
```

### Backpressure Handling

The framework automatically handles backpressure through reactive streams with configurable strategies. You can override backpressure per step at runtime:

```properties
# application.properties
pipeline.step."com.example.ProcessPaymentService".backpressure-strategy=DROP
pipeline.step."com.example.ProcessPaymentService".backpressure-buffer-capacity=256
```

The available overflow strategies are:

- **BUFFER** (default): Buffers items when the downstream consumer cannot keep up (using `onOverflow().buffer(capacity)`)
- **DROP**: Drops items when the downstream consumer cannot keep up (using `onOverflow().drop()`)

Programmatic configuration is also possible:

```java
@Override
public Uni<PaymentStatus> process(PaymentRecord paymentRecord) {
    return externalService.process(paymentRecord)
        .onFailure().retry().withBackOff(Duration.ofMillis(100), Duration.ofSeconds(5))
        .atMost(3);
}

// The framework automatically applies the configured backpressure strategy
// based on runtime configuration.
```

### Relationship Between Concurrency and Buffer

Concurrency and buffer settings work together to control flow:

- **Concurrency** limits the number of simultaneous operations that can be processed
- **Buffer** controls how many items are queued when downstream operations are slower than upstream production

Best practices:
- Set concurrency based on system resources and external service limits
- Set buffer capacity based on expected load spikes and acceptable memory usage
- Monitor system performance to balance between throughput and resource utilization
- Consider using virtual threads when dealing with I/O-bound operations to improve concurrency efficiency

## Error Handling

The Pipeline Framework provides comprehensive error handling with multiple recovery strategies.

### Retry Mechanisms

Built-in retry with exponential backoff:

```properties
# application.properties
pipeline.defaults.retry-limit=5
pipeline.defaults.retry-wait-ms=1000
pipeline.defaults.max-backoff=30000
pipeline.defaults.jitter=true
```

Programmatic retry configuration:

```java
@Override
public Uni<PaymentStatus> process(PaymentRecord paymentRecord) {
    return processPayment(paymentRecord)
        .onFailure().retry()
        .withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(30))
        .withJitter(0.5)  // 50% jitter
        .atMost(5);       // Maximum 5 retries
}

private Uni<PaymentStatus> processPayment(PaymentRecord record) {
    return externalPaymentService.process(record)
        .onFailure(PaymentServiceException.class)
        .retry().withBackOff(Duration.ofMillis(500))
        .atMost(3);
}
```

### Retry Filtering

The framework skips retries for:

- `NullPointerException`
- `NonRetryableException` (or any failure with one in its cause chain)
- `CachePolicyViolation` from the runner when `pipeline.cache.policy=require-cache` and the cache plugin reports a miss

Use `NonRetryableException` to mark failures that should fail fast:

```java
throw new NonRetryableException("Invalid payload");
```

The persistence plugin applies this automatically:

- Duplicate key errors (SQLState `23505`) follow `pipeline.persistence.duplicate-key` (`fail`, `ignore`, `upsert`).
- Non-transient database errors are wrapped in `NonRetryableException`.
- Transient connectivity errors are retried according to the step configuration.

### Circuit Breaker Pattern

Implement circuit breaker for external service calls:

```java
@Inject
CircuitBreaker circuitBreaker;

@Override
public Uni<PaymentStatus> process(PaymentRecord paymentRecord) {
    return circuitBreaker.execute(
        () -> processPayment(paymentRecord),
        // Fallback method when circuit is open
        error -> handleCircuitBreakerFallback(paymentRecord, error)
    );
}

private Uni<PaymentStatus> handleCircuitBreakerFallback(PaymentRecord record, Throwable error) {
    LOG.warn("Circuit breaker triggered for payment: {}", record.getId(), error);
    
    // Return a default/fallback response
    return Uni.createFrom().item(
        PaymentStatus.builder()
            .paymentRecord(record)
            .status("CIRCUIT_BREAKER")
            .message("Service temporarily unavailable")
            .build()
    );
}
```

### Error Classification

Differentiate between recoverable and unrecoverable errors:

```java
@Override
public Uni<PaymentStatus> process(PaymentRecord paymentRecord) {
    return processPayment(paymentRecord)
        .onFailure().recoverWithUni(error -> {
            if (isRecoverableError(error)) {
                return handleRecoverableError(paymentRecord, error);
            } else {
                return handleUnrecoverableError(paymentRecord, error);
            }
        });
}

private boolean isRecoverableError(Throwable error) {
    return error instanceof TimeoutException || 
           error instanceof NetworkException ||
           error instanceof ServiceUnavailableException;
}

private Uni<PaymentStatus> handleRecoverableError(PaymentRecord record, Throwable error) {
    LOG.warn("Recoverable error processing payment: {}", record.getId(), error);
    
    // Retry or send to DLQ
    if (shouldRetry(record)) {
        return Uni.createFrom().failure(error); // Will trigger retry
    } else {
        return deadLetter(record, error); // Send to DLQ
    }
}

private Uni<PaymentStatus> handleUnrecoverableError(PaymentRecord record, Throwable error) {
    LOG.error("Unrecoverable error processing payment: {}", record.getId(), error);
    
    // Send directly to DLQ
    return deadLetter(record, error);
}
```

## Dead Letter Queue (DLQ)

The DLQ mechanism captures failed items for later inspection and reprocessing.

### DLQ Configuration

DLQ/recovery is configured at runtime, not via @PipelineStep. Use StepConfig or application.properties (see Configuration Reference and Runtime Configuration below) for the exact settings.

### Persistence Dependencies

If you plan to persist DLQ items, include the necessary persistence dependencies in your `pom.xml`:

```xml
<dependency>
    <groupId>io.quarkus</groupId>
    <artifactId>quarkus-reactive-pg-client</artifactId>
</dependency>
<dependency>
    <groupId>io.quarkus</groupId>
    <artifactId>quarkus-hibernate-reactive-panache</artifactId>
</dependency>
```

If you do not need persistence functionality, you can omit these dependencies.

### Custom DLQ Implementation

Implement custom DLQ handling:

```java
@Override
public Uni<PaymentStatus> deadLetter(PaymentRecord paymentRecord, Throwable error) {
    LOG.warn("Sending failed payment record to dead letter queue: {}", paymentRecord.getId(), error);
    
    // Send to DLQ (database, message queue, file system, etc.)
    return persistToDeadLetterQueue(paymentRecord, error)
        .onItem().transform(v -> createDlqStatus(paymentRecord))
        .onFailure().recoverWithUni(failure -> {
            LOG.error("Failed to persist to DLQ", failure);
            // Fallback to error status
            return Uni.createFrom().item(createErrorStatus(paymentRecord, failure));
        });
}

private Uni<Void> persistToDeadLetterQueue(PaymentRecord record, Throwable error) {
    DeadLetterEntry entry = DeadLetterEntry.builder()
        .itemId(record.getId())
        .itemType("PaymentRecord")
        .errorMessage(error.getMessage())
        .errorStackTrace(getStackTraceAsString(error))
        .timestamp(Instant.now())
        .build();
    
    // Persist to database or message queue
    return deadLetterRepository.save(entry);
}

private PaymentStatus createDlqStatus(PaymentRecord record) {
    return PaymentStatus.builder()
        .paymentRecord(record)
        .status("DLQ")
        .message("Moved to dead letter queue after failed processing")
        .build();
}

private PaymentStatus createErrorStatus(PaymentRecord record, Throwable error) {
    return PaymentStatus.builder()
        .paymentRecord(record)
        .status("ERROR")
        .message("Failed to process and unable to persist to DLQ: " + error.getMessage())
        .build();
}
```

### DLQ Monitoring

Monitor DLQ for failed items:

```java
@ApplicationScoped
public class DeadLetterQueueMonitor {
    
    @Inject
    DeadLetterRepository deadLetterRepository;
    
    @Scheduled(every = "5m")  // Check every 5 minutes
    public void checkDeadLetterQueue() {
        deadLetterRepository.findUnprocessedEntries()
            .onItem().transformToMulti(entries -> Multi.createFrom().iterable(entries))
            .subscribe().with(this::processDeadLetterEntry);
    }
    
    private void processDeadLetterEntry(DeadLetterEntry entry) {
        LOG.info("Processing DLQ entry: {}", entry.getItemId());
        
        // Attempt reprocessing or notify administrators
        attemptReprocessing(entry)
            .onItem().invoke(success -> {
                if (success) {
                    // Mark as processed
                    deadLetterRepository.markAsProcessed(entry.getId());
                }
            })
            .subscribe().with(
                _ -> LOG.info("DLQ entry processed: {}", entry.getItemId()),
                error -> LOG.error("Error processing DLQ entry: {}", entry.getItemId(), error)
            );
    }
    
    private Uni<Boolean> attemptReprocessing(DeadLetterEntry entry) {
        // Logic to reprocess the failed item
        return Uni.createFrom().item(true);
    }
}
```

## Advanced Error Handling Patterns

### Error Context Preservation

Preserve error context for better debugging:

```java
@Override
public Uni<PaymentStatus> process(PaymentRecord paymentRecord) {
    MDC.put("paymentId", paymentRecord.getId().toString());
    MDC.put("customerId", paymentRecord.getCustomerId());
    
    try {
        return processPayment(paymentRecord)
            .onItem().invoke(result -> {
                LOG.info("Payment processed successfully: {}", result.getStatus());
                MDC.clear();
            })
            .onFailure().invoke(error -> {
                LOG.error("Payment processing failed", error);
                MDC.clear();
            });
    } finally {
        MDC.clear();
    }
}
```

### Error Enrichment

Add contextual information to errors:

```java
private Uni<PaymentStatus> processPayment(PaymentRecord record) {
    return externalService.process(record)
        .onFailure().transform(error -> {
            // Enrich error with contextual information
            return new PaymentProcessingException(
                "Failed to process payment for customer: " + record.getCustomerId(),
                error,
                record.getId(),
                record.getCustomerId()
            );
        });
}
```

### Graceful Degradation

Implement graceful degradation for partial failures:

```java
@Override
public Uni<PaymentStatus> process(PaymentRecord paymentRecord) {
    return processPrimaryService(paymentRecord)
        .onFailure().recoverWithUni(error -> {
            LOG.warn("Primary service failed, falling back to secondary", error);
            return processSecondaryService(paymentRecord);
        })
        .onFailure().recoverWithUni(error -> {
            LOG.warn("Both services failed, using cached data", error);
            return processCachedData(paymentRecord);
        });
}
```

## Configuration Reference

### Global Configuration

```properties
# application.properties
# Retry configuration
pipeline.defaults.retry-limit=3
pipeline.defaults.retry-wait-ms=500
pipeline.defaults.max-backoff=30000
pipeline.defaults.jitter=true

# Error handling
pipeline.defaults.recover-on-failure=true
```

### Profile-Specific Configuration

```properties
# application-dev.properties
pipeline.defaults.retry-limit=1
pipeline.defaults.retry-wait-ms=100

# application-prod.properties
pipeline.defaults.retry-limit=5
pipeline.defaults.retry-wait-ms=1000
pipeline.defaults.max-backoff=60000
```

### Per-Step Overrides

Override retry settings for a single step:

```properties
pipeline.step."com.example.ProcessPaymentService".retry-limit=10
pipeline.step."com.example.ProcessPaymentService".retry-wait-ms=2000
```

## Best Practices

### Concurrency

1. **Use Virtual Threads**: Enable for I/O-bound operations
2. **Set Appropriate Limits**: Don't overwhelm external systems
3. **Monitor Resource Usage**: Watch for bottlenecks and adjust accordingly
4. **Consider Batch Processing**: For high-volume scenarios

### Error Handling

1. **Classify Errors**: Differentiate recoverable from unrecoverable errors
2. **Implement Timeouts**: Prevent indefinite hanging operations
3. **Use Circuit Breakers**: Protect against cascading failures
4. **Log Meaningfully**: Include context and stack traces

### DLQ Management

1. **Regular Monitoring**: Check DLQ for failed items regularly
2. **Automated Retries**: Implement automated retry mechanisms
3. **Alerting**: Notify on DLQ accumulation
4. **Root Cause Analysis**: Investigate and fix recurring issues

### Performance

1. **Profile Regularly**: Monitor performance under load
2. **Optimize Hot Paths**: Focus on frequently executed code
3. **Use Efficient Mappers**: Optimize data conversion operations
4. **Cache Strategically**: Cache expensive operations when appropriate

The Pipeline Framework's concurrency, error handling, and DLQ features provide a robust foundation for building resilient, high-performance pipeline applications that can gracefully handle failures and scale efficiently.
