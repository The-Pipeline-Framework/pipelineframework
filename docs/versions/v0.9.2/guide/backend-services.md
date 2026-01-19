---
search: false
---

# Creating Backend Services

This guide explains how to create backend services using The Pipeline Framework, following the patterns demonstrated in the CSV Payments reference implementation.

<Callout type="tip" title="Visual Service Design">
Design your backend services using the Canvas designer at <a href="https://app.pipelineframework.org" target="_blank">https://app.pipelineframework.org</a>. The Canvas provides a visual interface for defining service types, input/output transformations, and step characteristics, which can then be exported as configuration for your application.
</Callout>

## Overview

Backend services implement your business logic using one of the reactive service interfaces. When you annotate your service with `@PipelineStep`, the framework's annotation processor automatically generates the complete server and client infrastructure at build time:

- **Server Component**: Generated gRPC and/or REST server implementations that run as standalone services
- **Client Component**: Generated thin client libraries that run within the orchestrator

You implement one of the reactive service interfaces from the framework - the framework handles the distributed execution automatically. During pipeline execution, the orchestrator uses the generated client to communicate with your service running as a remote backend.

## Service Creation Steps

### 1. Choose the Service Interface

Select the appropriate reactive service interface based on your data flow needs:

- `ReactiveService<I, O>`: Transforms one input to one output
- `ReactiveStreamingService<I, O>`: Transforms one input to multiple outputs (stream of outputs)
- `ReactiveStreamingClientService<I, O>`: Transforms multiple inputs (stream) to one output

### 2. Create the Service Class

Create your service class with the `@PipelineStep` annotation:

```java
@PipelineStep(
    order = 1,
    inputType = PaymentRecord.class,
    outputType = PaymentStatus.class,
    stepType = StepOneToOne.class,
    backendType = GenericGrpcReactiveServiceAdapter.class,
    grpcStub = MutinyProcessPaymentServiceGrpc.MutinyProcessPaymentServiceStub.class,
    grpcImpl = MutinyProcessPaymentServiceGrpc.ProcessPaymentServiceImplBase.class,
    inboundMapper = PaymentRecordMapper.class,
    outboundMapper = PaymentStatusMapper.class,
    grpcClient = "process-payment",
    autoPersist = true
)
@ApplicationScoped
public class ProcessPaymentService implements ReactiveService<PaymentRecord, PaymentStatus> {

    @Override
    public Uni<PaymentStatus> process(PaymentRecord paymentRecord) {
        // Your business logic here
        return Uni.createFrom().item(/* processed payment status */);
    }
}
```

### 3. Considerations for Reactive Processing

When implementing reactive services, keep in mind:

- Use Mutiny's `Uni` for single result operations
- Use Mutiny's `Multi` for streaming operations
- All operations should be non-blocking and reactive
- Handle errors using Mutiny's error handling methods like `onFailure().recoverWithUni()`

### 4. Implement the Business Logic

The core of your service is the implementation of the reactive service interface method:

```java
@Override
public Uni<PaymentStatus> process(PaymentRecord paymentRecord) {
    // Validate input
    if (paymentRecord == null) {
        return Uni.createFrom().failure(new IllegalArgumentException("Payment record cannot be null"));
    }

    // Perform business logic
    return processPayment(paymentRecord)
        .onItem().transform(result -> createPaymentStatus(paymentRecord, result))
        .onFailure().recoverWithUni(error -> {
            // Handle errors appropriately
            LOG.error("Failed to process payment: {}", error.getMessage(), error);
            return Uni.createFrom().item(createErrorStatus(paymentRecord, error));
        });
}

private Uni<PaymentProcessingResult> processPayment(PaymentRecord record) {
    // Implementation details
    // This might involve calling external services, database operations, etc.
    return Uni.createFrom().item(/* processing result */);
}

private PaymentStatus createPaymentStatus(PaymentRecord record, PaymentProcessingResult result) {
    // Create success status
    return PaymentStatus.builder()
        .paymentRecord(record)
        .status("SUCCESS")
        .message("Payment processed successfully")
        .build();
}

private PaymentStatus createErrorStatus(PaymentRecord record, Throwable error) {
    // Create error status
    return PaymentStatus.builder()
        .paymentRecord(record)
        .status("ERROR")
        .message("Payment processing failed: " + error.getMessage())
        .build();
}
```




Test your service logic in isolation:

```java
@QuarkusTest
class ProcessPaymentServiceTest {
    
    @Inject
    ProcessPaymentService service;
    
    @Test
    void testSuccessfulPaymentProcessing() {
        PaymentRecord record = createTestPaymentRecord();
        
        Uni<PaymentStatus> result = service.process(record);
        
        UniAssertSubscriber<PaymentStatus> subscriber = 
            result.subscribe().withSubscriber(UniAssertSubscriber.create());
            
        PaymentStatus status = subscriber.awaitItem().getItem();
        
        assertEquals("SUCCESS", status.getStatus());
        assertEquals(record.getId(), status.getPaymentRecord().getId());
    }
    
    @Test
    void testErrorHandling() {
        PaymentRecord invalidRecord = null;
        
        Uni<PaymentStatus> result = service.process(invalidRecord);
        
        UniAssertSubscriber<PaymentStatus> subscriber = 
            result.subscribe().withSubscriber(UniAssertSubscriber.create());
            
        subscriber.assertFailedWith(IllegalArgumentException.class);
    }
    
    private PaymentRecord createTestPaymentRecord() {
        return PaymentRecord.builder()
            .id(UUID.randomUUID())
            .csvId("test-csv-123")
            .recipient("John Doe")
            .amount(new BigDecimal("100.00"))
            .currency(Currency.getInstance("USD"))
            .build();
    }

