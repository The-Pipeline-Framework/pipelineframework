# Configuration

The Pipeline Framework uses a Quarkus-based configuration system for managing step configuration properties. This system provides both global defaults and per-step overrides for various configuration properties.

## Configuration Properties

### Global Configuration

All configuration properties are accessed under the `pipeline.defaults` prefix:

```properties
# Global defaults for all steps
pipeline.defaults.retry-limit=10
pipeline.defaults.retry-wait-ms=3000
pipeline.defaults.parallel=false
pipeline.defaults.recover-on-failure=true
pipeline.defaults.max-backoff=60000
pipeline.defaults.jitter=true
pipeline.defaults.backpressure-buffer-capacity=2048
pipeline.defaults.backpressure-strategy=DROP

# gRPC clients
quarkus.grpc.clients.process-payment.host=localhost
quarkus.grpc.clients.process-payment.port=8080
```

### Per-Step Configuration

You can override configuration for specific steps using the fully qualified class name:

```properties
# Override for specific step
pipeline.step."org.example.MyStep".retry-limit=5
pipeline.step."org.example.MyStep".parallel=true
pipeline.step."org.example.MyStep".recover-on-failure=false
```

## Parallel Processing Configuration

For any step type, you can configure parallel processing to process multiple items from the same stream concurrently. This can be set globally or per-step using the same configuration mechanisms described in the [Global Configuration](#global-configuration) and [Per-Step Configuration](#per-step-configuration) sections above.

### Parallel Processing Parameters

- **`parallel`**: Controls whether to enable parallel processing for this step. Default is `false` (sequential processing). When set to `true`, the service can process multiple items from the same input stream concurrently, dramatically improving throughput when some items take longer than others. For example, in a payment processing service, if one payment takes 10 seconds but others take 1 second, setting `parallel = true` allows the fast payments to complete without waiting for the slow ones.

### Choosing the Right Parallel Strategy

For maximum performance when order doesn't matter, use:
- `pipeline.step."FQCN".parallel=true`

For strict sequential processing, leave that as false (the default).

## Avoid breaking parallelism in the pipeline

### Important

If any previous step uses `parallel = false` (the default), the pipeline will serialize the stream at that point.

Downstream, the pipeline cannot "rewind" concurrency — the upstream won't push items faster than it finished
sequentially.

Hence, the more "downstream" you can push the `parallel = false` moment, the faster the pipeline will process streams.

### Parallel Processing vs Virtual Threads

It's important to understand the difference between these two configuration options:

- **`parallel`**: For client-side steps, enables concurrent processing of multiple items from the same stream
- **`runOnVirtualThreads`**: For server-side gRPC services, enables execution on virtual threads for better I/O handling

## Understanding Pipeline Step Cardinalities

The Pipeline Framework supports four different cardinality types that determine how input and output streams are processed:

### 1. One-to-One (1→1) - Single Input to Single Output
- **Use case**: Transform each individual item into another item
- **Example**: Convert a payment record into a payment status
- **Best for**: Individual processing operations that can benefit from concurrent execution

### 2. One-to-Many (1→N) - Single Input to Multiple Outputs
- **Use case**: Expand a single item into multiple related items
- **Example**: Split a batch job into individual tasks

### 3. Many-to-One (N→1) - Multiple Inputs to Single Output
- **Use case**: Aggregate multiple related items into a single result
- **Example**: Collect payment outputs and write them to a single CSV file
- **Best for**: Aggregation operations that can benefit from concurrent processing

```java
@PipelineStep(
    ...
    stepType = StepManyToOne.class,
    ...
)
public class ProcessCsvPaymentsOutputFileReactiveService
    implements ReactiveStreamingClientService<PaymentOutput, CsvPaymentsOutputFile> {

    @Override
    public Uni<CsvPaymentsOutputFile> process(Multi<PaymentOutput> paymentOutputList) {
        // Process all payment outputs to generate a single CSV file
        // With parallel=true, different processing tasks can be executed concurrently
        return writeToFile(paymentOutputList);
    }
}
```

### 4. Many-to-Many (N→N) - Multiple Inputs to Multiple Outputs
- **Use case**: Transform a stream of items where each may produce multiple outputs
- **Example**: Filter and transform a stream of records

## @PipelineStep Annotation

The `@PipelineStep` annotation contains build-time properties:

- `inputType`, `outputType` - Type information for code generation
- `inboundMapper`, `outboundMapper` - Mapper classes
- `stepType` - Step type class
- `backendType` - Backend adapter type
- `grpcEnabled` - Whether to enable gRPC generation
- `restEnabled` - Whether to enable REST generation
- `grpcServiceBaseClass` - gRPC service base class
- `local` - Whether step is local to the runner
- `runOnVirtualThreads` - Whether the service entrypoint method should be run on a virtual thread, instead of a Vert.x event thread.

## Performance Optimization Guidelines
Hibernate Reactive requires queries to run on a Vert.x event thread/context. When `runOnVirtualThreads=true` and persistence is enabled via configuration, the framework will make sure to "hop back on" onto an event thread to persist entities. Same when the service code is offloaded to run on a worker thread.

### For Item-Level Processing (1→1 Steps):
1. **Set `parallel = true`** to enable concurrent processing of multiple input items
2. **Monitor system resources** under load to determine optimal concurrency level

### For Aggregation Processing (N→1 Steps):
1. **Use** `parallel = true` to enable concurrent processing when beneficial

### Monitoring and Tuning:
- Start with conservative parallelism values and increase gradually
- Monitor system resource usage (CPU, memory, network) under load
- Adjust `parallel` setting based on observed performance
- Consider the downstream service capacity limits when setting parallelism values
- Enable virtual threads (`run-on-virtual-threads=true`) for I/O-bound server-side operations to improve throughput
