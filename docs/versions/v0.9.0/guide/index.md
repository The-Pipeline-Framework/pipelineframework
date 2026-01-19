# Introduction

The Pipeline Framework is a powerful tool for building reactive pipeline processing systems. It simplifies the development of distributed systems by providing a consistent way to create, configure, and deploy pipeline steps.

## Key Features

- **Reactive Programming**: Built on top of Mutiny for non-blocking operations
- **Annotation-Based Configuration**: Simplifies adapter generation with `@PipelineStep`
- **gRPC and REST Support**: Automatically generates adapters for both communication protocols
- **Modular Design**: Clear separation between runtime and deployment components

## How It Works

The framework allows you to define pipeline steps as simple classes annotated with `@PipelineStep`. The framework automatically generates the necessary adapters at build time, eliminating the need for manual configuration.

```java
@PipelineStep(
   inputType = Input.class,
   outputType = Output.class,
   stepType = StepOneToOne.class,
   backendType = MyAdapter.class,
   inboundMapper = FooRequestToDomainMapper.class,
   outboundMapper = DomainToBarResponseMapper.class
)
public class MyPipelineStep implements StepOneToOne<Input, Output> {
    // Implementation here
}
```

This approach reduces boilerplate code and ensures consistency across your pipeline steps.
