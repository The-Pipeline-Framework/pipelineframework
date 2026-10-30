# Spring Support Status

Quarkus is the mature production runtime for TPF today.

Spring support has started, but it is intentionally limited. Treat it as an emerging adapter path, not as production parity with the Quarkus runtime.

## What Works Today

Current Spring work proves a narrow generated application path:

- YAML-declared unary local steps,
- constrained `REST + COMPUTE` unary smoke coverage,
- a generated unary `GRPC + COMPUTE` pipeline endpoint,
- generated Spring `@Component` local step beans,
- Spring WebFlux `@RestController` resources for the supported smoke path,
- `Mono<Out>` and `CompletionStage<Out>` authored service methods for YAML-declared Spring-profile unary services,
- unary delegated Spring bean steps for local pipeline execution,
- unary `Class::method` Spring operator beans for local pipeline execution,
- unary REST client-step boundaries for Spring WebFlux pipelines,
- unary gRPC client steps using Spring-managed grpc-java future stubs, pending publication of the compiler and runtime development snapshots,
- shared runner-core sequencing through the neutral runtime seam.

## Not Yet Supported

Spring does not yet have parity for:

- streaming RPCs,
- function handlers,
- await, checkpoint, durable coordinator, or broker paths,
- persistence providers,
- non-local delegated/operator paths,
- side effects and plugins,
- streaming or non-unary shapes,
- production Spring observability and deployment guidance.

Unsupported combinations should fail at build time instead of silently falling back to Quarkus generation.

## Authoring Guidance

Use Quarkus for production TPF applications today.

Use the Spring path only when you are validating the emerging renderer/runtime seam or contributing to portability work. Keep business contracts neutral: typed inputs, typed outputs, mappers, and YAML declarations should not rely on Quarkus-only application code unless the application is explicitly Quarkus-targeted.

Spring-profile service steps are supported only for unary local steps declared from YAML. Normal `service:` beans can use `process(In): Mono<Out>`, `process(In): CompletionStage<Out>`, or `processBlocking(In): Out`.

Delegated Spring beans are supported only for unary local steps declared from YAML with either a Spring bean class or an explicit `Class::method` operator reference. Class-only delegates use supported TPF-style service shapes such as `process(In): Mono<Out>` or `processBlocking(In): Out`. `Class::method` delegates support narrow unary Spring bean methods: `Out method(In)`, `Mono<Out> method(In)`, or `CompletionStage<Out> method(In)`.

Spring REST client-step boundaries are supported only for unary `REST + COMPUTE` client-role steps. Configure each generated Spring REST client with `tpf.rest-client.<client-name>.url`; generated Spring REST resources and client steps forward the TPF version, replay, and cache-policy headers for unary REST hops.

For a unary `GRPC + COMPUTE` pipeline, invoke `PipelineProtoGenerator` with `--spring-grpc-unary` before protobuf compilation. The opt-in `pipeline-unary.proto` declares `PipelineService.Run` from the first pipeline input message to the final pipeline output message. Compile it with grpc-java and pass the descriptor set to the annotation processor with `-Aprotobuf.descriptor.file`. Set `-Apipeline.codegen.rendererProfile=spring` and use `spring-boot-starter-grpc-server` (Spring Boot 4.1.2). The generated `@Service` extends the grpc-java `PipelineServiceImplBase`; Spring Boot registers it as a gRPC service. Request and response mapper pairs are checked against their exact domain and protobuf types at build time; field-level compatibility is not checked. The generated service runs the ordered local and client unary steps through `SpringPipelineRunner` and returns one response or a gRPC error.

### Unary gRPC client steps

For unary `GRPC + COMPUTE` client-role steps, add `spring-boot-starter-grpc-client` (Spring Boot 4.1.2). Compile the downstream per-step protobuf contract with grpc-java and supply its descriptor set to the annotation processor. The generated `@Component` implements `PipelineUnaryStep<In, Out>` and calls the downstream `Process…Service.remoteProcess` RPC asynchronously. Existing YAML discovery assigns the client role to a service declared outside the caller module; no client-specific DSL is required.

Each boundary requires the exact `Mapper<DomainIn, ProtoRequest>` and `Mapper<DomainOut, ProtoResponse>` pairs, or the applicable generated canonical adapters. Explicit `Mapper` beans use `toExternal` for requests and `fromExternal` for responses. Generated canonical adapters use `toProto` for requests and `fromProto` for responses. Missing or incompatible mappings and non-unary descriptors fail the build.

Configure an explicit target for every generated client. The client name follows the existing kebab-case service convention: `ProcessRemoteService` becomes `process-remote`.

```properties
spring.grpc.client.channel.process-remote.target=static://localhost:9090
```

The generated configuration creates a qualified future-stub bean through Spring's `GrpcChannelFactory`. Spring owns channel lifecycle, TLS, and customisation. Missing, blank, unresolved, or invalid target configuration fails during bean creation and identifies the property and step. An unreachable downstream service fails the RPC; it does not prevent application startup. See [Spring Boot gRPC client configuration](https://docs.spring.io/spring-boot/reference/io/grpc.html#io.grpc.client).

Each outgoing call forwards the neutral TPF version, replay, and cache-policy values as gRPC metadata using `x-pipeline-version`, `x-pipeline-replay`, and `x-pipeline-cache-policy`. `SpringPipelineRunner` preserves these values across asynchronous step completion and restores the executing thread's previous context after each invocation. The protobuf payload is unchanged. Downstream services must consume these metadata keys to use the propagated context.

Client-only compilations need the downstream per-step descriptors and do not require `PipelineService.Run` or emit a Spring pipeline server adapter. A pipeline containing local server steps can also expose the pipeline entry point described above. gRPC failures retain their status, mapper failures complete the stage exceptionally, and cancellation of a generated client step's returned future cancels its active RPC.

Streaming RPCs, function paths, durable orchestration, and broker paths remain unsupported. Spring support still does not imply production parity with the Quarkus runtime.

## Deeper Architecture

- [Runtime Core Decoupling](/evolve/runtime-core-decoupling)
- [Framework Portability Assessment](/evolve/framework-portability-assessment/)
- [Runtime Split](/evolve/framework-portability-assessment/runtime-split)
- [Code Generation Portability](/evolve/framework-portability-assessment/code-generation)
