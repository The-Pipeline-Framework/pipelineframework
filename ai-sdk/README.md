# AI SDK for TPF Delegation Stress Testing

This is a standalone Java library that provides a reactive AI processing SDK designed specifically to stress-test TPF delegation capabilities.

## Purpose

This SDK is designed to:
- Exercise all cardinalities (UnaryUnary, UnaryMany, ManyUnary, ManyMany)
- Force DTO/Proto + mappers for transport layer
- Be delegation-friendly
- Be realistic (LLM/RAG style)
- Be transport-agnostic
- Not depend on @PipelineStep
- Cleanly integrate with TPF's ExternalMapper model

## Architecture

The SDK follows a clean separation of concerns:

```
com.example.ai.sdk
├── entity/           # Business entities
├── dto/              # Data transfer objects
├── proto/            # .proto definition files
├── mapper/           # Conversion between layers implementing Mapper<Grpc, Dto, Entity>
└── service/          # Reactive service implementations
```

## gRPC Integration

With Quarkus gRPC, the `.proto` files are compiled during the build process to generate:
- Client and server stubs
- Message classes (e.g., `Document`, `Chunk`, `Vector`, `Prompt`, `Completion`, etc.) - these will be generated in `com.example.ai.sdk.grpc` package
- Service interfaces

During the build, Quarkus gRPC plugin will:
1. Process `.proto` files in `src/main/proto/` directory:
   - `document-chunking-svc.proto`
   - `embedding-svc.proto`
   - `vector-store-svc.proto`
   - `similarity-search-svc.proto`
   - `llm-completion-svc.proto`
2. Generate gRPC classes in the target directory
3. These generated classes will be available for compilation of the mappers
4. The mappers will convert between the generated gRPC classes, DTOs, and Entity objects

The generated classes will be located in the package specified in the `.proto` files (`com.example.ai.sdk.grpc`).

## Services

### 1. DocumentChunkingService
- **Cardinality**: Uni → Multi (UnaryMany)
- **Function**: Splits document text into chunks
- **Implementation**: `org.pipelineframework.service.ReactiveStreamingService<Document, Chunk>`

### 2. EmbeddingService
- **Cardinality**: Uni → Uni (UnaryUnary)
- **Function**: Generates vector embeddings from text
- **Implementation**: `org.pipelineframework.service.ReactiveService<String, Vector>`

### 3. VectorStoreService
- **Cardinality**: Uni → Uni (UnaryUnary)
- **Function**: Stores vectors in in-memory store
- **Implementation**: `org.pipelineframework.service.ReactiveService<Vector, StoreResult>`

### 4. SimilaritySearchService
- **Cardinality**: Uni → Multi (UnaryMany)
- **Function**: Finds similar vectors using fake similarity logic
- **Implementation**: `org.pipelineframework.service.ReactiveStreamingService<Vector, ScoredChunk>`

### 5. LLMCompletionService
- **Cardinality**: Uni → Uni (UnaryUnary)
- **Function**: Generates text completions from prompts
- **Implementation**: `org.pipelineframework.service.ReactiveService<Prompt, Completion>`

## Phase 1 Unary Operator Variants

To demonstrate Phase 1 operator constraints (1->1 invocation path), the SDK also includes unary adapters:

- `DocumentChunkingUnaryService` (`Document -> Uni<Chunk>`)
- `ChunkEmbeddingService` (`Chunk -> Uni<Vector>`)
- `VectorStoreService` (`Vector -> Uni<StoreResult>`)
- `SimilaritySearchUnaryService` (`Vector -> Uni<ScoredChunk>`)
- `ScoredChunkPromptService` (`ScoredChunk -> Uni<Prompt>`)
- `LLMCompletionService` (`Prompt -> Uni<Completion>`)

These can be chained into a complete unary pipeline suitable for current operator invocation boundaries.

## Mapper Classes

All mappers follow the naming convention of the TPF Mapper interface for easy integration when the SDK is used with TPF. The methods are:
- `toDto(Proto proto)` - Proto to DTO
- `toGrpc(Dto dto)` - DTO to Proto (gRPC)
- `fromDto(Dto dto)` - DTO to Entity
- `toDto(Entity entity)` - Entity to DTO
- `fromGrpc(Proto proto)` - Proto to DTO

## Usage with TPF Operators (Phase 1)

The SDK can be used with build-time operator references:

```yaml
steps:
  - name: "Chunk Document"
    operator: "com.example.ai.sdk.service.DocumentChunkingUnaryService::process"
  - name: "Embed Chunk"
    operator: "com.example.ai.sdk.service.ChunkEmbeddingService::process"
  - name: "Store Vector"
    operator: "com.example.ai.sdk.service.VectorStoreService::process"
  - name: "Search Similar"
    operator: "com.example.ai.sdk.service.SimilaritySearchUnaryService::process"
  - name: "Build Prompt"
    operator: "com.example.ai.sdk.service.ScoredChunkPromptService::process"
  - name: "LLM Complete"
    operator: "com.example.ai.sdk.service.LLMCompletionService::process"
```

The same full 6-step chain is committed in this module at `ai-sdk/config/pipeline.yaml`.
This is the PoC centerpiece: pipeline assembly via YAML operator references, without `@PipelineStep` wrapper classes.

## Key Features

- **Mutiny Integration**: All services use Uni and Multi for reactive programming
- **Cardinality Variety**: Covers all major streaming patterns
- **Layered Architecture**: Clear separation of Entity/DTO/Proto layers
- **TPF Compatible**: Mappers follow TPF interface naming for easy integration
- **Deterministic Behavior**: All operations are predictable for testing
- **Standalone**: Can be used independently or integrated with TPF
- **Delegation Ready**: Designed for TPF delegation with proper mappers
- **Quarkus gRPC Integration**: Uses Quarkus gRPC for automatic stub generation with Mutiny support

## Building with Quarkus gRPC

To build this project with Quarkus gRPC and generate the gRPC stubs:

1. Run the build process:
   ```bash
   mvn clean compile
   ```
   This will:
   - Generate the gRPC classes from the `.proto` files in `target/generated-sources/grpc`
   - Compile the mappers that reference these generated classes
   - Compile the services

2. The generated classes will be available in the `com.example.ai.sdk.grpc` package and include:
   - Message classes (e.g., `DocumentProto`, `ChunkProto`, etc.)
   - Service stubs with Mutiny support (e.g., `MutinyDocumentChunkingServiceGrpc`)
   - Standard gRPC stubs

## Testability

- In-memory vector storage
- Deterministic embedding (hash-based vectors)
- Predictable similarity ranking
- No randomness for reproducible tests
- Full reactive pipeline testable end-to-end
