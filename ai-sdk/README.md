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
- Message classes (e.g., `DocumentProto`, `ChunkProto`, etc.) - these will be generated in `com.example.ai.sdk.grpc` package
- Service interfaces

During the build, Quarkus gRPC plugin will:
1. Process `src/main/proto/ai_sdk.proto` 
2. Generate gRPC classes in the target directory
3. These generated classes will be available for compilation of the mappers
4. The mappers will convert between the generated gRPC classes, DTOs, and Entity objects

The generated classes will be located in the package specified in the `.proto` file (`com.example.ai.sdk.grpc`).

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

## Mapper Classes

All mappers follow the naming convention of the TPF Mapper interface for easy integration when the SDK is used with TPF. The methods are:
- `toDto(Proto proto)` - Proto to DTO
- `toGrpc(Dto dto)` - DTO to Proto (gRPC)
- `fromDto(Dto dto)` - DTO to Entity
- `toDto(Entity entity)` - Entity to DTO
- `fromGrpc(Proto proto)` - Proto to DTO

## Usage with TPF Delegation

The SDK is designed to be used with TPF delegation:

```yaml
steps:
  name: chunkDoc
  delegate: DocumentChunkingService.class
  
steps:
  name: generateEmbedding
  delegate: EmbeddingService.class
  
steps:
  name: storeVector
  delegate: VectorStoreService.class
  
steps:
  name: searchSimilar
  delegate: SimilaritySearchService.class
  
steps:
  name: llmComplete
  delegate: LLMCompletionService.class
```

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