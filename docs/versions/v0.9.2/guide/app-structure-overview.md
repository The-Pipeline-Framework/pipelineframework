---
search: false
---

# Application Structure Overview

This guide explains how to structure applications using The Pipeline Framework, following the patterns demonstrated in the CSV Payments reference implementation.

## Overview

Applications built with The Pipeline Framework follow a modular architecture with clear separation of concerns. The framework promotes a clean division between:

1. **Orchestrator Service**: Coordinates the overall pipeline execution
2. **Backend Services**: Implement individual pipeline steps
3. **Common Module**: Shared domain objects, DTOs, and mappers
4. **Framework**: Provides the pipeline infrastructure

## Project Structure Overview

A typical pipeline application follows this structure. Note that the deployment module is not typically included as a module in the application's parent POM since it's used at build time with provided scope:

```text
my-pipeline-application/
├── pom.xml                           # Parent POM
├── common/                           # Shared components
│   ├── pom.xml
│   └── src/
│       └── main/java/
│           └── com/example/app/common/
│               ├── domain/           # Domain entities
│               ├── dto/              # Data transfer objects
│               └── mapper/           # Shared mappers
├── orchestrator-svc/                 # Pipeline orchestrator
│   ├── pom.xml
│   └── src/
│       └── main/java/
│           └── com/example/app/orchestrator/
│               ├── service/         # Pipeline execution service
│               └── OrchestratorApplication.java
├── step-one-svc/                     # First pipeline step
│   ├── pom.xml
│   └── src/
│       └── main/java/
│           └── com/example/app/stepone/
│               ├── service/         # Step implementation
│               └── mapper/          # Step-specific mappers
├── step-two-svc/                     # Second pipeline step
│   ├── pom.xml
│   └── src/
│       └── main/java/
│           └── com/example/app/steptwo/
│               ├── service/        # Step implementation
│               └── mapper/          # Step-specific mappers
└── pipeline-framework/              # Framework modules
    ├── runtime/                     # Runtime components (dependency)
    └── deployment/                  # Build-time components (provided scope)
```

## Architecture Diagram

```mermaid
graph TB
    subgraph "Pipeline Application"
        A[Common Module]
        B[Orchestrator Service]
        C[Step 1 Service]
        D[Step 2 Service]
    end
    
    A --> B
    A --> C
    A --> D
    B --> C
    C --> D