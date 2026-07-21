# Builds and Continuous Integration (CI)
The project uses three independent workflows:

1. **build.yml** — PR/non‑main builds
    - Fast build
    - Unit tests only
    - No Jib, no native, no integration tests

2. **full-tests.yml** — push to `main`
    - Full clean build
    - Jib Docker images
    - Integration tests
    - Native builds (matrix)

3. **publish.yml** — `v*` tags
    - Release build
    - Deploys to Maven Central
    - No tests (already validated in main)

## 📛 CI Status
[![Build (PR)](https://github.com/The-Pipeline-Framework/pipelineframework/actions/workflows/build.yml/badge.svg)](https://github.com/The-Pipeline-Framework/pipelineframework/actions/workflows/build.yml)
[![Full Tests (Main)](https://github.com/The-Pipeline-Framework/pipelineframework/actions/workflows/full-tests.yml/badge.svg)](https://github.com/The-Pipeline-Framework/pipelineframework/actions/workflows/full-tests.yml)
[![Release](https://github.com/The-Pipeline-Framework/pipelineframework/actions/workflows/publish.yml/badge.svg)](https://github.com/The-Pipeline-Framework/pipelineframework/actions/workflows/publish.yml)

## Container-backed CI caching

The container-backed CSV Payments, Restaurant Approval, and Search E2E lanes
cache the expensive inputs that are safe to reuse:

- Jib layer cache, scoped by runner OS and architecture.
- Testcontainers and Compose base-image sets, keyed by their exact image list.
- Built topology images for the CSV, Restaurant, and Search stacks, keyed by
  their relevant source inputs. CSV and Restaurant bundles also include the
  generated release artifact needed to register the pipeline after a cache hit.

The first run that encounters a new cache key still pulls or builds the image,
then saves it for later jobs. Re-runs of the same PR reuse its cache. Caches
warmed on `main` are also available to later PRs when their exact keys match;
topology-image caches intentionally miss when their application or framework
inputs change.

Container bootstrap scripts use quieter pull output only in CI. Local runs keep
their normal Docker output for diagnosis. The shared CI setup does not set a
global Java logging-manager option: Quarkus test JVMs configure JBoss LogManager
through their Maven test configuration, while Maven bootstrap JVMs must not try
to load it.

## 🛠️ Build Flags Cheat Sheet

- `-DskipITs` — Skip integration tests
- `-DskipNative=true` — Skip native builds
- `-Dquarkus.container-image.build=false` — Skips building Jib image
- `-Pcoverage` — Enable coverage for unit tests only
- `-Pcentral-publishing` — Release mode for Maven Central deploy
- Avoid mixing `skipTests` and `skipITs`
- Quarkus extensions require full reactor builds (`clean install`)

## CI Architecture Diagram
```mermaid
flowchart TD

   subgraph Release_Flow["Tag v* — Publishing"]
      D1[Checkout] --> D2[Maven Clean Install]
      D2 --> D3[Release Build -Pcentral-publishing]
      D3 --> D4[Deploy to Maven Central]
      D4 --> D5[GitHub Release]
   end

   subgraph Main_Flow["Push to Main"]
      B1[Checkout] --> B2[Maven Clean Install]
      B2 --> B3[Build Jib Images]
      B3 --> B4[Integration Tests - Failsafe]

      B4 --> C1_Orch[Native Build - Orchestrator]
      B4 --> C2_In[Native Build - Input Service]
      B4 --> C3_Proc[Native Build - Processing Service]
      B4 --> C4_Stat[Native Build - Status Service]
      B4 --> C5_Out[Native Build - Output Service]
   end

   subgraph PR_Flow["PR / Non-Main Branches"]
      A1[Checkout] --> A2[Maven Clean Install]
      A2 --> A3[Run Unit Tests - Surefire]
      A3 --> A4[Skip ITs and Native]
      A4 --> A5[Done]
   end
```

## 🧩 CLI Flags — TL;DR

| Flag                                    | Meaning                                   | When to Use               |
|-----------------------------------------|-------------------------------------------|---------------------------|
| `-DskipITs`                             | Skips `*IT.java`                          | PRs, fast builds          |
| `-DskipNative=true`                     | Skips native images                       | Everything except main    |
| `-Dquarkus.container-image.build=false` | Skips Jib images (but uses Docker builds) | Full tests on main        |
| `-Pcoverage`                            | Run coverage on unit tests                | PRs, quality gates        |
| `-Pcentral-publishing`                  | Release signing + GPG + deploy            | Only on tags              |
| `-DskipTests`                           | Skips **all** tests                       | ⚠️ Avoid — rarely correct |
| `-Dquarkus.native.enabled=true`         | Enables native build                      | Native matrix stage       |

### Golden Rules
- ❌ **Never** mix `skipTests` + `skipITs`.
- ✔ Always run framework builds with:
  `mvn clean install`
- ✔ Examples (CSV Payments) may be built individually.
- ✔ Native builds must run after integration tests.
