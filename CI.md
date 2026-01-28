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
