# Restaurant Approval

`restaurant-approval` is the canonical TPF `interaction-api` await example.

It shows a compact human decision flow:

1. validate an order request
2. create a pending approval
3. suspend durably after the pending-approval operation
4. resume from a later human decision
5. finalize a terminal order state

The backend is generator-backed. The UI is a small handwritten Next.js app that talks only to the generated TPF REST APIs.

## What This Example Proves

- an authored pending-approval service decorated with `await.transport.type: interaction-api`
- queue-async suspend/resume with durable wait state
- pending interaction query through `GET /pipeline/interactions/pending`
- completion admission through `POST /pipeline/interactions/complete`
- typed resume payloads for accepted and declined restaurant decisions

Use `examples/csv-payments` for the canonical Kafka await example. Use this example for the human/UI await path.

## Modules

- `common`: shared domain types, DTOs, protobuf types, and mappers
- `validate-order-request-svc`: validates the incoming order
- `create-pending-approval-svc`: creates the pending approval payload
- `finalize-restaurant-decision-svc`: converts the human decision into the terminal order state
- `orchestrator-svc`: generated orchestrator runtime for modular/grouped layouts
- `monolith-svc`: default demo runtime for local UI work
- `nextjs-ui`: local UI that consumes the generated REST APIs

## Verify The Backend

```bash
./mvnw -f examples/restaurant-approval/pom.xml -pl monolith-svc -am \
  -Dtest=RestaurantApprovalAwaitMonolithTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

This proves the monolith path runs the pending-approval operation once, pauses for completion through the interaction API, and resumes at the following step.

## Run The Demo

This is the UI-focused monolith demo. For the self-hosted coordinator and separate worker path, see [Self-Hosted Coordinator + Worker](#self-hosted-coordinator--worker).

### 1. Start the monolith backend

The dev profile enables plain HTTP on port `8081` so the Next.js app can call the generated REST APIs without dealing with local TLS trust first.

From a fresh source worktree, first install the framework snapshot and the example's shared `common` module into that worktree's Maven repository:

```bash
./mvnw -N install -DskipTests -Dgpg.skip \
  -Dmaven.repo.local="$PWD/.m2/repository"
./mvnw -f framework/pom.xml install -DskipTests -Dgpg.skip \
  -Dmaven.repo.local="$PWD/.m2/repository"
./mvnw -f examples/restaurant-approval/pom.xml -pl common -am install -DskipTests \
  -Dmaven.repo.local="$PWD/.m2/repository"
```

Then start dev mode for the monolith only:

```bash
./mvnw -f examples/restaurant-approval/pom.xml -pl monolith-svc compile quarkus:dev \
  -Dmaven.repo.local="$PWD/.m2/repository"
```

Do not include `common` in the `quarkus:dev` reactor selection. It is a library module with the Quarkus build plugin, so Maven would start dev mode for `common` on port `8080` instead of starting the monolith on port `8081`.

### 2. Install and run the Next.js UI

```bash
cd examples/restaurant-approval/nextjs-ui
npm install
npm run dev
```

Open `http://localhost:3000`.

For a recording-oriented walkthrough, see [Restaurant Approval Interaction API Demo Runbook](./DEMO-RUNBOOK.md).

Default UI environment:

- `TPF_BASE_URL=http://localhost:8081`
- `TPF_TENANT_ID=restaurant-demo`
- `TPF_AWAIT_STEP_ID=ProcessCreatePendingApprovalService`

Override them before `npm run dev` if you need a different runtime target.

## Generated REST APIs Used By The UI

- `POST /pipeline/run-async`
- `GET /pipeline/interactions/pending?stepId=...`
- `POST /pipeline/interactions/complete`
- `GET /pipeline/executions/{executionId}`
- `GET /pipeline/executions/{executionId}/result`

The UI is presentation only. TPF owns orchestration, durable waiting, completion admission, and resume semantics.

## Self-Hosted Coordinator

The self-hosted reference path runs one packaged `monolith-svc` process with the generic control-plane and release-admin APIs enabled. The coordinator uses the local in-process transition worker, so the first demo is batteries-included: one Java process owns durable execution state and executes the restaurant business transitions.

Run it from the repository root:

```bash
./examples/restaurant-approval/self-host/run-self-hosted-demo.sh --ci
```

The script packages `monolith-svc`, starts the coordinator, creates a local `pipeline-release.json`, registers and activates the release, submits accepted and declined orders through `/tpf/control-plane/...`, completes the await interaction, and verifies terminal results.

See [Self-Hosted Coordinator Runbook](./self-host/README.md) for manual commands, environment defaults, and the current local/dev limits.

## Deferred-Completion Contract

The `Create pending approval` operation in `config/pipeline.yaml` declares:

- `service: CreatePendingApprovalService`
- `cardinality: ONE_TO_ONE`
- `await.operationOutput: PendingRestaurantApproval`
- `await.transport.type: interaction-api`
- `await.correlation.strategy: interactionId`
- `pipeline.orchestrator.mode=QUEUE_ASYNC`

The decision output is a typed union:

- `accepted -> RestaurantOrderAccepted`
- `declined -> RestaurantOrderDeclined`

## Notes

- The default local monolith demo path is intentionally HTTP-first for UI ergonomics. The non-dev runtime configs still keep the generated TLS-oriented wiring.
- The self-hosted coordinator path is a public adoption reference, not a managed service. The default demo is one process with the local worker; the REST worker split remains available as a protocol proof.
- The current example includes manual union DTO/mapper support for the decision type because the generator does not scaffold that part yet. This is tracked as generator debt in [#305](https://github.com/The-Pipeline-Framework/pipelineframework/issues/305).
- The generated `common` module currently emits a non-blocking `com.google.protobuf` split-package warning in Quarkus builds. That scaffold issue is tracked in [#304](https://github.com/The-Pipeline-Framework/pipelineframework/issues/304).
