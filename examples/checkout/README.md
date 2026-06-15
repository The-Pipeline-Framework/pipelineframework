# TPFGo Canonical Checkout Flow

`examples/checkout` is the canonical TPFGo example for reliable cross-pipeline handoff.

It demonstrates an eight-pipeline application using the framework checkpoint-publication model plus two interaction
checkpoints. The canonical composition manifest at `config/canonical/pipeline-composition.yaml` lists the pipeline YAML
files; typed handoff edges are derived from each pipeline's `input.subscription` and `output.checkpoint` declarations.

1. checkout
2. consumer-validation
3. restaurant-acceptance
4. kitchen-preparation
5. dispatch
6. delivery-execution
7. payment-capture
8. compensation-failure

Every boundary is declared in pipeline YAML with:

- `output.checkpoint.publication`
- `output.checkpoint.idempotencyKeyFields`
- `input.subscription.publication`

Concrete handoff targets are supplied at runtime through `pipeline.handoff.bindings.*`. There are no hand-written bridge classes, connector helpers, or app-owned boundary forwarders in the happy path.

## Modules

- `common`: shared TPFGo domain records and runtime helpers
- `pipeline-runtime-svc`: grouped runtime for all regular step execution
- `checkout-orchestrator-svc`
- `consumer-validation-orchestrator-svc`
- `restaurant-acceptance-orchestrator-svc`
- `kitchen-preparation-orchestrator-svc`
- `consumer-validation` and `restaurant-acceptance` each include one `interaction-api` await boundary before publishing their checkpoints.
- `dispatch-orchestrator-svc`
- `delivery-execution-orchestrator-svc`
- `payment-capture-orchestrator-svc`
- `compensation-failure-orchestrator-svc`
- `tpfgo-e2e-tests`: process-based end-to-end verification for the full chain

## Service intro UI

An educational checkpoint-flow explorer is available under `examples/checkout/nextjs-ui`:

- it maps each checkout service to a responsibility and contract path,
- it can submit a sample order through the generated REST entrypoint,
- it includes a pending interaction inbox for two human-in-loop checkpoint handoff steps
- (consumer approval and restaurant acceptance),
- it links to execution status and result views.

```bash
cd examples/checkout/nextjs-ui
npm install
npm run build
npm run dev
```

## Run the live backend stack

Start all checkout services locally (runtime + orchestrators) as live processes:

```bash
./examples/checkout/start-stack.sh
```

This builds the required modules, starts each service with matching in-process handoff bindings, and keeps them running until you stop the command. UI can then use the default base URL.

For one-shot runs that should start services, run a command, and then exit, use:

```bash
./examples/checkout/start-stack.sh --run-cmd "./mvnw -f examples/checkout/pom.xml -pl tpfgo-e2e-tests -am -Dtest=TpfgoCheckpointFlowIT -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false verify"
```

Or use the convenience verify mode:

```bash
./examples/checkout/start-stack.sh --verify
```

To bring the UI with the stack, add `--with-ui` (optional, interactive mode only):

```bash
./examples/checkout/start-stack.sh --skip-build --with-ui
```

You can override the UI host/port through `TPF_UI_PORT` and point it at a custom backend URL with `TPF_BASE_URL`:

```bash
TPF_UI_PORT=3001 TPF_BASE_URL=http://127.0.0.1:8080 ./examples/checkout/start-stack.sh --with-ui
```

## Canonical contracts

The canonical YAML contracts and composition manifest live under `config/canonical/`:

- `pipeline-composition.yaml`
- `01-checkout-pipeline.yaml`
- `02-consumer-validation-pipeline.yaml`
- `03-restaurant-acceptance-pipeline.yaml`
- `04-kitchen-preparation-pipeline.yaml`
- `05-dispatch-pipeline.yaml`
- `06-delivery-execution-pipeline.yaml`
- `07-payment-capture-pipeline.yaml`
- `08-compensation-failure-pipeline.yaml`

Each pipeline file is kept in sync with the runnable module `pipeline.yaml`. The composition manifest is validated as a
single typed handoff graph so publication, subscription, terminal output, and entry input contracts cannot drift.

## Runtime model

- default topology is `pipeline-runtime`
- internal `process-*` step calls collapse onto `pipeline-runtime-svc`
- all pipelines use `platform: COMPUTE`
- all pipelines use `pipeline.orchestrator.mode=QUEUE_ASYNC`
- checkpoint handoff stays externalized through orchestrator-to-orchestrator gRPC runtime bindings
- downstream retry and DLQ ownership begins only after downstream admission
- idempotency is explicit on each publication boundary

## Validation

Validate the canonical composition contract through the framework runtime tests:

```bash
./mvnw -f framework/pom.xml -pl runtime -am '-Dtest=PipelineComposition*Test,CheckoutReferenceContractTest' \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Build the full example:

```bash
./mvnw -f examples/checkout/pom.xml verify
```

Run the full TPFGo checkpoint-flow integration suite directly:

```bash
./mvnw -f examples/checkout/pom.xml \
  -pl tpfgo-e2e-tests \
  -am \
  -Dtest=NoMatchingUnitTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dit.test=TpfgoCheckpointFlowIT \
  -Dfailsafe.failIfNoSpecifiedTests=false \
  verify
```

## Docs

The user-facing walkthrough is in [docs/guide/development/tpfgo-example.md](../../docs/guide/development/tpfgo-example.md).
