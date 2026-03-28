# TPFGo Canonical Checkout Flow

`examples/checkout` is the canonical TPFGo example for reliable cross-pipeline handoff.

It demonstrates an eight-pipeline application using only the framework checkpoint-publication model:

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
- `checkout-orchestrator-svc`
- `consumer-validation-orchestrator-svc`
- `restaurant-acceptance-orchestrator-svc`
- `kitchen-preparation-orchestrator-svc`
- `dispatch-orchestrator-svc`
- `delivery-execution-orchestrator-svc`
- `payment-capture-orchestrator-svc`
- `compensation-failure-orchestrator-svc`
- `tpfgo-e2e-tests`: process-based end-to-end verification for the full chain

## Canonical contracts

The canonical YAML contracts live under `config/canonical/`:

- `01-checkout-pipeline.yaml`
- `02-consumer-validation-pipeline.yaml`
- `03-restaurant-acceptance-pipeline.yaml`
- `04-kitchen-preparation-pipeline.yaml`
- `05-dispatch-pipeline.yaml`
- `06-delivery-execution-pipeline.yaml`
- `07-payment-capture-pipeline.yaml`
- `08-compensation-failure-pipeline.yaml`

Each file is kept in sync with the runnable module `pipeline.yaml`.

## Runtime model

- all pipelines use `platform: COMPUTE`
- all pipelines use `pipeline.orchestrator.mode=QUEUE_ASYNC`
- checkpoint handoff is externalized through gRPC runtime bindings
- downstream retry and DLQ ownership begins only after downstream admission
- idempotency is explicit on each publication boundary

## Validation

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

The user-facing walkthrough is in [docs/guide/development/tpfgo-example.md](/Users/mari/tpf5/docs/guide/development/tpfgo-example.md).
