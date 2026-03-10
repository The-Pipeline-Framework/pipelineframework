# Checkout Canonical Reference (TPFGo)

This folder is the FTGo-inspired canonical flow reference for TPFGo.

The executable bridge lane starts with two separate checkpoint pipelines:

- Pipeline A: `create-order-orchestrator-svc/pipeline.yaml`
- Pipeline B: `deliver-order-orchestrator-svc/pipeline.yaml`

The full canonical FTGo chain contract is tracked in:

- `config/canonical/01-checkout-pipeline.yaml`
- `config/canonical/02-consumer-validation-pipeline.yaml`
- `config/canonical/03-restaurant-acceptance-pipeline.yaml`
- `config/canonical/04-kitchen-preparation-pipeline.yaml`
- `config/canonical/05-dispatch-pipeline.yaml`
- `config/canonical/06-delivery-execution-pipeline.yaml`
- `config/canonical/07-payment-capture-pipeline.yaml`
- `config/canonical/08-compensation-failure-pipeline.yaml`

## Intent

- Pipeline A (`CreateOrder`) ends at checkpoint type `ReadyOrder`.
- Pipeline B (`DeliverOrder`) starts from `ReadyOrder`.
- The handoff contract is explicit and type-based.

This mirrors the TPF checkpoint model:

- Intra-pipeline consistency is guaranteed by pipeline completion.
- Cross-pipeline composition is explicit and policy-driven.

## Canonical lane role

This lane is the executable FTGo progression starter and CI gate for connector semantics:

- deterministic lineage at pipeline handoff,
- strict idempotency/dedup at connector boundaries,
- gRPC ingest handoff correctness.

The canonical chain config extends this lane with additional bounded contexts (consumer validation, restaurant acceptance, kitchen fan-out/fan-in, dispatch, delivery, payment, and explicit failure/compensation pipeline contracts). Full SYNC-path canonical execution proof is covered by `CanonicalFtgoSyncFlowTest`.

## Config files

- `create-order-orchestrator-svc/pipeline.yaml`
- `deliver-order-orchestrator-svc/pipeline.yaml`

## Current module scaffold

- `common`: shared DTO contracts used by both pipelines.
- `create-order-orchestrator-svc`: Pipeline A orchestrator runtime plus concrete step services:
  - `ProcessOrderRequestProcessService`
  - `ProcessOrderCreateService`
  - `ProcessOrderReadyService`
  - `CreateToDeliverIngestBridge` (streams checkpoint outputs into Pipeline B `ingest`)
- `deliver-order-orchestrator-svc`: Pipeline B orchestrator runtime plus concrete step services:
  - `ProcessOrderDispatchService`
  - `ProcessOrderDeliveredService`

These modules are intentionally not yet wired into the root project `pom.xml`.
Build them explicitly from `examples/checkout` while the reference implementation is evolving.

## Current implementation focus

1. Keep executable chain and canonical chain contracts in sync as additional lane services are added.
2. Keep parity and diagnostics suites green for REST, gRPC, FUNCTION, and Protobuf-over-HTTP semantic mapping.
3. Keep observer/tap validation diagnostics explicit (required = fail-fast, optional = warn/skip).

## Testing

Bridge behavior is covered in two modes:

- Local capture mode (fast, deterministic):
  - `CreateToDeliverBridgeE2ETest`
  - `DeliverForwardBridgeE2ETest`
- Real gRPC handoff mode (create bridge streams into an embedded downstream gRPC `ingest` endpoint):
  - `CreateToDeliverGrpcBridgeE2ETest`

Run the stable checkout deliver bridge smoke test:

`./mvnw -f examples/checkout/pom.xml -pl deliver-order-orchestrator-svc -am -Dtest=DeliverForwardBridgeE2ETest -Dsurefire.failIfNoSpecifiedTests=false test`

Validate canonical cross-pipeline contract compatibility:

`./mvnw -f framework/pom.xml -pl runtime -Dtest=CheckoutCanonicalFlowContractTest test`

CI also runs this lane via reusable workflow:

- `.github/workflows/e2e-checkout-ftgo-smoke.yml`

The create-order and deliver-order bridge lanes are both active and validated in CI through local and embedded gRPC tests.

## Notes

- This starter is deliberately minimal so design constraints remain visible.
- The first hard gate is A/B handoff contract compatibility, validated by tests.
- Keep exactly one pipeline config per orchestrator module for generation (`<module>/pipeline.yaml`) to avoid ambiguous config resolution.
- Pipeline A and B use distinct generated base packages (`...createorder` and `...deliverorder`) to avoid gRPC type collisions during composition.
- Generated protos now include explicit `package` (derived from `basePackage`) so gRPC full method names stay unique across composed pipelines.
