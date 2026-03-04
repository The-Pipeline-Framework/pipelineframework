# Checkout Reference Starter (TPFGo)

This folder is the FTGo-inspired checkout starter for TPFGo.

It intentionally starts as two separate checkpoint pipelines:

- Pipeline A: `create-order-pipeline.yaml`
- Pipeline B: `deliver-order-orchestrator-svc/pipeline.yaml`

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

The full FTGo clone target extends this lane with additional bounded contexts (consumer validation, restaurant acceptance, kitchen, dispatch, delivery, payment, and explicit failure pipelines).

## Config files

- `config/create-order-pipeline.yaml`
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

## Next implementation steps

1. Expand from checkout/deliver to full FTGo bounded-context chain.
2. Add parity E2E for REST, gRPC, and FUNCTION/LAMBDA execution paths.
3. Add observer/tap runtime assertions on checkpoint vs mid-step outputs.

## Testing

Bridge behavior is covered in two modes:

- Local capture mode (fast, deterministic):
  - `CreateToDeliverBridgeE2ETest`
  - `DeliverForwardBridgeE2ETest`
- Real gRPC handoff mode (create bridge streams into an embedded downstream gRPC `ingest` endpoint):
  - `CreateToDeliverGrpcBridgeE2ETest`

Run the stable checkout deliver bridge smoke test:

`./mvnw -f examples/checkout/pom.xml -pl deliver-order-orchestrator-svc -am -Dtest=DeliverForwardBridgeE2ETest -Dsurefire.failIfNoSpecifiedTests=false test`

CI also runs this lane via reusable workflow:

- `.github/workflows/e2e-checkout-ftgo-smoke.yml`

The create-order bridge lane remains part of the FTGo clone target.
It is tracked for follow-up once generated orchestrator-client MANY_TO_ONE shape compatibility is aligned.

## Notes

- This starter is deliberately minimal so design constraints remain visible.
- The first hard gate is A/B handoff contract compatibility, validated by tests.
- Keep exactly one pipeline config per orchestrator module for generation (`<module>/pipeline.yaml`) to avoid ambiguous config resolution.
- Pipeline A and B use distinct generated base packages (`...createorder` and `...deliverorder`) to avoid gRPC type collisions during composition.
- Generated protos now include explicit `package` (derived from `basePackage`) so gRPC full method names stay unique across composed pipelines.
