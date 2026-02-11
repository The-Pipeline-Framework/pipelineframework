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

1. Add connector policies (idempotency + backpressure) before production-style sync composition.
2. Add operational controls for the bridge (enable/disable flag, reconnect/backoff tuning, metrics).
3. Add end-to-end workflow assertions that include business service execution (not only bridge-level handoff).

## Testing

Bridge behavior is covered in two modes:

- Local capture mode (fast, deterministic):
  - `CreateToDeliverBridgeE2ETest`
  - `DeliverForwardBridgeE2ETest`
- Real gRPC handoff mode (create bridge streams into an embedded downstream gRPC `ingest` endpoint):
  - `CreateToDeliverGrpcBridgeE2ETest`

Run all checkout bridge tests:

`./mvnw -f examples/checkout/pom.xml -pl create-order-orchestrator-svc,deliver-order-orchestrator-svc -am -Dtest=CreateToDeliverBridgeE2ETest,CreateToDeliverGrpcBridgeE2ETest,DeliverForwardBridgeE2ETest -Dsurefire.failIfNoSpecifiedTests=false test`

## Notes

- This starter is deliberately minimal so design constraints remain visible.
- The first hard gate is A/B handoff contract compatibility, validated by tests.
- Keep exactly one pipeline config per orchestrator module for generation (`<module>/pipeline.yaml`) to avoid ambiguous config resolution.
- Pipeline A and B use distinct generated base packages (`...createorder` and `...deliverorder`) to avoid gRPC type collisions during composition.
- Generated protos now include explicit `package` (derived from `basePackage`) so gRPC full method names stay unique across composed pipelines.
