# Self-Hosted HA Roadmap

This page tracks internal follow-up work after the restaurant self-host container reference. The example README stays focused on running the local stack; this page captures the remaining coordinator hardening work.

## Present In The Container Reference

The restaurant container reference proves the compute-first HA shape locally:

1. coordinator and REST worker run as separate containers,
2. LocalStack provides DynamoDB-, SQS-, and S3-compatible endpoints,
3. release registration, activation, and worker lifecycle are exercised,
4. happy-path await completion and terminal result inspection are automated,
5. incident handling reaches terminal failure and demonstrates single-execution re-drive.

## Remaining HA Hardening

The reference intentionally does not implement:

1. Kubernetes manifests, Helm charts, Terraform modules, or production IAM,
2. worker autoscaling, fleet routing, or deployment orchestration,
3. dynamic JAR loading in the coordinator,
4. append-only execution and await state storage,
5. bulk DLQ-message consumers or automated replay campaigns,
6. production tenancy, RBAC, org/principal management, or support-console flows.

## Storage Direction

New coordinator metadata stores should keep following the immutable-record rule: conditional writes, immutable records, and append-only event records where practical.

Existing execution and await Dynamo stores still use conditional updates for leases and state transitions. Moving those stores to an append-only model is a separate storage-design PR because it changes read paths, write volume, retention, and recovery semantics.

## Replay Direction

Single-execution re-drive is present and reads the durable execution record. DLQ messages are operational evidence, not the replay source.

Bulk replay remains a separate design. It needs operator controls for selection, rate limits, idempotency warnings, and audit trails before it belongs in the runtime.
