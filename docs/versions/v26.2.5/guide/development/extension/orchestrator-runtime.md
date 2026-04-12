---
search: false
---

# Orchestrator Runtime Extensions

The orchestrator runtime is generated but editable. You can extend it to control inputs, retries, and execution boundaries.

## Common Extensions

1. Custom input provisioning (CLI, files, queues, or APIs)
2. Additional logging and metrics around pipeline execution
3. Retry and backoff logic tailored to your domain

## Custom Orchestrator (REST)

You can skip `OrchestratorApplication` entirely and build a custom orchestrator that drives the pipeline via REST.
See `examples/csv-payments/ui-dashboard/src/services/optimizedRestOrchestrationService.js` for a real example.
