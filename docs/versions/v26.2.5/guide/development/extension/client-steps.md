---
search: false
---

# Client Step Extensions

Client steps are generated classes used by the orchestrator to call backend services. You can extend them when you need custom behavior.

## Typical Extensions

1. Add metrics around remote calls
2. Wrap calls with retries or fallbacks
3. Enrich headers or metadata before invoking gRPC
