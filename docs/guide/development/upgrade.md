# Upgrade Guide

## Descriptor-Driven gRPC Type Resolution

If you are upgrading to a version that requires descriptor-driven gRPC resolution, you must enable descriptor set generation in Quarkus. Without it, gRPC service/client generation will fail at build time.

Add the following to `application.properties` (or your profile-specific file):

```properties
quarkus.generate-code.grpc.descriptor-set.generate=true
```

See [protobuf descriptor integration](/guide/evolve/protobuf-integration-descriptor-res) for full setup details and troubleshooting.
