# Stdio Object Pipeline Demo

This is a generated TPF pipeline, not a connector harness. It maps one EOF-delimited JSON value into a typed
`GreetingRequests` input, normalizes and composes greetings through two business steps, then renders terminal output
through Object Publish to stdout.

Build the module, then use the generated orchestrator CLI's one-shot ingest mode:

```bash
./mvnw -pl examples/stdio-object-demo package
echo '{"name":"Mariano"}' |
  PIPELINE_CONFIG="$PWD/examples/stdio-object-demo/pipeline.yaml" java -jar examples/stdio-object-demo/target/quarkus-app/quarkus-run.jar \
    --ingest-once --async-timeout-minutes 1 | jq .
```

A JSON collection remains one object-ingest admission because its declared input contract is a collection:

```bash
echo '[{"name":"Mariano"},{"name":"Ada"}]' |
  PIPELINE_CONFIG="$PWD/examples/stdio-object-demo/pipeline.yaml" java -jar examples/stdio-object-demo/target/quarkus-app/quarkus-run.jar \
    --ingest-once --async-timeout-minutes 1 | jq .
```

Diagnostics are written to stderr; stdout is reserved for the JSON payload.
