package org.pipelineframework.config.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.StringReader;
import java.util.List;
import org.junit.jupiter.api.Test;

class PipelineYamlConfigLoaderTest {

    @Test
    void loadsCheckpointBoundaryDeclarations() {
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(new StringReader("""
            basePackage: "com.example"
            transport: "GRPC"
            platform: "COMPUTE"
            steps:
              - name: "Process Foo"
                inputTypeName: "com.example.domain.FooInput"
                inboundMapper: "com.example.mapper.FooInputMapper"
                outputTypeName: "com.example.domain.FooOutput"
                outboundMapper: "com.example.mapper.FooOutputMapper"
            input:
              subscription:
                publication: "orders-ready"
                mapper: "com.example.bridge.ReadyOrderMapper"
            output:
              checkpoint:
                publication: "orders-dispatched"
                idempotencyKeyFields: ["orderId", "customerId"]
            """));

        assertNotNull(config.input());
        assertEquals("orders-ready", config.input().subscription().publication());
        assertEquals("com.example.bridge.ReadyOrderMapper", config.input().subscription().mapper());
        assertNotNull(config.output());
        assertEquals("orders-dispatched", config.output().checkpoint().publication());
        assertEquals(List.of("orderId", "customerId"), config.output().checkpoint().idempotencyKeyFields());
        assertEquals(1, config.steps().size());
        PipelineYamlStep step = config.steps().getFirst();
        assertEquals("com.example.domain.FooInput", step.inputType());
        assertEquals("com.example.mapper.FooInputMapper", step.inboundMapper());
        assertEquals("com.example.domain.FooOutput", step.outputType());
        assertEquals("com.example.mapper.FooOutputMapper", step.outboundMapper());
    }

    @Test
    void loadsObjectSourceInputBoundary() {
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(new StringReader("""
            basePackage: "com.example"
            transport: "GRPC"
            platform: "COMPUTE"
            sources:
              documents:
                kind: object
                provider: filesystem
                location:
                  root: "/tmp/incoming"
                filter:
                  include: ["*.csv"]
                poll:
                  enabled: true
                  interval: PT5S
                  batchSize: 10
                identity:
                  fields: [provider, container, key, etag]
                payload:
                  mode: reference
            input:
              from: documents
              emits:
                type: com.example.DocumentInput
                typeName: DocumentInput
                mapper: com.example.DocumentObjectMapper
            steps:
              - name: "Process Document"
                inputTypeName: "DocumentInput"
                outputTypeName: "DocumentOutput"
            """));

        assertEquals(1, config.sources().size());
        assertEquals("filesystem", config.sources().get("documents").provider());
        assertEquals("/tmp/incoming", config.sources().get("documents").location().get("root"));
        assertEquals(List.of("*.csv"), config.sources().get("documents").filter().include());
        assertEquals(10, config.sources().get("documents").poll().batchSize());
        assertNotNull(config.input());
        assertEquals("documents", config.input().object().source());
        assertEquals("com.example.DocumentInput", config.input().object().type());
        assertEquals("DocumentInput", config.input().object().typeName());
        assertEquals("com.example.DocumentObjectMapper", config.input().object().mapper());
    }

    @Test
    void loadsObjectPublishOutputBoundary() {
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(new StringReader("""
            basePackage: "com.example"
            transport: "GRPC"
            platform: "COMPUTE"
            publish:
              results:
                kind: object
                provider: filesystem
                location:
                  root: "/tmp/outgoing"
                naming:
                  keyTemplate: "{groupKey}.out"
                payload:
                  contentType: text/csv
            output:
              to: results
              consumes:
                type: com.example.DocumentOutput
                typeName: DocumentOutput
                mapper: com.example.DocumentOutputPublishMapper
            steps:
              - name: "Process Document"
                inputTypeName: "DocumentInput"
                outputTypeName: "DocumentOutput"
            """));

        assertEquals(1, config.publish().size());
        assertEquals("filesystem", config.publish().get("results").provider());
        assertEquals("/tmp/outgoing", config.publish().get("results").location().get("root"));
        assertEquals("{groupKey}.out", config.publish().get("results").naming().keyTemplate());
        assertEquals("text/csv", config.publish().get("results").payload().contentType());
        assertNotNull(config.output());
        assertEquals("results", config.output().object().target());
        assertEquals("com.example.DocumentOutput", config.output().object().type());
        assertEquals("DocumentOutput", config.output().object().typeName());
        assertEquals("com.example.DocumentOutputPublishMapper", config.output().object().mapper());
    }

    @Test
    void rejectsSubscriptionAndObjectInputTogether() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                transport: "GRPC"
                steps: []
                input:
                  subscription:
                    publication: orders-ready
                  from: documents
                  emits:
                    type: com.example.DocumentInput
                    mapper: com.example.DocumentObjectMapper
                """)));

        assertEquals("pipeline input boundary cannot declare both subscription and object", exception.getMessage());
    }

    @Test
    void rejectsObjectPayloadMaxBytesOutsideLongRange() {
        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                transport: "GRPC"
                sources:
                  documents:
                    kind: object
                    provider: filesystem
                    payload:
                      maxBytes: 9223372036854775808
                steps: []
                """)));

        assertEquals("Invalid long value '9223372036854775808' for key 'maxBytes'", exception.getMessage());
    }

    @Test
    void rejectsObjectInputWithoutSourceReference() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                transport: "GRPC"
                input:
                  emits:
                    type: com.example.DocumentInput
                    mapper: com.example.DocumentObjectMapper
                steps: []
                """)));

        assertEquals("input.object must declare source or from", exception.getMessage());
    }

    @Test
    void rejectsObjectOutputWithoutPublishTargetReference() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                transport: "GRPC"
                output:
                  consumes:
                    type: com.example.DocumentOutput
                    mapper: com.example.DocumentOutputPublishMapper
                steps: []
                """)));

        assertEquals("output.object must declare target or to", exception.getMessage());
    }

    @Test
    void rejectsObjectOutputReferencingMissingPublishTarget() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                transport: "GRPC"
                output:
                  to: missing
                  consumes:
                    type: com.example.DocumentOutput
                    mapper: com.example.DocumentOutputPublishMapper
                steps: []
                """)));

        assertEquals("output.object publish target not found: missing", exception.getMessage());
    }

    @Test
    void rejectsMalformedObjectSourcePayloadSection() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                transport: "GRPC"
                sources:
                  documents:
                    kind: object
                    provider: filesystem
                    payload: text
                steps: []
                """)));

        assertEquals("source.payload must be defined as a map", exception.getMessage());
    }

    @Test
    void rejectsNonPositiveObjectSourcePollBatchSize() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                transport: "GRPC"
                sources:
                  documents:
                    kind: object
                    provider: filesystem
                    poll:
                      batchSize: 0
                steps: []
                """)));

        assertEquals("object source poll.batchSize must be positive", exception.getMessage());
    }

    @Test
    void rejectsNonPositiveObjectSourcePollInterval() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                transport: "GRPC"
                sources:
                  documents:
                    kind: object
                    provider: filesystem
                    poll:
                      interval: PT0S
                steps: []
                """)));

        assertEquals("object source poll.interval must be positive", exception.getMessage());
    }

    @Test
    void loadsConfigWithoutCheckpointBoundaries() {
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(new StringReader("""
            basePackage: "com.example"
            transport: "GRPC"
            platform: "COMPUTE"
            steps: []
            """));

        assertNull(config.input());
        assertNull(config.output());
    }

    @Test
    void loadsAwaitStepConfiguration() {
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(new StringReader("""
            basePackage: "com.example"
            transport: "GRPC"
            platform: "COMPUTE"
            steps:
              - name: "Fraud Check"
                kind: "await"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "com.example.FraudCheckRequest"
                outputTypeName: "com.example.FraudCheckDecision"
                timeout: "PT10M"
                idempotencyKeyFields: ["orderId"]
                await:
                  correlation:
                    strategy: "interactionId"
                  transport:
                    type: "webhook"
                    request:
                      url: "https://partner.example/check"
                    completion:
                      path: "/pipeline/await/fraud-check/complete"
            """));

        PipelineYamlStep step = config.steps().getFirst();
        assertEquals("await", step.kind());
        assertEquals("ONE_TO_ONE", step.cardinality());
        assertEquals("PT10M", step.timeout());
        assertEquals(List.of("orderId"), step.idempotencyKeyFields());
        assertNotNull(step.awaitConfig());
        assertEquals("interactionId", step.awaitConfig().correlation().strategy());
        assertEquals("webhook", step.awaitConfig().transport().type());
        assertNotNull(step.awaitConfig().transport().config().get("request"));
        assertNotNull(step.awaitConfig().transport().config().get("completion"));
    }

    @Test
    void rejectsAwaitDispatchConfigurationAtRuntime() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                transport: "GRPC"
                platform: "COMPUTE"
                steps:
                  - name: "Await Batch"
                    kind: "await"
                    cardinality: "MANY_TO_MANY"
                    inputTypeName: "com.example.BatchRequest"
                    outputTypeName: "com.example.BatchDecision"
                    timeout: "PT10M"
                    await:
                      dispatch:
                        mode: "per-item"
                      correlation:
                        strategy: "signedResumeToken"
                      transport:
                        type: "kafka"
                        request:
                          topic: "batch.requests"
                        response:
                          topic: "batch.responses"
                """)));

        assertEquals("step 'Await Batch' await.dispatch is not supported", exception.getMessage());
    }

    @Test
    void rejectsBlankAwaitCorrelationStrategy() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                transport: "GRPC"
                platform: "COMPUTE"
                steps:
                  - name: "Fraud Check"
                    kind: "await"
                    inputTypeName: "com.example.FraudCheckRequest"
                    outputTypeName: "com.example.FraudCheckDecision"
                    await:
                      correlation:
                        strategy: "  "
                      transport:
                        type: "interaction-api"
                """)));

        assertEquals("await.correlation.strategy is required", exception.getMessage());
    }

    @Test
    void rejectsWebhookAwaitTransportWithoutUrl() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                transport: "GRPC"
                platform: "COMPUTE"
                steps:
                  - name: "Fraud Check"
                    kind: "await"
                    inputTypeName: "com.example.FraudCheckRequest"
                    outputTypeName: "com.example.FraudCheckDecision"
                    await:
                      transport:
                        type: "webhook"
                """)));

        assertEquals("step 'Fraud Check' requires a webhook URL in one of: url, request.url, or dispatch.url",
            exception.getMessage());
    }

    @Test
    void rejectsLegacyConnectorSection() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                transport: "GRPC"
                platform: "COMPUTE"
                steps: []
                connectors: []
                """)));

        assertEquals(
            "Top-level connectors are no longer supported; use input.subscription and output.checkpoint",
            exception.getMessage());
    }

    @Test
    void rejectsMalformedCheckpointBoundaryBlocks() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                transport: "GRPC"
                platform: "COMPUTE"
                steps: []
                output:
                  checkpoint: "not-a-map"
                """)));

        assertEquals("output.checkpoint must be defined as a map", exception.getMessage());
    }

    @Test
    void rejectsNonListIdempotencyKeyFieldsInCheckpoint() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                transport: "GRPC"
                platform: "COMPUTE"
                steps: []
                output:
                  checkpoint:
                    publication: "orders-dispatched"
                    idempotencyKeyFields: "orderId"
                """)));

        assertEquals("output.checkpoint.idempotencyKeyFields must be defined as a list", exception.getMessage());
    }

    @Test
    void rejectsMalformedSubscriptionBlock() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                transport: "GRPC"
                platform: "COMPUTE"
                steps: []
                input:
                  subscription: "not-a-map"
                """)));

        assertEquals("input.subscription must be defined as a map", exception.getMessage());
    }

    @Test
    void rejectsBlankPublicationInSubscription() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                transport: "GRPC"
                platform: "COMPUTE"
                steps: []
                input:
                  subscription:
                    publication: "  "
                """)));

        assertEquals("input.subscription.publication must not be blank", exception.getMessage());
    }

    @Test
    void loadsQueriesSectionWithConnectorInputAndOutput() {
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(new StringReader("""
            basePackage: "com.example"
            transport: "GRPC"
            queries:
              customer-risk-by-id:
                connector: "jpa"
                input: "com.example.CustomerRiskLookup"
                output: "com.example.CustomerRiskSnapshot"
                version: "v1"
                jpa:
                  entity: "com.example.CustomerRiskEntity"
                  where:
                    customerId: "input.customerId"
            steps: []
            """));

        assertNotNull(config.queries());
        assertEquals(1, config.queries().size());
        PipelineYamlQuery query = config.queries().get("customer-risk-by-id");
        assertNotNull(query);
        assertEquals("customer-risk-by-id", query.id());
        assertEquals("jpa", query.connector());
        assertEquals("com.example.CustomerRiskLookup", query.inputType());
        assertEquals("com.example.CustomerRiskSnapshot", query.outputType());
        assertEquals("v1", query.version());
        assertEquals("com.example.CustomerRiskEntity", query.jpa().entity());
    }

    @Test
    void loadsQueriesSectionWithInputTypeAndOutputTypeAliases() {
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(new StringReader("""
            basePackage: "com.example"
            transport: "GRPC"
            queries:
              order-risk-by-id:
                connector: "jpa"
                inputType: "com.example.OrderRiskLookup"
                outputType: "com.example.OrderRiskSnapshot"
                jpa:
                  entity: "com.example.OrderRiskEntity"
                  where:
                    orderId: "input.orderId"
            steps: []
            """));

        PipelineYamlQuery query = config.queries().get("order-risk-by-id");
        assertNotNull(query);
        assertEquals("com.example.OrderRiskLookup", query.inputType());
        assertEquals("com.example.OrderRiskSnapshot", query.outputType());
    }

    @Test
    void loadsQueriesWithJpaConfig() {
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(new StringReader("""
            basePackage: "com.example"
            transport: "GRPC"
            queries:
              risk-query:
                connector: "jpa"
                input: "com.example.RiskLookup"
                output: "com.example.RiskSnapshot"
                jpa:
                  entity: "com.example.RiskEntity"
                  where:
                    customerId: "input.customerId"
                  projection:
                    riskBand: "riskBand"
                  result: "single"
            steps: []
            """));

        PipelineYamlQuery query = config.queries().get("risk-query");
        assertNotNull(query);
        assertEquals("com.example.RiskEntity", query.jpa().entity());
        assertEquals("input.customerId", query.jpa().where().get("customerId"));
        assertEquals("riskBand", query.jpa().projection().get("riskBand"));
    }

    @Test
    void rejectsRawQueryConfigSection() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                transport: "GRPC"
                queries:
                  customer-risk-by-id:
                    connector: jpa
                    input: com.example.CustomerRiskLookup
                    output: com.example.CustomerRiskSnapshot
                    config: "not-a-map"
                    jpa:
                      entity: com.example.CustomerRiskEntity
                      where:
                        customerId: input.customerId
                steps: []
                """)));

        assertEquals("query 'customer-risk-by-id' config is not supported; use jpa", exception.getMessage());
    }

    @Test
    void loadsQueryStepFieldsFromStepsSection() {
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(new StringReader("""
            basePackage: "com.example"
            transport: "GRPC"
            queries:
              customer-risk-by-id:
                connector: "jpa"
                input: "com.example.CustomerRiskLookup"
                output: "com.example.CustomerRiskSnapshot"
                jpa:
                  entity: "com.example.CustomerRiskEntity"
                  where:
                    customerId: "input.customerId"
            steps:
              - name: "Load Customer Risk"
                kind: "query"
                cardinality: "ONE_TO_ONE"
                query: "customer-risk-by-id"
                input: "com.example.CustomerRiskLookup"
                output: "com.example.CustomerRiskSnapshot"
                capture:
                  keyFields: ["customerId"]
            """));

        assertEquals(1, config.steps().size());
        PipelineYamlStep step = config.steps().getFirst();
        assertEquals("Load Customer Risk", step.name());
        assertEquals("query", step.kind());
        assertEquals("customer-risk-by-id", step.queryId());
        assertNotNull(step.queryCapture());
        assertEquals(List.of("customerId"), step.queryCapture().keyFields());
    }

    @Test
    void trimsStepQueryReferences() {
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(new StringReader("""
            basePackage: "com.example"
            transport: "GRPC"
            queries:
              customer-risk-by-id:
                connector: jpa
                input: com.example.CustomerRiskLookup
                output: com.example.CustomerRiskSnapshot
                jpa:
                  entity: com.example.CustomerRiskEntity
                  where:
                    customerId: input.customerId
            steps:
              - name: "Load Customer Risk"
                kind: query
                cardinality: ONE_TO_ONE
                query: " customer-risk-by-id "
                input: com.example.CustomerRiskLookup
                output: com.example.CustomerRiskSnapshot
            """));

        assertEquals("customer-risk-by-id", config.steps().getFirst().queryId());
    }

    @Test
    void defaultsQueryVersionToV1WhenNotProvided() {
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(new StringReader("""
            basePackage: "com.example"
            transport: "GRPC"
            queries:
              my-query:
                connector: "jpa"
                input: "com.example.LookupType"
                output: "com.example.SnapshotType"
                jpa:
                  entity: "com.example.Entity"
                  where:
                    id: "input.id"
            steps: []
            """));

        PipelineYamlQuery query = config.queries().get("my-query");
        assertEquals("v1", query.version());
    }

    @Test
    void rejectsQueryWithMissingConnector() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                queries:
                  bad-query:
                    input: "com.example.LookupType"
                    output: "com.example.SnapshotType"
                    jpa:
                      entity: "com.example.Entity"
                      where:
                        id: "input.id"
                steps: []
                """)));
    }

    @Test
    void rejectsQueryWithMissingInput() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                queries:
                  bad-query:
                    connector: "jpa"
                    output: "com.example.SnapshotType"
                    jpa:
                      entity: "com.example.Entity"
                      where:
                        id: "input.id"
                steps: []
                """)));
    }

    @Test
    void rejectsQueryWithMissingOutput() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                queries:
                  bad-query:
                    connector: "jpa"
                    input: "com.example.LookupType"
                    jpa:
                      entity: "com.example.Entity"
                      where:
                        id: "input.id"
                steps: []
                """)));
    }

    @Test
    void rejectsQueryCaptureModeV1() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                queries:
                  my-query:
                    connector: "jpa"
                    input: "com.example.LookupType"
                    output: "com.example.SnapshotType"
                    jpa:
                      entity: "com.example.Entity"
                      where:
                        id: "input.id"
                steps:
                  - name: "My Step"
                    kind: "query"
                    query: "my-query"
                    input: "com.example.LookupType"
                    output: "com.example.SnapshotType"
                    capture:
                      mode: "CAPTURED"
                """)));
    }

    @Test
    void queriesDefaultsToEmptyMapWhenNotPresent() {
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(new StringReader("""
            basePackage: "com.example"
            transport: "GRPC"
            steps: []
            """));

        assertNotNull(config.queries());
        assertTrue(config.queries().isEmpty());
    }

    @Test
    void loadsMultipleQueryDefinitions() {
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(new StringReader("""
            basePackage: "com.example"
            transport: "GRPC"
            queries:
              customer-risk-by-id:
                connector: "jpa"
                input: "com.example.CustomerRiskLookup"
                output: "com.example.CustomerRiskSnapshot"
                jpa:
                  entity: "com.example.CustomerRiskEntity"
                  where:
                    customerId: "input.customerId"
              order-history:
                connector: "jpa"
                input: "com.example.OrderHistoryLookup"
                output: "com.example.OrderHistorySnapshot"
                version: "v2"
                jpa:
                  entity: "com.example.OrderHistoryEntity"
                  where:
                    orderId: "input.orderId"
            steps: []
            """));

        assertEquals(2, config.queries().size());
        assertNotNull(config.queries().get("customer-risk-by-id"));
        assertEquals("v2", config.queries().get("order-history").version());
    }

    @Test
    void queryCaptureWithEmptyKeyFieldsIsAllowed() {
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(new StringReader("""
            basePackage: "com.example"
            queries:
              my-query:
                connector: "jpa"
                input: "com.example.LookupType"
                output: "com.example.SnapshotType"
                jpa:
                  entity: "com.example.Entity"
                  where:
                    id: "input.id"
            steps:
              - name: "My Step"
                kind: "query"
                query: "my-query"
                input: "com.example.LookupType"
                output: "com.example.SnapshotType"
                capture:
                  keyFields: []
            """));

        PipelineYamlStep step = config.steps().getFirst();
        assertNotNull(step.queryCapture());
        assertTrue(step.queryCapture().keyFields().isEmpty());
    }
}
