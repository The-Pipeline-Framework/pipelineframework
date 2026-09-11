package org.pipelineframework.examples.openapiproof;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/** Provisions the existing TPF table contracts for the packaged restart test. */
final class DynamoProofTables {
    private final DynamoDbClient client;

    DynamoProofTables(DynamoDbClient client) { this.client = client; }

    void create() {
        table("tpf_execution", "tenant_id", Optional.of("execution_id"), List.of());
        table("tpf_execution_key", "tenant_execution_key", Optional.empty(), List.of());
        table("tpf_execution_payload", "payload_id", Optional.of("payload_part"), List.of());
        table("tpf_await_unit", "tenant_id", Optional.of("unit_id"), List.of());
        table("tpf_await_interaction_key", "lookup_key", Optional.empty(), List.of());
        table("tpf_await_interaction", "tenant_id", Optional.of("interaction_id"), List.of(
            new Index("await-interaction-by-unit", "query_unit_key", "query_unit_sort"),
            new Index("await-interaction-pending-by-tenant", "query_pending_tenant_key", "query_pending_deadline_sort"),
            new Index("await-interaction-pending-by-assignee", "query_pending_assignee_key", "query_pending_deadline_sort"),
            new Index("await-interaction-pending-by-group", "query_pending_group_key", "query_pending_deadline_sort"),
            new Index("await-interaction-pending-by-step", "query_pending_step_key", "query_pending_deadline_sort"),
            new Index("await-interaction-pending-by-deadline", "query_deadline_key", "query_deadline_sort")));
        numericTable("tpf_await_admission", "scope_key", "slot");
        numericTable("tpf_command_effect", "command_key", "revision");
    }

    private void table(String name, String partition, Optional<String> sort, List<Index> indexes) {
        var attributes = new LinkedHashMap<String, ScalarAttributeType>();
        attributes.put(partition, ScalarAttributeType.S);
        sort.ifPresent(key -> attributes.put(key, ScalarAttributeType.S));
        indexes.forEach(index -> {
            attributes.put(index.partition(), ScalarAttributeType.S);
            attributes.put(index.sort(), ScalarAttributeType.S);
        });
        var keys = new java.util.ArrayList<KeySchemaElement>();
        keys.add(key(partition, KeyType.HASH));
        sort.ifPresent(value -> keys.add(key(value, KeyType.RANGE)));
        var request = CreateTableRequest.builder().tableName(name).billingMode(BillingMode.PAY_PER_REQUEST)
            .keySchema(keys).attributeDefinitions(attributes.entrySet().stream().map(entry ->
                AttributeDefinition.builder().attributeName(entry.getKey()).attributeType(entry.getValue()).build()).toList());
        if (!indexes.isEmpty()) request.globalSecondaryIndexes(indexes.stream().map(index ->
            GlobalSecondaryIndex.builder().indexName(index.name())
                .keySchema(key(index.partition(), KeyType.HASH), key(index.sort(), KeyType.RANGE))
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build()).build()).toList());
        client.createTable(request.build());
    }

    private void numericTable(String name, String partition, String sort) {
        client.createTable(CreateTableRequest.builder().tableName(name).billingMode(BillingMode.PAY_PER_REQUEST)
            .keySchema(key(partition, KeyType.HASH), key(sort, KeyType.RANGE))
            .attributeDefinitions(
                AttributeDefinition.builder().attributeName(partition).attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName(sort).attributeType(ScalarAttributeType.N).build()).build());
    }

    private KeySchemaElement key(String name, KeyType type) {
        return KeySchemaElement.builder().attributeName(name).keyType(type).build();
    }

    private record Index(String name, String partition, String sort) {}
}
