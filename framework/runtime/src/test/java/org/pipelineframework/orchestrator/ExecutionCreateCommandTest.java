package org.pipelineframework.orchestrator;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ExecutionCreateCommandTest {

    @Test
    void createsCommandWithAllFields() {
        Object payload = Map.of("key", "value");
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 86400;

        ExecutionCreateCommand command = new ExecutionCreateCommand(
            "tenant1",
            "exec-key-1",
            payload,
            now,
            ttl);

        assertEquals("tenant1", command.tenantId());
        assertEquals("exec-key-1", command.executionKey());
        assertEquals(payload, command.inputPayload());
        assertEquals(now, command.nowEpochMs());
        assertEquals(ttl, command.ttlEpochS());
    }

    @Test
    void handlesNullPayload() {
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 86400;

        ExecutionCreateCommand command = new ExecutionCreateCommand(
            "tenant2",
            "exec-key-2",
            null,
            now,
            ttl);

        assertNull(command.inputPayload());
    }

    @Test
    void preservesComplexPayload() {
        Map<String, Object> complexPayload = Map.of(
            "field1", "value1",
            "field2", 123,
            "nested", Map.of("inner", "data"));

        ExecutionCreateCommand command = new ExecutionCreateCommand(
            "tenant3",
            "exec-key-3",
            complexPayload,
            1234567890L,
            7654321L);

        assertEquals(complexPayload, command.inputPayload());
        @SuppressWarnings("unchecked")
        Map<String, Object> retrievedPayload = (Map<String, Object>) command.inputPayload();
        assertEquals("value1", retrievedPayload.get("field1"));
        assertEquals(123, retrievedPayload.get("field2"));
    }

    @Test
    void timestampsArePreserved() {
        long now = 1234567890123L;
        long ttl = 7654321L;

        ExecutionCreateCommand command = new ExecutionCreateCommand(
            "tenant4",
            "exec-key-4",
            "payload",
            now,
            ttl);

        assertEquals(now, command.nowEpochMs());
        assertEquals(ttl, command.ttlEpochS());
    }

    @Test
    void executionKeyIsPreserved() {
        String key = "custom-idempotency-key-12345";

        ExecutionCreateCommand command = new ExecutionCreateCommand(
            "tenant5",
            key,
            "payload",
            System.currentTimeMillis(),
            System.currentTimeMillis() / 1000 + 3600);

        assertEquals(key, command.executionKey());
    }

    @Test
    void rejectsNullExecutionKey() {
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;

        assertThrows(NullPointerException.class, () -> new ExecutionCreateCommand(
            "tenant6",
            null,
            "payload",
            now,
            ttl));
    }

    @Test
    void rejectsNullTenantId() {
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;

        NullPointerException failure = assertThrows(NullPointerException.class, () -> new ExecutionCreateCommand(
            null,
            "exec-key-7",
            "payload",
            now,
            ttl));
        assertEquals("ExecutionCreateCommand.tenantId must not be null", failure.getMessage());
    }
}
