package org.pipelineframework.checkout.common.connector;

import com.google.protobuf.Descriptors;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConnectorUtilsTest {

    @Test
    void deterministicHandoffKeyIsStableForSameInputs() {
        String first = ConnectorUtils.deterministicHandoffKey("create-to-deliver", "order-1", "cust-1", "2026-03-05T10:00:00Z");
        String second = ConnectorUtils.deterministicHandoffKey("create-to-deliver", "order-1", "cust-1", "2026-03-05T10:00:00Z");

        assertEquals(first, second, "Deterministic handoff key must be stable for identical inputs.");
    }

    @Test
    void deterministicHandoffKeyDoesNotCollideOnDelimiterLikeInputs() {
        String single = ConnectorUtils.deterministicHandoffKey("handoff", "a|b");
        String split = ConnectorUtils.deterministicHandoffKey("handoff", "a", "b");

        assertNotEquals(single, split,
            "Length-prefixed seed should avoid ambiguous delimiter collisions.");
    }

    @Test
    void deterministicHandoffKeyDiffersAcrossNamespaces() {
        String create = ConnectorUtils.deterministicHandoffKey("create-to-deliver", "same", "payload");
        String deliver = ConnectorUtils.deterministicHandoffKey("deliver-to-next", "same", "payload");

        assertNotEquals(create, deliver, "Namespace must scope deterministic handoff keys.");
    }

    @Test
    void failureSignatureIsStructuredAndNormalizesBlanks() {
        String signature = ConnectorUtils.failureSignature(" ", "mapping", "missing_fields", "", "id-1");

        assertTrue(signature.contains("connector=unknown"));
        assertTrue(signature.contains("phase=mapping"));
        assertTrue(signature.contains("reason=missing_fields"));
        assertTrue(signature.contains("traceId=na"));
        assertTrue(signature.contains("itemId=id-1"));
    }

    @Test
    void normalizeBackpressureStrategyReturnsBufferForNull() {
        assertEquals(ConnectorUtils.BACKPRESSURE_BUFFER, ConnectorUtils.normalizeBackpressureStrategy(null));
    }

    @Test
    void normalizeBackpressureStrategyReturnsBufferForBlank() {
        assertEquals(ConnectorUtils.BACKPRESSURE_BUFFER, ConnectorUtils.normalizeBackpressureStrategy(""));
        assertEquals(ConnectorUtils.BACKPRESSURE_BUFFER, ConnectorUtils.normalizeBackpressureStrategy("   "));
    }

    @Test
    void normalizeBackpressureStrategyReturnsBufferForInvalidValue() {
        assertEquals(ConnectorUtils.BACKPRESSURE_BUFFER, ConnectorUtils.normalizeBackpressureStrategy("INVALID"));
        assertEquals(ConnectorUtils.BACKPRESSURE_BUFFER, ConnectorUtils.normalizeBackpressureStrategy("random"));
    }

    @Test
    void normalizeBackpressureStrategyAcceptsBufferCaseInsensitive() {
        assertEquals(ConnectorUtils.BACKPRESSURE_BUFFER, ConnectorUtils.normalizeBackpressureStrategy("BUFFER"));
        assertEquals(ConnectorUtils.BACKPRESSURE_BUFFER, ConnectorUtils.normalizeBackpressureStrategy("buffer"));
        assertEquals(ConnectorUtils.BACKPRESSURE_BUFFER, ConnectorUtils.normalizeBackpressureStrategy("BuFfEr"));
    }

    @Test
    void normalizeBackpressureStrategyAcceptsDropCaseInsensitive() {
        assertEquals(ConnectorUtils.BACKPRESSURE_DROP, ConnectorUtils.normalizeBackpressureStrategy("DROP"));
        assertEquals(ConnectorUtils.BACKPRESSURE_DROP, ConnectorUtils.normalizeBackpressureStrategy("drop"));
        assertEquals(ConnectorUtils.BACKPRESSURE_DROP, ConnectorUtils.normalizeBackpressureStrategy("DrOp"));
    }

    @Test
    void applyBackpressureDropsOnOverflow() {
        Multi<Integer> source = Multi.createFrom().range(1, 1001);
        Multi<Integer> result = ConnectorUtils.applyBackpressure(source, ConnectorUtils.BACKPRESSURE_DROP, 10);

        AssertSubscriber<Integer> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(5));
        subscriber.request(5).assertCompleted();
        assertEquals(5, subscriber.getItems().size());
    }

    @Test
    void applyBackpressureBuffersWithPositiveCapacity() {
        Multi<Integer> source = Multi.createFrom().items(1, 2, 3, 4, 5);
        Multi<Integer> result = ConnectorUtils.applyBackpressure(source, ConnectorUtils.BACKPRESSURE_BUFFER, 10);

        AssertSubscriber<Integer> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));
        subscriber.assertCompleted();
        assertEquals(5, subscriber.getItems().size());
    }

    @Test
    void applyBackpressureUsesDefaultCapacityWhenZero() {
        Multi<Integer> source = Multi.createFrom().items(1, 2, 3);
        Multi<Integer> result = ConnectorUtils.applyBackpressure(source, ConnectorUtils.BACKPRESSURE_BUFFER, 0);

        AssertSubscriber<Integer> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));
        subscriber.assertCompleted();
        assertEquals(3, subscriber.getItems().size());
    }

    @Test
    void applyBackpressureUsesDefaultCapacityWhenNegative() {
        Multi<Integer> source = Multi.createFrom().items(1, 2, 3);
        Multi<Integer> result = ConnectorUtils.applyBackpressure(source, ConnectorUtils.BACKPRESSURE_BUFFER, -1);

        AssertSubscriber<Integer> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));
        subscriber.assertCompleted();
        assertEquals(3, subscriber.getItems().size());
    }

    @Test
    void applyBackpressureThrowsOnInvalidStrategy() {
        Multi<Integer> source = Multi.createFrom().items(1, 2, 3);
        assertThrows(IllegalStateException.class, () ->
            ConnectorUtils.applyBackpressure(source, "INVALID", 10));
    }

    @Test
    void readFieldReturnsStringValue() {
        com.google.protobuf.Message message = mock(com.google.protobuf.Message.class);
        Descriptors.Descriptor descriptor = mock(Descriptors.Descriptor.class);
        Descriptors.FieldDescriptor field = mock(Descriptors.FieldDescriptor.class);

        when(message.getDescriptorForType()).thenReturn(descriptor);
        when(descriptor.findFieldByName("test_field")).thenReturn(field);
        when(field.isRepeated()).thenReturn(false);
        when(field.isMapField()).thenReturn(false);
        when(field.getJavaType()).thenReturn(Descriptors.FieldDescriptor.JavaType.STRING);
        when(message.getField(field)).thenReturn("value123");

        String result = ConnectorUtils.readField(message, "test_field");
        assertEquals("value123", result);
    }

    @Test
    void readFieldReturnsIntValue() {
        com.google.protobuf.Message message = mock(com.google.protobuf.Message.class);
        Descriptors.Descriptor descriptor = mock(Descriptors.Descriptor.class);
        Descriptors.FieldDescriptor field = mock(Descriptors.FieldDescriptor.class);

        when(message.getDescriptorForType()).thenReturn(descriptor);
        when(descriptor.findFieldByName("count")).thenReturn(field);
        when(field.isRepeated()).thenReturn(false);
        when(field.isMapField()).thenReturn(false);
        when(field.getJavaType()).thenReturn(Descriptors.FieldDescriptor.JavaType.INT);
        when(message.getField(field)).thenReturn(42);

        String result = ConnectorUtils.readField(message, "count");
        assertEquals("42", result);
    }

    @Test
    void readFieldReturnsEmptyForNonexistentField() {
        com.google.protobuf.Message message = mock(com.google.protobuf.Message.class);
        Descriptors.Descriptor descriptor = mock(Descriptors.Descriptor.class);

        when(message.getDescriptorForType()).thenReturn(descriptor);
        when(descriptor.findFieldByName("nonexistent")).thenReturn(null);

        String result = ConnectorUtils.readField(message, "nonexistent");
        assertEquals("", result);
    }

    @Test
    void readFieldReturnsEmptyForRepeatedField() {
        com.google.protobuf.Message message = mock(com.google.protobuf.Message.class);
        Descriptors.Descriptor descriptor = mock(Descriptors.Descriptor.class);
        Descriptors.FieldDescriptor field = mock(Descriptors.FieldDescriptor.class);

        when(message.getDescriptorForType()).thenReturn(descriptor);
        when(descriptor.findFieldByName("repeated_field")).thenReturn(field);
        when(field.isRepeated()).thenReturn(true);

        String result = ConnectorUtils.readField(message, "repeated_field");
        assertEquals("", result);
    }

    @Test
    void readFieldReturnsEmptyForMapField() {
        com.google.protobuf.Message message = mock(com.google.protobuf.Message.class);
        Descriptors.Descriptor descriptor = mock(Descriptors.Descriptor.class);
        Descriptors.FieldDescriptor field = mock(Descriptors.FieldDescriptor.class);

        when(message.getDescriptorForType()).thenReturn(descriptor);
        when(descriptor.findFieldByName("map_field")).thenReturn(field);
        when(field.isRepeated()).thenReturn(false);
        when(field.isMapField()).thenReturn(true);

        String result = ConnectorUtils.readField(message, "map_field");
        assertEquals("", result);
    }

    @Test
    void readFieldReturnsEmptyForByteStringField() {
        com.google.protobuf.Message message = mock(com.google.protobuf.Message.class);
        Descriptors.Descriptor descriptor = mock(Descriptors.Descriptor.class);
        Descriptors.FieldDescriptor field = mock(Descriptors.FieldDescriptor.class);

        when(message.getDescriptorForType()).thenReturn(descriptor);
        when(descriptor.findFieldByName("bytes_field")).thenReturn(field);
        when(field.isRepeated()).thenReturn(false);
        when(field.isMapField()).thenReturn(false);
        when(field.getJavaType()).thenReturn(Descriptors.FieldDescriptor.JavaType.BYTE_STRING);

        String result = ConnectorUtils.readField(message, "bytes_field");
        assertEquals("", result);
    }

    @Test
    void readFieldReturnsEmptyForMessageField() {
        com.google.protobuf.Message message = mock(com.google.protobuf.Message.class);
        Descriptors.Descriptor descriptor = mock(Descriptors.Descriptor.class);
        Descriptors.FieldDescriptor field = mock(Descriptors.FieldDescriptor.class);

        when(message.getDescriptorForType()).thenReturn(descriptor);
        when(descriptor.findFieldByName("message_field")).thenReturn(field);
        when(field.isRepeated()).thenReturn(false);
        when(field.isMapField()).thenReturn(false);
        when(field.getJavaType()).thenReturn(Descriptors.FieldDescriptor.JavaType.MESSAGE);

        String result = ConnectorUtils.readField(message, "message_field");
        assertEquals("", result);
    }

    @Test
    void normalizeOrDefaultReturnsValueWhenNonBlank() {
        assertEquals("value", ConnectorUtils.normalizeOrDefault("value", "fallback"));
        assertEquals("trimmed", ConnectorUtils.normalizeOrDefault("  trimmed  ", "fallback"));
    }

    @Test
    void normalizeOrDefaultReturnsFallbackWhenNull() {
        assertEquals("fallback", ConnectorUtils.normalizeOrDefault(null, "fallback"));
    }

    @Test
    void normalizeOrDefaultReturnsFallbackWhenBlank() {
        assertEquals("fallback", ConnectorUtils.normalizeOrDefault("", "fallback"));
        assertEquals("fallback", ConnectorUtils.normalizeOrDefault("   ", "fallback"));
    }

    @Test
    void deterministicHandoffKeyHandlesNullComponents() {
        String result = ConnectorUtils.deterministicHandoffKey("namespace", (String[]) null);
        assertTrue(result.startsWith("namespace:"));
    }

    @Test
    void deterministicHandoffKeyHandlesEmptyComponents() {
        String result = ConnectorUtils.deterministicHandoffKey("namespace");
        assertTrue(result.startsWith("namespace:"));
    }

    @Test
    void deterministicHandoffKeyUsesDefaultNamespaceWhenNull() {
        String result = ConnectorUtils.deterministicHandoffKey(null, "comp1");
        assertTrue(result.startsWith("handoff:"));
    }

    @Test
    void deterministicHandoffKeyUsesDefaultNamespaceWhenBlank() {
        String result = ConnectorUtils.deterministicHandoffKey("  ", "comp1");
        assertTrue(result.startsWith("handoff:"));
    }

    @Test
    void failureSignatureHandlesAllNullValues() {
        String signature = ConnectorUtils.failureSignature(null, null, null, null, null);

        assertTrue(signature.contains("connector=unknown"));
        assertTrue(signature.contains("phase=unknown"));
        assertTrue(signature.contains("reason=unspecified"));
        assertTrue(signature.contains("traceId=na"));
        assertTrue(signature.contains("itemId=na"));
    }

    @Test
    void failureSignatureDoesNotTrimNonBlankValues() {
        String signature = ConnectorUtils.failureSignature("conn1", "phase2", "reason3", "trace4", "item5");

        assertTrue(signature.contains("connector=conn1"));
        assertTrue(signature.contains("phase=phase2"));
        assertTrue(signature.contains("reason=reason3"));
        assertTrue(signature.contains("traceId=trace4"));
        assertTrue(signature.contains("itemId=item5"));
    }
}