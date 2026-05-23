package org.pipelineframework.awaitable;

import com.google.protobuf.BoolValue;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class AwaitPayloadSupportTest {

    @Test
    void resolvePayloadClassSupportsCanonicalNestedClassNames() throws Exception {
        Class<?> resolved = AwaitPayloadSupport.resolvePayloadClass(
            NestedPayload.Decision.class.getCanonicalName(),
            Thread.currentThread().getContextClassLoader());

        assertEquals(NestedPayload.Decision.class, resolved);
    }

    @Test
    void coercePayloadSupportsProtobufTargets() {
        Object coerced = AwaitPayloadSupport.coercePayload(true, BoolValue.class);

        assertInstanceOf(BoolValue.class, coerced);
        assertEquals(BoolValue.of(true), coerced);
    }

    static final class NestedPayload {
        public record Decision(String value) {
        }
    }
}
