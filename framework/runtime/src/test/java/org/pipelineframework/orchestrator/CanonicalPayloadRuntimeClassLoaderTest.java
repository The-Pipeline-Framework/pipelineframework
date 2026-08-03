package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class CanonicalPayloadRuntimeClassLoaderTest {

    @Test
    void resolvesNestedTypesNamedWithJavaSourceNotation() throws Exception {
        String sourceName = PipelineTypes.OrderApproved.class.getCanonicalName();

        assertEquals(PipelineTypes.OrderApproved.class,
            CanonicalPayloadRuntimeClassLoader.load(sourceName, getClass().getClassLoader()));
    }

    @Test
    void retainsTheOriginalClassNotFoundFailureForUnknownBindings() {
        assertThrows(ClassNotFoundException.class,
            () -> CanonicalPayloadRuntimeClassLoader.load(
                "org.pipelineframework.Missing.Nested", getClass().getClassLoader()));
    }

    static final class PipelineTypes {
        static final class OrderApproved {
        }
    }
}
