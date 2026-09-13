package org.pipelineframework.processor.renderer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class JavaPoetLiteralTest {

    @Test
    void rendersNonFiniteFloatingPointValuesAsJavaConstants() {
        assertEquals("java.lang.Float.NaN", JavaPoetLiteral.value(Float.NaN).toString());
        assertEquals("java.lang.Double.POSITIVE_INFINITY",
            JavaPoetLiteral.value(Double.POSITIVE_INFINITY).toString());
        assertEquals("java.lang.Double.NEGATIVE_INFINITY",
            JavaPoetLiteral.value(Double.NEGATIVE_INFINITY).toString());
    }
}
