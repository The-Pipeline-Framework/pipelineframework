package org.pipelineframework.config.template;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;

import org.junit.jupiter.api.Test;

class PipelineTemplateRepeatedFieldConstraintsTest {
    @Test
    void zeroMinimumDoesNotNarrowAnUnboundedRepeatedField() {
        var absent = PipelineTemplateRepeatedFieldConstraints.empty();
        var zero = new PipelineTemplateRepeatedFieldConstraints(Optional.of(0), Optional.empty());
        assertEquals(PipelineTemplateWrapperConstraints.Compatibility.UNCHANGED, zero.classifyChangeFrom(absent));
        assertEquals(PipelineTemplateWrapperConstraints.Compatibility.UNCHANGED, absent.classifyChangeFrom(zero));
    }
}
