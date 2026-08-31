package org.pipelineframework.examples.ragproof;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pipelineframework.command.CommandDescriptor;
import org.pipelineframework.command.CommandDuplicatePolicy;
import org.pipelineframework.connector.vector.VectorUpsertRequest;

class VectorUpsertCommandIdGeneratorTest {
    private final VectorUpsertCommandIdGenerator generator = new VectorUpsertCommandIdGenerator();

    @Test
    void isStableAndSeparatesFieldsAndVectorValuesUnambiguously() {
        VectorUpsertRequest request = new VectorUpsertRequest("a\nb", "c", List.of(1.0f, -0.0f));

        assertEquals(generator.commandId(descriptor("vector/upsert"), request),
            generator.commandId(descriptor("vector/upsert"), request));
        assertNotEquals(generator.commandId(descriptor("vector/upsert"), request),
            generator.commandId(descriptor("vector/upsert"),
                new VectorUpsertRequest("a", "b\nc", List.of(1.0f, -0.0f))));
        assertNotEquals(generator.commandId(descriptor("vector/upsert"), request),
            generator.commandId(descriptor("vector/upsert"),
                new VectorUpsertRequest("a\nb", "c", List.of(1.0f, 0.0f))));
    }

    private static CommandDescriptor descriptor(String command) {
        return new CommandDescriptor("step", command, VectorUpsertRequest.class.getName(), "result",
            VectorUpsertCommandIdGenerator.class.getName(), CommandDuplicatePolicy.RETURN_RECORDED, Map.of());
    }
}
