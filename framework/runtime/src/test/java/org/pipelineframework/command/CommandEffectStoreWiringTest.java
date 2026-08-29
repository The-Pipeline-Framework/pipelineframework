package org.pipelineframework.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import io.quarkus.arc.properties.IfBuildProperty;
import org.junit.jupiter.api.Test;

class CommandEffectStoreWiringTest {

    @Test
    void builtInBeansUseMutuallyExclusiveProviderValues() {
        IfBuildProperty memory = InMemoryCommandEffectStore.class.getAnnotation(IfBuildProperty.class);
        IfBuildProperty dynamo = DynamoCommandEffectStore.class.getAnnotation(IfBuildProperty.class);

        assertEquals("pipeline.command.effect-store.provider", memory.name());
        assertEquals("memory", memory.stringValue());
        assertTrue(memory.enableIfMissing());
        assertEquals("pipeline.command.effect-store.provider", dynamo.name());
        assertEquals("dynamo", dynamo.stringValue());
        assertFalse(dynamo.enableIfMissing());
    }

    @Test
    void configurationExposesMemoryDynamoAndCustomSelections() {
        assertEquals(
            Set.of("MEMORY", "DYNAMO", "CUSTOM"),
            java.util.Arrays.stream(CommandEffectStoreConfig.Provider.values())
                .map(Enum::name)
                .collect(java.util.stream.Collectors.toSet()));
    }
}
