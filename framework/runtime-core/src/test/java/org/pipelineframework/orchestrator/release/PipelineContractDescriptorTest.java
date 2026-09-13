package org.pipelineframework.orchestrator.release;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
import org.pipelineframework.orchestrator.composition.PipelineCompositionDescriptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineContractDescriptorTest {

    @Test
    void recordShapeRemainsTheGeneratedContractCompatibilitySurface() {
        assertEquals(
            List.of(
                "schemaVersion",
                "pipelineId",
                "contractVersion",
                "contractHash",
                "platform",
                "transport",
                "module",
                "pluginHost",
                "runtimeLayout",
                "steps",
                "capabilities",
                "canonicalTypes",
                "canonicalCatalogFingerprint",
                "composition",
                "importedDefinitions",
                "capabilityImports"),
            Arrays.stream(PipelineContractDescriptor.class.getRecordComponents())
                .map(RecordComponent::getName)
                .toList());
    }

    @Test
    void constructorSnapshotsNestedContractCollections() {
        Map<String, Object> canonicalFields = new LinkedHashMap<>();
        canonicalFields.put("type", "string");
        Map<String, Map<String, Object>> canonicalTypes = new LinkedHashMap<>();
        canonicalTypes.put("example.Name", canonicalFields);

        PipelineContractDescriptor descriptor = new PipelineContractDescriptor(
            PipelineContractDescriptor.CURRENT_SCHEMA_VERSION,
            "example",
            "contract-v1",
            "sha256:contract",
            "LOCAL",
            "LOCAL",
            "example-module",
            false,
            "MONOLITH",
            List.of(new PipelineBundleStepDescriptor(
                0, "step", "service", "ONE_TO_ONE", "input", "output", "Service", "Step", Map.of())),
            PipelineBundleCapabilities.defaults(),
            canonicalTypes,
            "sha256:catalog",
            PipelineCompositionDescriptor.empty(),
            List.of(),
            List.of());

        canonicalFields.put("format", "uuid");
        canonicalTypes.clear();

        assertEquals(Map.of("type", "string"), descriptor.canonicalTypes().get("example.Name"));
        assertThrows(
            UnsupportedOperationException.class,
            () -> descriptor.canonicalTypes().put("other", Map.of()));
    }

    @Test
    void rejectsContractSchemasNewerThanTheRuntimeUnderstands() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new PipelineContractDescriptor(
                PipelineContractDescriptor.CURRENT_SCHEMA_VERSION + 1,
                "example",
                "contract-v1",
                "sha256:contract",
                "LOCAL",
                "LOCAL",
                "example-module",
                false,
                "MONOLITH",
                List.of(),
                PipelineBundleCapabilities.defaults(),
                Map.of(),
                "sha256:catalog",
                PipelineCompositionDescriptor.empty(),
                List.of(),
                List.of()));
    }
}
