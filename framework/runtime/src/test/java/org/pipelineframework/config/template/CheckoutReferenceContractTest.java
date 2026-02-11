package org.pipelineframework.config.template;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CheckoutReferenceContractTest {

    private static final String CREATE_ORDER_CONFIG = "examples/checkout/config/create-order-pipeline.yaml";
    private static final String DELIVER_ORDER_CONFIG = "examples/checkout/config/deliver-order-pipeline.yaml";

    @Test
    void checkpointOutputMatchesNextPipelineInputContract() {
        PipelineTemplateConfigLoader loader = new PipelineTemplateConfigLoader();

        PipelineTemplateConfig createOrder = loader.load(resolveFromWorkspaceRoot(CREATE_ORDER_CONFIG));
        PipelineTemplateConfig deliverOrder = loader.load(resolveFromWorkspaceRoot(DELIVER_ORDER_CONFIG));

        assertEquals("GRPC", createOrder.transport(), "Create-order transport should be GRPC.");
        assertEquals("GRPC", deliverOrder.transport(), "Deliver-order transport should be GRPC.");
        assertFalse(createOrder.steps().isEmpty(), "Create-order pipeline must define steps.");
        assertFalse(deliverOrder.steps().isEmpty(), "Deliver-order pipeline must define steps.");

        PipelineTemplateStep createOrderTerminalStep = createOrder.steps().getLast();
        PipelineTemplateStep deliverOrderEntryStep = deliverOrder.steps().getFirst();

        assertEquals(createOrderTerminalStep.outputTypeName(), deliverOrderEntryStep.inputTypeName(),
            "Pipeline handoff type mismatch between create-order output and deliver-order input.");

        Map<String, PipelineTemplateField> createOrderOutputFields = byFieldName(createOrderTerminalStep.outputFields());
        Map<String, PipelineTemplateField> deliverOrderInputFields = byFieldName(deliverOrderEntryStep.inputFields());

        assertEquals(createOrderOutputFields.keySet(), deliverOrderInputFields.keySet(),
            "Pipeline handoff fields mismatch between create-order output and deliver-order input.");

        for (Map.Entry<String, PipelineTemplateField> entry : createOrderOutputFields.entrySet()) {
            String fieldName = entry.getKey();
            PipelineTemplateField outputField = entry.getValue();
            PipelineTemplateField inputField = deliverOrderInputFields.get(fieldName);
            assertEquals(outputField.type(), inputField.type(),
                "Java type mismatch for handoff field: " + fieldName);
            assertEquals(outputField.protoType(), inputField.protoType(),
                "Proto type mismatch for handoff field: " + fieldName);
        }
    }

    private Map<String, PipelineTemplateField> byFieldName(List<PipelineTemplateField> fields) {
        Map<String, PipelineTemplateField> byName = new LinkedHashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            PipelineTemplateField field = fields.get(i);
            if (field == null || field.name() == null || field.name().isBlank()) {
                throw new IllegalStateException("Invalid field definition at index " + i + ": " + field);
            }
            if (byName.containsKey(field.name())) {
                throw new IllegalStateException("Duplicate field name: " + field.name());
            }
            byName.put(field.name(), field);
        }
        return byName;
    }

    private Path resolveFromWorkspaceRoot(String relativePath) {
        String workspaceRoot = System.getProperty("workspace.root");
        if (workspaceRoot == null || workspaceRoot.isBlank()) {
            workspaceRoot = System.getenv("WORKSPACE_ROOT");
        }
        if (workspaceRoot != null && !workspaceRoot.isBlank()) {
            Path explicitRoot = Path.of(workspaceRoot).normalize();
            Path explicitCandidate = explicitRoot.resolve(relativePath).normalize();
            if (Files.exists(explicitCandidate)) {
                return explicitCandidate;
            }
        }

        var classpathResource = CheckoutReferenceContractTest.class.getClassLoader().getResource(relativePath);
        if (classpathResource != null && "file".equalsIgnoreCase(classpathResource.getProtocol())) {
            return Path.of(java.net.URI.create(classpathResource.toString())).normalize();
        }

        Path userDir = Path.of(System.getProperty("user.dir")).normalize();
        List<Path> candidates = List.of(
            userDir.resolve(relativePath).normalize(),
            userDir.resolve("..").resolve(relativePath).normalize(),
            userDir.resolve("..").resolve("..").resolve(relativePath).normalize());

        Optional<Path> resolved = candidates.stream().filter(Files::exists).findFirst();
        if (resolved.isPresent()) {
            return resolved.get();
        }

        throw new AssertionError("Could not resolve checkout reference config file for '" + relativePath +
            "'. Set system property 'workspace.root' (or env WORKSPACE_ROOT), or ensure file is available in test resources. " +
            "user.dir=" + userDir + ", tried: " + candidates);
    }
}
