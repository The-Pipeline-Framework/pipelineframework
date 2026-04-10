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

    private static final String CHECKOUT_CONFIG = "examples/checkout/checkout-orchestrator-svc/pipeline.yaml";
    private static final String CONSUMER_VALIDATION_CONFIG =
        "examples/checkout/consumer-validation-orchestrator-svc/pipeline.yaml";

    @Test
    void checkpointOutputMatchesNextPipelineInputContract() {
        PipelineTemplateConfigLoader loader = new PipelineTemplateConfigLoader();

        PipelineTemplateConfig checkout = loader.load(resolveFromWorkspaceRoot(CHECKOUT_CONFIG));
        PipelineTemplateConfig consumerValidation =
            loader.load(resolveFromWorkspaceRoot(CONSUMER_VALIDATION_CONFIG));

        assertEquals("GRPC", checkout.transport(), "Checkout transport should be GRPC.");
        assertEquals("GRPC", consumerValidation.transport(), "Consumer-validation transport should be GRPC.");
        assertFalse(checkout.steps().isEmpty(), "Checkout pipeline must define steps.");
        assertFalse(consumerValidation.steps().isEmpty(), "Consumer-validation pipeline must define steps.");

        PipelineTemplateStep checkoutTerminalStep = checkout.steps().getLast();
        PipelineTemplateStep consumerValidationEntryStep = consumerValidation.steps().getFirst();

        assertEquals(checkoutTerminalStep.outputTypeName(), consumerValidationEntryStep.inputTypeName(),
            "Pipeline handoff type mismatch between checkout output and consumer-validation input.");

        Map<String, PipelineTemplateField> checkoutOutputFields = byFieldName(checkoutTerminalStep.outputFields());
        Map<String, PipelineTemplateField> consumerValidationInputFields =
            byFieldName(consumerValidationEntryStep.inputFields());

        assertEquals(checkoutOutputFields.keySet(), consumerValidationInputFields.keySet(),
            "Pipeline handoff fields mismatch between checkout output and consumer-validation input.");

        for (Map.Entry<String, PipelineTemplateField> entry : checkoutOutputFields.entrySet()) {
            String fieldName = entry.getKey();
            PipelineTemplateField outputField = entry.getValue();
            PipelineTemplateField inputField = consumerValidationInputFields.get(fieldName);
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
            if (!Files.exists(explicitCandidate)) {
                throw new AssertionError("workspace.root/WORKSPACE_ROOT is set but file was not found: " + explicitCandidate);
            }
            if (Files.exists(explicitCandidate)) {
                return explicitCandidate;
            }
        }

        var classpathResource = CheckoutReferenceContractTest.class.getClassLoader().getResource(relativePath);
        if (classpathResource != null && "file".equalsIgnoreCase(classpathResource.getProtocol())) {
            try {
                return Path.of(classpathResource.toURI()).normalize();
            } catch (java.net.URISyntaxException e) {
                throw new AssertionError("Could not convert classpath resource URI: " + classpathResource, e);
            }
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
