package org.pipelineframework.config.template;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CheckoutCanonicalFlowContractTest {

    private static final List<String> CANONICAL_CHAIN = List.of(
        "examples/checkout/config/canonical/01-checkout-pipeline.yaml",
        "examples/checkout/config/canonical/02-consumer-validation-pipeline.yaml",
        "examples/checkout/config/canonical/03-restaurant-acceptance-pipeline.yaml",
        "examples/checkout/config/canonical/04-kitchen-preparation-pipeline.yaml",
        "examples/checkout/config/canonical/05-dispatch-pipeline.yaml",
        "examples/checkout/config/canonical/06-delivery-execution-pipeline.yaml",
        "examples/checkout/config/canonical/07-payment-capture-pipeline.yaml");

    private static final String COMPENSATION_PIPELINE =
        "examples/checkout/config/canonical/08-compensation-failure-pipeline.yaml";

    @Test
    void canonicalFtgoChainHasStrictHandoffContracts() {
        PipelineTemplateConfigLoader loader = new PipelineTemplateConfigLoader();

        List<PipelineTemplateConfig> chain = CANONICAL_CHAIN.stream()
            .map(this::resolveFromWorkspaceRoot)
            .map(loader::load)
            .toList();

        assertEquals(CANONICAL_CHAIN.size(), chain.size(), "Canonical chain file count mismatch.");

        for (int i = 0; i < chain.size(); i++) {
            PipelineTemplateConfig pipeline = chain.get(i);
            String sourceFile = CANONICAL_CHAIN.get(i);
            assertEquals("GRPC", pipeline.transport(),
                "Canonical pipeline transport must be GRPC for " + sourceFile);
            assertFalse(pipeline.steps().isEmpty(), "Canonical pipeline has no steps: " + sourceFile);
        }

        for (int i = 0; i < chain.size() - 1; i++) {
            PipelineTemplateConfig current = chain.get(i);
            PipelineTemplateConfig next = chain.get(i + 1);
            String currentFile = CANONICAL_CHAIN.get(i);
            String nextFile = CANONICAL_CHAIN.get(i + 1);

            PipelineTemplateStep producedStep = current.steps().getLast();
            PipelineTemplateStep expectedStep = next.steps().getFirst();

            String mismatchPrefix = "Pipeline handoff mismatch between "
                + currentFile + " step '" + producedStep.name() + "'"
                + " and " + nextFile + " step '" + expectedStep.name() + "': ";

            assertEquals(producedStep.outputTypeName(), expectedStep.inputTypeName(),
                mismatchPrefix + "type expected=" + producedStep.outputTypeName()
                    + " actual=" + expectedStep.inputTypeName());

            Map<String, PipelineTemplateField> producedFields = byFieldName(producedStep.outputFields());
            Map<String, PipelineTemplateField> expectedFields = byFieldName(expectedStep.inputFields());

            assertEquals(producedFields.keySet(), expectedFields.keySet(),
                mismatchPrefix + "field names expected=" + producedFields.keySet()
                    + " actual=" + expectedFields.keySet());

            for (Map.Entry<String, PipelineTemplateField> entry : producedFields.entrySet()) {
                String fieldName = entry.getKey();
                PipelineTemplateField producedField = entry.getValue();
                PipelineTemplateField expectedField = expectedFields.get(fieldName);

                assertEquals(producedField.type(), expectedField.type(),
                    mismatchPrefix + "java type for field '" + fieldName + "' expected="
                        + producedField.type() + " actual=" + expectedField.type());
                assertEquals(producedField.protoType(), expectedField.protoType(),
                    mismatchPrefix + "proto type for field '" + fieldName + "' expected="
                        + producedField.protoType() + " actual=" + expectedField.protoType());
            }
        }
    }

    @Test
    void compensationPipelineRemainsExplicitFailureTerminal() {
        PipelineTemplateConfigLoader loader = new PipelineTemplateConfigLoader();
        PipelineTemplateConfig compensation = loader.load(resolveFromWorkspaceRoot(COMPENSATION_PIPELINE));

        assertEquals("GRPC", compensation.transport(), "Compensation pipeline transport must be GRPC.");
        assertEquals(1, compensation.steps().size(), "Compensation pipeline should stay single-purpose.");

        PipelineTemplateStep terminal = compensation.steps().getFirst();
        assertEquals("PaymentFailed", terminal.inputTypeName(),
            "Compensation input must remain explicit failure checkpoint type.");
        assertEquals("FailureTerminal", terminal.outputTypeName(),
            "Compensation output must be stable terminal failure type.");

        Map<String, PipelineTemplateField> input = byFieldName(terminal.inputFields());
        Map<String, PipelineTemplateField> output = byFieldName(terminal.outputFields());

        assertTrue(input.containsKey("orderId"), "Compensation input must include orderId.");
        assertTrue(input.containsKey("failureCode"), "Compensation input must include failureCode.");
        assertTrue(output.containsKey("resolutionAction"),
            "Compensation terminal must include resolutionAction for deterministic remediation signatures.");
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
            return explicitCandidate;
        }

        var classpathResource = CheckoutCanonicalFlowContractTest.class.getClassLoader().getResource(relativePath);
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

        throw new AssertionError("Could not resolve checkout canonical config file for '" + relativePath
            + "'. Set system property 'workspace.root' (or env WORKSPACE_ROOT), or ensure file is available in test resources. "
            + "user.dir=" + userDir + ", tried: " + candidates);
    }
}
