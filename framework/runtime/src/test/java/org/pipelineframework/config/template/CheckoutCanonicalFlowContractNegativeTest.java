package org.pipelineframework.config.template;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CheckoutCanonicalFlowContractNegativeTest {

    private static final String PRODUCER_CONFIG =
        "examples/checkout/config/canonical-negative/01-producer-pipeline.yaml";
    private static final String CONSUMER_TYPE_MISMATCH_CONFIG =
        "examples/checkout/config/canonical-negative/02-consumer-type-mismatch-pipeline.yaml";
    private static final String CONSUMER_FIELD_MISMATCH_CONFIG =
        "examples/checkout/config/canonical-negative/03-consumer-field-mismatch-pipeline.yaml";

    @Test
    void failsFastWithProducerAndConsumerIdentityOnTypeMismatch() {
        PipelineTemplateConfig producer = load(PRODUCER_CONFIG);
        PipelineTemplateConfig consumer = load(CONSUMER_TYPE_MISMATCH_CONFIG);

        AssertionError ex = assertThrows(AssertionError.class, () ->
            assertHandoffCompatible(
                producer.steps().getLast(),
                consumer.steps().getFirst(),
                PRODUCER_CONFIG,
                CONSUMER_TYPE_MISMATCH_CONFIG));

        assertTrue(ex.getMessage().contains(PRODUCER_CONFIG));
        assertTrue(ex.getMessage().contains(CONSUMER_TYPE_MISMATCH_CONFIG));
        assertTrue(ex.getMessage().contains("Producer Terminal"));
        assertTrue(ex.getMessage().contains("Consumer Entry Type Mismatch"));
        assertTrue(ex.getMessage().contains("type expected=OrderPending actual=OrderApproved"));
    }

    @Test
    void failsFastWithProducerAndConsumerIdentityOnFieldMismatch() {
        PipelineTemplateConfig producer = load(PRODUCER_CONFIG);
        PipelineTemplateConfig consumer = load(CONSUMER_FIELD_MISMATCH_CONFIG);

        AssertionError ex = assertThrows(AssertionError.class, () ->
            assertHandoffCompatible(
                producer.steps().getLast(),
                consumer.steps().getFirst(),
                PRODUCER_CONFIG,
                CONSUMER_FIELD_MISMATCH_CONFIG));

        assertTrue(ex.getMessage().contains(PRODUCER_CONFIG));
        assertTrue(ex.getMessage().contains(CONSUMER_FIELD_MISMATCH_CONFIG));
        assertTrue(ex.getMessage().contains("Producer Terminal"));
        assertTrue(ex.getMessage().contains("Consumer Entry Field Mismatch"));
        assertTrue(ex.getMessage().contains("field names expected="));
        assertTrue(ex.getMessage().contains("customerId"));
        assertTrue(ex.getMessage().contains("restaurantId"));
    }

    private PipelineTemplateConfig load(String path) {
        return new PipelineTemplateConfigLoader().load(resolveFromWorkspaceRoot(path));
    }

    private void assertHandoffCompatible(
        PipelineTemplateStep producedStep,
        PipelineTemplateStep expectedStep,
        String currentFile,
        String nextFile
    ) {
        String mismatchPrefix = "Pipeline handoff mismatch between "
            + currentFile + " step '" + producedStep.name() + "'"
            + " and " + nextFile + " step '" + expectedStep.name() + "': ";

        if (!producedStep.outputTypeName().equals(expectedStep.inputTypeName())) {
            throw new AssertionError(mismatchPrefix + "type expected=" + producedStep.outputTypeName()
                + " actual=" + expectedStep.inputTypeName());
        }

        Map<String, PipelineTemplateField> producedFields = byFieldName(producedStep.outputFields());
        Map<String, PipelineTemplateField> expectedFields = byFieldName(expectedStep.inputFields());
        if (!producedFields.keySet().equals(expectedFields.keySet())) {
            throw new AssertionError(mismatchPrefix + "field names expected=" + producedFields.keySet()
                + " actual=" + expectedFields.keySet());
        }

        for (Map.Entry<String, PipelineTemplateField> entry : producedFields.entrySet()) {
            String fieldName = entry.getKey();
            PipelineTemplateField producedField = entry.getValue();
            PipelineTemplateField expectedField = expectedFields.get(fieldName);
            if (!producedField.type().equals(expectedField.type())) {
                throw new AssertionError(mismatchPrefix + "java type for field '" + fieldName
                    + "' expected=" + producedField.type() + " actual=" + expectedField.type());
            }
            if (!producedField.protoType().equals(expectedField.protoType())) {
                throw new AssertionError(mismatchPrefix + "proto type for field '" + fieldName
                    + "' expected=" + producedField.protoType() + " actual=" + expectedField.protoType());
            }
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
        String explicitWorkspaceRoot = System.getProperty("workspace.root");
        if (explicitWorkspaceRoot != null && !explicitWorkspaceRoot.isBlank()) {
            Path explicitRoot = Path.of(explicitWorkspaceRoot).normalize();
            Path explicitCandidate = explicitRoot.resolve(relativePath).normalize();
            if (!Files.exists(explicitCandidate)) {
                throw new AssertionError("workspace.root is set but file was not found: " + explicitCandidate);
            }
            return explicitCandidate;
        }

        String envWorkspaceRoot = System.getenv("WORKSPACE_ROOT");
        if (envWorkspaceRoot != null && !envWorkspaceRoot.isBlank()) {
            Path envRoot = Path.of(envWorkspaceRoot).normalize();
            Path envCandidate = envRoot.resolve(relativePath).normalize();
            if (Files.exists(envCandidate)) {
                return envCandidate;
            }
        }

        var classpathResource = CheckoutCanonicalFlowContractNegativeTest.class.getClassLoader().getResource(relativePath);
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

        throw new AssertionError("Could not resolve checkout canonical negative config file for '" + relativePath
            + "'. Set system property 'workspace.root' (or env WORKSPACE_ROOT), or ensure file is available in test resources. "
            + "user.dir=" + userDir + ", tried: " + candidates);
    }
}
