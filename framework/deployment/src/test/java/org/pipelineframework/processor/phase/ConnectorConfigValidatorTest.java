package org.pipelineframework.processor.phase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.connector.ConnectorConfig;
import org.pipelineframework.config.connector.ConnectorSourceConfig;
import org.pipelineframework.config.connector.ConnectorTargetConfig;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateStep;

class ConnectorConfigValidatorTest {

    private final ConnectorConfigValidator validator = new ConnectorConfigValidator();

    @Test
    void validateReturnsEmptyListWhenTemplateConfigIsNull() {
        List<ConnectorConfig> result = validator.validate(null, List.of(), null, null);
        assertTrue(result.isEmpty());
    }

    @Test
    void validateReturnsEmptyListWhenConnectorsIsNull() {
        PipelineTemplateConfig config = createTemplateConfig(List.of());
        List<ConnectorConfig> result = validator.validate(config, List.of(), null, null);
        assertTrue(result.isEmpty());
    }

    @Test
    void validateReturnsEmptyListWhenConnectorsIsEmpty() {
        PipelineTemplateConfig config = createTemplateConfig(List.of());
        List<ConnectorConfig> result = validator.validate(config, List.of(), null, null);
        assertTrue(result.isEmpty());
    }

    @Test
    void validateFailsWhenConnectorNameIsNull() {
        assertThrows(NullPointerException.class, () -> new ConnectorConfig(
            null,
            true,
            new ConnectorSourceConfig("OUTPUT_BUS", "step", "Type"),
            new ConnectorTargetConfig("LIVE_INGEST", "pipeline", "Type", "Adapter"),
            null,
            "GRPC",
            "PRE_FORWARD",
            "BUFFER",
            "PROPAGATE",
            256,
            10000,
            List.of(),
            null));
    }

    @Test
    void validateFailsWhenConnectorNameIsBlank() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> new ConnectorConfig(
            "   ",
            true,
            new ConnectorSourceConfig("OUTPUT_BUS", "step", "Type"),
            new ConnectorTargetConfig("LIVE_INGEST", "pipeline", "Type", "Adapter"),
            null,
            "GRPC",
            "PRE_FORWARD",
            "BUFFER",
            "PROPAGATE",
            256,
            10000,
            List.of(),
            null));
        assertTrue(exception.getMessage().contains("must not be blank"));
    }

    @Test
    void validateFailsWhenDuplicateConnectorNames() {
        ConnectorConfig connector1 = new ConnectorConfig(
            "duplicate-name",
            true,
            new ConnectorSourceConfig("OUTPUT_BUS", "step1", "Type1"),
            new ConnectorTargetConfig("LIVE_INGEST", "pipeline1", "Type1", "Adapter1"),
            null,
            "GRPC",
            "PRE_FORWARD",
            "BUFFER",
            "PROPAGATE",
            256,
            10000,
            List.of(),
            null);

        ConnectorConfig connector2 = new ConnectorConfig(
            "duplicate-name",
            true,
            new ConnectorSourceConfig("OUTPUT_BUS", "step2", "Type2"),
            new ConnectorTargetConfig("LIVE_INGEST", "pipeline2", "Type2", "Adapter2"),
            null,
            "GRPC",
            "PRE_FORWARD",
            "BUFFER",
            "PROPAGATE",
            256,
            10000,
            List.of(),
            null);

        PipelineTemplateConfig config = createTemplateConfig(List.of(connector1, connector2));

        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
            validator.validate(config, List.of(), null, null));
        assertTrue(exception.getMessage().contains("Duplicate connector name"));
    }

    @Test
    void validateFailsWhenSourceKindIsNotOutputBus() {
        ConnectorConfig connector = new ConnectorConfig(
            "connector-1",
            true,
            new ConnectorSourceConfig("BROKER", "step", "Type"),
            new ConnectorTargetConfig("LIVE_INGEST", "pipeline", "Type", "Adapter"),
            null,
            "GRPC",
            "PRE_FORWARD",
            "BUFFER",
            "PROPAGATE",
            256,
            10000,
            List.of(),
            null);

        PipelineTemplateConfig config = createTemplateConfig(List.of(connector));

        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
            validator.validate(config, List.of(), null, null));
        assertTrue(exception.getMessage().contains("only source.kind=OUTPUT_BUS is supported"));
    }

    @Test
    void validateFailsWhenTargetKindIsNotLiveIngest() {
        ConnectorConfig connector = new ConnectorConfig(
            "connector-1",
            true,
            new ConnectorSourceConfig("OUTPUT_BUS", "step", "Type"),
            new ConnectorTargetConfig("BROKER", "pipeline", "Type", "Adapter"),
            null,
            "GRPC",
            "PRE_FORWARD",
            "BUFFER",
            "PROPAGATE",
            256,
            10000,
            List.of(),
            null);

        PipelineTemplateConfig config = createTemplateConfig(List.of(connector));

        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
            validator.validate(config, List.of(), null, null));
        assertTrue(exception.getMessage().contains("only target.kind=LIVE_INGEST is supported"));
    }

    @Test
    void validateNormalizesSourceKindToUpperCase() {
        ConnectorConfig connector = new ConnectorConfig(
            "connector-1",
            true,
            new ConnectorSourceConfig("output_bus", "Order Ready", "Type"),
            new ConnectorTargetConfig("LIVE_INGEST", "pipeline", "Type", "Adapter"),
            null,
            "GRPC",
            "PRE_FORWARD",
            "BUFFER",
            "PROPAGATE",
            256,
            10000,
            List.of(),
            null);

        PipelineTemplateConfig config = createTemplateConfigWithSteps(
            List.of(connector),
            List.of(new PipelineTemplateStep("Order Ready", "ONE_TO_ONE", "In", null, "Out", null, null)));

        List<ConnectorConfig> result = validator.validate(config, List.of(), null, null);
        assertEquals("OUTPUT_BUS", result.get(0).source().kind());
    }

    @Test
    void validateNormalizesTargetKindToUpperCase() {
        ConnectorConfig connector = new ConnectorConfig(
            "connector-1",
            true,
            new ConnectorSourceConfig("OUTPUT_BUS", "Order Ready", "Type"),
            new ConnectorTargetConfig("live_ingest", "pipeline", "Type", "Adapter"),
            null,
            "GRPC",
            "PRE_FORWARD",
            "BUFFER",
            "PROPAGATE",
            256,
            10000,
            List.of(),
            null);

        PipelineTemplateConfig config = createTemplateConfigWithSteps(
            List.of(connector),
            List.of(new PipelineTemplateStep("Order Ready", "ONE_TO_ONE", "In", null, "Out", null, null)));

        List<ConnectorConfig> result = validator.validate(config, List.of(), null, null);
        assertEquals("LIVE_INGEST", result.get(0).target().kind());
    }

    @Test
    void validateNormalizesTransportToUpperCase() {
        ConnectorConfig connector = new ConnectorConfig(
            "connector-1",
            true,
            new ConnectorSourceConfig("OUTPUT_BUS", "Order Ready", "Type"),
            new ConnectorTargetConfig("LIVE_INGEST", "pipeline", "Type", "Adapter"),
            null,
            "grpc",
            "PRE_FORWARD",
            "BUFFER",
            "PROPAGATE",
            256,
            10000,
            List.of(),
            null);

        PipelineTemplateConfig config = createTemplateConfigWithSteps(
            List.of(connector),
            List.of(new PipelineTemplateStep("Order Ready", "ONE_TO_ONE", "In", null, "Out", null, null)));

        List<ConnectorConfig> result = validator.validate(config, List.of(), null, null);
        assertEquals("GRPC", result.get(0).transport());
    }

    @Test
    void validateFailsWhenSourceStepIsNotDeclared() {
        ConnectorConfig connector = new ConnectorConfig(
            "connector-1",
            true,
            new ConnectorSourceConfig("OUTPUT_BUS", "Unknown Step", "Type"),
            new ConnectorTargetConfig("LIVE_INGEST", "pipeline", "Type", "Adapter"),
            null,
            "GRPC",
            "PRE_FORWARD",
            "BUFFER",
            "PROPAGATE",
            256,
            10000,
            List.of(),
            null);

        PipelineTemplateConfig config = createTemplateConfigWithSteps(
            List.of(connector),
            List.of(new PipelineTemplateStep("Known Step", "ONE_TO_ONE", "In", null, "Out", null, null)));

        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
            validator.validate(config, List.of(), null, null));
        assertTrue(exception.getMessage().contains("does not match any declared pipeline step"));
    }

    @Test
    void validateDefaultsBackpressureBufferCapacityTo256() {
        ConnectorConfig connector = new ConnectorConfig(
            "connector-1",
            true,
            new ConnectorSourceConfig("OUTPUT_BUS", "Order Ready", "Type"),
            new ConnectorTargetConfig("LIVE_INGEST", "pipeline", "Type", "Adapter"),
            null,
            "GRPC",
            "PRE_FORWARD",
            "BUFFER",
            "PROPAGATE",
            0,
            10000,
            List.of(),
            null);

        PipelineTemplateConfig config = createTemplateConfigWithSteps(
            List.of(connector),
            List.of(new PipelineTemplateStep("Order Ready", "ONE_TO_ONE", "In", null, "Out", null, null)));

        List<ConnectorConfig> result = validator.validate(config, List.of(), null, null);
        assertEquals(256, result.get(0).backpressureBufferCapacity());
    }

    @Test
    void validateDefaultsIdempotencyMaxKeysTo10000() {
        ConnectorConfig connector = new ConnectorConfig(
            "connector-1",
            true,
            new ConnectorSourceConfig("OUTPUT_BUS", "Order Ready", "Type"),
            new ConnectorTargetConfig("LIVE_INGEST", "pipeline", "Type", "Adapter"),
            null,
            "GRPC",
            "PRE_FORWARD",
            "BUFFER",
            "PROPAGATE",
            256,
            0,
            List.of(),
            null);

        PipelineTemplateConfig config = createTemplateConfigWithSteps(
            List.of(connector),
            List.of(new PipelineTemplateStep("Order Ready", "ONE_TO_ONE", "In", null, "Out", null, null)));

        List<ConnectorConfig> result = validator.validate(config, List.of(), null, null);
        assertEquals(10000, result.get(0).idempotencyMaxKeys());
    }

    private PipelineTemplateConfig createTemplateConfig(List<ConnectorConfig> connectors) {
        return new PipelineTemplateConfig(
            1,
            "TestApp",
            "com.example.test",
            "GRPC",
            org.pipelineframework.config.template.PipelinePlatform.COMPUTE,
            java.util.Map.of(),
            List.of(),
            java.util.Map.of(),
            connectors);
    }

    private PipelineTemplateConfig createTemplateConfigWithSteps(
            List<ConnectorConfig> connectors,
            List<PipelineTemplateStep> steps) {
        return new PipelineTemplateConfig(
            1,
            "TestApp",
            "com.example.test",
            "GRPC",
            org.pipelineframework.config.template.PipelinePlatform.COMPUTE,
            java.util.Map.of(),
            steps,
            java.util.Map.of(),
            connectors);
    }
}
