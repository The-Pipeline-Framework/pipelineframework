package org.pipelineframework.connector;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.pipelineframework.command.CommandEffectRecord;
import org.pipelineframework.command.CommandEffectStatus;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStatus;

import static org.junit.jupiter.api.Assertions.assertFalse;

@SuppressWarnings("removal")
class ConnectorConfigurationSerializationTest {
    private static final String RESOLVED_SECRET = "resolved-secret-must-not-be-serialized";

    @Test
    void excludesResolvedSecretsAndSecretReferencesFromDefaultDurableAndMetadataJson() throws Exception {
        ConnectorConfigSchema<Config> schema = ConnectorConfigSchema.record(Config.class, "serialization.config", 1);
        ConnectorConfigurationSnapshot snapshot = ConnectorConfigurationSnapshot.from(
            schema,
            new ConnectorConfigurationDocument(Map.of(
                "connection", "primary-connection",
                "secret", "secret-reference-must-not-be-serialized",
                "timeout", "PT3S")),
            true);
        CommandEffectRecord effect = new CommandEffectRecord(
            "tenant", "execution", "step", "command", "command-id", CommandEffectStatus.PENDING,
            "business-input", "business-output", "", "", 1L, 1L);
        ExecutionRecord<String, String> execution = new ExecutionRecord<>(
            "tenant", "execution", "key", ExecutionResultShape.SINGLE, ExecutionStatus.QUEUED,
            0L, 0, 0, "", 0L, 0L, "", "business-input", "", "business-output", "", "", 1L, 1L, 0L);

        String metadataJson = PipelineJson.mapper().writeValueAsString(snapshot);
        String effectJson = PipelineJson.mapper().writeValueAsString(effect);
        String executionJson = PipelineJson.mapper().writeValueAsString(execution);

        for (String serialized : List.of(metadataJson, effectJson, executionJson)) {
            assertFalse(serialized.contains(RESOLVED_SECRET));
            assertFalse(serialized.contains("secret-reference-must-not-be-serialized"));
        }
    }

    record Config(ConnectionRef connection, SecretRef secret, Duration timeout) {
    }
}
