package org.pipelineframework.connector.llm.langchain4j;

import java.time.Duration;
import java.util.Objects;

import dev.langchain4j.model.chat.StreamingChatModel;
import org.pipelineframework.connector.ResolvedConnection;

/**
 * Runtime-only authenticated access for one OpenAI-compatible connection.
 *
 * <p>The host-owned factory captures credential handling. The connector supplies only its
 * binding-owned model and endpoint semantics and never receives credential material.</p>
 */
public final class AuthenticatedOpenAiCompatibleReactiveConnection implements ResolvedConnection {
    private final ModelFactory modelFactory;

    public AuthenticatedOpenAiCompatibleReactiveConnection(ModelFactory modelFactory) {
        this.modelFactory = Objects.requireNonNull(
            modelFactory, "authenticated OpenAI-compatible model factory must not be null");
    }

    StreamingChatModel createModel(String baseUrl, String modelName, Duration timeout) {
        return Objects.requireNonNull(
            modelFactory.create(new ModelConfiguration(
                baseUrl,
                modelName,
                timeout,
                true)),
            "authenticated OpenAI-compatible model factory returned a null model");
    }

    /**
     * Non-secret model settings which the connector requires the host-owned factory to preserve.
     *
     * <p>In particular, {@code strictJsonSchema} must be applied to the provider SDK model. TPF's
     * required structured-output contract depends on the provider receiving strict JSON Schema,
     * while credential attachment remains entirely inside the host factory.</p>
     */
    public record ModelConfiguration(
        String baseUrl,
        String modelName,
        Duration timeout,
        boolean strictJsonSchema
    ) {
        public ModelConfiguration {
            Objects.requireNonNull(baseUrl, "OpenAI-compatible base URL must not be null");
            Objects.requireNonNull(modelName, "OpenAI-compatible model name must not be null");
            Objects.requireNonNull(timeout, "OpenAI-compatible timeout must not be null");
        }
    }

    /**
     * Host-owned factory that constructs an authenticated SDK client without exposing credentials.
     * Implementations must apply every supplied {@link ModelConfiguration} setting to the model.
     */
    @FunctionalInterface
    public interface ModelFactory {
        StreamingChatModel create(ModelConfiguration configuration);
    }
}
