package org.pipelineframework.connector.llm.langchain4j;

import java.time.Duration;
import java.util.Objects;

import dev.langchain4j.model.chat.ChatModel;
import org.pipelineframework.connector.ResolvedConnection;

/**
 * Runtime-only authenticated access for one OpenAI-compatible connection.
 *
 * <p>The host-owned factory captures credential handling. The connector supplies only its
 * binding-owned model and endpoint semantics and never receives credential material.</p>
 */
public final class AuthenticatedOpenAiCompatibleConnection implements ResolvedConnection {
    private final ModelFactory modelFactory;

    public AuthenticatedOpenAiCompatibleConnection(ModelFactory modelFactory) {
        this.modelFactory = Objects.requireNonNull(
            modelFactory, "authenticated OpenAI-compatible model factory must not be null");
    }

    ChatModel createModel(String baseUrl, String modelName, Duration timeout, int maxRetries) {
        return Objects.requireNonNull(
            modelFactory.create(baseUrl, modelName, timeout, maxRetries),
            "authenticated OpenAI-compatible model factory returned a null model");
    }

    /** Host-owned factory that constructs an authenticated SDK client without exposing credentials. */
    @FunctionalInterface
    public interface ModelFactory {
        ChatModel create(String baseUrl, String modelName, Duration timeout, int maxRetries);
    }
}
