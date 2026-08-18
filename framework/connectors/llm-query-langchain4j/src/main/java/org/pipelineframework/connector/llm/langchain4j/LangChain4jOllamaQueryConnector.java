package org.pipelineframework.connector.llm.langchain4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.agent.tool.ToolSpecification;
import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.Content;
import dev.langchain4j.data.message.ImageContent;
import dev.langchain4j.data.message.PdfFileContent;
import dev.langchain4j.data.message.TextContent;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.data.pdf.PdfFile;
import dev.langchain4j.data.image.Image;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.ollama.OllamaChatModel;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.llm.LlmDecisionClient;
import org.pipelineframework.connector.llm.LlmProviderConfiguration;
import org.pipelineframework.connector.llm.LlmQueryConnectorProvider;
import org.pipelineframework.connector.llm.LlmToolDefinition;
import org.pipelineframework.connector.llm.LlmToolProposal;
import org.pipelineframework.connector.llm.LlmTurnRequest;

/** LangChain4j adapter that observes one tool proposal and never invokes a tool executor. */
@ApplicationScoped
public final class LangChain4jOllamaQueryConnector extends LlmQueryConnectorProvider {
    private static final String DEFAULT_BASE_URL = "http://localhost:11434";
    static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private final OllamaModelFactory modelFactory;
    private final OllamaRuntimeSettings runtimeSettings;

    public LangChain4jOllamaQueryConnector() {
        this(defaultModelFactory(), OllamaRuntimeSettings.defaults());
    }

    @Inject
    LangChain4jOllamaQueryConnector(OllamaRuntimeConfiguration configuration) {
        this(defaultModelFactory(), new OllamaRuntimeSettings(
            configuration.requestTimeout(), configuration.thinking()));
    }

    private static OllamaModelFactory defaultModelFactory() {
        return (baseUrl, modelName, timeout, thinking, maxRetries) -> OllamaChatModel.builder()
            .baseUrl(baseUrl)
            .modelName(modelName)
            .timeout(timeout)
            .think(thinking)
            .maxRetries(maxRetries)
            .build();
    }

    LangChain4jOllamaQueryConnector(OllamaModelFactory modelFactory) {
        this(modelFactory, OllamaRuntimeSettings.defaults());
    }

    LangChain4jOllamaQueryConnector(OllamaModelFactory modelFactory, OllamaRuntimeSettings runtimeSettings) {
        this.modelFactory = Objects.requireNonNull(modelFactory, "Ollama model factory must not be null");
        this.runtimeSettings = Objects.requireNonNull(runtimeSettings, "Ollama runtime settings must not be null");
    }

    @Override
    protected LlmDecisionClient createClient(
        LlmProviderConfiguration configuration,
        ConnectorRuntimeContext context
    ) {
        ChatModel model = modelFactory.create(
            configuration.baseUrl().orElse(DEFAULT_BASE_URL),
            configuration.model(),
            runtimeSettings.requestTimeout(),
            runtimeSettings.thinking(),
            0);
        return new LangChain4jDecisionClient(model, context.executor());
    }

    @FunctionalInterface
    interface OllamaModelFactory {
        ChatModel create(String baseUrl, String modelName, Duration timeout, boolean thinking, int maxRetries);
    }

    @ConfigMapping(prefix = "pipeline.llm.langchain4j.ollama")
    interface OllamaRuntimeConfiguration {
        /** Maximum wall-clock time for one Ollama HTTP request. */
        @WithDefault("PT30S")
        Duration requestTimeout();

        /** Whether Ollama should enable model thinking/reasoning output. */
        @WithDefault("true")
        boolean thinking();
    }

    record OllamaRuntimeSettings(Duration requestTimeout, boolean thinking) {
        OllamaRuntimeSettings {
            Objects.requireNonNull(requestTimeout, "Ollama request timeout must not be null");
            if (requestTimeout.isZero() || requestTimeout.isNegative()) {
                throw new IllegalArgumentException("Ollama request timeout must be positive");
            }
        }

        static OllamaRuntimeSettings defaults() {
            return new OllamaRuntimeSettings(REQUEST_TIMEOUT, true);
        }

    }

    static final class LangChain4jDecisionClient implements LlmDecisionClient {
        private final ChatModel model;
        private final Executor executor;

        LangChain4jDecisionClient(ChatModel model, Executor executor) {
            this.model = java.util.Objects.requireNonNull(model, "LangChain4j model must not be null");
            this.executor = java.util.Objects.requireNonNull(executor, "LLM executor must not be null");
        }

        @Override
        public boolean supportsNativeStructuredOutput() {
            return true;
        }

        @Override
        public java.util.concurrent.CompletionStage<LlmToolProposal> decide(LlmTurnRequest request) {
            return CompletableFuture.supplyAsync(() -> decideBlocking(request), executor);
        }

        private LlmToolProposal decideBlocking(LlmTurnRequest request) {
            List<ToolSpecification> tools = request.tools().stream().map(this::tool).toList();
            ChatRequest chat = ChatRequest.builder()
                .messages(
                    SystemMessage.from(request.instructions()),
                    userMessage(request))
                .toolSpecifications(tools)
                .build();
            List<ToolExecutionRequest> proposals = model.chat(chat).aiMessage().toolExecutionRequests();
            proposals = proposals == null ? List.of() : proposals;
            if (proposals.size() != 1) {
                return new LlmToolProposal("", "{}");
            }
            ToolExecutionRequest proposal = proposals.getFirst();
            String name = proposal.name() == null ? "" : proposal.name();
            String arguments = proposal.arguments() == null ? "{}" : proposal.arguments();
            return new LlmToolProposal(name, arguments);
        }

        private UserMessage userMessage(LlmTurnRequest request) {
            List<Content> contents = new ArrayList<>();
            contents.add(TextContent.from("Application state:\n" + request.applicationStateJson()
                + "\nSelect exactly one available function. Do not execute it."));
            request.media().forEach(payload -> {
                String contentType = payload.contentType() == null ? "" : payload.contentType().toLowerCase(java.util.Locale.ROOT);
                String base64 = Base64.getEncoder().encodeToString(payload.bytes());
                if (contentType.startsWith("image/")) {
                    contents.add(ImageContent.from(Image.builder()
                        .base64Data(base64)
                        .mimeType(payload.contentType())
                        .build()));
                    return;
                }
                if ("application/pdf".equals(contentType)) {
                    contents.add(PdfFileContent.from(PdfFile.builder()
                        .base64Data(base64)
                        .mimeType(payload.contentType())
                        .build()));
                    return;
                }
                throw new IllegalArgumentException(
                    "LangChain4j LLM Query does not support materialized media type: " + payload.contentType());
            });
            return UserMessage.from(contents);
        }

        private ToolSpecification tool(LlmToolDefinition definition) {
            try {
                ObjectNode value = PipelineJson.mapper().createObjectNode();
                value.put("name", definition.alias());
                value.put("description", definition.description());
                value.set("parameters", PipelineJson.mapper().readTree(definition.inputSchemaJson()));
                return ToolSpecification.fromJson(PipelineJson.mapper().writeValueAsString(value));
            } catch (Exception failure) {
                throw new IllegalStateException("Failed adapting canonical tool schema for '"
                    + definition.alias() + "'", failure);
            }
        }
    }
}
