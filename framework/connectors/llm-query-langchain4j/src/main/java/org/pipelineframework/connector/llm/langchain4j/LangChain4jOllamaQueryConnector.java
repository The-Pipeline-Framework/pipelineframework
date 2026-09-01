package org.pipelineframework.connector.llm.langchain4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

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
import dev.langchain4j.model.chat.StreamingChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import dev.langchain4j.model.chat.response.StreamingChatResponseHandler;
import dev.langchain4j.model.output.TokenUsage;
import io.quarkiverse.langchain4j.ollama.OllamaStreamingChatLanguageModel;
import io.quarkiverse.langchain4j.ollama.Options;
import jakarta.enterprise.context.ApplicationScoped;
import io.quarkus.arc.Unremovable;
import jakarta.inject.Inject;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.QueryObservation;
import org.pipelineframework.connector.QueryTokenUsage;
import org.pipelineframework.connector.llm.LlmDecision;
import org.pipelineframework.connector.llm.LlmDecisionClient;
import org.pipelineframework.connector.llm.LlmDecisionClientResolver;
import org.pipelineframework.connector.llm.LlmProviderConfiguration;
import org.pipelineframework.connector.llm.LlmQueryConnectorProvider;
import org.pipelineframework.connector.llm.LlmToolDefinition;
import org.pipelineframework.connector.llm.LlmToolProposal;
import org.pipelineframework.connector.llm.LlmTurnRequest;

/** LangChain4j/Ollama adapter that observes one model decision and never invokes a tool executor. */
@ApplicationScoped
@Unremovable
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
            configuration.baseUrl(), configuration.requestTimeout(), configuration.thinking()));
    }

    private static OllamaModelFactory defaultModelFactory() {
        return ReactiveOllamaChatModel::new;
    }

    LangChain4jOllamaQueryConnector(OllamaModelFactory modelFactory) {
        this(modelFactory, OllamaRuntimeSettings.defaults());
    }

    LangChain4jOllamaQueryConnector(OllamaModelFactory modelFactory, OllamaRuntimeSettings runtimeSettings) {
        this.modelFactory = Objects.requireNonNull(modelFactory, "Ollama model factory must not be null");
        this.runtimeSettings = Objects.requireNonNull(runtimeSettings, "Ollama runtime settings must not be null");
    }

    @Override
    protected LlmDecisionClientResolver createClientResolver(
        LlmProviderConfiguration configuration,
        ConnectorRuntimeContext context
    ) {
        AsyncChatModel model = modelFactory.create(
            configuration.baseUrl().orElse(runtimeSettings.baseUrl()),
            configuration.model(),
            runtimeSettings.requestTimeout(),
            runtimeSettings.thinking());
        LlmDecisionClient client = new LangChain4jDecisionClient(model);
        return ignored -> CompletableFuture.completedStage(client);
    }

    @FunctionalInterface
    interface OllamaModelFactory {
        AsyncChatModel create(String baseUrl, String modelName, Duration timeout, boolean thinking);
    }

    record OllamaRuntimeSettings(String baseUrl, Duration requestTimeout, boolean thinking) {
        OllamaRuntimeSettings {
            baseUrl = Objects.requireNonNull(baseUrl, "Ollama base URL must not be null").trim();
            if (baseUrl.isEmpty()) {
                throw new IllegalArgumentException("Ollama base URL must not be blank");
            }
            Objects.requireNonNull(requestTimeout, "Ollama request timeout must not be null");
            if (requestTimeout.isZero() || requestTimeout.isNegative()) {
                throw new IllegalArgumentException("Ollama request timeout must be positive");
            }
        }

        static OllamaRuntimeSettings defaults() {
            return new OllamaRuntimeSettings(DEFAULT_BASE_URL, REQUEST_TIMEOUT, true);
        }

    }

    @FunctionalInterface
    interface AsyncChatModel {
        CompletionStage<ChatResponse> chat(ChatRequest request, Optional<String> responseSchema);
    }

    static final class ReactiveOllamaChatModel implements AsyncChatModel {
        private final String baseUrl;
        private final String modelName;
        private final Duration timeout;
        private final boolean thinking;
        private final ConcurrentHashMap<String, StreamingChatModel> models = new ConcurrentHashMap<>();

        ReactiveOllamaChatModel(String baseUrl, String modelName, Duration timeout, boolean thinking) {
            this.baseUrl = Objects.requireNonNull(baseUrl, "Ollama base URL must not be null");
            this.modelName = Objects.requireNonNull(modelName, "Ollama model name must not be null");
            this.timeout = Objects.requireNonNull(timeout, "Ollama timeout must not be null");
            this.thinking = thinking;
        }

        @Override
        public CompletionStage<ChatResponse> chat(ChatRequest request, Optional<String> responseSchema) {
            Objects.requireNonNull(request, "Ollama chat request must not be null");
            String schema = Objects.requireNonNull(responseSchema, "response schema must not be null").orElse("");
            StreamingChatModel model = models.computeIfAbsent(schema, this::createModel);
            CompletableFuture<ChatResponse> response = new CompletableFuture<>();
            model.chat(request, new StreamingChatResponseHandler() {
                @Override
                public void onPartialResponse(String partialResponse) {
                    // The Quarkus client aggregates partials into the final ChatResponse.
                }

                @Override
                public void onCompleteResponse(ChatResponse completeResponse) {
                    response.complete(completeResponse);
                }

                @Override
                public void onError(Throwable error) {
                    response.completeExceptionally(error);
                }
            });
            return response;
        }

        private StreamingChatModel createModel(String schema) {
            var builder = OllamaStreamingChatLanguageModel.builder()
                .baseUrl(baseUrl)
                .model(modelName)
                .timeout(timeout)
                .options(Options.builder().option("think", thinking).build());
            if (!schema.isEmpty()) {
                builder.format(schema);
            }
            return builder.build();
        }
    }

    static final class LangChain4jDecisionClient implements LlmDecisionClient {
        private final AsyncChatModel model;

        LangChain4jDecisionClient(AsyncChatModel model) {
            this.model = java.util.Objects.requireNonNull(model, "LangChain4j model must not be null");
        }

        @Override
        public boolean supportsNativeStructuredOutput(List<LlmToolDefinition> tools) {
            return directCompletion(tools).isPresent();
        }

        @Override
        public java.util.concurrent.CompletionStage<LlmDecision> decide(LlmTurnRequest request) {
            if (request.structuredOutputSchema()
                == org.pipelineframework.connector.llm.StructuredOutputSchemaMode.REQUIRED) {
                return decideNativeCompletion(request);
            }
            return decideToolProposal(request);
        }

        private CompletionStage<LlmDecision> decideNativeCompletion(LlmTurnRequest request) {
            LlmToolDefinition completion = directCompletion(request.tools()).orElseThrow(() ->
                new IllegalArgumentException(
                    "Native structured output requires one direct 'complete' alternative"));
            ChatRequest chat = ChatRequest.builder()
                .messages(
                    SystemMessage.from(request.instructions()),
                    userMessage(request, true))
                .build();
            return model.chat(chat, Optional.of(completion.inputSchemaJson())).thenApply(response -> {
                String arguments = response.aiMessage().text();
                return decision(response, new LlmToolProposal(
                    completion.alias(), arguments == null ? "{}" : arguments));
            });
        }

        private CompletionStage<LlmDecision> decideToolProposal(LlmTurnRequest request) {
            List<ToolSpecification> tools = request.tools().stream().map(this::tool).toList();
            ChatRequest chat = ChatRequest.builder()
                .messages(
                    SystemMessage.from(request.instructions()),
                    userMessage(request, false))
                .toolSpecifications(tools)
                .build();
            return model.chat(chat, Optional.empty()).thenApply(response -> {
                List<ToolExecutionRequest> proposals = response.aiMessage().toolExecutionRequests();
                proposals = proposals == null ? List.of() : proposals;
                if (proposals.size() != 1) {
                    return decision(response, new LlmToolProposal("", "{}"));
                }
                ToolExecutionRequest proposal = proposals.getFirst();
                String name = proposal.name() == null ? "" : proposal.name();
                String arguments = proposal.arguments() == null ? "{}" : proposal.arguments();
                return decision(response, new LlmToolProposal(name, arguments));
            });
        }

        private static LlmDecision decision(ChatResponse response, LlmToolProposal proposal) {
            return new LlmDecision(proposal, Optional.of(observation(response)));
        }

        private static QueryObservation observation(ChatResponse response) {
            try {
                Optional<QueryTokenUsage> usage = tokenUsage(response.tokenUsage());
                Optional<String> model = boundedText(response.modelName(), 256);
                Optional<String> finishReason = response.finishReason() == null
                    ? Optional.empty()
                    : boundedText(response.finishReason().name().toLowerCase(java.util.Locale.ROOT), 128);
                return QueryObservation.live(usage, model, finishReason);
            } catch (RuntimeException invalidMetadata) {
                return QueryObservation.live(Optional.empty(), Optional.empty(), Optional.empty());
            }
        }

        private static Optional<QueryTokenUsage> tokenUsage(TokenUsage usage) {
            if (usage == null) {
                return Optional.empty();
            }
            QueryTokenUsage reported = new QueryTokenUsage(
                nonNegative(usage.inputTokenCount()),
                nonNegative(usage.outputTokenCount()),
                nonNegative(usage.totalTokenCount()));
            return reported.inputTokens().isPresent()
                || reported.outputTokens().isPresent()
                || reported.totalTokens().isPresent()
                ? Optional.of(reported)
                : Optional.empty();
        }

        private static OptionalLong nonNegative(Integer value) {
            return value == null || value < 0 ? OptionalLong.empty() : OptionalLong.of(value.longValue());
        }

        private static Optional<String> boundedText(String value, int maximumLength) {
            if (value == null) {
                return Optional.empty();
            }
            String normalized = value.trim();
            return normalized.isEmpty() || normalized.length() > maximumLength
                ? Optional.empty()
                : Optional.of(normalized);
        }

        private static Optional<LlmToolDefinition> directCompletion(List<LlmToolDefinition> tools) {
            return tools.size() == 1 && "complete".equals(tools.getFirst().alias())
                ? Optional.of(tools.getFirst())
                : Optional.empty();
        }

        private UserMessage userMessage(LlmTurnRequest request, boolean nativeCompletion) {
            List<Content> contents = new ArrayList<>();
            contents.add(TextContent.from("Application state:\n" + request.applicationStateJson()
                + (nativeCompletion
                    ? "\nReturn exactly one JSON object matching the required response schema."
                    : "\nSelect exactly one available function. Do not execute it.")));
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
