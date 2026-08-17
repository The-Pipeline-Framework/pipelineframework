package org.pipelineframework.connector.llm.langchain4j;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.agent.tool.ToolSpecification;
import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.ollama.OllamaChatModel;
import jakarta.enterprise.context.ApplicationScoped;
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

    public LangChain4jOllamaQueryConnector() {
        this((baseUrl, modelName, timeout) -> OllamaChatModel.builder()
            .baseUrl(baseUrl)
            .modelName(modelName)
            .timeout(timeout)
            .build());
    }

    LangChain4jOllamaQueryConnector(OllamaModelFactory modelFactory) {
        this.modelFactory = Objects.requireNonNull(modelFactory, "Ollama model factory must not be null");
    }

    @Override
    protected LlmDecisionClient createClient(
        LlmProviderConfiguration configuration,
        ConnectorRuntimeContext context
    ) {
        ChatModel model = modelFactory.create(
            configuration.baseUrl().orElse(DEFAULT_BASE_URL), configuration.model(), REQUEST_TIMEOUT);
        return new LangChain4jDecisionClient(model, context.executor());
    }

    @FunctionalInterface
    interface OllamaModelFactory {
        ChatModel create(String baseUrl, String modelName, Duration timeout);
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
                    UserMessage.from("Application state:\n" + request.applicationStateJson()
                        + "\nSelect exactly one available function. Do not execute it."))
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
