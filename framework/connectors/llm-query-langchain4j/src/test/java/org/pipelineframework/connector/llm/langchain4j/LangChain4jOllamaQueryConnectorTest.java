package org.pipelineframework.connector.llm.langchain4j;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import org.pipelineframework.connector.llm.LlmToolDefinition;
import org.pipelineframework.connector.llm.LlmTurnRequest;

class LangChain4jOllamaQueryConnectorTest {
    @Test
    void performsOneLowLevelChatCallAndReturnsTheProposalWithoutExecutingIt() {
        AtomicInteger calls = new AtomicInteger();
        ChatModel model = new ChatModel() {
            @Override
            public ChatResponse doChat(ChatRequest request) {
                calls.incrementAndGet();
                assertEquals(List.of("charge"),
                    request.toolSpecifications().stream().map(specification -> specification.name()).toList());
                return ChatResponse.builder()
                    .aiMessage(AiMessage.from(ToolExecutionRequest.builder()
                        .name("charge")
                        .arguments("{\"amount\":42}")
                        .build()))
                    .build();
            }
        };
        var client = new LangChain4jOllamaQueryConnector.LangChain4jDecisionClient(model, Runnable::run);

        var proposal = client.decide(new LlmTurnRequest(
            "Decide once.",
            "{\"invoiceId\":\"7\"}",
            List.of(new LlmToolDefinition(
                "charge", "Propose a charge", """
                    {"type":"object","properties":{"amount":{"type":"integer"}},
                     "required":["amount"],"additionalProperties":false,
                     "$defs":{"Arguments":{"type":"object","properties":{"amount":{"type":"integer"}},
                     "required":["amount"],"additionalProperties":false}}}
                    """))))
            .toCompletableFuture().join();

        assertEquals("charge", proposal.alias());
        assertEquals("{\"amount\":42}", proposal.argumentsJson());
        assertEquals(1, calls.get());
    }
}
