package org.pipelineframework.connector.llm.langchain4j;

import java.util.Objects;
import java.util.Optional;
import dev.langchain4j.exception.AuthenticationException;
import dev.langchain4j.exception.ContentFilteredException;
import dev.langchain4j.exception.HttpException;
import dev.langchain4j.exception.InternalServerException;
import dev.langchain4j.exception.InvalidRequestException;
import dev.langchain4j.exception.ModelNotFoundException;
import dev.langchain4j.exception.RateLimitException;
import dev.langchain4j.exception.TimeoutException;
import dev.langchain4j.exception.UnresolvedModelServerException;
import dev.langchain4j.exception.UnsupportedFeatureException;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.request.ResponseFormat;
import dev.langchain4j.model.chat.request.ResponseFormatType;
import dev.langchain4j.model.chat.request.json.JsonRawSchema;
import dev.langchain4j.model.chat.request.json.JsonSchema;
import org.pipelineframework.connector.llm.LlmProviderFailureException;

final class LangChain4jOpenAiSupport {
    private LangChain4jOpenAiSupport() {
    }

    static ChatRequest withResponseSchema(ChatRequest request, Optional<String> responseSchema) {
        if (responseSchema.isEmpty()) {
            return request;
        }
        ResponseFormat format = ResponseFormat.builder()
            .type(ResponseFormatType.JSON)
            .jsonSchema(JsonSchema.builder()
                .name("tpf_completion")
                .rootElement(JsonRawSchema.from(responseSchema.orElseThrow()))
                .build())
            .build();
        return new ChatRequest.Builder(request).responseFormat(format).build();
    }

    static LlmProviderFailureException classifyProviderFailure(Throwable failure) {
        Throwable cause = unwrap(failure);
        if (cause instanceof LlmProviderFailureException classified) {
            return classified;
        }
        if (cause instanceof AuthenticationException) {
            return providerFailure(
                LlmProviderFailureException.Kind.AUTHENTICATION_REQUIRED,
                "llm-provider-authentication-required",
                cause);
        }
        if (cause instanceof RateLimitException) {
            return providerFailure(
                LlmProviderFailureException.Kind.TEMPORARILY_UNAVAILABLE,
                "llm-provider-rate-limited",
                cause);
        }
        if (cause instanceof TimeoutException) {
            return providerFailure(
                LlmProviderFailureException.Kind.TEMPORARILY_UNAVAILABLE,
                "llm-provider-timeout",
                cause);
        }
        if (cause instanceof InternalServerException || cause instanceof UnresolvedModelServerException) {
            return providerFailure(
                LlmProviderFailureException.Kind.TEMPORARILY_UNAVAILABLE,
                "llm-provider-unavailable",
                cause);
        }
        if (cause instanceof ModelNotFoundException) {
            return providerFailure(
                LlmProviderFailureException.Kind.TERMINAL,
                "llm-provider-model-unavailable",
                cause);
        }
        if (cause instanceof ContentFilteredException) {
            return providerFailure(
                LlmProviderFailureException.Kind.TERMINAL,
                "llm-provider-content-filtered",
                cause);
        }
        if (cause instanceof InvalidRequestException || cause instanceof UnsupportedFeatureException) {
            return providerFailure(
                LlmProviderFailureException.Kind.TERMINAL,
                "llm-provider-request-rejected",
                cause);
        }
        if (cause instanceof HttpException http) {
            return classifyHttpFailure(http);
        }
        return providerFailure(LlmProviderFailureException.Kind.TERMINAL, "llm-provider-failed", cause);
    }

    private static LlmProviderFailureException classifyHttpFailure(HttpException failure) {
        int status = failure.statusCode();
        if (status == 401 || status == 403) {
            return providerFailure(
                LlmProviderFailureException.Kind.AUTHENTICATION_REQUIRED,
                "llm-provider-authentication-required",
                failure);
        }
        if (status == 402) {
            return providerFailure(
                LlmProviderFailureException.Kind.TERMINAL,
                "llm-provider-quota-exhausted",
                failure);
        }
        if (status == 408 || status == 429 || status >= 500) {
            return providerFailure(
                LlmProviderFailureException.Kind.TEMPORARILY_UNAVAILABLE,
                status == 429 ? "llm-provider-rate-limited" : "llm-provider-unavailable",
                failure);
        }
        if (status == 404) {
            return providerFailure(
                LlmProviderFailureException.Kind.TERMINAL,
                "llm-provider-model-unavailable",
                failure);
        }
        return providerFailure(
            LlmProviderFailureException.Kind.TERMINAL,
            "llm-provider-request-rejected",
            failure);
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = Objects.requireNonNull(failure, "LLM provider failure must not be null");
        while ((current instanceof java.util.concurrent.CompletionException
                || current instanceof java.util.concurrent.ExecutionException)
            && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static LlmProviderFailureException providerFailure(
        LlmProviderFailureException.Kind kind,
        String outcomeCode,
        Throwable cause
    ) {
        return new LlmProviderFailureException(kind, outcomeCode, cause);
    }
}
