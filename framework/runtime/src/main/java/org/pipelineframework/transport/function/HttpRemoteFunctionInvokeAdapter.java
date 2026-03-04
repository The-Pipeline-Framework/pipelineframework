/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.transport.function;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.BytesValue;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.pipelineframework.context.PipelineContextHeaders;
import org.pipelineframework.transport.http.ProtobufHttpContentTypes;
import org.pipelineframework.transport.http.ProtobufHttpStatusMapper;

/**
 * Remote invoke adapter that dispatches envelopes to an HTTP endpoint.
 *
 * <p>Target endpoint resolution precedence is:
 * {@link FunctionTransportContext#ATTR_TARGET_URL}, then target metadata
 * ({@link FunctionTransportContext#ATTR_TARGET_HANDLER} and
 * {@link FunctionTransportContext#ATTR_TARGET_MODULE}) against runtime config
 * (`quarkus.rest-client.&lt;client&gt;.url` then `pipeline.module.&lt;module&gt;.{host,port}`).</p>
 * <p>For N->1 and N->M shapes, this adapter collects the full input stream with
 * {@code input.collect().asList()} before issuing a single HTTP call. This means memory usage grows with
 * input size. Avoid unbounded/very large streams here; prefer upstream chunking or single-item invocations
 * when you need strict streaming memory bounds.</p>
 *
 * @param <I> input payload type
 * @param <O> output payload type
 */
public final class HttpRemoteFunctionInvokeAdapter<I, O> implements FunctionInvokeAdapter<I, O> {
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);
    private static final String PROTOCOL_PROTOBUF_HTTP_V1 = "PROTOBUF_HTTP_V1";

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public HttpRemoteFunctionInvokeAdapter() {
        this(
            HttpClient.newBuilder()
                .connectTimeout(DEFAULT_TIMEOUT)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build(),
            new ObjectMapper().findAndRegisterModules());
    }

    HttpRemoteFunctionInvokeAdapter(HttpClient httpClient, ObjectMapper objectMapper) {
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    }

    @Override
    public Uni<TraceEnvelope<O>> invokeOneToOne(TraceEnvelope<I> input, FunctionTransportContext context) {
        Objects.requireNonNull(input, "input envelope must not be null");
        Objects.requireNonNull(context, "context must not be null");
        return postForSingle(input, context);
    }

    @Override
    public Multi<TraceEnvelope<O>> invokeOneToMany(TraceEnvelope<I> input, FunctionTransportContext context) {
        Objects.requireNonNull(input, "input envelope must not be null");
        Objects.requireNonNull(context, "context must not be null");
        return postForMany(input, context)
            .onItem().transformToMulti(Multi.createFrom()::iterable);
    }

    @Override
    public Uni<TraceEnvelope<O>> invokeManyToOne(Multi<TraceEnvelope<I>> input, FunctionTransportContext context) {
        Objects.requireNonNull(input, "input stream must not be null");
        Objects.requireNonNull(context, "context must not be null");
        return input.collect().asList()
            .flatMap(items -> postForSingle(items, context));
    }

    @Override
    public Multi<TraceEnvelope<O>> invokeManyToMany(Multi<TraceEnvelope<I>> input, FunctionTransportContext context) {
        Objects.requireNonNull(input, "input stream must not be null");
        Objects.requireNonNull(context, "context must not be null");
        return input.collect().asList()
            .flatMap(items -> postForMany(items, context))
            .onItem().transformToMulti(Multi.createFrom()::iterable);
    }

    /**
     * Sends the given payload to the remote function endpoint and returns the single deserialized TraceEnvelope response.
     *
     * @param payload the request payload (a TraceEnvelope or a collection of TraceEnvelopes depending on invocation)
     * @param context transport context providing routing, protocol, and metadata used for the HTTP request
     * @return the deserialized TraceEnvelope response from the remote function
     * @throws IllegalStateException if the remote response body is empty, the invocation is interrupted, the target URL is malformed, the response cannot be parsed, or the remote endpoint returns a non-successful HTTP status
     */
    private Uni<TraceEnvelope<O>> postForSingle(Object payload, FunctionTransportContext context) {
        return Uni.createFrom().item(() -> {
            try {
                HttpResponse<byte[]> response = send(payload, context);
                JsonNode node = decodeResponseAsJsonTree(response, context);
                if (node == null || node.isNull()) {
                    throw new IllegalStateException("Remote function response body was empty");
                }
                return castEnvelope(objectMapper.convertValue(node, TraceEnvelope.class));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Remote function invocation was interrupted", e);
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("Malformed target URL for remote function invocation", e);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to parse remote function response", e);
            }
        }).runSubscriptionOn(Infrastructure.getDefaultExecutor());
    }

    /**
     * Sends the given payload to the remote function and returns the list of output trace envelopes.
     *
     * @param payload the request payload to send (a single TraceEnvelope or a collection)
     * @param context transport context containing routing and protocol metadata
     * @return an immutable list of TraceEnvelope objects produced by the remote function
     * @throws IllegalStateException if the response body is empty, not a JSON array, the invocation was interrupted,
     *         the target URL is malformed, or the response cannot be parsed
     */
    private Uni<List<TraceEnvelope<O>>> postForMany(Object payload, FunctionTransportContext context) {
        return Uni.createFrom().item(() -> {
            try {
                HttpResponse<byte[]> response = send(payload, context);
                JsonNode node = decodeResponseAsJsonTree(response, context);
                if (node == null || node.isNull()) {
                    throw new IllegalStateException("Remote function response body was empty");
                }
                if (!node.isArray()) {
                    throw new IllegalStateException("Remote function response must be a JSON array for streaming output");
                }
                List<TraceEnvelope<O>> envelopes = new ArrayList<>(node.size());
                for (JsonNode item : node) {
                    envelopes.add(castEnvelope(objectMapper.convertValue(item, TraceEnvelope.class)));
                }
                return List.copyOf(envelopes);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Remote function invocation was interrupted", e);
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("Malformed target URL for remote function invocation", e);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to parse remote function response", e);
            }
        }).runSubscriptionOn(Infrastructure.getDefaultExecutor());
    }

    /**
     * Send an HTTP POST to the resolved target URL using the encoded payload and canonical headers.
     *
     * @param payload the object to encode as the request body
     * @param context transport context used to resolve the target URL, select protocol, and populate headers
     * @return the HTTP response with its body as a byte array for successful (2xx) responses
     * @throws IOException if an I/O error occurs while sending the request or reading the response
     * @throws InterruptedException if the thread is interrupted while sending the request
     */
    private HttpResponse<byte[]> send(Object payload, FunctionTransportContext context) throws IOException, InterruptedException {
        String targetUrl = resolveTargetUrl(context);
        WirePayload wirePayload = encodeRequestPayload(payload, context);
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(URI.create(targetUrl))
            .timeout(DEFAULT_TIMEOUT)
            .header("Content-Type", wirePayload.contentType())
            .header("Accept", wirePayload.accept())
            .POST(HttpRequest.BodyPublishers.ofByteArray(wirePayload.body()));
        applyCanonicalHeaders(requestBuilder, payload, context);
        HttpRequest request = requestBuilder.build();
        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw toRemoteFailure(response, targetUrl);
        }
        return response;
    }

    /**
     * Adds canonical pipeline headers to the given HTTP request builder using values derived from
     * the provided payload and transport context.
     *
     * <p>Headers set:
     * - TPF_CORRELATION_ID, TPF_EXECUTION_ID: sourced from context (or derived from requestId when absent)
     * - TPF_IDEMPOTENCY_KEY: resolved from payload or context
     * - TPF_RETRY_ATTEMPT, TPF_DISPATCH_TS_EPOCH_MS: sourced from context (dispatch timestamp defaults to now)
     * - TPF_DEADLINE_EPOCH_MS, TPF_PARENT_ITEM_ID: added when present in context
     *
     * @param requestBuilder the HTTP request builder to modify with canonical headers
     * @param payload the request payload used to resolve an idempotency key when applicable
     * @param context the transport context providing correlation/execution ids, retry and timing metadata
     */
    private void applyCanonicalHeaders(
        HttpRequest.Builder requestBuilder,
        Object payload,
        FunctionTransportContext context
    ) {
        String correlationId = context.correlationId().orElseGet(() -> AdapterUtils.deriveTraceId(context.requestId()));
        String executionId = context.executionId().orElseGet(context::requestId);
        String idempotencyKey = resolveIdempotencyKey(payload, context);
        int retryAttempt = context.retryAttempt().orElse(0);
        long dispatchTsEpochMs = context.dispatchTsEpochMs().orElseGet(() -> Instant.now().toEpochMilli());

        requestBuilder.header(PipelineContextHeaders.TPF_CORRELATION_ID, correlationId);
        requestBuilder.header(PipelineContextHeaders.TPF_EXECUTION_ID, executionId);
        requestBuilder.header(PipelineContextHeaders.TPF_IDEMPOTENCY_KEY, idempotencyKey);
        requestBuilder.header(PipelineContextHeaders.TPF_RETRY_ATTEMPT, Integer.toString(retryAttempt));
        requestBuilder.header(PipelineContextHeaders.TPF_DISPATCH_TS_EPOCH_MS, Long.toString(dispatchTsEpochMs));
        context.deadlineEpochMs().ifPresent(deadline ->
            requestBuilder.header(PipelineContextHeaders.TPF_DEADLINE_EPOCH_MS, Long.toString(deadline)));
        context.parentItemId().ifPresent(parent ->
            requestBuilder.header(PipelineContextHeaders.TPF_PARENT_ITEM_ID, parent));
    }

    /**
     * Determines the idempotency key to use for the outgoing request.
     *
     * Chooses the idempotency key in this order: a non-blank idempotencyKey present on the input TraceEnvelope;
     * if the payload is a non-empty List, the non-blank idempotencyKey of the first TraceEnvelope in that list;
     * the explicit idempotency key from the transport context if present; otherwise a deterministic trace id derived
     * from the context's requestId.
     *
     * @param payload the request payload, which may be a TraceEnvelope, a List of TraceEnvelope, or another type
     * @param context the transport context providing optional explicit idempotency key and requestId for derivation
     * @return the resolved idempotency key string to include on the outgoing request
     */
    private String resolveIdempotencyKey(Object payload, FunctionTransportContext context) {
        if (payload instanceof TraceEnvelope<?> envelope && envelope.idempotencyKey() != null && !envelope.idempotencyKey().isBlank()) {
            return envelope.idempotencyKey();
        }
        // Batch payloads are treated as a single idempotent remote operation.
        // We intentionally use the first envelope key when available, then fall back to
        // explicit context override and finally deterministic request-id derivation.
        if (payload instanceof List<?> list && !list.isEmpty() && list.get(0) instanceof TraceEnvelope<?> envelope
            && envelope.idempotencyKey() != null && !envelope.idempotencyKey().isBlank()) {
            return envelope.idempotencyKey();
        }
        return context.explicitIdempotencyKey().orElseGet(() -> AdapterUtils.deriveTraceId(context.requestId()));
    }

    /**
     * Encode the given payload into a WirePayload using JSON or Protobuf-wrapped JSON depending on the transport protocol specified in the context.
     *
     * @param payload the object to serialize for the HTTP request body
     * @param context the transport context that may specify the protocol (e.g., Protobuf HTTP v1)
     * @return a WirePayload containing the serialized request bytes and the appropriate Content-Type and Accept header values
     * @throws IOException if serialization to JSON or protobuf bytes fails
     */
    private WirePayload encodeRequestPayload(Object payload, FunctionTransportContext context) throws IOException {
        if (useProtobufHttpV1(context)) {
            String jsonPayload = objectMapper.writeValueAsString(payload);
            BytesValue bytesValue = BytesValue.newBuilder()
                .setValue(com.google.protobuf.ByteString.copyFromUtf8(jsonPayload))
                .build();
            return new WirePayload(
                bytesValue.toByteArray(),
                ProtobufHttpContentTypes.APPLICATION_X_PROTOBUF,
                ProtobufHttpContentTypes.APPLICATION_X_PROTOBUF);
        }
        return new WirePayload(
            objectMapper.writeValueAsBytes(payload),
            ProtobufHttpContentTypes.APPLICATION_JSON,
            ProtobufHttpContentTypes.APPLICATION_JSON);
    }

    /**
     * Parse the HTTP response body into a Jackson JsonNode, decoding protobuf-wrapped JSON when applicable.
     *
     * @param response the HTTP response whose Content-Type header and body will be inspected
     * @param context  transport context used to select protobuf-http fallback when the Content-Type header is missing
     * @return the parsed JSON tree from the response body
     * @throws IOException if JSON parsing of the response body fails
     * @throws IllegalStateException if the response is protobuf-formatted but cannot be parsed as a BytesValue
     */
    private JsonNode decodeResponseAsJsonTree(HttpResponse<byte[]> response, FunctionTransportContext context) throws IOException {
        String contentType = response.headers().firstValue("Content-Type").orElse("");
        byte[] body = response.body() == null ? new byte[0] : response.body();
        String normalizedContentType = contentType.toLowerCase(java.util.Locale.ROOT);
        boolean protobufContentType = normalizedContentType.contains(ProtobufHttpContentTypes.APPLICATION_X_PROTOBUF);
        boolean missingContentType = normalizedContentType.isBlank();
        if (protobufContentType || (missingContentType && useProtobufHttpV1(context))) {
            try {
                BytesValue bytesValue = BytesValue.parseFrom(body);
                String json = bytesValue.getValue().toStringUtf8();
                return objectMapper.readTree(json);
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalStateException("Failed to decode protobuf response payload", e);
            }
        }
        return objectMapper.readTree(body);
    }

    /**
     * Constructs a RuntimeException describing a remote function invocation failure using information
     * extracted from the HTTP response and the provided target URL.
     *
     * <p>If the response contains a protobuf Status payload, that status's code and message are used;
     * otherwise the response body (truncated if long) is mapped to a status and its code/message are used.
     *
     * @param response the HTTP response received from the remote target
     * @param targetUrl the URL that was invoked
     * @return an IllegalStateException containing the HTTP status, target URL, and a mapped or parsed
     *         status code and message describing the remote failure
     */
    private RuntimeException toRemoteFailure(HttpResponse<byte[]> response, String targetUrl) {
        String contentType = response.headers().firstValue("Content-Type").orElse("");
        byte[] body = response.body() == null ? new byte[0] : response.body();
        if (contentType.toLowerCase(java.util.Locale.ROOT).contains(ProtobufHttpContentTypes.APPLICATION_X_PROTOBUF)) {
            try {
                Status status = Status.parseFrom(body);
                return new IllegalStateException(
                    "Remote function invocation failed with HTTP " + response.statusCode() + " at " + targetUrl
                        + " (" + Code.forNumber(status.getCode()) + ": " + status.getMessage() + ")");
            } catch (InvalidProtocolBufferException ignored) {
                // Fall through to generic body decoding.
            }
        }
        String responseBody = new String(body, java.nio.charset.StandardCharsets.UTF_8);
        if (responseBody.length() > 512) {
            responseBody = responseBody.substring(0, 512) + "...";
        }
        Status mappedStatus = ProtobufHttpStatusMapper.fromThrowable(
            new IllegalStateException(responseBody), null, targetUrl);
        return new IllegalStateException(
            "Remote function invocation failed with HTTP " + response.statusCode() + " at " + targetUrl
                + " (" + Code.forNumber(mappedStatus.getCode()) + ": " + mappedStatus.getMessage() + ")");
    }

    /**
     * Determines whether the transport context requests the Protobuf-over-HTTP v1 protocol.
     *
     * @param context the transport context to inspect for a protocol preference
     * @return `true` if the context's `transportProtocol` equals `PROTOBUF_HTTP_V1` (case-insensitive), `false` otherwise
     */
    private boolean useProtobufHttpV1(FunctionTransportContext context) {
        return context.transportProtocol()
            .map(protocol -> PROTOCOL_PROTOBUF_HTTP_V1.equalsIgnoreCase(protocol))
            .orElse(false);
    }

    /**
     * Resolve the HTTP target URL for the remote function invocation.
     *
     * Attempts to use the explicit target URL from the transport context; if absent, attempts to
     * resolve a URL from routing metadata. If neither source yields a URL an exception is thrown.
     *
     * @param context the transport context containing optional target URL, target handler, and target module
     * @return the resolved target URL
     * @throws IllegalStateException if no target URL can be resolved from the context or routing metadata
     */
    private String resolveTargetUrl(FunctionTransportContext context) {
        return context.targetUrl()
            .or(() -> resolveFromRoutingMetadata(context))
            .orElseThrow(() -> new IllegalStateException(
                "Function invocation mode is REMOTE but no target URL is configured "
                    + "(expected context attribute '" + FunctionTransportContext.ATTR_TARGET_URL + "' "
                    + "or resolvable target metadata from '"
                    + FunctionTransportContext.ATTR_TARGET_HANDLER + "'/'"
                    + FunctionTransportContext.ATTR_TARGET_MODULE + "')."));
    }

    private java.util.Optional<String> resolveFromRoutingMetadata(FunctionTransportContext context) {
        Config config = ConfigProvider.getConfig();
        java.util.Optional<String> byHandler = context.targetHandler()
            .flatMap(handler -> resolveByHandler(config, handler));
        if (byHandler.isPresent()) {
            return byHandler;
        }
        return context.targetModule()
            .flatMap(module -> resolveByModule(config, module));
    }

    private java.util.Optional<String> resolveByHandler(Config config, String targetHandler) {
        String clientName = toClientName(targetHandler);
        if (clientName.isBlank()) {
            return java.util.Optional.empty();
        }
        String directKey = "quarkus.rest-client." + clientName + ".url";
        java.util.Optional<String> direct = config.getOptionalValue(directKey, String.class)
            .map(String::strip)
            .filter(value -> !value.isBlank());
        if (direct.isPresent()) {
            return direct;
        }
        String quotedKey = "quarkus.rest-client.\"" + clientName + "\".url";
        return config.getOptionalValue(quotedKey, String.class)
            .map(String::strip)
            .filter(value -> !value.isBlank());
    }

    private java.util.Optional<String> resolveByModule(Config config, String module) {
        String normalizedModule = module.strip();
        if (normalizedModule.isBlank()) {
            return java.util.Optional.empty();
        }
        String moduleClientKey = "quarkus.rest-client." + normalizedModule + ".url";
        java.util.Optional<String> moduleUrl = config.getOptionalValue(moduleClientKey, String.class)
            .map(String::strip)
            .filter(value -> !value.isBlank());
        if (moduleUrl.isPresent()) {
            return moduleUrl;
        }
        String hostKey = "pipeline.module." + normalizedModule + ".host";
        String portKey = "pipeline.module." + normalizedModule + ".port";
        java.util.Optional<String> host = config.getOptionalValue(hostKey, String.class)
            .map(String::strip)
            .filter(value -> !value.isBlank());
        java.util.Optional<Integer> port = config.getOptionalValue(portKey, Integer.class);
        if (host.isPresent() && port.isPresent()) {
            return java.util.Optional.of("https://" + host.get() + ":" + port.get());
        }
        return java.util.Optional.empty();
    }

    private String toClientName(String targetHandler) {
        String trimmed = targetHandler == null ? "" : targetHandler.strip();
        if (trimmed.isBlank()) {
            return "";
        }
        String base = trimmed.endsWith("FunctionHandler")
            ? trimmed.substring(0, trimmed.length() - "FunctionHandler".length())
            : trimmed;
        return toKebabCase(base);
    }

    private String toKebabCase(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        String withBoundaries = value.replaceAll("([a-z0-9])([A-Z])", "$1-$2");
        return withBoundaries.toLowerCase(java.util.Locale.ROOT);
    }

    /**
     * Casts a TraceEnvelope with an unknown payload type to a TraceEnvelope parameterized with O.
     *
     * @param envelope the envelope to cast
     * @return the same envelope cast to TraceEnvelope\<O>
     */
    @SuppressWarnings("unchecked")
    private TraceEnvelope<O> castEnvelope(TraceEnvelope<?> envelope) {
        return (TraceEnvelope<O>) envelope;
    }

    private record WirePayload(byte[] body, String contentType, String accept) {
    }
}
