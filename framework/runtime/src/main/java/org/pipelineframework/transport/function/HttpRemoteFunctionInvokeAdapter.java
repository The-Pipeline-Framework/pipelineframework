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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;

/**
 * Remote invoke adapter that dispatches envelopes to an HTTP endpoint.
 *
 * <p>Target endpoint is resolved from {@link FunctionTransportContext#ATTR_TARGET_URL}.</p>
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
            .onItem().transformToUni(items -> postForSingle(items, context));
    }

    @Override
    public Multi<TraceEnvelope<O>> invokeManyToMany(Multi<TraceEnvelope<I>> input, FunctionTransportContext context) {
        Objects.requireNonNull(input, "input stream must not be null");
        Objects.requireNonNull(context, "context must not be null");
        return input.collect().asList()
            .onItem().transformToUni(items -> postForMany(items, context))
            .onItem().transformToMulti(Multi.createFrom()::iterable);
    }

    private Uni<TraceEnvelope<O>> postForSingle(Object payload, FunctionTransportContext context) {
        return Uni.createFrom().item(() -> {
            try {
                String responseBody = send(payload, context);
                JsonNode node = objectMapper.readTree(responseBody);
                if (node == null || node.isNull()) {
                    throw new IllegalStateException("Remote function response body was empty");
                }
                return castEnvelope(objectMapper.convertValue(node, TraceEnvelope.class));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Remote function invocation was interrupted", e);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to parse remote function response", e);
            }
        }).runSubscriptionOn(Infrastructure.getDefaultExecutor());
    }

    private Uni<List<TraceEnvelope<O>>> postForMany(Object payload, FunctionTransportContext context) {
        return Uni.createFrom().item(() -> {
            try {
                String responseBody = send(payload, context);
                JsonNode node = objectMapper.readTree(responseBody);
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
            } catch (IOException e) {
                throw new IllegalStateException("Failed to parse remote function response", e);
            }
        }).runSubscriptionOn(Infrastructure.getDefaultExecutor());
    }

    private String send(Object payload, FunctionTransportContext context) throws IOException, InterruptedException {
        String targetUrl = context.targetUrl()
            .orElseThrow(() -> new IllegalStateException(
                "Function invocation mode is REMOTE but no target URL is configured "
                    + "(expected context attribute '" + FunctionTransportContext.ATTR_TARGET_URL + "')."));
        String requestBody = objectMapper.writeValueAsString(payload);
        HttpRequest request = HttpRequest.newBuilder(URI.create(targetUrl))
            .timeout(DEFAULT_TIMEOUT)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException(
                "Remote function invocation failed with HTTP " + response.statusCode() + " at " + targetUrl);
        }
        return response.body();
    }

    @SuppressWarnings("unchecked")
    private TraceEnvelope<O> castEnvelope(TraceEnvelope<?> envelope) {
        return (TraceEnvelope<O>) envelope;
    }
}
